// Package translate converts between OpenAI and the internal model.
//
// Inbound: validate/normalize a Chat Completions request, resolve the model id, and
// flatten the conversation into one Kagi prompt. Outbound: assemble internal turn
// events (never Kagi JSON) into answer text and format OpenAI chunks / responses.
package translate

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"kagiopenaiproxy/internal/citations"
	"kagiopenaiproxy/internal/events"
	"kagiopenaiproxy/internal/logx"
	"kagiopenaiproxy/internal/redact"
	"kagiopenaiproxy/internal/textutil"
)

// ------------------------------------------------------------------------- errors

// TurnError is an OpenAI-style error. Its fields are unexported so NewTurnError, the
// single redaction choke point for every error body and error chunk, cannot be bypassed:
// code and message can carry upstream data or echoed client input.
type TurnError struct {
	status  int
	typ     string
	code    string
	message string
}

// NewTurnError builds a redacted error.
func NewTurnError(status int, typ, code, message string) *TurnError {
	return &TurnError{status: status, typ: typ, code: redact.String(code), message: redact.String(message)}
}

func (e *TurnError) Status() int     { return e.status }
func (e *TurnError) Type() string    { return e.typ }
func (e *TurnError) Code() string    { return e.code }
func (e *TurnError) Message() string { return e.message }
func (e *TurnError) Error() string   { return e.code + ": " + e.message }

type errorBody struct {
	Error errorDetail `json:"error"`
}

type errorDetail struct {
	Message string  `json:"message"`
	Type    string  `json:"type"`
	Param   *string `json:"param"`
	Code    string  `json:"code"`
}

// Body is the OpenAI error response body.
func (e *TurnError) Body() any {
	return errorBody{errorDetail{Message: e.message, Type: e.typ, Code: e.code}}
}

// InvalidRequest is a 400 invalid_request_error.
func InvalidRequest(message string) *TurnError {
	return NewTurnError(400, "invalid_request_error", "invalid_request", message)
}

// ------------------------------------------------------------------ request parsing

var handledParams = map[string]bool{"model": true, "messages": true, "stream": true, "stream_options": true, "temperature": true}

var (
	loggedIgnoredMu sync.Mutex
	loggedIgnored   = map[string]bool{}
)

// Role is a normalized message role.
type Role string

const (
	RoleSystem    Role = "system"
	RoleUser      Role = "user"
	RoleAssistant Role = "assistant"
	RoleTool      Role = "tool"
)

var roleMap = map[string]Role{
	"system":    RoleSystem,
	"developer": RoleSystem,
	"user":      RoleUser,
	"assistant": RoleAssistant,
	"tool":      RoleTool,
	"function":  RoleTool, // legacy OpenAI name for tool results
}

// ImagePlaceholder replaces image parts (vision is not supported).
const ImagePlaceholder = "[image omitted]"

// Message is a normalized message.
type Message struct {
	Role    Role
	Content string
}

// ChatRequest is a validated request. Model is "" when omitted.
type ChatRequest struct {
	Model        string
	Messages     []Message
	Stream       bool
	IncludeUsage bool
}

// DecodeBody parses a request body, keeping numbers as json.Number.
func DecodeBody(body []byte) (any, error) {
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, err
	}
	if dec.More() {
		return nil, fmt.Errorf("trailing data after JSON body")
	}
	return v, nil
}

// NormalizeContent turns a string, an array of parts, or null into plain text.
func NormalizeContent(content any) (string, *TurnError) {
	switch c := content.(type) {
	case nil:
		return "", nil
	case string:
		return c, nil
	case []any:
		var pieces []string
		for _, raw := range c {
			part, ok := raw.(map[string]any)
			if !ok {
				continue
			}
			switch part["type"] {
			case "text":
				if text, ok := part["text"].(string); ok {
					pieces = append(pieces, text)
				}
			case "image_url":
				pieces = append(pieces, ImagePlaceholder)
			}
			// other part types (audio, files, ...) are skipped
		}
		return strings.Join(pieces, "\n"), nil
	}
	return "", InvalidRequest("message content must be a string, an array of content parts, or null")
}

// ParseChatRequest validates a decoded Chat Completions body.
func ParseChatRequest(body any) (ChatRequest, *TurnError) {
	obj, ok := body.(map[string]any)
	if !ok {
		return ChatRequest{}, InvalidRequest("request body must be a JSON object")
	}
	for key := range obj {
		if handledParams[key] {
			continue
		}
		loggedIgnoredMu.Lock()
		first := !loggedIgnored[key]
		loggedIgnored[key] = true
		loggedIgnoredMu.Unlock()
		if first {
			logx.Info("ignoring unsupported request parameter %s (logged once)", textutil.Repr(key))
		}
	}

	var req ChatRequest
	if raw, present := obj["model"]; present && raw != nil {
		model, ok := raw.(string)
		if !ok || textutil.Strip(model) == "" {
			return ChatRequest{}, InvalidRequest("'model' must be a non-empty string")
		}
		req.Model = textutil.Strip(model)
	}
	if raw := obj["temperature"]; raw != nil {
		if _, ok := raw.(json.Number); !ok {
			return ChatRequest{}, InvalidRequest("'temperature' must be a number")
		}
	}
	if raw := obj["stream"]; raw != nil {
		stream, ok := raw.(bool)
		if !ok {
			return ChatRequest{}, InvalidRequest("'stream' must be a boolean")
		}
		req.Stream = stream
	}
	var streamOptions map[string]any
	if raw := obj["stream_options"]; raw != nil {
		opts, ok := raw.(map[string]any)
		if !ok {
			return ChatRequest{}, InvalidRequest("'stream_options' must be an object")
		}
		streamOptions = opts
	}
	includeUsage, _ := streamOptions["include_usage"].(bool)
	req.IncludeUsage = req.Stream && includeUsage

	rawMessages, ok := obj["messages"].([]any)
	if !ok || len(rawMessages) == 0 {
		return ChatRequest{}, InvalidRequest("'messages' must be a non-empty array")
	}
	allSystem := true
	for i, raw := range rawMessages {
		msg, ok := raw.(map[string]any)
		if !ok {
			return ChatRequest{}, InvalidRequest(fmt.Sprintf("messages[%d] must be an object", i))
		}
		name, _ := msg["role"].(string)
		role, known := roleMap[name]
		if _, isString := msg["role"].(string); !isString || !known {
			return ChatRequest{}, InvalidRequest(fmt.Sprintf("messages[%d].role %s is not supported", i, textutil.Repr(msg["role"])))
		}
		content, terr := NormalizeContent(msg["content"])
		if terr != nil {
			return ChatRequest{}, terr
		}
		req.Messages = append(req.Messages, Message{Role: role, Content: content})
		allSystem = allSystem && role == RoleSystem
	}
	if allSystem {
		return ChatRequest{}, InvalidRequest("'messages' must contain at least one non-system message")
	}
	return req, nil
}

// ResolveModel strips a ":online" / ":offline" suffix: "gpt-x:offline" -> ("gpt-x", false).
// Without a suffix the configured default applies.
func ResolveModel(requested string, defaultOnline bool) (string, bool) {
	for _, s := range []struct {
		suffix string
		online bool
	}{{":online", true}, {":offline", false}} {
		if strings.HasSuffix(requested, s.suffix) && len(requested) > len(s.suffix) {
			return strings.TrimSuffix(requested, s.suffix), s.online
		}
	}
	return requested, defaultOnline
}

// ---------------------------------------------------------------------- flattening

const (
	DefaultContextWindow  = 128_000
	ContextBudgetFraction = 0.70
	// CharsPerToken is a rough estimate to compare prompt length with the context window.
	CharsPerToken  = 4
	OmissionMarker = "[...earlier messages omitted...]"
)

var labels = map[Role]string{RoleUser: "User", RoleAssistant: "Assistant", RoleTool: "Tool result"}

func render(system string, lines []string, omitted bool) string {
	if system == "" {
		system = "None"
	}
	convo := lines
	if omitted {
		convo = append([]string{OmissionMarker}, lines...)
	}
	return "[System instructions]\n" + system + "\n\n[Conversation so far]\n" + strings.Join(convo, "\n") +
		"\n\n[Respond as the Assistant to the last message, following the system instructions above.]"
}

// Flatten builds the single Kagi prompt. contextWindow <= 0 means unknown.
func Flatten(messages []Message, contextWindow int) string {
	var systemParts, lines []string
	for _, m := range messages {
		if m.Role == RoleSystem {
			if textutil.Strip(m.Content) != "" {
				systemParts = append(systemParts, m.Content)
			}
			continue
		}
		lines = append(lines, labels[m.Role]+": "+m.Content)
	}
	system := strings.Join(systemParts, "\n\n")
	if contextWindow <= 0 {
		contextWindow = DefaultContextWindow
	}
	budget := int(float64(contextWindow) * ContextBudgetFraction * CharsPerToken)
	text := render(system, lines, false)
	dropped := 0
	// Drop the oldest turns; never the system block, never the last message.
	for textutil.Len(text) > budget && len(lines)-dropped > 1 {
		dropped++
		text = render(system, lines[dropped:], true)
	}
	if dropped > 0 {
		logx.Info("prompt over budget (%d chars): omitted %d oldest message(s)", budget, dropped)
	}
	if n := textutil.Len(text); n > budget {
		logx.Warn("prompt still exceeds budget after truncation (%d > %d chars)", n, budget)
	}
	return text
}

// --------------------------------------------------------------- answer assembly

// Assembler consumes internal turn events and produces answer text and the outcome.
type Assembler struct {
	nodeKinds         map[string]string
	citations         *citations.Rewriter
	firstTextSeen     bool
	warnedUnknownNode bool
	seenEvents        []string
	seenKinds         []string
	emittedChars      int

	Usage     *events.Usage
	Completed bool
	Final     bool
	Failure   *events.TurnFailed
}

// NewAssembler returns an empty Assembler.
func NewAssembler() *Assembler {
	return &Assembler{nodeKinds: map[string]string{}, citations: citations.New()}
}

// Done reports whether the answer is terminal: turn.completed or a failure was seen.
func (a *Assembler) Done() bool { return a.Completed || a.Failure != nil }

// Feed returns answer text to emit now (possibly empty).
func (a *Assembler) Feed(event events.Event) string {
	a.seenEvents = remember(a.seenEvents, event.Name())
	if a.Done() {
		// The answer is over (failed, or turn.completed): nothing after it is part of it.
		return ""
	}
	switch e := event.(type) {
	case events.NodeCreated:
		a.nodeKinds[e.NodeID] = e.Kind
		a.seenKinds = remember(a.seenKinds, e.Kind)
	case events.ReferenceAdded:
		a.citations.AddReference(e.RefID, e.Payload)
	case events.TextDelta:
		kind, known := a.nodeKinds[e.NodeID]
		if !known {
			if !a.warnedUnknownNode {
				a.warnedUnknownNode = true
				logx.Warn("delta for unknown node %s; treating it as answer text", textutil.Repr(e.NodeID))
			}
			kind = "text"
		}
		if kind == "text" {
			return a.emit(e.Text)
		}
	case events.TurnCompleted:
		a.Completed = true
		a.Usage = e.Usage
	case events.TurnFailed:
		failure := e
		a.Failure = &failure
	case events.Final:
		a.Final = true
	}
	return ""
}

func (a *Assembler) emit(text string) string {
	if !a.firstTextSeen {
		// Some models lead with whitespace: strip it from the very first text delta only.
		a.firstTextSeen = true
		text = textutil.LStrip(text)
	}
	out := a.citations.Feed(text)
	a.emittedChars += textutil.Len(out)
	return out
}

// Finish ends the stream: it returns the held text to emit and the error, if any.
func (a *Assembler) Finish(interrupted *TurnError) (string, *TurnError) {
	tail := a.citations.Flush()
	a.emittedChars += textutil.Len(tail)
	if f := a.Failure; f != nil {
		if f.RateLimited {
			return tail, NewTurnError(429, "rate_limit_error", f.Code, f.Message)
		}
		return tail, NewTurnError(502, "api_error", f.Code, f.Message)
	}
	if !a.Completed && !a.Final {
		if interrupted != nil {
			return tail, interrupted
		}
		return tail, NewTurnError(502, "api_error", "kagi_stream_truncated", "Kagi stream ended before the turn completed (no turn.completed or is_final)")
	}
	if a.emittedChars == 0 {
		return tail, NewTurnError(502, "api_error", "kagi_empty_answer", fmt.Sprintf(
			"Kagi turn finished with no answer text; events seen: %s; node kinds seen: %s",
			orNone(a.seenEvents), orNone(a.seenKinds)))
	}
	return tail, nil
}

func remember(seen []string, item string) []string {
	for _, s := range seen {
		if s == item {
			return seen
		}
	}
	return append(seen, item)
}

func orNone(items []string) string {
	if len(items) == 0 {
		return "none"
	}
	return strings.Join(items, ", ")
}

// -------------------------------------------------------------------- formatting

// UsageJSON is the OpenAI usage object.
type UsageJSON struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
	TotalTokens      int `json:"total_tokens"`
}

// UsageDict converts internal usage; nil stays nil.
func UsageDict(u *events.Usage) *UsageJSON {
	if u == nil {
		return nil
	}
	return &UsageJSON{u.InputTokens, u.OutputTokens, u.InputTokens + u.OutputTokens}
}

// Marshal encodes compact JSON without HTML escaping (like json.dumps(ensure_ascii=False)).
func Marshal(v any) []byte {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		panic(fmt.Sprintf("translate: cannot encode %T: %v", v, err)) // only fixed shapes are encoded
	}
	return bytes.TrimSuffix(buf.Bytes(), []byte("\n"))
}

func frame(v any) string { return "data: " + string(Marshal(v)) + "\n\n" }

// DoneFrame ends a successful stream.
const DoneFrame = "data: [DONE]\n\n"

// ChunkFormatter builds SSE frames for one completion; all chunks share ID and Created.
type ChunkFormatter struct {
	ID      string
	Created int64
	Model   string
}

type delta struct {
	Role    string  `json:"role,omitempty"`
	Content *string `json:"content,omitempty"`
}

type chunkChoice struct {
	Index        int     `json:"index"`
	Delta        delta   `json:"delta"`
	FinishReason *string `json:"finish_reason"`
}

type chunk struct {
	ID      string        `json:"id"`
	Object  string        `json:"object"`
	Created int64         `json:"created"`
	Model   string        `json:"model"`
	Choices []chunkChoice `json:"choices"`
}

type usageChunk struct {
	ID      string        `json:"id"`
	Object  string        `json:"object"`
	Created int64         `json:"created"`
	Model   string        `json:"model"`
	Choices []chunkChoice `json:"choices"`
	Usage   *UsageJSON    `json:"usage"`
}

func (f ChunkFormatter) chunk(d delta, finishReason *string) string {
	return frame(chunk{f.ID, "chat.completion.chunk", f.Created, f.Model, []chunkChoice{{0, d, finishReason}}})
}

func ptr(s string) *string { return &s }

// Role is the first chunk: {"role":"assistant","content":""}.
func (f ChunkFormatter) Role() string {
	return f.chunk(delta{Role: "assistant", Content: ptr("")}, nil)
}

// Content carries answer text.
func (f ChunkFormatter) Content(text string) string { return f.chunk(delta{Content: ptr(text)}, nil) }

// Keepalive is an empty-content chunk (not an SSE comment).
func (f ChunkFormatter) Keepalive() string { return f.chunk(delta{Content: ptr("")}, nil) }

// Finish is the final chunk: empty delta, finish_reason "stop".
func (f ChunkFormatter) Finish() string { return f.chunk(delta{}, ptr("stop")) }

// Usage is the include_usage chunk with empty choices.
func (f ChunkFormatter) Usage(u *events.Usage) string {
	return frame(usageChunk{f.ID, "chat.completion.chunk", f.Created, f.Model, []chunkChoice{}, UsageDict(u)})
}

type errorChunk struct {
	Error struct {
		Message string `json:"message"`
		Type    string `json:"type"`
		Code    string `json:"code"`
	} `json:"error"`
}

// Error is a mid-stream error frame.
func (f ChunkFormatter) Error(e *TurnError) string {
	var c errorChunk
	c.Error.Message, c.Error.Type, c.Error.Code = e.message, e.typ, e.code
	return frame(c)
}

type completionMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type completionChoice struct {
	Index        int               `json:"index"`
	Message      completionMessage `json:"message"`
	FinishReason string            `json:"finish_reason"`
}

type completion struct {
	ID      string             `json:"id"`
	Object  string             `json:"object"`
	Created int64              `json:"created"`
	Model   string             `json:"model"`
	Choices []completionChoice `json:"choices"`
	Usage   *UsageJSON         `json:"usage,omitempty"`
}

// CompletionBody is the non-streaming response.
func CompletionBody(id string, created int64, model, content string, u *events.Usage) any {
	return completion{id, "chat.completion", created, model,
		[]completionChoice{{0, completionMessage{"assistant", content}, "stop"}}, UsageDict(u)}
}
