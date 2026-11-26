// tools.go adds prompt-mediated tool calling to the OpenAI surface.
//
// Kagi's Assistant protocol has no native tool calling, so the proxy synthesizes
// it: the request's tools are rendered into a system block with a strict reply
// grammar, and the model's answer is scanned for TOOL_CALL / TOOL_CALLS markers
// that are reshaped into OpenAI tool_calls. The client's tool loop then works
// unchanged, because every request carries the whole conversation.

package translate

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"kagiopenaiproxy/internal/events"
)

// tools and tool_choice are handled here, so they must not be logged as ignored.
func init() {
	handledParams["tools"] = true
	handledParams["tool_choice"] = true
}

// ToolSpec is one entry of the request's tools array.
type ToolSpec struct {
	Name        string
	Description string
	Parameters  any // JSON schema, passed through to the prompt
}

// ParseTools validates the request's tools array. It returns nil when the request
// has no tools or tool_choice is "none".
func ParseTools(body any) ([]ToolSpec, *TurnError) {
	obj, _ := body.(map[string]any)
	raw := obj["tools"]
	if raw == nil {
		return nil, nil
	}
	if choice, _ := obj["tool_choice"].(string); choice == "none" {
		return nil, nil
	}
	arr, ok := raw.([]any)
	if !ok {
		return nil, InvalidRequest("'tools' must be an array")
	}
	var out []ToolSpec
	for i, item := range arr {
		entry, ok := item.(map[string]any)
		if !ok {
			return nil, InvalidRequest(fmt.Sprintf("tools[%d] must be an object", i))
		}
		fn, ok := entry["function"].(map[string]any)
		if !ok {
			return nil, InvalidRequest(fmt.Sprintf("tools[%d].function must be an object", i))
		}
		name, _ := fn["name"].(string)
		if strings.TrimSpace(name) == "" {
			return nil, InvalidRequest(fmt.Sprintf("tools[%d].function.name must be a non-empty string", i))
		}
		desc, _ := fn["description"].(string)
		out = append(out, ToolSpec{Name: name, Description: desc, Parameters: fn["parameters"]})
	}
	return out, nil
}

// ToolPrompt renders the tools and the reply grammar as a system block.
func ToolPrompt(tools []ToolSpec) string {
	var b strings.Builder
	b.WriteString("You can call tools. To call a tool, reply with a line of exactly this form and nothing else:\n")
	b.WriteString("TOOL_CALL:{\"name\":\"<tool name>\",\"arguments\":{...}}\n")
	b.WriteString("To call several tools at once, reply with one line:\n")
	b.WriteString("TOOL_CALLS:[{\"name\":\"<tool name>\",\"arguments\":{...}},...]\n")
	b.WriteString("Rules: \"arguments\" must be a JSON object that follows the tool's parameter schema. ")
	b.WriteString("When calling tools, output no prose and no markdown code fences. ")
	b.WriteString("Tool results arrive later as \"Tool result:\" messages. ")
	b.WriteString("When you need no tool, answer normally and never write TOOL_CALL.\n\n")
	b.WriteString("Available tools:\n")
	for _, t := range tools {
		b.WriteString("- " + t.Name)
		if t.Description != "" {
			b.WriteString(": " + t.Description)
		}
		b.WriteString("\n")
		if t.Parameters != nil {
			b.WriteString("  parameters: " + string(Marshal(t.Parameters)) + "\n")
		}
	}
	return b.String()
}

// WithToolPrompt returns messages plus a trailing system message with the tool block.
// The input slice is not modified. System messages are never dropped by Flatten.
func WithToolPrompt(messages []Message, tools []ToolSpec) []Message {
	if len(tools) == 0 {
		return messages
	}
	out := make([]Message, 0, len(messages)+1)
	out = append(out, messages...)
	return append(out, Message{Role: RoleSystem, Content: ToolPrompt(tools)})
}

// appendToolCalls folds an assistant message's tool_calls into its text, so the
// flattened transcript shows what was called. Other roles pass through unchanged.
func appendToolCalls(content string, role Role, raw any, i int) (string, *TurnError) {
	if role != RoleAssistant || raw == nil {
		return content, nil
	}
	arr, ok := raw.([]any)
	if !ok {
		return "", InvalidRequest(fmt.Sprintf("messages[%d].tool_calls must be an array", i))
	}
	var lines []string
	if strings.TrimSpace(content) != "" {
		lines = append(lines, content)
	}
	for j, item := range arr {
		call, ok := item.(map[string]any)
		if !ok {
			return "", InvalidRequest(fmt.Sprintf("messages[%d].tool_calls[%d] must be an object", i, j))
		}
		fn, _ := call["function"].(map[string]any)
		name, _ := fn["name"].(string)
		if name == "" {
			continue
		}
		args, _ := fn["arguments"].(string)
		if strings.TrimSpace(args) == "" {
			args = "{}"
		}
		lines = append(lines, "[called tool "+strconv.Quote(name)+" with arguments "+args+"]")
	}
	return strings.Join(lines, "\n"), nil
}

// ToolCall is one tool call in OpenAI wire shape.
type ToolCall struct {
	ID       string       `json:"id"`
	Type     string       `json:"type"`
	Function functionJSON `json:"function"`
}

type functionJSON struct {
	Name      string `json:"name"`
	Arguments string `json:"arguments"` // compact JSON
}

const (
	toolCallMarker  = "TOOL_CALL:"
	toolCallsMarker = "TOOL_CALLS:["
)

// ToolCallResult is the outcome of scanning a final answer for tool calls.
type ToolCallResult struct {
	Calls    []ToolCall
	Preamble string // answer text before the first marker, trimmed
	Found    bool   // a marker was present
	Err      error  // set when Found and the payload is unusable
}

// findMarker returns the index of the first marker and whether it is TOOL_CALLS.
func findMarker(text string) (int, bool) {
	idx := strings.Index(text, toolCallMarker)
	if j := strings.Index(text, toolCallsMarker); j >= 0 && (idx < 0 || j < idx) {
		return j, true
	}
	return idx, false
}

// ParseToolCalls scans the model's answer for TOOL_CALL / TOOL_CALLS markers. JSON is
// decoded with a json.Decoder, so trailing prose or a closing code fence is ignored.
// Unknown tool names or non-object arguments are errors; callers fall back to plain content.
func ParseToolCalls(text string, tools []ToolSpec) ToolCallResult {
	res := ToolCallResult{}
	idx, _ := findMarker(text)
	if idx < 0 {
		return res
	}
	res.Found = true
	res.Preamble = strings.TrimSpace(text[:idx])
	res.Preamble = strings.TrimSpace(strings.TrimSuffix(res.Preamble, "\x60\x60\x60"))
	known := make(map[string]bool, len(tools))
	for _, t := range tools {
		known[t.Name] = true
	}
	rest := text[idx:]
	for {
		i, multi := findMarker(rest)
		if i < 0 {
			break
		}
		start := i + len(toolCallMarker)
		if multi {
			start = i + len(toolCallsMarker) - 1 // point at the "["
		}
		dec := json.NewDecoder(strings.NewReader(rest[start:]))
		dec.UseNumber()
		var payload any
		if err := dec.Decode(&payload); err != nil {
			res.Calls, res.Err = nil, fmt.Errorf("tool-call payload is not valid JSON: %w", err)
			return res
		}
		calls, err := toCalls(payload, known)
		if err != nil {
			res.Calls, res.Err = nil, err
			return res
		}
		res.Calls = append(res.Calls, calls...)
		rest = rest[start+int(dec.InputOffset()):]
	}
	return res
}

// toCalls validates one decoded payload (an object or an array of objects).
func toCalls(payload any, known map[string]bool) ([]ToolCall, error) {
	var objs []map[string]any
	switch v := payload.(type) {
	case map[string]any:
		objs = append(objs, v)
	case []any:
		for i, item := range v {
			obj, ok := item.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("tool call %d must be an object", i)
			}
			objs = append(objs, obj)
		}
	default:
		return nil, errors.New("tool-call payload must be an object or an array of objects")
	}
	var out []ToolCall
	for i, obj := range objs {
		name, _ := obj["name"].(string)
		if !known[name] {
			return nil, fmt.Errorf("tool call %d names unknown tool %q", i, name)
		}
		args := obj["arguments"]
		if args == nil {
			args = map[string]any{}
		}
		if _, ok := args.(map[string]any); !ok {
			return nil, fmt.Errorf("tool call %d: arguments must be a JSON object", i)
		}
		compact, err := json.Marshal(args)
		if err != nil {
			return nil, err
		}
		out = append(out, ToolCall{newToolCallID(), "function", functionJSON{name, string(compact)}})
	}
	return out, nil
}

func newToolCallID() string {
	var b [12]byte
	_, _ = rand.Read(b[:])
	return "call_" + hex.EncodeToString(b[:])
}

// streamToolCall is one tool call inside a chunk's delta (indexed, per OpenAI).
type streamToolCall struct {
	Index    int          `json:"index"`
	ID       string       `json:"id"`
	Type     string       `json:"type"`
	Function functionJSON `json:"function"`
}

type toolDelta struct {
	ToolCalls []streamToolCall `json:"tool_calls"`
}

type toolChunkChoice struct {
	Index        int       `json:"index"`
	Delta        toolDelta `json:"delta"`
	FinishReason *string   `json:"finish_reason"`
}

type toolChunk struct {
	ID      string            `json:"id"`
	Object  string            `json:"object"`
	Created int64             `json:"created"`
	Model   string            `json:"model"`
	Choices []toolChunkChoice `json:"choices"`
}

// ToolCallChunk carries one complete tool call as a streaming delta.
func (f ChunkFormatter) ToolCallChunk(index int, c ToolCall) string {
	return frame(toolChunk{f.ID, "chat.completion.chunk", f.Created, f.Model,
		[]toolChunkChoice{{0, toolDelta{[]streamToolCall{{index, c.ID, c.Type, c.Function}}}, nil}}})
}

// FinishToolCalls is the final chunk of a tool-call stream.
func (f ChunkFormatter) FinishToolCalls() string { return f.chunk(delta{}, ptr("tool_calls")) }

type toolCallMessage struct {
	Role      string     `json:"role"`
	Content   *string    `json:"content"`
	ToolCalls []ToolCall `json:"tool_calls"`
}

type toolCallChoice struct {
	Index        int             `json:"index"`
	Message      toolCallMessage `json:"message"`
	FinishReason string          `json:"finish_reason"`
}

type toolCompletion struct {
	ID      string           `json:"id"`
	Object  string           `json:"object"`
	Created int64            `json:"created"`
	Model   string           `json:"model"`
	Choices []toolCallChoice `json:"choices"`
	Usage   *UsageJSON       `json:"usage,omitempty"`
}

// ToolCallsBody is the non-streaming response for a tool-call turn.
func ToolCallsBody(id string, created int64, model, content string, calls []ToolCall, u *events.Usage) any {
	var c *string
	if content != "" {
		c = &content
	}
	return toolCompletion{id, "chat.completion", created, model,
		[]toolCallChoice{{0, toolCallMessage{"assistant", c, calls}, "tool_calls"}}, UsageDict(u)}
}
