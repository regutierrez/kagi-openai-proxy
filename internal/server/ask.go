package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"kagiopenaiproxy/internal/config"
	"kagiopenaiproxy/internal/events"
	"kagiopenaiproxy/internal/kagi"
	"kagiopenaiproxy/internal/translate"
)

// ThreadURLPrefix + conversation uuid opens a thread in Kagi Assistant's web app.
const ThreadURLPrefix = "https://assistant.kagi.com/chat/"

// askRequest is the POST /v1/ask body.
type askRequest struct {
	Query string `json:"query"`
	// Model is a Kagi model id, optionally with :online/:offline/effort suffixes;
	// "" = the catalog default.
	Model string `json:"model"`
	// Search overrides web search (and any :online/:offline suffix); nil = default.
	Search          *bool  `json:"search"`
	ReasoningEffort string `json:"reasoning_effort"`
	// Branch continues a thread: the branch id from an earlier thread event.
	Branch string `json:"branch"`
}

// Event payloads of the /v1/ask stream. Each SSE frame is "event: <name>" plus
// "data: <json>".
type (
	askThread struct {
		Conversation string `json:"conversation"`
		Branch       string `json:"branch"`
		Turn         string `json:"turn"`
		URL          string `json:"url"`
	}
	askTitle struct {
		Title string `json:"title"`
	}
	// askActivity is an upsert keyed by ID: a search (Text = query), a thinking step
	// (Text = summary), or another tool step. Status is running, completed, failed or
	// cancelled.
	askActivity struct {
		ID     string `json:"id"`
		Type   string `json:"type"`
		Text   string `json:"text"`
		Status string `json:"status"`
	}
	askSource struct {
		N           int    `json:"n"`
		URL         string `json:"url"`
		Title       string `json:"title"`
		Snippet     string `json:"snippet"`
		Favicon     string `json:"favicon,omitempty"`
		PublishedAt string `json:"published_at,omitempty"`
	}
	askText struct {
		Delta string `json:"delta"`
	}
	askDone struct {
		Model        string   `json:"model"`
		ModelVersion string   `json:"model_version,omitempty"`
		InputTokens  int      `json:"input_tokens"`
		OutputTokens int      `json:"output_tokens"`
		CostUSD      *float64 `json:"cost_usd,omitempty"`
	}
	askError struct {
		Type    string `json:"type"`
		Code    string `json:"code"`
		Message string `json:"message"`
	}
)

// ask is POST /v1/ask: one question (or a follow-up in an existing thread) answered as
// a stream of typed events: thread, title, activity, source, text, then done or error.
// Unlike /v1/chat/completions, the question is sent as-is (no flattening) and threads
// are never deleted, so they can be continued.
func (p *Proxy) ask(w http.ResponseWriter, r *http.Request) {
	if !p.requireAuth(w, r) {
		return
	}
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		return // client gone while sending the body
	}
	var req askRequest
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		writeError(w, translate.InvalidRequest("invalid request body: "+err.Error()))
		return
	}
	query := strings.TrimSpace(req.Query)
	if query == "" {
		writeError(w, translate.InvalidRequest("'query' must be a non-empty string"))
		return
	}
	if req.ReasoningEffort != "" && !kagi.IsReasoningEffort(req.ReasoningEffort) {
		writeError(w, translate.NewTurnError(400, "invalid_request_error", "invalid_reasoning_effort",
			"'reasoning_effort' must be one of "+strings.Join(kagi.ReasoningEfforts, ", ")))
		return
	}

	settings := p.settings
	catalog, staleReason := p.catalog.get(context.WithoutCancel(r.Context()))
	spec := translate.ModelSpec{EnableSearch: settings.SearchDefaultOnline}
	if req.Model == "" {
		if catalog == nil || catalog.DefaultID == "" {
			writeError(w, translate.NewTurnError(503, "api_error", "kagi_catalog_unavailable", "no model given and the Kagi model catalog is unavailable"))
			return
		}
		spec.Model = catalog.DefaultID
	} else {
		var terr *translate.TurnError
		if spec, terr = translate.ParseModel(req.Model, settings.SearchDefaultOnline); terr != nil {
			writeError(w, terr)
			return
		}
	}
	if req.Search != nil {
		spec.EnableSearch = *req.Search
	}
	var trusted *kagi.Catalog
	if staleReason == "" {
		trusted = catalog
	}
	effort, terr := p.resolveEffort(trusted, spec.Model, req.ReasoningEffort, spec.ReasoningEffort)
	if terr != nil {
		writeError(w, terr)
		return
	}

	switch p.acquireSlot(r.Context()) {
	case slotClientGone:
		return
	case slotTimeout:
		writeError(w, translate.NewTurnError(429, "rate_limit_error", "proxy_busy", "another Kagi turn is in progress"))
		return
	}
	// Threads stay so they can be continued: under the delete lifecycle they are
	// created as Temporary instead.
	isSaved := settings.ThreadLifecycle.IsSaved()
	if settings.ThreadLifecycle == config.LifecycleDelete {
		temporary := false
		isSaved = &temporary
	}
	opts := kagi.TurnOptions{EnableSearch: spec.EnableSearch, Personalization: settings.Personalization, ReasoningEffort: effort, IsSaved: isSaved}
	runner, terr := p.startTurn(spec.Model, false, func(ctx context.Context) (kagi.CreatedTurn, error) {
		if req.Branch != "" {
			return p.kagi.Respond(ctx, req.Branch, query, spec.Model, opts)
		}
		return p.kagi.CreateTurn(ctx, query, spec.Model, opts)
	})
	if terr != nil {
		writeError(w, terr)
		return
	}
	w.Header().Set("X-Kagi-Thread", runner.conversationUUID)
	p.streamAsk(w, r, runner, spec.Model)
}

// streamAsk writes the /v1/ask event stream until the answer ends.
func (p *Proxy) streamAsk(w http.ResponseWriter, r *http.Request, runner *turnRunner, model string) {
	defer runner.consumerGone()
	flusher, _ := w.(http.Flusher)
	h := w.Header()
	h.Set("Content-Type", "text/event-stream; charset=utf-8")
	h.Set("Cache-Control", "no-cache")
	h.Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	writeRaw := func(frame string) bool {
		if _, err := io.WriteString(w, frame); err != nil {
			return false // client gone
		}
		if flusher != nil {
			flusher.Flush()
		}
		return true
	}
	send := func(event string, data any) bool {
		return writeRaw("event: " + event + "\ndata: " + string(translate.Marshal(data)) + "\n\n")
	}

	if !send("thread", askThread{runner.conversationUUID, runner.branchUUID, runner.turnUUID, ThreadURLPrefix + url.PathEscape(runner.conversationUUID)}) {
		return
	}
	assembler := translate.NewAssembler()
	activities := map[string]*askActivity{}
	title, sentSources := "", 0
	var interrupted *translate.TurnError
	lastEmit := time.Now()
	for {
		timer := time.NewTimer(time.Until(lastEmit.Add(p.settings.Keepalive)))
		it, err := runner.queue.pop(r.Context(), timer.C)
		timer.Stop()
		if errors.Is(err, errQueueTimeout) {
			if !writeRaw(": keepalive\n\n") {
				return
			}
			lastEmit = time.Now()
			continue
		}
		if err != nil {
			return // client disconnected: consumerGone cancels the Kagi turn
		}
		if end, ok := it.(streamEnd); ok {
			interrupted = end.err
			break
		}
		ev := it.(events.Event)
		ok := true
		switch e := ev.(type) {
		case events.Title:
			if e.Text != title {
				title = e.Text
				ok = send("title", askTitle{title})
			}
		case events.NodeCreated:
			if e.Kind == "activity" && e.Type != "config" {
				a := &askActivity{ID: e.NodeID, Type: e.Type, Text: e.Content, Status: "running"}
				if e.Type == "tool_call" && e.ToolName != "" {
					a.Type = e.ToolName
				}
				activities[e.NodeID] = a
				ok = send("activity", a)
			}
		case events.NodeUpdated:
			if a := activities[e.NodeID]; a != nil {
				before := *a
				if e.Status != "" {
					a.Status = e.Status
				}
				if e.Query != "" {
					a.Text = e.Query
				} else if e.Content != "" {
					a.Text = e.Content
				}
				if *a != before {
					ok = send("activity", a)
				}
			}
		}
		text := assembler.Feed(ev)
		for _, s := range assembler.Sources()[sentSources:] {
			ok = ok && send("source", askSource{s.N, s.URL, s.Title, s.Snippet, s.Favicon, s.PublishedAt})
			sentSources++
		}
		if text != "" {
			ok = ok && send("text", askText{text})
			lastEmit = time.Now()
		}
		if !ok {
			return
		}
		if assembler.Done() {
			break
		}
	}
	tail, terr := assembler.Finish(interrupted)
	if tail != "" && !send("text", askText{tail}) {
		return
	}
	if terr != nil {
		send("error", askError{terr.Type(), terr.Code(), terr.Message()})
		return
	}
	done := askDone{Model: model}
	if u := assembler.Usage; u != nil {
		done.InputTokens, done.OutputTokens, done.CostUSD = u.InputTokens, u.OutputTokens, u.CostUSD
		if u.ModelVersion != nil {
			done.ModelVersion = *u.ModelVersion
		}
	}
	send("done", done)
}
