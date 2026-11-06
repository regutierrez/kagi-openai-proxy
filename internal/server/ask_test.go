package server

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"kagiopenaiproxy/internal/config"
	"kagiopenaiproxy/internal/kagitest"
)


// liveShapedTurn mirrors a live Kagi search turn: config and search activity, a title
// frame, sources, a thinking step, then the answer citing two sources.
func liveShapedTurn() []byte {
	ev := kagitest.Ev
	return kagitest.SSE(
		ev("turn.started", nil, obj{"root_node_id": "msg", "branch_uuid": kagitest.Branch}),
		ev("node.created", "msg", obj{"kind": "assistant_message", "status": "running"}),
		ev("node.created", "cfg", obj{"kind": "activity", "type": "config", "status": "running"}),
		ev("node.completed", "cfg", obj{"status": "completed"}),
		ev("node.created", "call", obj{"kind": "activity", "type": "tool_call", "status": "running", "tool_name": "search", "arguments_partial": nil}),
		ev("node.updated", "call", obj{"status": "running", "tool_name": "search", "arguments": obj{"count": 5, "query": "Lyon population"}}),
		obj{"title": "Population of Lyon"},
		ev("reference.added", nil, obj{"ref_id": "fe3f5a", "url": "https://en.wikipedia.org/wiki/Lyon", "title": "Lyon", "snippet": "Third-largest city",
			"favicon": "https://kagi.com/f", "reference_kind": "search", "extra": obj{"published_at": nil}}),
		ev("reference.added", nil, obj{"ref_id": "11460d", "url": "https://www.insee.fr/lyon", "title": "Insee", "snippet": "All the statistics",
			"reference_kind": "search", "extra": obj{"published_at": "Jan 13, 2026"}}),
		ev("node.completed", "call", obj{"status": "completed", "result": obj{"type": "tool_return", "content": "Search results below"}}),
		ev("node.created", "think", obj{"kind": "activity", "type": "thinking", "status": "running", "content": "Compare"}),
		ev("node.delta", "think", obj{"delta": " sources."}),
		ev("node.completed", "think", obj{"status": "completed", "content": "Compare sources."}),
		ev("node.created", "text", obj{"kind": "text", "format": "markdown", "status": "running"}),
		ev("node.delta", "text", obj{"delta": "Lyon has 519,127 people [^fe3f5a][^114"}),
		ev("node.delta", "text", obj{"delta": "60d]."}),
		ev("node.completed", "text", obj{"status": "completed", "content": "Lyon has 519,127 people [^fe3f5a][^11460d]."}),
		ev("node.completed", "msg", obj{"status": "completed"}),
		ev("source.read", nil, obj{"reads": []any{}}),
		ev("turn.completed", nil, obj{"usage": obj{"input_tokens": 7948, "output_tokens": 292, "cost_usd": 0.001182, "model_version": "zai-org/GLM-5.3-Flash"}}),
		obj{"is_final": true, "billing": obj{"limit": "none"}},
	)
}

// askEvents splits a /v1/ask stream into [name, payload] pairs, asserting exact framing.
func askEvents(t *testing.T, text string) [][2]any {
	t.Helper()
	var out [][2]any
	for _, raw := range strings.Split(strings.TrimSuffix(text, "\n\n"), "\n\n") {
		name, data, ok := strings.Cut(raw, "\n")
		if !ok || !strings.HasPrefix(name, "event: ") || !strings.HasPrefix(data, "data: ") || strings.Contains(data, "\n") {
			t.Fatalf("bad frame: %q", raw)
		}
		var payload any
		if err := json.Unmarshal([]byte(strings.TrimPrefix(data, "data: ")), &payload); err != nil {
			t.Fatalf("bad JSON in %q: %v", raw, err)
		}
		out = append(out, [2]any{strings.TrimPrefix(name, "event: "), payload})
	}
	return out
}

func (e *env) ask(body any) result {
	e.t.Helper()
	r, err := e.do(context.Background(), "POST", "/v1/ask", body, nil)
	if err != nil {
		e.t.Fatal(err)
	}
	return r
}

func TestAskStreamsTypedEventsForANewThread(t *testing.T) {
	fake := kagitest.New()
	fake.StreamBody = liveShapedTurn()
	e := newEnv(t, fake) // delete lifecycle: /v1/ask must still keep the thread
	r := e.ask(obj{"query": "  How many people live in Lyon? ", "model": "kagi-default:low"})
	if r.status != 200 || r.header.Get("X-Kagi-Thread") != kagitest.Conv {
		t.Fatalf("%d %s", r.status, r.body)
	}
	mustJSONEqual(t, askEvents(t, r.body), `[
		["thread", {"conversation": "conv-1", "branch": "branch-1", "turn": "turn-1", "url": "https://assistant.kagi.com/chat/conv-1"}],
		["activity", {"id": "call", "type": "search", "text": "", "status": "running"}],
		["activity", {"id": "call", "type": "search", "text": "Lyon population", "status": "running"}],
		["title", {"title": "Population of Lyon"}],
		["source", {"n": 1, "url": "https://en.wikipedia.org/wiki/Lyon", "title": "Lyon", "snippet": "Third-largest city", "favicon": "https://kagi.com/f"}],
		["source", {"n": 2, "url": "https://www.insee.fr/lyon", "title": "Insee", "snippet": "All the statistics", "published_at": "Jan 13, 2026"}],
		["activity", {"id": "call", "type": "search", "text": "Lyon population", "status": "completed"}],
		["activity", {"id": "think", "type": "thinking", "text": "Compare", "status": "running"}],
		["activity", {"id": "think", "type": "thinking", "text": "Compare sources.", "status": "completed"}],
		["text", {"delta": "Lyon has 519,127 people [[1]](https://en.wikipedia.org/wiki/Lyon)"}],
		["text", {"delta": "[[2]](https://www.insee.fr/lyon)."}],
		["done", {"model": "kagi-default", "model_version": "zai-org/GLM-5.3-Flash", "input_tokens": 7948, "output_tokens": 292, "cost_usd": 0.001182}]]`)

	var create obj
	_ = json.Unmarshal(fake.Request(kagitest.Create).Body, &create)
	mustJSONEqual(t, create, `{"content": "How many people live in Lyon?", "model": "kagi-default", "enable_search": true,
		"personalization": false, "reasoning_effort": "low", "is_saved": false}`)
	e.waitIdle()
	if fake.Has(kagitest.Delete) || fake.Has(kagitest.Cancel) || e.proxy.Busy() {
		t.Fatalf("calls %v", fake.Calls())
	}
}

func TestAskWithBranchContinuesTheThread(t *testing.T) {
	fake := kagitest.New()
	fake.StreamBody = kagitest.SimpleStream(true, true, "And Lyon has fewer.")
	e := newEnv(t, fake, func(s *config.Settings) { s.ThreadLifecycle = config.LifecycleTemporary })
	r := e.ask(obj{"query": "And Lyon?", "branch": kagitest.Branch, "search": false})
	evs := askEvents(t, r.body)
	if r.status != 200 || evs[0][0] != "thread" || evs[len(evs)-1][0] != "done" {
		t.Fatalf("%d %v", r.status, evs)
	}
	if fake.Has(kagitest.Create) || !fake.Has(kagitest.Respond) {
		t.Fatalf("calls %v", fake.Calls())
	}
	var respond obj
	_ = json.Unmarshal(fake.Request(kagitest.Respond).Body, &respond)
	// No model given: the catalog default. is_saved is not sent for an existing thread.
	mustJSONEqual(t, respond, `{"content": "And Lyon?", "model": "kagi-default", "enable_search": false, "personalization": false, "operation": "new_turn"}`)
}

func TestAskFailureMidStreamEndsWithErrorEvent(t *testing.T) {
	fake := kagitest.New()
	fake.StreamBody = kagitest.SSE(append(kagitest.Partial(), kagitest.Ev("turn.failed", nil, obj{"error_code": "model_overloaded"}))...)
	e := newEnv(t, fake)
	evs := askEvents(t, e.ask(obj{"query": "hi"}).body)
	mustJSONEqual(t, evs[len(evs)-2:], `[["text", {"delta": "Hi"}],
		["error", {"type": "api_error", "code": "model_overloaded", "message": "Kagi turn.failed (model_overloaded)"}]]`)
}

func TestAskRejectsBadRequestsBeforeCallingKagi(t *testing.T) {
	fake := kagitest.New()
	e := newEnv(t, fake)
	for _, c := range []struct {
		body any
		code string
	}{
		{obj{"query": "  "}, "invalid_request"},
		{obj{"query": "hi", "qurey": "typo"}, "invalid_request"},
		{obj{"query": "hi", "reasoning_effort": "turbo"}, "invalid_reasoning_effort"},
		{obj{"query": "hi", "model": "kagi-default:fast"}, "invalid_model_suffix"},
	} {
		r := e.ask(c.body)
		if r.status != 400 || r.errorField(t, "code") != c.code {
			t.Fatalf("%v -> %d %s", c.body, r.status, r.body)
		}
	}
	if fake.Has(kagitest.Create) {
		t.Fatalf("calls %v", fake.Calls())
	}
}
