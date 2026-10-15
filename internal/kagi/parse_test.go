package kagi

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"

	"kagiopenaiproxy/internal/events"
	"kagiopenaiproxy/internal/logx"
	"kagiopenaiproxy/internal/sse"
)

func TestMain(m *testing.M) {
	logx.SetOutput(io.Discard)
	os.Exit(m.Run())
}

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func captureLogs(t *testing.T) *syncBuffer {
	buf := &syncBuffer{}
	t.Cleanup(logx.SetOutput(buf))
	return buf
}

func dump(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}

func eventFrame(typ string, nodeID any, payload map[string]any) string {
	ev := map[string]any{"type": typ, "payload": payload}
	if nodeID != nil {
		ev["node_id"] = nodeID
	}
	return dump(map[string]any{"event": ev})
}

func eqEvents(t *testing.T, got []events.Event, want ...events.Event) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("events = %#v\nwant %#v", got, want)
	}
}

func ptrF(f float64) *float64 { return &f }
func ptrS(s string) *string   { return &s }

func TestIsFinalIsTerminalEvent(t *testing.T) {
	eqEvents(t, ParseFrame(`{"is_final": true}`), events.Final{})
}

func TestRealisticFixtureEvents(t *testing.T) {
	fixture, err := os.ReadFile("../kagitest/testdata/realistic_turn.sse")
	if err != nil {
		t.Fatal(err)
	}
	r := sse.NewReader(bytes.NewReader(fixture))
	var evs []events.Event
	for {
		frame, err := r.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		evs = append(evs, ParseFrame(frame)...)
	}
	eqEvents(t, evs[:2], events.Unknown{Type: "turn.started"}, events.NodeCreated{NodeID: "n-think", Kind: "thinking"})
	var refs []string
	for _, e := range evs {
		if ref, ok := e.(events.ReferenceAdded); ok {
			refs = append(refs, ref.RefID)
		}
	}
	if !reflect.DeepEqual(refs, []string{"r_a1", "r_b2"}) { // r_b2 came from a multi-data-line frame
		t.Fatalf("refs = %v", refs)
	}
	eqEvents(t, evs[len(evs)-2:], events.TurnCompleted{Usage: &events.Usage{InputTokens: 412, OutputTokens: 37, CostUSD: ptrF(0.00184), ModelVersion: ptrS("example-model-2026-09-01")}}, events.Final{})
}

func TestParseFrameEventTypes(t *testing.T) {
	eqEvents(t, ParseFrame(eventFrame("node.delta", "n1", map[string]any{"delta": "hi"})), events.TextDelta{NodeID: "n1", Text: "hi"})
	eqEvents(t, ParseFrame(eventFrame("turn.failed", nil, map[string]any{"error_code": "model_overloaded"})),
		events.TurnFailed{Code: "model_overloaded", Message: "Kagi turn.failed (model_overloaded)"})
	if f := ParseFrame(eventFrame("turn.cancelled", nil, map[string]any{"error_code": "quota_exceeded"}))[0].(events.TurnFailed); !f.RateLimited {
		t.Fatal("quota_exceeded should be rate limited")
	}
	eqEvents(t, ParseFrame(eventFrame("something.new", nil, map[string]any{})), events.Unknown{Type: "something.new"})
	// integer node ids become strings; floats do not
	eqEvents(t, ParseFrame(`{"event": {"type": "node.delta", "node_id": 7, "payload": {"delta": "x"}}}`), events.TextDelta{NodeID: "7", Text: "x"})
	// frame-level error alongside a normal event
	eqEvents(t, ParseFrame(`{"event": {"type": "turn.started", "payload": {}}, "error": "boom"}`),
		events.Unknown{Type: "turn.started"}, events.TurnFailed{Code: "kagi_stream_error", Message: "boom"})
	// Well-formed but unknown types are ignored (not drift).
	eqEvents(t, ParseFrame(`{"title": "New title"}`), events.Unknown{Type: "frame_without_event"})
}

func TestMalformedFramesAreProtocolDrift(t *testing.T) {
	for _, data := range []string{
		"not json",
		"[1, 2]",
		`"just a string"`,
		`{"event": ["not", "an", "object"]}`,
		`{"event": {"payload": {}}}`, // no type
		`{"event": {"type": "node.delta", "node_id": "t"}}`,                            // no payload
		`{"event": {"type": "node.delta", "payload": {"delta": "x"}}}`,                 // no node_id
		`{"event": {"type": "node.delta", "node_id": 1.5, "payload": {"delta": "x"}}}`, // float id
		`{"event": {"type": "node.created", "node_id": "t", "payload": {}}}`,           // no kind
		`{"event": {"type": "reference.added", "payload": {"url": "u"}}}`,              // no ref_id
		`{"event": null}`, // present but null (absent key is a title-only frame)
		`{"event": {"type": "turn.failed", "payload": {}}}`, // no error_code
		`{"event": {"type": "turn.cancelled"}}`,             // no payload at all
		`{"a": 1} trailing`,                                 // trailing data
	} {
		t.Run(data, func(t *testing.T) {
			evs := ParseFrame(data)
			f, ok := evs[0].(events.TurnFailed)
			if len(evs) != 1 || !ok || f.Code != ProtocolDriftCode || f.RateLimited {
				t.Fatalf("events = %#v, want one kagi_protocol_drift failure", evs)
			}
		})
	}
}

func TestUnknownEventTypesLoggedOnce(t *testing.T) {
	loggedUnknownMu.Lock()
	loggedUnknown = map[string]bool{}
	loggedUnknownMu.Unlock()
	logs := captureLogs(t)
	frame := eventFrame("brand.new", nil, map[string]any{})
	eqEvents(t, ParseFrame(frame), events.Unknown{Type: "brand.new"})
	eqEvents(t, ParseFrame(frame), events.Unknown{Type: "brand.new"})
	ParseFrame(eventFrame("turn.started", nil, map[string]any{})) // known, ignored silently
	if n := strings.Count(logs.String(), "unknown event type"); n != 1 {
		t.Fatalf("logged %d times:\n%s", n, logs)
	}
	if !strings.Contains(logs.String(), "Kagi stream: ignoring unknown event type 'brand.new' (logged once)") {
		t.Fatalf("log line: %s", logs)
	}
}

func TestSameFrameErrorWinsOverTurnCompleted(t *testing.T) {
	eqEvents(t, ParseFrame(`{"event": {"type": "turn.completed", "payload": {"usage": {}}}, "error": {"code": "quota_exceeded", "message": "Quota exceeded"}}`),
		events.TurnFailed{Code: "quota_exceeded", Message: "Quota exceeded", RateLimited: true})
	// Other events on an error frame are kept, in order.
	eqEvents(t, ParseFrame(`{"event": {"type": "node.delta", "node_id": "t", "payload": {"delta": "x"}}, "error": "boom"}`),
		events.TextDelta{NodeID: "t", Text: "x"}, events.TurnFailed{Code: "kagi_stream_error", Message: "boom"})
}

func TestUsageTolerantParsing(t *testing.T) {
	eqEvents(t, ParseFrame(eventFrame("turn.completed", nil, map[string]any{})), events.TurnCompleted{})
	eqEvents(t, ParseFrame(`{"event": {"type": "turn.completed", "payload": {"usage": {"input_tokens": 3.5, "output_tokens": true}}}}`),
		events.TurnCompleted{Usage: &events.Usage{}})
}

func TestCatalogFilterAndDefault(t *testing.T) {
	obj, err := DecodeJSON([]byte(`{"models": {"models": [
		{"id": "a", "provider": "p", "context_window": 1000, "supported": true, "deprecated": false, "retired": false},
		{"id": "b", "supported": true, "deprecated": true},
		{"id": "c", "supported": true, "retired": true},
		{"id": "d", "supported": false},
		{"id": "e", "supported": 1},
		{"id": "f", "supported": true, "context_window": 1.5}
	]}, "default_model": {"id": "f"}}`))
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := ParseCatalog(obj)
	if err != nil {
		t.Fatal(err)
	}
	want := []ModelInfo{{ID: "a", Provider: "p", ContextWindow: 1000}, {ID: "f"}}
	if !reflect.DeepEqual(catalog.Models, want) || catalog.DefaultID != "f" {
		t.Fatalf("catalog = %+v", catalog)
	}
	if _, err := ParseCatalog(map[string]any{}); err == nil {
		t.Fatal("missing models.models must be protocol drift")
	}
}

func TestQuote(t *testing.T) {
	if got := quote("a b/c~_.-ü"); got != "a%20b%2Fc~_.-%C3%BC" {
		t.Fatalf("quote = %q", got)
	}
}
