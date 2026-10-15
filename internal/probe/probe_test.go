package probe

import (
	"bytes"
	"context"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"kagiopenaiproxy/internal/kagi"
	"kagiopenaiproxy/internal/kagitest"
	"kagiopenaiproxy/internal/logx"
	"kagiopenaiproxy/internal/redact"
)

type obj = map[string]any

func TestMain(m *testing.M) {
	logx.SetOutput(io.Discard)
	os.Exit(m.Run())
}

func catalog(ids ...string) *kagi.Catalog {
	c := &kagi.Catalog{DefaultID: ids[0]}
	for _, id := range ids {
		c.Models = append(c.Models, kagi.ModelInfo{ID: id})
	}
	return c
}

func TestPickModelMatchesWholeTokens(t *testing.T) {
	for want, ids := range map[string][]string{
		"gpt-5-mini":            {"gemini-2.5-pro", "gpt-5-mini"},
		"gemini-2.5-flash-lite": {"gemini-2.5-pro", "gemini-2.5-flash-lite", "gemini-2.5-flash"},
		"big-model":             {"big-model", "other"}, // falls back to the default
	} {
		if got := PickModel(catalog(ids...)); got != want {
			t.Fatalf("PickModel(%v) = %q, want %q", ids, got, want)
		}
	}
}

func run(t *testing.T, fake *kagitest.Fake) (int, string) {
	t.Helper()
	client := kagi.NewClient(kagitest.Token, fake.URL(), 5*time.Second, 5*time.Second)
	defer client.Close()
	var out bytes.Buffer
	status := Run(context.Background(), client, "m", &out)
	return status, out.String()
}

func TestPartialCreateFailureStillDeletesThread(t *testing.T) {
	fake := kagitest.New()
	defer fake.Close()
	fake.CreateBody = []byte(`{"conversation": {"uuid": "conv-1"}}`) // no assistant_turn_uuid
	status, out := run(t, fake)
	if status != 1 || !fake.Has(kagitest.Delete) || !strings.Contains(out, "Kagi error (protocol)") || !strings.Contains(out, "deleted thread conv-1") {
		t.Fatalf("status=%d calls=%v\n%s", status, fake.Calls(), out)
	}
}

func TestFrameWithoutPayloadReportsDriftNotCrash(t *testing.T) {
	fake := kagitest.New()
	defer fake.Close()
	fake.AddScript(kagitest.Script{Steps: []kagitest.Step{kagitest.SSE(
		kagitest.Ev("turn.started", nil, nil),
		kagitest.Ev("node.created", "t", obj{"kind": "text"}),
		obj{"event": obj{"type": "node.delta", "node_id": "t"}}, // payload missing
		kagitest.Ev("turn.completed", nil, obj{"usage": obj{}}),
		obj{"is_final": true},
	)}})
	status, out := run(t, fake)
	if status != 1 || !strings.Contains(out, "PROTOCOL DRIFT:") || !strings.Contains(out, "node.delta without node_id or payload.delta") || !fake.Has(kagitest.Delete) {
		t.Fatalf("status=%d\n%s", status, out)
	}
}

func TestRealisticStreamPasses(t *testing.T) {
	fake := kagitest.New()
	defer fake.Close()
	status, out := run(t, fake)
	if status != 0 || !strings.Contains(out, "OK: event sequence") || !fake.Has(kagitest.Delete) {
		t.Fatalf("status=%d\n%s", status, out)
	}
}

func TestNonStringNodeIDsReportDriftNotCrash(t *testing.T) {
	fake := kagitest.New()
	defer fake.Close()
	fake.AddScript(kagitest.Script{Steps: []kagitest.Step{kagitest.SSE(
		kagitest.Ev("turn.started", nil, nil),
		obj{"event": obj{"type": "node.created", "node_id": []any{}, "payload": obj{"kind": "text"}}},
		obj{"event": obj{"type": "node.delta", "node_id": []any{}, "payload": obj{"delta": "PONG"}}},
		obj{"event": obj{"type": "node.delta", "node_id": obj{"a": 1}, "payload": obj{"delta": "!"}}},
		kagitest.Ev("turn.completed", nil, obj{"usage": obj{}}),
		obj{"is_final": true},
	)}})
	status, out := run(t, fake)
	if status != 1 || !strings.Contains(out, "PROTOCOL DRIFT:") || !strings.Contains(out, "node.created without node_id") ||
		!strings.Contains(out, "node.delta without node_id") || !fake.Has(kagitest.Delete) {
		t.Fatalf("status=%d\n%s", status, out)
	}
}

func TestUnparseableFrameOutputRedactsBeforeTruncating(t *testing.T) {
	redact.RegisterSecret(kagitest.Token) // as cmd/probe does
	fake := kagitest.New()
	defer fake.Close()
	leaky := strings.Repeat("z", 110) + kagitest.Token // the token straddles the 120-char print limit
	fake.AddScript(kagitest.Script{Steps: []kagitest.Step{[]byte("data: " + leaky + "\n\n"), kagitest.SSE(obj{"is_final": true})}})
	status, out := run(t, fake)
	if status != 1 || !strings.Contains(out, "unparseable frame") || !strings.Contains(out, "[REDACTED]") || strings.Contains(out, kagitest.Token[:6]) {
		t.Fatalf("status=%d\n%s", status, out)
	}
}

func TestFrameLevelErrorInProbeOutputIsRedacted(t *testing.T) {
	redact.RegisterSecret(kagitest.Token)
	fake := kagitest.New()
	defer fake.Close()
	fake.AddScript(kagitest.Script{Steps: []kagitest.Step{kagitest.SSE(
		kagitest.Ev("turn.started", nil, nil), obj{"error": "token " + kagitest.Token}, obj{"is_final": true},
	)}})
	status, out := run(t, fake)
	if status != 1 || !strings.Contains(out, "frame-level error") || strings.Contains(out, kagitest.Token) {
		t.Fatalf("status=%d\n%s", status, out)
	}
}
