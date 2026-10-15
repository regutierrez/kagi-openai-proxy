package translate

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"strings"
	"sync"
	"testing"

	"kagiopenaiproxy/internal/events"
	"kagiopenaiproxy/internal/logx"
	"kagiopenaiproxy/internal/redact"
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

var usage = &events.Usage{InputTokens: 5, OutputTokens: 3}

func feed(evs ...events.Event) (string, *Assembler) {
	a := NewAssembler()
	var b strings.Builder
	for _, e := range evs {
		b.WriteString(a.Feed(e))
	}
	return b.String(), a
}

func TestLeadingWhitespaceStrippedFromFirstTextDeltaOnly(t *testing.T) {
	out, _ := feed(events.NodeCreated{NodeID: "t", Kind: "text"}, events.TextDelta{NodeID: "t", Text: "  Hello"},
		events.TextDelta{NodeID: "t", Text: " world"}, events.TextDelta{NodeID: "t", Text: "  !"})
	if out != "Hello world  !" {
		t.Fatalf("got %q", out)
	}
}

func TestWhitespaceOnlyFirstDeltaDoesNotStripLaterDeltas(t *testing.T) {
	out, _ := feed(events.NodeCreated{NodeID: "t", Kind: "text"}, events.TextDelta{NodeID: "t", Text: " "}, events.TextDelta{NodeID: "t", Text: "    indented"})
	if out != "    indented" {
		t.Fatalf("got %q", out)
	}
}

func TestNoTextAfterTurnCompleted(t *testing.T) {
	out, a := feed(events.NodeCreated{NodeID: "t", Kind: "text"}, events.TextDelta{NodeID: "t", Text: "Answer"}, events.TurnCompleted{Usage: usage},
		events.TextDelta{NodeID: "t", Text: " LEAK"}, events.TurnFailed{Code: "late", Message: "late"})
	if out != "Answer" || !a.Done() || a.Failure != nil {
		t.Fatalf("out=%q done=%v failure=%v", out, a.Done(), a.Failure)
	}
	if tail, err := a.Finish(nil); tail != "" || err != nil {
		t.Fatalf("finish = %q, %v", tail, err)
	}
}

func TestThinkingAndActivityNodesSkipped(t *testing.T) {
	out, a := feed(
		events.NodeCreated{NodeID: "think", Kind: "thinking"}, events.TextDelta{NodeID: "think", Text: "reasoning..."},
		events.NodeCreated{NodeID: "act", Kind: "activity"}, events.TextDelta{NodeID: "act", Text: "searching"},
		events.NodeCreated{NodeID: "t", Kind: "text"}, events.TextDelta{NodeID: "t", Text: "Answer"},
		events.TextDelta{NodeID: "think", Text: "more reasoning"},
	)
	if out != "Answer" || a.emittedChars != len("Answer") {
		t.Fatalf("out=%q emitted=%d", out, a.emittedChars)
	}
}

func TestDeltaForUnknownNodeTreatedAsTextAndWarnedOnce(t *testing.T) {
	logs := captureLogs(t)
	out, _ := feed(events.TextDelta{NodeID: "mystery", Text: "Hi"}, events.TextDelta{NodeID: "mystery", Text: " there"})
	if out != "Hi there" {
		t.Fatalf("got %q", out)
	}
	if n := strings.Count(logs.String(), "unknown node"); n != 1 {
		t.Fatalf("warned %d times", n)
	}
}

func TestCitationsRewrittenInAssembledText(t *testing.T) {
	a := NewAssembler()
	var outs []string
	for _, e := range []events.Event{events.ReferenceAdded{RefID: "r1", Payload: map[string]any{}}, events.NodeCreated{NodeID: "t", Kind: "text"},
		events.TextDelta{NodeID: "t", Text: "Fact [^r"}, events.TextDelta{NodeID: "t", Text: "1#a]."}} {
		outs = append(outs, a.Feed(e))
	}
	if outs[2] != "Fact " || outs[3] != "[^1]." {
		t.Fatalf("outs = %q", outs)
	}
}

func TestEmptyAnswerNamesSeenEventsAndKinds(t *testing.T) {
	_, a := feed(events.Unknown{Type: "turn.started"}, events.NodeCreated{NodeID: "x", Kind: "thinking"}, events.TextDelta{NodeID: "x", Text: "hmm"},
		events.TurnCompleted{Usage: usage}, events.Final{})
	tail, err := a.Finish(nil)
	if tail != "" || err == nil || err.Status() != 502 || err.Code() != "kagi_empty_answer" {
		t.Fatalf("tail=%q err=%v", tail, err)
	}
	for _, want := range []string{"Unknown(turn.started)", "TextDelta", "thinking"} {
		if !strings.Contains(err.Message(), want) {
			t.Fatalf("message %q lacks %q", err.Message(), want)
		}
	}
}

func TestTruncatedWithoutTerminal(t *testing.T) {
	_, a := feed(events.NodeCreated{NodeID: "t", Kind: "text"}, events.TextDelta{NodeID: "t", Text: "partial"})
	if _, err := a.Finish(nil); err == nil || err.Code() != "kagi_stream_truncated" {
		t.Fatalf("err = %v", err)
	}
}

func TestQuotaFailureMapsToRateLimit(t *testing.T) {
	_, a := feed(events.TurnFailed{Code: "quota_exceeded", Message: "too much", RateLimited: true})
	_, err := a.Finish(nil)
	if err.Status() != 429 || err.Type() != "rate_limit_error" || err.Code() != "quota_exceeded" {
		t.Fatalf("err = %v", err)
	}
}

func TestTurnErrorRedactsCodeAndMessage(t *testing.T) {
	redact.RegisterSecret("tok-secret-value")
	err := NewTurnError(502, "api_error", "code-tok-secret-value", "msg tok-secret-value")
	body := string(Marshal(err.Body()))
	chunk := ChunkFormatter{}.Error(err)
	for _, s := range []string{body, chunk} {
		if strings.Contains(s, "tok-secret-value") || !strings.Contains(s, "[REDACTED]") {
			t.Fatalf("not redacted: %s", s)
		}
	}
}

func TestChunkShapes(t *testing.T) {
	f := ChunkFormatter{ID: "chatcmpl-x", Created: 123, Model: "m"}
	for got, want := range map[string]string{
		f.Role():           `data: {"id":"chatcmpl-x","object":"chat.completion.chunk","created":123,"model":"m","choices":[{"index":0,"delta":{"role":"assistant","content":""},"finish_reason":null}]}` + "\n\n",
		f.Content("<a&b>"): `data: {"id":"chatcmpl-x","object":"chat.completion.chunk","created":123,"model":"m","choices":[{"index":0,"delta":{"content":"<a&b>"},"finish_reason":null}]}` + "\n\n",
		f.Finish():         `data: {"id":"chatcmpl-x","object":"chat.completion.chunk","created":123,"model":"m","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}` + "\n\n",
		f.Usage(nil):       `data: {"id":"chatcmpl-x","object":"chat.completion.chunk","created":123,"model":"m","choices":[],"usage":null}` + "\n\n",
	} {
		if got != want {
			t.Fatalf("got  %s\nwant %s", got, want)
		}
	}
}

func decode(t *testing.T, s string) any {
	t.Helper()
	v, err := DecodeBody([]byte(s))
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func TestRequestParsingRules(t *testing.T) {
	loggedIgnoredMu.Lock()
	loggedIgnored = map[string]bool{}
	loggedIgnoredMu.Unlock()
	logs := captureLogs(t)
	req, err := ParseChatRequest(decode(t, `{"model": "m", "messages": [{"role": "user", "content": "x"}], "stream": true, "stream_options": {"include_usage": true}, "temperature": 0.2, "top_p": 1, "tools": []}`))
	if err != nil || !req.Stream || !req.IncludeUsage {
		t.Fatalf("req=%+v err=%v", req, err)
	}
	if _, err := ParseChatRequest(decode(t, `{"model": "m", "messages": [{"role": "user", "content": "x"}], "top_p": 1}`)); err != nil {
		t.Fatal(err)
	}
	if n := strings.Count(logs.String(), "'top_p'"); n != 1 { // once per process per key
		t.Fatalf("top_p logged %d times", n)
	}
	if req, _ := ParseChatRequest(decode(t, `{"messages": [{"role": "user", "content": "x"}], "stream_options": {"include_usage": true}}`)); req.IncludeUsage {
		t.Fatal("include_usage without stream")
	}
	for _, bad := range []string{
		`{"messages": [{"role": "user", "content": "x"}], "temperature": "hot"}`,
		`{"messages": [{"role": "user", "content": "x"}], "temperature": true}`,
		`{"messages": []}`,
		`{"messages": [{"role": "wizard", "content": "x"}]}`,
		`{"messages": [{"role": "system", "content": "x"}]}`,
		`{"model": " ", "messages": [{"role": "user", "content": "x"}]}`,
		`{"messages": [{"role": "user", "content": "x"}], "stream": "yes"}`,
		`{"messages": [{"role": "user", "content": 5}]}`,
		`[1]`,
	} {
		if _, err := ParseChatRequest(decode(t, bad)); err == nil || err.Status() != 400 {
			t.Fatalf("%s accepted", bad)
		}
	}
	_, err = ParseChatRequest(decode(t, `{"messages": [{"role": 7, "content": "x"}]}`))
	if err.Message() != "messages[0].role 7 is not supported" {
		t.Fatalf("message = %q", err.Message())
	}
}

func TestModelSuffixResolution(t *testing.T) {
	for _, c := range []struct {
		in     string
		def    bool
		model  string
		online bool
	}{
		{"gpt-x:online", false, "gpt-x", true},
		{"gpt-x:offline", true, "gpt-x", false},
		{"gpt-x", true, "gpt-x", true},
		{"gpt-x", false, "gpt-x", false},
		{":online", false, ":online", false},
	} {
		if m, o := ResolveModel(c.in, c.def); m != c.model || o != c.online {
			t.Fatalf("%q -> %q %v", c.in, m, o)
		}
	}
}

// ---------------------------------------------------------------- flattening

const footer = "[Respond as the Assistant to the last message, following the system instructions above.]"

func build(t *testing.T, messagesJSON string, contextWindow int) string {
	t.Helper()
	req, err := ParseChatRequest(decode(t, `{"messages": `+messagesJSON+`}`))
	if err != nil {
		t.Fatal(err)
	}
	return Flatten(req.Messages, contextWindow)
}

func TestExactLayoutWithSystemAndDeveloperMerged(t *testing.T) {
	text := build(t, `[{"role": "system", "content": "Be terse."}, {"role": "user", "content": "Hi"},
		{"role": "developer", "content": "Answer in French."}, {"role": "assistant", "content": "Bonjour"},
		{"role": "user", "content": "Capital of Italy?"}]`, 0)
	want := "[System instructions]\nBe terse.\n\nAnswer in French.\n\n[Conversation so far]\nUser: Hi\nAssistant: Bonjour\nUser: Capital of Italy?\n\n" + footer
	if text != want {
		t.Fatalf("got:\n%s\nwant:\n%s", text, want)
	}
}

func TestNoSystemRendersNone(t *testing.T) {
	if text := build(t, `[{"role": "user", "content": "Hi"}]`, 0); !strings.HasPrefix(text, "[System instructions]\nNone\n\n[Conversation so far]\nUser: Hi\n") {
		t.Fatalf("got %q", text)
	}
}

func TestToolMessagesFlattened(t *testing.T) {
	text := build(t, `[{"role": "user", "content": "Weather?"}, {"role": "assistant", "content": null, "tool_calls": [{"id": "c1"}]},
		{"role": "tool", "tool_call_id": "c1", "content": "{\"temp\": 21}"}, {"role": "user", "content": "So?"}]`, 0)
	if !strings.Contains(text, "User: Weather?\nAssistant: \nTool result: {\"temp\": 21}\nUser: So?\n") {
		t.Fatalf("got %q", text)
	}
}

func TestContentPartsAndNullContent(t *testing.T) {
	text := build(t, `[{"role": "user", "content": [{"type": "text", "text": "Look at this"},
		{"type": "image_url", "image_url": {"url": "data:image/png;base64,AAAA"}},
		{"type": "input_audio", "input_audio": {"data": "..."}}, {"type": "text", "text": "and describe it"}]},
		{"role": "assistant", "content": null}, {"role": "user", "content": "?"}]`, 0)
	if !strings.Contains(text, "User: Look at this\n[image omitted]\nand describe it\nAssistant: \nUser: ?\n") || strings.Contains(text, "AAAA") || strings.Contains(text, "input_audio") {
		t.Fatalf("got %q", text)
	}
}

func TestTruncationDropsOldestKeepsSystemAndLastMessage(t *testing.T) {
	const contextWindow = 200 // tokens -> budget of 200 * 0.7 * 4 = 560 chars
	budget := int(float64(contextWindow) * ContextBudgetFraction * CharsPerToken)
	msgs := []map[string]any{{"role": "system", "content": "SYSTEM RULES"}}
	for i := range 10 {
		role := "user"
		if i%2 == 1 {
			role = "assistant"
		}
		msgs = append(msgs, map[string]any{"role": role, "content": "message " + string(rune('0'+i)) + " " + strings.Repeat("x", 80)})
	}
	msgs = append(msgs, map[string]any{"role": "user", "content": "FINAL QUESTION"})
	raw, _ := json.Marshal(msgs)
	text := build(t, string(raw), contextWindow)
	if len(text) > budget {
		t.Fatalf("len %d > budget %d", len(text), budget)
	}
	if !strings.HasPrefix(text, "[System instructions]\nSYSTEM RULES\n\n[Conversation so far]\n"+OmissionMarker+"\n") ||
		!strings.Contains(text, "User: FINAL QUESTION\n\n"+footer) || strings.Contains(text, "message 0 ") || !strings.Contains(text, "message 9 ") {
		t.Fatalf("got %q", text)
	}
}

func TestTruncationNeverDropsLastMessageEvenIfOverBudget(t *testing.T) {
	long := strings.Repeat("y", 5000)
	text := build(t, `[{"role": "user", "content": "old"}, {"role": "user", "content": "`+long+`"}]`, 100)
	if !strings.Contains(text, OmissionMarker) || !strings.Contains(text, "User: "+long) || strings.Contains(text, "old") {
		t.Fatalf("got %q", text[:200])
	}
}

func TestNoTruncationMarkerWhenWithinBudget(t *testing.T) {
	if strings.Contains(build(t, `[{"role": "user", "content": "a"}, {"role": "assistant", "content": "b"}, {"role": "user", "content": "c"}]`, 0), OmissionMarker) {
		t.Fatal("unexpected marker")
	}
}

func TestBudgetCountsCharactersNotBytes(t *testing.T) {
	// 100 tokens -> 280 chars. 200 two-byte chars fit by characters but not by bytes.
	text := build(t, `[{"role": "user", "content": "old"}, {"role": "user", "content": "`+strings.Repeat("é", 120)+`"}]`, 100)
	if strings.Contains(text, OmissionMarker) {
		t.Fatal("dropped a message that fits by character count")
	}
}
