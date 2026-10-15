package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	"kagiopenaiproxy/internal/config"
	"kagiopenaiproxy/internal/events"
	"kagiopenaiproxy/internal/kagitest"
	"kagiopenaiproxy/internal/translate"
)

type obj = map[string]any

func ev(typ string, nodeID any, payload obj) obj { return kagitest.Ev(typ, nodeID, payload) }

func TestStreamingHappyPathExactChunksAndCleanup(t *testing.T) {
	e := newEnv(t, kagitest.New())
	r := e.post(chatBody(true, "kagi-default", nil))
	if r.status != 200 || !strings.HasPrefix(r.header.Get("Content-Type"), "text/event-stream") {
		t.Fatalf("status %d content-type %q", r.status, r.header.Get("Content-Type"))
	}
	frames := parseSSE(t, r.body)
	ids := map[string]bool{}
	var choices []any
	for _, f := range frames[:len(frames)-1] {
		m := f.(map[string]any)
		ids[fmt.Sprint(m["id"], m["created"])] = true
		if m["object"] != "chat.completion.chunk" || m["model"] != "kagi-default" {
			t.Fatalf("chunk %v", m)
		}
		choices = append(choices, choice(f))
	}
	if len(ids) != 1 {
		t.Fatalf("id/created differ across chunks: %v", ids)
	}
	mustJSONEqual(t, choices, `[
		{"index": 0, "delta": {"role": "assistant", "content": ""}, "finish_reason": null},
		{"index": 0, "delta": {"content": "The capital of France is Paris"}, "finish_reason": null},
		{"index": 0, "delta": {"content": " "}, "finish_reason": null},
		{"index": 0, "delta": {"content": "[^1]. About 2.1 million people live there[^2]"}, "finish_reason": null},
		{"index": 0, "delta": {"content": ", and it hosts the national government[^1]."}, "finish_reason": null},
		{"index": 0, "delta": {}, "finish_reason": "stop"}]`)
	if last(frames) != "[DONE]" || strings.Join(contents(frames), "") != kagitest.RealisticAnswer {
		t.Fatalf("frames end %v", last(frames))
	}

	// Upstream request shape.
	create := e.fake.Request(kagitest.Create)
	for header, want := range map[string]string{
		"Cookie": "kagi_session=" + kagitest.Token, "Accept": "application/json",
		"Content-Type": "application/json", "User-Agent": "kagi-openai-proxy/0.1 (personal)",
	} {
		if got := create.Header.Get(header); got != want {
			t.Fatalf("%s = %q, want %q", header, got, want)
		}
	}
	var sent obj
	_ = json.Unmarshal(create.Body, &sent)
	if sent["model"] != "kagi-default" || sent["enable_search"] != true || sent["personalization"] != false ||
		!strings.Contains(sent["content"].(string), "User: What is the capital of France?") {
		t.Fatalf("create body %v", sent)
	}
	stream := e.fake.Request(kagitest.Stream)
	if stream.Header.Get("Accept") != "text/event-stream" || stream.Header.Get("Cookie") != "kagi_session="+kagitest.Token {
		t.Fatalf("stream headers %v", stream.Header)
	}

	// Lifecycle: drained, then thread deleted, then slot released.
	e.waitIdle()
	if e.fake.Index("stream:drained") > e.fake.Index(kagitest.Delete) || e.fake.Index(kagitest.Delete) < 0 {
		t.Fatalf("calls %v", e.fake.Calls())
	}
	if e.proxy.Busy() {
		t.Fatal("slot still held")
	}
}

func TestNonStreamingHappyPathWithUsage(t *testing.T) {
	e := newEnv(t, kagitest.New())
	r := e.post(chatBody(false, "kagi-default", nil))
	body := r.json(t)
	if r.status != 200 || body["object"] != "chat.completion" || !strings.HasPrefix(body["id"].(string), "chatcmpl-") {
		t.Fatalf("%d %v", r.status, body)
	}
	mustJSONEqual(t, body["choices"], `[{"index": 0, "message": {"role": "assistant", "content": "`+kagitest.RealisticAnswer+`"}, "finish_reason": "stop"}]`)
	mustJSONEqual(t, body["usage"], `{"prompt_tokens": 412, "completion_tokens": 37, "total_tokens": 449}`)
	e.waitIdle()
	if !e.fake.Has(kagitest.Delete) {
		t.Fatal("thread not deleted")
	}
}

func TestIncludeUsageChunk(t *testing.T) {
	e := newEnv(t, kagitest.New())
	frames := parseSSE(t, e.post(chatBody(true, "kagi-default", obj{"stream_options": obj{"include_usage": true}})).body)
	n := len(frames)
	if choice(frames[n-3])["finish_reason"] != "stop" || frames[n-1] != "[DONE]" {
		t.Fatalf("tail %v", frames[n-3:])
	}
	usageChunk := frames[n-2].(map[string]any)
	mustJSONEqual(t, usageChunk["choices"], `[]`)
	mustJSONEqual(t, usageChunk["usage"], `{"prompt_tokens": 412, "completion_tokens": 37, "total_tokens": 449}`)
	if usageChunk["id"] != frames[0].(map[string]any)["id"] {
		t.Fatal("usage chunk id differs")
	}
}

func TestUsageChunkOnlyWhenRequested(t *testing.T) {
	e := newEnv(t, kagitest.New())
	for _, f := range parseSSE(t, e.post(chatBody(true, "kagi-default", nil)).body) {
		if m, ok := f.(map[string]any); ok && m["usage"] != nil {
			t.Fatalf("usage chunk without include_usage: %v", m)
		}
	}
}

func TestUsageIsLogged(t *testing.T) {
	logs := captureLogs(t)
	e := newEnv(t, kagitest.New())
	e.post(chatBody(false, "kagi-default", nil))
	e.waitIdle()
	want := "turn completed: model=kagi-default model_version=example-model-2026-09-01 input_tokens=412 output_tokens=37 cost_usd=0.00184"
	if !strings.Contains(logs.String(), want) {
		t.Fatalf("logs lack usage line:\n%s", logs)
	}
	if strings.Contains(logs.String(), kagitest.Token) || strings.Contains(logs.String(), kagitest.Key) {
		t.Fatal("secret in logs")
	}
}

func TestKagiAuthFailureMapsTo401(t *testing.T) {
	for _, status := range []int{401, 403} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			fake := kagitest.New()
			fake.CreateStatus, fake.CreateBody = status, []byte(`{"error":"unauthorized"}`)
			e := newEnv(t, fake)
			r := e.post(chatBody(true, "kagi-default", nil))
			if r.status != 401 || r.errorField(t, "type") != "authentication_error" || r.errorField(t, "code") != "kagi_session_expired" {
				t.Fatalf("%d %s", r.status, r.body)
			}
			if fake.Has(kagitest.Stream) || e.proxy.Busy() {
				t.Fatal("stream opened or slot held")
			}
		})
	}
}

func TestHTMLBodyMapsToSessionExpired(t *testing.T) {
	fake := kagitest.New()
	fake.CreateBody = []byte("<!DOCTYPE html><html><body>Sign in to Kagi</body></html>")
	fake.CreateHeaders = map[string]string{"Content-Type": "text/html; charset=utf-8"}
	e := newEnv(t, fake)
	r := e.post(chatBody(false, "kagi-default", nil))
	if r.status != 401 {
		t.Fatalf("status %d", r.status)
	}
	errObj := r.json(t)["error"].(map[string]any)
	if errObj["type"] != "authentication_error" || errObj["param"] != nil || errObj["code"] != "kagi_session_expired" || len(errObj) != 4 {
		t.Fatalf("error %v", errObj)
	}
	if e.proxy.Busy() {
		t.Fatal("slot held")
	}
}

func TestStreamOpenFailureGetsRealStatusAndDeletesThread(t *testing.T) {
	fake := kagitest.New()
	fake.StreamStatus = 401
	e := newEnv(t, fake)
	r := e.post(chatBody(true, "kagi-default", nil))
	if r.status != 401 || r.errorField(t, "code") != "kagi_session_expired" || !fake.Has(kagitest.Delete) || e.proxy.Busy() {
		t.Fatalf("%d %s calls=%v", r.status, r.body, fake.Calls())
	}
}

func TestKagi429MapsToRateLimitWithoutRetry(t *testing.T) {
	fake := kagitest.New()
	fake.CreateStatus, fake.CreateBody = 429, []byte(`{"error":"slow down"}`)
	e := newEnv(t, fake)
	r := e.post(chatBody(false, "kagi-default", nil))
	if r.status != 429 || r.errorField(t, "type") != "rate_limit_error" || fake.Count(kagitest.Create) != 1 {
		t.Fatalf("%d %s", r.status, r.body)
	}
}

func TestUnexpectedCreateShapeIsProtocolDrift(t *testing.T) {
	fake := kagitest.New()
	fake.CreateBody = []byte(`{"conversation": {"uuid": "conv-1"}}`)
	e := newEnv(t, fake)
	r := e.post(chatBody(false, "kagi-default", nil))
	if r.status != 502 || r.errorField(t, "code") != "kagi_protocol_drift" {
		t.Fatalf("%d %s", r.status, r.body)
	}
	if !fake.Has(kagitest.Delete) || e.proxy.Busy() { // thread was created, so it is cleaned up
		t.Fatalf("calls %v", fake.Calls())
	}
}

func TestUpstreamErrorMessagesAreRedacted(t *testing.T) {
	fake := kagitest.New()
	fake.CreateStatus = 400
	fake.CreateBody, _ = json.Marshal(obj{"error": "bad cookie kagi_session=" + kagitest.Token + " key " + kagitest.Key})
	e := newEnv(t, fake)
	r := e.post(chatBody(false, "kagi-default", nil))
	if r.status != 400 || strings.Contains(r.body, kagitest.Token) || strings.Contains(r.body, kagitest.Key) || !strings.Contains(r.body, "[REDACTED]") {
		t.Fatalf("%d %s", r.status, r.body)
	}
}

func TestTurnFailedMidStream(t *testing.T) {
	fake := kagitest.New()
	fake.StreamBody = kagitest.SSE(ev("turn.started", nil, nil), ev("node.created", "t", obj{"kind": "text"}), ev("node.delta", "t", obj{"delta": "Partial"}),
		ev("turn.failed", nil, obj{"error_code": "model_unavailable"}), obj{"is_final": true})
	e := newEnv(t, fake)
	r := e.post(chatBody(true, "kagi-default", nil))
	frames := parseSSE(t, r.body)
	errObj := lastError(t, frames)
	if r.status != 200 || !slices.Equal(contents(frames), []string{"", "Partial"}) || errObj["type"] != "api_error" || errObj["code"] != "model_unavailable" || hasDone(frames) {
		t.Fatalf("frames %v", frames)
	}
	e.waitIdle()
	if !fake.Has(kagitest.Delete) || e.proxy.Busy() {
		t.Fatal("cleanup missing")
	}
}

func TestTurnFailedNonStreamingQuotaIs429(t *testing.T) {
	fake := kagitest.New()
	fake.StreamBody = kagitest.SSE(ev("turn.started", nil, nil), ev("turn.failed", nil, obj{"error_code": "quota_exceeded"}), obj{"is_final": true})
	e := newEnv(t, fake)
	r := e.post(chatBody(false, "kagi-default", nil))
	if r.status != 429 || r.errorField(t, "type") != "rate_limit_error" || r.errorField(t, "code") != "quota_exceeded" {
		t.Fatalf("%d %s", r.status, r.body)
	}
}

func TestEmptyAnswerGuard(t *testing.T) {
	body := kagitest.SSE(ev("turn.started", nil, nil), ev("node.created", "th", obj{"kind": "thinking"}), ev("node.delta", "th", obj{"delta": "thinking only"}),
		ev("node.created", "x", obj{"kind": "essay"}), ev("node.delta", "x", obj{"delta": "the real answer"}), // drifted kind name
		ev("turn.completed", nil, obj{"usage": obj{"input_tokens": 1, "output_tokens": 1}}), obj{"is_final": true})
	fake := kagitest.New()
	fake.StreamBody = body
	frames := parseSSE(t, newEnv(t, fake).post(chatBody(true, "kagi-default", nil)).body)
	errObj := lastError(t, frames)
	msg := errObj["message"].(string)
	if errObj["code"] != "kagi_empty_answer" || hasDone(frames) || !strings.Contains(msg, "thinking") || !strings.Contains(msg, "essay") || !strings.Contains(msg, "TurnCompleted") {
		t.Fatalf("error %v", errObj)
	}
	fake2 := kagitest.New()
	fake2.StreamBody = body
	r := newEnv(t, fake2).post(chatBody(false, "kagi-default", nil))
	if r.status != 502 || r.errorField(t, "code") != "kagi_empty_answer" {
		t.Fatalf("%d %s", r.status, r.body)
	}
}

func TestEOFWithoutTerminalIsTruncated(t *testing.T) {
	fake := kagitest.New()
	fake.StreamBody = kagitest.SimpleStream(false, false, "Half an ans")
	e := newEnv(t, fake)
	frames := parseSSE(t, e.post(chatBody(true, "kagi-default", nil)).body)
	if !slices.Equal(contents(frames), []string{"", "Half an ans"}) || lastError(t, frames)["code"] != "kagi_stream_truncated" || hasDone(frames) {
		t.Fatalf("frames %v", frames)
	}
	e.waitIdle()
	if !fake.Has(kagitest.Delete) || e.proxy.Busy() {
		t.Fatal("cleanup missing")
	}
}

func TestTurnCompletedWithoutIsFinalIsSuccess(t *testing.T) {
	fake := kagitest.New()
	fake.StreamBody = kagitest.SimpleStream(true, false, "Done")
	frames := parseSSE(t, newEnv(t, fake).post(chatBody(true, "kagi-default", nil)).body)
	if last(frames) != "[DONE]" || !slices.Equal(contents(frames), []string{"", "Done"}) {
		t.Fatalf("frames %v", frames)
	}
}

func TestKeepaliveChunksWhileUpstreamIsQuiet(t *testing.T) {
	fake := kagitest.New()
	fake.StreamBody = kagitest.SimpleStream(true, true, "Hi")
	fake.PauseAfter, fake.Delay = 3, 300*time.Millisecond
	e := newEnv(t, fake, func(s *config.Settings) { s.Keepalive = 50 * time.Millisecond })
	frames := parseSSE(t, e.post(chatBody(true, "kagi-default", nil)).body)
	keepalives := 0
	for _, f := range frames[1 : len(frames)-2] {
		if c := choice(f); c != nil && fmt.Sprint(c["delta"]) == "map[content:]" {
			keepalives++
		}
	}
	if last(frames) != "[DONE]" || keepalives < 2 || strings.Join(contents(frames), "") != "Hi" {
		t.Fatalf("keepalives=%d frames=%v", keepalives, frames)
	}
}

func TestClientDisconnectDrainsUpstreamDeletesThreadReleasesSlot(t *testing.T) {
	fake := kagitest.New()
	gate := kagitest.NewGate()
	fake.Gate, fake.PauseAfter = gate, 4 // upstream stalls early in the turn
	e := newEnv(t, fake, func(s *config.Settings) { s.Keepalive = 50 * time.Millisecond })
	e.streamThenHangUp()
	time.Sleep(200 * time.Millisecond) // let the server notice the hang-up
	// The response is over, but the upstream is still mid-turn and holds the slot.
	if fake.Has("stream:drained") || fake.Has(kagitest.Delete) || !e.proxy.Busy() {
		t.Fatalf("calls %v busy=%v", fake.Calls(), e.proxy.Busy())
	}
	gate.Open()
	e.waitIdle()
	if fake.Index("stream:drained") < 0 || fake.Index("stream:drained") > fake.Index(kagitest.Delete) || e.proxy.Busy() {
		t.Fatalf("calls %v", fake.Calls())
	}
	if r := e.post(chatBody(false, "kagi-default", nil)); r.status != 200 { // the proxy keeps working
		t.Fatalf("follow-up %d", r.status)
	}
}

func TestQueueTimeoutReturns429(t *testing.T) {
	fake := kagitest.New()
	gate := kagitest.NewGate()
	fake.Gate, fake.PauseAfter = gate, 2
	e := newEnv(t, fake, func(s *config.Settings) { s.QueueWait = 100 * time.Millisecond })
	first := make(chan result, 1)
	go func() {
		r, _ := e.do(context.Background(), "POST", "/v1/chat/completions", chatBody(true, "kagi-default", nil), nil)
		first <- r
	}()
	eventually(t, e.proxy.Busy, "first turn to take the slot")
	second := e.post(chatBody(false, "kagi-default", nil))
	if second.status != 429 || second.errorField(t, "type") != "rate_limit_error" || fake.Count(kagitest.Create) != 1 {
		t.Fatalf("%d %s", second.status, second.body)
	}
	gate.Open()
	if r := <-first; last(parseSSE(t, r.body)) != "[DONE]" {
		t.Fatalf("first: %s", r.body)
	}
}

func TestWrongOrMissingBearerIs401(t *testing.T) {
	for _, auth := range []string{"", "Bearer wrong", kagitest.Key} {
		t.Run(auth, func(t *testing.T) {
			fake := kagitest.New()
			e := newEnv(t, fake)
			h := map[string]string{"Authorization": auth}
			r, _ := e.do(context.Background(), "POST", "/v1/chat/completions", chatBody(false, "kagi-default", nil), h)
			models, _ := e.do(context.Background(), "GET", "/v1/models", nil, h)
			if r.status != 401 || r.errorField(t, "code") != "invalid_api_key" || models.status != 401 || len(fake.Calls()) != 0 {
				t.Fatalf("%d %d calls=%v", r.status, models.status, fake.Calls())
			}
		})
	}
}

func TestWrongHostHeaderRejected(t *testing.T) {
	for _, host := range []string{"evil.example.com", "evil.example.com:8000", "192.168.1.10:8000", "LOCALHOST", "[::1]:8000"} {
		t.Run(host, func(t *testing.T) {
			fake := kagitest.New()
			e := newEnv(t, fake)
			r, _ := e.do(context.Background(), "POST", "/v1/chat/completions", chatBody(false, "kagi-default", nil), map[string]string{"Host": host})
			if r.status != 400 || r.body != "Invalid host header" || len(fake.Calls()) != 0 {
				t.Fatalf("%d %q", r.status, r.body)
			}
		})
	}
}

func TestAllowedHostHeaders(t *testing.T) {
	for _, host := range []string{"localhost:8000", "127.0.0.1", "localhost"} {
		e := newEnv(t, kagitest.New())
		if r := e.get("/health", map[string]string{"Host": host}); r.status != 200 {
			t.Fatalf("%s -> %d", host, r.status)
		}
	}
}

func TestHealthShallowAndDeep(t *testing.T) {
	fake := kagitest.New()
	e := newEnv(t, fake)
	if r := e.get("/health", map[string]string{"Authorization": ""}); r.body != `{"status":"ok","version":"0.1.0"}` || len(fake.Calls()) != 0 {
		t.Fatalf("shallow %s", r.body)
	}
	if r := e.get("/health?deep=1", map[string]string{"Authorization": ""}); r.status != 401 {
		t.Fatalf("deep without auth %d", r.status)
	}
	if r := e.get("/health?deep=1", nil); r.status != 200 || r.json(t)["kagi"] != "ok" {
		t.Fatalf("deep %d %s", r.status, r.body)
	}
	fake2 := kagitest.New()
	fake2.InitStatus = 401
	r := newEnv(t, fake2).get("/health?deep=1", nil)
	if r.status != 503 || r.json(t)["status"] != "error" || r.json(t)["kagi"] != "auth" {
		t.Fatalf("deep failure %d %s", r.status, r.body)
	}
}

func TestModelsListFilteredCachedAndStaleOnFailure(t *testing.T) {
	fake := kagitest.New()
	e := newEnv(t, fake, func(s *config.Settings) { s.ModelsTTL = 0 }) // always stale -> refresh every call
	first := e.get("/v1/models", nil)
	mustJSONEqual(t, first.json(t), `{"object": "list", "data": [
		{"id": "kagi-default", "object": "model", "created": 0, "owned_by": "kagi"},
		{"id": "tiny", "object": "model", "created": 0, "owned_by": "acme"}]}`)
	fake.SetInitStatus(500)
	stale := e.get("/v1/models", nil)
	if stale.status != 200 || stale.body != first.body || fake.Count(kagitest.Init) != 2 {
		t.Fatalf("stale %d %s", stale.status, stale.body)
	}

	fake2 := kagitest.New()
	e2 := newEnv(t, fake2) // default 6h TTL
	e2.get("/v1/models", nil)
	e2.get("/v1/models", nil)
	if fake2.Count(kagitest.Init) != 1 {
		t.Fatalf("init calls %d", fake2.Count(kagitest.Init))
	}
}

func TestModels503WhenNeverFetched(t *testing.T) {
	fake := kagitest.New()
	fake.InitStatus = 500
	r := newEnv(t, fake).get("/v1/models", nil)
	if r.status != 503 || r.errorField(t, "code") != "kagi_catalog_unavailable" {
		t.Fatalf("%d %s", r.status, r.body)
	}
}

func TestModelResolutionSuffixAndDefault(t *testing.T) {
	fake := kagitest.New()
	newEnv(t, fake).post(chatBody(false, "some-model:offline", nil))
	var sent obj
	_ = json.Unmarshal(fake.Request(kagitest.Create).Body, &sent)
	if sent["model"] != "some-model" || sent["enable_search"] != false {
		t.Fatalf("sent %v", sent)
	}

	fake2 := kagitest.New()
	r := newEnv(t, fake2, func(s *config.Settings) { s.SearchDefaultOnline = false }).post(chatBody(false, "", nil))
	_ = json.Unmarshal(fake2.Request(kagitest.Create).Body, &sent)
	if sent["model"] != "kagi-default" || sent["enable_search"] != false || r.json(t)["model"] != "kagi-default" {
		t.Fatalf("sent %v response %s", sent, r.body)
	}

	fake3 := kagitest.New()
	fake3.InitStatus = 500
	if r := newEnv(t, fake3).post(chatBody(false, "", nil)); r.status != 503 {
		t.Fatalf("%d", r.status)
	}
}

func TestContextWindowFromCatalogDrivesTruncation(t *testing.T) {
	fake := kagitest.New()
	var messages []any
	for i := range 5 {
		messages = append(messages, obj{"role": "user", "content": fmt.Sprintf("turn %d ", i) + strings.Repeat("z", 100)})
	}
	messages = append(messages, obj{"role": "user", "content": "LAST"})
	newEnv(t, fake).post(obj{"model": "tiny", "messages": messages}) // catalog: tiny has context_window=100
	var sent obj
	_ = json.Unmarshal(fake.Request(kagitest.Create).Body, &sent)
	content := sent["content"].(string)
	if !strings.Contains(content, "[...earlier messages omitted...]") || !strings.Contains(content, "User: LAST") || strings.Contains(content, "turn 0") {
		t.Fatalf("content %q", content)
	}
}

func TestKeepThreadsWhenDeletionDisabled(t *testing.T) {
	fake := kagitest.New()
	e := newEnv(t, fake, func(s *config.Settings) { s.ThreadLifecycle = config.LifecycleKeep })
	e.post(chatBody(false, "kagi-default", nil))
	e.waitIdle()
	if fake.Has(kagitest.Delete) || e.proxy.Busy() {
		t.Fatalf("calls %v", fake.Calls())
	}
}

func TestInvalidRequestIsOpenAI400(t *testing.T) {
	e := newEnv(t, kagitest.New())
	if r := e.post(obj{"model": "m", "messages": "hi"}); r.status != 400 || r.errorField(t, "type") != "invalid_request_error" {
		t.Fatalf("%d %s", r.status, r.body)
	}
	if r := e.post([]byte("{not json")); r.status != 400 {
		t.Fatalf("%d", r.status)
	}
}

func TestUnroutedRequests(t *testing.T) {
	e := newEnv(t, kagitest.New())
	if r := e.get("/nope", nil); r.status != 404 || r.errorField(t, "code") != "http_404" {
		t.Fatalf("%d %s", r.status, r.body)
	}
	if r, _ := e.do(context.Background(), "DELETE", "/v1/models", nil, nil); r.status != 405 || r.errorField(t, "code") != "http_405" {
		t.Fatalf("%d %s", r.status, r.body)
	}
}

// ---------------------------------------------------- termination & drain rules

func TestAnswerEndsAtTurnCompletedBeforeDelayedIsFinal(t *testing.T) {
	for _, stream := range []bool{true, false} {
		t.Run(fmt.Sprint("stream=", stream), func(t *testing.T) {
			fake := kagitest.New()
			gate := kagitest.NewGate() // upstream holds is_final back until the test releases it
			fake.AddScript(kagitest.Script{Steps: []kagitest.Step{kagitest.SSE(kagitest.HeadCompleted()...), gate, kagitest.SSE(obj{"is_final": true})}})
			e := newEnv(t, fake)
			r := e.post(chatBody(stream, "kagi-default", nil))
			if stream {
				frames := parseSSE(t, r.body)
				if !slices.Equal(contents(frames), []string{"", "Hi"}) || choice(frames[len(frames)-2])["finish_reason"] != "stop" || last(frames) != "[DONE]" {
					t.Fatalf("frames %v", frames)
				}
			} else if r.status != 200 || r.json(t)["choices"].([]any)[0].(map[string]any)["message"].(map[string]any)["content"] != "Hi" {
				t.Fatalf("%d %s", r.status, r.body)
			}
			// The client already has the full answer; upstream has not sent is_final yet and still holds the slot.
			if fake.Has("stream:drained") || fake.Has(kagitest.Delete) || !e.proxy.Busy() {
				t.Fatalf("calls %v", fake.Calls())
			}
			gate.Open()
			e.waitIdle()
			if fake.Index("stream:drained") > fake.Index(kagitest.Delete) || e.proxy.Busy() {
				t.Fatalf("calls %v", fake.Calls())
			}
		})
	}
}

func TestDelayedIsFinalWallClock(t *testing.T) {
	fake := kagitest.New()
	fake.AddScript(kagitest.Script{Steps: []kagitest.Step{kagitest.SSE(kagitest.HeadCompleted()...), 1500 * time.Millisecond, kagitest.SSE(obj{"is_final": true})}})
	e := newEnv(t, fake)
	start := time.Now()
	r := e.post(chatBody(true, "kagi-default", nil))
	if elapsed := time.Since(start); last(parseSSE(t, r.body)) != "[DONE]" || elapsed >= time.Second {
		t.Fatalf("[DONE] after %v", elapsed)
	}
	e.waitIdle()
	if fake.Index("stream:drained") > fake.Index(kagitest.Delete) {
		t.Fatalf("calls %v", fake.Calls())
	}
}

func TestTextAfterTurnCompletedIsNotEmitted(t *testing.T) {
	for _, stream := range []bool{true, false} {
		t.Run(fmt.Sprint("stream=", stream), func(t *testing.T) {
			fake := kagitest.New()
			frames := append(kagitest.HeadCompleted(), ev("node.delta", "t", obj{"delta": " LEAK"}), obj{"is_final": true})
			fake.AddScript(kagitest.Script{Steps: []kagitest.Step{kagitest.SSE(frames...)}})
			r := newEnv(t, fake).post(chatBody(stream, "kagi-default", nil))
			if strings.Contains(r.body, "LEAK") {
				t.Fatalf("leaked: %s", r.body)
			}
			if stream && last(parseSSE(t, r.body)) != "[DONE]" {
				t.Fatalf("%s", r.body)
			}
		})
	}
}

func TestHeartbeatsAfterTurnCompletedDoNotHoldResponse(t *testing.T) {
	fake := kagitest.New()
	fake.AddScript(kagitest.Script{Steps: []kagitest.Step{kagitest.SSE(kagitest.HeadCompleted()...)}, HeartbeatEvery: 10 * time.Millisecond})
	e := newEnv(t, fake, withDrain(300*time.Millisecond))
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	r, err := e.do(ctx, "POST", "/v1/chat/completions", chatBody(true, "kagi-default", nil), nil)
	if err != nil || last(parseSSE(t, r.body)) != "[DONE]" {
		t.Fatalf("response held open: %v", err)
	}
	e.waitIdle() // the background drain is bounded by DrainTimeout
	if !fake.Has("stream:heartbeating") || !fake.Has(kagitest.Delete) || e.proxy.Busy() {
		t.Fatalf("calls %v", fake.Calls())
	}
}

func TestNonStreamingDisconnectArmsBoundedDrain(t *testing.T) {
	fake := kagitest.New()
	fake.AddScript(kagitest.Script{Steps: []kagitest.Step{kagitest.SSE(kagitest.Partial()...)}, HeartbeatEvery: 10 * time.Millisecond}) // never ends
	e := newEnv(t, fake, withDrain(300*time.Millisecond))
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond) // client hangs up
	defer cancel()
	if _, err := e.do(ctx, "POST", "/v1/chat/completions", chatBody(false, "kagi-default", nil), nil); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v", err)
	}
	eventually(t, func() bool { return fake.Has("stream:heartbeating") }, "the turn to start upstream")
	e.waitIdle()
	if !fake.Has(kagitest.Delete) || e.proxy.Busy() {
		t.Fatalf("calls %v", fake.Calls())
	}
	if r := e.post(chatBody(false, "kagi-default", nil)); r.status != 200 { // the slot is usable again
		t.Fatalf("follow-up %d", r.status)
	}
}

func TestStreamingDisconnectWithNeverEndingUpstreamIsBounded(t *testing.T) {
	logs := captureLogs(t)
	fake := kagitest.New()
	fake.AddScript(kagitest.Script{Steps: []kagitest.Step{kagitest.SSE(kagitest.Partial()...)}, HeartbeatEvery: 10 * time.Millisecond})
	e := newEnv(t, fake, withDrain(300*time.Millisecond))
	e.streamThenHangUp()
	e.waitIdle()
	if !strings.Contains(logs.String(), "drain exceeded") || !fake.Has(kagitest.Delete) || e.proxy.Busy() {
		t.Fatalf("calls %v\n%s", fake.Calls(), logs)
	}
	if r := e.post(chatBody(false, "kagi-default", nil)); r.status != 200 {
		t.Fatalf("follow-up %d", r.status)
	}
}

func TestCancelledNonStreamingRequestLeavesNoGoroutines(t *testing.T) {
	fake := kagitest.New()
	fake.AddScript(kagitest.Script{Steps: []kagitest.Step{kagitest.SSE(kagitest.Partial()...)}, HeartbeatEvery: 10 * time.Millisecond})
	e := newEnv(t, fake, withDrain(300*time.Millisecond))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_, _ = e.do(ctx, "POST", "/v1/chat/completions", chatBody(false, "kagi-default", nil), nil)
		close(done)
	}()
	eventually(t, func() bool { return fake.Has("stream:heartbeating") }, "upstream heartbeats")
	time.Sleep(50 * time.Millisecond) // the handler is parked waiting for the runner
	cancel()
	<-done
	e.waitIdle()
	if !fake.Has(kagitest.Delete) || e.proxy.Busy() {
		t.Fatalf("calls %v", fake.Calls())
	}
	eventually(t, func() bool {
		buf := make([]byte, 1<<20)
		stacks := string(buf[:runtime.Stack(buf, true)])
		return !strings.Contains(stacks, "(*Proxy).complete") && !strings.Contains(stacks, "(*turnRunner).run") && !strings.Contains(stacks, "(*eventQueue).pop")
	}, "proxy goroutines to exit")
}

// ------------------------------------------------------------ malformed frames

func TestMalformedFrameMidAnswerIsProtocolDrift(t *testing.T) {
	cases := map[string][]byte{
		"invalid-json":              []byte("data: {not json\n\n"),
		"delta-without-payload":     kagitest.SSE(obj{"event": obj{"type": "node.delta", "node_id": "t"}}),
		"non-object":                kagitest.SSE([]any{1, 2}),
		"event-null":                kagitest.SSE(obj{"event": nil}),
		"failed-without-error-code": kagitest.SSE(ev("turn.failed", nil, nil)),
	}
	for name, malformed := range cases {
		for _, stream := range []bool{true, false} {
			t.Run(fmt.Sprint(name, "/stream=", stream), func(t *testing.T) {
				body := append(kagitest.SSE(kagitest.Partial()...), malformed...)
				body = append(body, kagitest.SSE(ev("turn.completed", nil, obj{}), obj{"is_final": true})...)
				fake := kagitest.New()
				fake.StreamBody = body
				e := newEnv(t, fake)
				r := e.post(chatBody(stream, "kagi-default", nil))
				if stream {
					frames := parseSSE(t, r.body)
					if !slices.Equal(contents(frames), []string{"", "Hi"}) || lastError(t, frames)["code"] != "kagi_protocol_drift" || hasDone(frames) {
						t.Fatalf("frames %v", frames)
					}
				} else if r.status != 502 || r.errorField(t, "code") != "kagi_protocol_drift" {
					t.Fatalf("%d %s", r.status, r.body)
				}
				e.waitIdle()
				if !fake.Has(kagitest.Delete) || e.proxy.Busy() {
					t.Fatal("cleanup missing")
				}
			})
		}
	}
}

func TestTitleOnlyFrameMidAnswerIsIgnored(t *testing.T) {
	fake := kagitest.New()
	fake.StreamBody = kagitest.SSE(append(kagitest.Partial(), obj{"title": "Capital cities"}, ev("turn.completed", nil, obj{}), obj{"is_final": true})...)
	frames := parseSSE(t, newEnv(t, fake).post(chatBody(true, "kagi-default", nil)).body)
	if !slices.Equal(contents(frames), []string{"", "Hi"}) || last(frames) != "[DONE]" {
		t.Fatalf("frames %v", frames)
	}
}

func TestSameFrameTurnCompletedAndErrorIsAnError(t *testing.T) {
	for _, stream := range []bool{true, false} {
		t.Run(fmt.Sprint("stream=", stream), func(t *testing.T) {
			frame := ev("turn.completed", nil, obj{"usage": obj{"input_tokens": 1, "output_tokens": 1}})
			frame["error"] = obj{"code": "quota_exceeded", "message": "Quota exceeded"}
			fake := kagitest.New()
			fake.StreamBody = kagitest.SSE(append(kagitest.Partial(), frame, obj{"is_final": true})...)
			e := newEnv(t, fake)
			r := e.post(chatBody(stream, "kagi-default", nil))
			if stream {
				frames := parseSSE(t, r.body)
				mustJSONEqual(t, last(frames), `{"error": {"message": "Quota exceeded", "type": "rate_limit_error", "code": "quota_exceeded"}}`)
				if hasDone(frames) {
					t.Fatal("[DONE] after error")
				}
			} else if r.status != 429 || r.errorField(t, "code") != "quota_exceeded" {
				t.Fatalf("%d %s", r.status, r.body)
			}
			e.waitIdle()
			if !fake.Has(kagitest.Delete) || e.proxy.Busy() {
				t.Fatal("cleanup missing")
			}
		})
	}
}

func TestHeldCitationTailFlushedAtEarlyCompletion(t *testing.T) {
	for _, stream := range []bool{true, false} {
		t.Run(fmt.Sprint("stream=", stream), func(t *testing.T) {
			fake := kagitest.New()
			gate := kagitest.NewGate() // is_final (and a late delta) are held back: completion alone must flush
			head := kagitest.SSE(ev("reference.added", "a", obj{"ref_id": "r1"}), ev("node.created", "t", obj{"kind": "text"}),
				ev("node.delta", "t", obj{"delta": "Known [^r1] and pending [^r"}), ev("turn.completed", nil, obj{}))
			fake.AddScript(kagitest.Script{Steps: []kagitest.Step{head, gate, kagitest.SSE(ev("node.delta", "t", obj{"delta": "1] late"}), obj{"is_final": true})}})
			e := newEnv(t, fake)
			r := e.post(chatBody(stream, "kagi-default", nil))
			if stream {
				frames := parseSSE(t, r.body)
				if !slices.Equal(contents(frames), []string{"", "Known [^1] and pending ", "[^r"}) || last(frames) != "[DONE]" {
					t.Fatalf("frames %v", frames)
				}
			} else if got := r.json(t)["choices"].([]any)[0].(map[string]any)["message"].(map[string]any)["content"]; got != "Known [^1] and pending [^r" {
				t.Fatalf("content %q", got)
			}
			gate.Open()
			e.waitIdle()
		})
	}
}

// ------------------------------------------------------------------- redaction

func TestSecretsNeverAppearInErrorBodiesOrLogs(t *testing.T) {
	logs := captureLogs(t)
	failed := kagitest.SSE(ev("turn.started", nil, nil), ev("node.created", "t", obj{"kind": "text"}),
		ev("turn.failed", nil, obj{"error_code": kagitest.Token, "message": "bad key " + kagitest.Key}), obj{"is_final": true})
	frameError := kagitest.SSE(ev("turn.started", nil, nil), obj{"error": obj{"code": kagitest.Token, "message": "key=" + kagitest.Key}}, obj{"is_final": true})
	var bodies []string
	for _, upstream := range [][]byte{failed, frameError} {
		for _, stream := range []bool{true, false} {
			fake := kagitest.New()
			fake.StreamBody = upstream
			e := newEnv(t, fake)
			bodies = append(bodies, e.post(chatBody(stream, "kagi-default", nil)).body)
			e.waitIdle()
		}
	}
	e := newEnv(t, kagitest.New())
	invalidRole := e.post(obj{"model": "m", "messages": []any{obj{"role": kagitest.Key, "content": "x"}}})
	if invalidRole.status != 400 {
		t.Fatalf("%d", invalidRole.status)
	}
	bodies = append(bodies, invalidRole.body)
	for _, text := range append(bodies, logs.String()) {
		if strings.Contains(text, kagitest.Token) || strings.Contains(text, kagitest.Key) {
			t.Fatalf("secret leaked: %s", text)
		}
	}
	for _, body := range bodies {
		if !strings.Contains(body, "[REDACTED]") {
			t.Fatalf("not redacted: %s", body)
		}
	}
}

func TestSecretStraddlingDetailLimitLeavesNoPrefix(t *testing.T) {
	logs := captureLogs(t)
	// HTTP error body: the token starts at char 290 of a 300-char detail limit.
	fake := kagitest.New()
	fake.CreateStatus = 400
	fake.CreateBody, _ = json.Marshal(obj{"error": strings.Repeat("x", 290) + kagitest.Token + " trailing"})
	httpErr := newEnv(t, fake).post(chatBody(false, "kagi-default", nil))
	// Frame-level error without a message: the bounded JSON dump of the error object.
	fake2 := kagitest.New()
	fake2.StreamBody = kagitest.SSE(ev("turn.started", nil, nil), obj{"error": obj{"detail": strings.Repeat("y", 270) + kagitest.Token}}, obj{"is_final": true})
	e2 := newEnv(t, fake2)
	frameErr := e2.post(chatBody(true, "kagi-default", nil))
	e2.waitIdle()
	if httpErr.status != 400 {
		t.Fatalf("%d", httpErr.status)
	}
	for _, text := range []string{httpErr.body, frameErr.body, logs.String()} {
		if strings.Contains(text, kagitest.Token[:6]) {
			t.Fatalf("token prefix leaked: %s", text)
		}
	}
}

// --------------------------------------------- stream writer with a fixed queue

func streamItems(t *testing.T, includeUsage bool, items ...item) []any {
	t.Helper()
	q := newEventQueue()
	for _, it := range items {
		q.push(it)
	}
	runner := &turnRunner{queue: q, consumerPresent: true, cancelUpstream: func() {}}
	p := &Proxy{settings: config.Settings{Keepalive: time.Minute}}
	rec := httptest.NewRecorder()
	p.stream(rec, httptest.NewRequest("POST", "/", nil), runner, translate.ChunkFormatter{ID: "chatcmpl-x", Created: 123, Model: "m"}, includeUsage)
	if runner.consumerPresent {
		t.Fatal("consumerGone not called")
	}
	return parseSSE(t, rec.Body.String())
}

var usage = &events.Usage{InputTokens: 5, OutputTokens: 3}

func TestStreamFirstChunkRoleThenFinalChunkAndDone(t *testing.T) {
	frames := streamItems(t, false, events.NodeCreated{NodeID: "t", Kind: "text"}, events.TextDelta{NodeID: "t", Text: " Hi"},
		events.TurnCompleted{Usage: usage}, events.Final{}, streamEnd{})
	if len(frames) != 4 {
		t.Fatalf("frames %v", frames)
	}
	mustJSONEqual(t, frames[0].(map[string]any)["choices"], `[{"index": 0, "delta": {"role": "assistant", "content": ""}, "finish_reason": null}]`)
	mustJSONEqual(t, choice(frames[1])["delta"], `{"content": "Hi"}`)
	mustJSONEqual(t, frames[2].(map[string]any)["choices"], `[{"index": 0, "delta": {}, "finish_reason": "stop"}]`)
	if frames[3] != "[DONE]" {
		t.Fatal("no [DONE]")
	}
	for _, f := range frames[:3] {
		m := f.(map[string]any)
		if m["id"] != "chatcmpl-x" || m["created"] != float64(123) || m["object"] != "chat.completion.chunk" {
			t.Fatalf("chunk %v", m)
		}
	}
}

func TestStreamTurnFailedEmitsErrorChunkWithoutDone(t *testing.T) {
	frames := streamItems(t, false, events.NodeCreated{NodeID: "t", Kind: "text"}, events.TextDelta{NodeID: "t", Text: "Par"},
		events.TurnFailed{Code: "model_error", Message: "it broke"}, streamEnd{})
	mustJSONEqual(t, last(frames), `{"error": {"message": "it broke", "type": "api_error", "code": "model_error"}}`)
	if hasDone(frames) {
		t.Fatal("[DONE] after error")
	}
}

func TestIncludeUsageChunkOnlyWhenRequested(t *testing.T) {
	items := []item{events.NodeCreated{NodeID: "t", Kind: "text"}, events.TextDelta{NodeID: "t", Text: "Hi"}, events.TurnCompleted{Usage: usage}, events.Final{}, streamEnd{}}
	with := streamItems(t, true, items...)
	mustJSONEqual(t, with[len(with)-2], `{"id": "chatcmpl-x", "object": "chat.completion.chunk", "created": 123, "model": "m", "choices": [], "usage": {"prompt_tokens": 5, "completion_tokens": 3, "total_tokens": 8}}`)
	for _, f := range streamItems(t, false, items...) {
		if m, ok := f.(map[string]any); ok && m["usage"] != nil {
			t.Fatal("usage chunk without include_usage")
		}
	}
}

// ---------------------------------------------------------- allowed hosts

func TestCustomAllowedHosts(t *testing.T) {
	fake := kagitest.New()
	e := newEnv(t, fake, func(s *config.Settings) {
		s.AllowedHosts = []string{"100.101.102.103", "mymac.tail1234.ts.net"}
	})
	for host, want := range map[string]int{
		"100.101.102.103":            200, // custom host accepted
		"100.101.102.103:8000":       200, // port stripped before matching
		"mymac.tail1234.ts.net:8000": 200,
		"evil.example.com":           400, // unknown host still rejected
		"localhost:8000":             400, // a custom list replaces the default
	} {
		if r := e.get("/health", map[string]string{"Host": host}); r.status != want {
			t.Fatalf("Host %s -> %d, want %d", host, r.status, want)
		}
	}
}

func TestAllowedHostsFromEnvEndToEnd(t *testing.T) {
	s, err := config.FromEnv(func(name string) (string, bool) {
		v, ok := map[string]string{"KAGI_SESSION_TOKEN": kagitest.Token, "PROXY_API_KEY": kagitest.Key,
			"PROXY_ALLOWED_HOSTS": "box.lan:9999, 10.0.0.5"}[name]
		return v, ok
	})
	if err != nil {
		t.Fatal(err)
	}
	fake := kagitest.New()
	e := newEnv(t, fake, func(settings *config.Settings) { settings.AllowedHosts = s.AllowedHosts })
	for host, want := range map[string]int{"box.lan": 200, "box.lan:8000": 200, "10.0.0.5:1": 200, "127.0.0.1": 400} {
		if r := e.get("/health", map[string]string{"Host": host}); r.status != want {
			t.Fatalf("Host %s -> %d, want %d", host, r.status, want)
		}
	}
}

// ------------------------------------------------------ queued disconnect

func TestQueuedClientDisconnectSpendsNoKagiTurn(t *testing.T) {
	fake := kagitest.New()
	gate := kagitest.NewGate()
	fake.Gate, fake.PauseAfter = gate, 2 // the first turn holds the slot until released
	e := newEnv(t, fake)
	first := make(chan result, 1)
	go func() {
		r, _ := e.do(context.Background(), "POST", "/v1/chat/completions", chatBody(true, "kagi-default", nil), nil)
		first <- r
	}()
	eventually(t, e.proxy.Busy, "first turn to take the slot")

	// Waiter B gives up while queued; waiter C keeps waiting.
	ctxB, cancelB := context.WithCancel(context.Background())
	bDone := make(chan error, 1)
	go func() {
		_, err := e.do(ctxB, "POST", "/v1/chat/completions", chatBody(false, "kagi-default", nil), nil)
		bDone <- err
	}()
	third := make(chan result, 1)
	time.Sleep(100 * time.Millisecond) // B is queued
	go func() {
		r, _ := e.do(context.Background(), "POST", "/v1/chat/completions", chatBody(false, "kagi-default", nil), nil)
		third <- r
	}()
	time.Sleep(100 * time.Millisecond) // C is queued
	cancelB()
	if err := <-bDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("B: %v", err)
	}
	time.Sleep(100 * time.Millisecond) // let the server notice B's hang-up

	gate.Open()
	if r := <-first; last(parseSSE(t, r.body)) != "[DONE]" {
		t.Fatalf("first: %s", r.body)
	}
	if r := <-third; r.status != 200 { // the slot went to the live waiter
		t.Fatalf("C: %d %s", r.status, r.body)
	}
	e.waitIdle()
	if n := fake.Count(kagitest.Create); n != 2 { // first + C; B never reached Kagi
		t.Fatalf("Kagi turns created: %d, calls %v", n, fake.Calls())
	}
	if e.proxy.Busy() {
		t.Fatal("slot still held")
	}
}

func TestClientGoneBeforeQueueingTakesNoFreeSlot(t *testing.T) {
	fake := kagitest.New()
	p := New(func() config.Settings {
		s := config.Defaults(kagitest.Token, kagitest.Key)
		s.KagiBaseURL = fake.URL()
		return s
	}())
	defer fake.Close()
	defer p.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if got := p.acquireSlot(ctx); got != slotClientGone || p.Busy() {
		t.Fatalf("acquireSlot = %v, busy=%v", got, p.Busy())
	}
}

// ------------------------------------------------------------ healthcheck

func TestHealthCheck(t *testing.T) {
	fake := kagitest.New()
	s := config.Defaults(kagitest.Token, kagitest.Key)
	s.KagiBaseURL = fake.URL()
	s.AllowedHosts = []string{"100.101.102.103"} // the probe must still pass the allow-list
	p := New(s)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: p}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close(); p.Close(); fake.Close() })

	s.Host, s.Port = "0.0.0.0", ln.Addr().(*net.TCPAddr).Port // unspecified bind -> loopback
	if err := HealthCheck(s, 2*time.Second); err != nil {
		t.Fatalf("healthy server: %v", err)
	}
	if len(fake.Calls()) != 0 {
		t.Fatalf("healthcheck called Kagi: %v", fake.Calls())
	}
	s.AllowedHosts = []string{"localhost"} // a Host the server rejects -> 400 -> unhealthy
	if err := HealthCheck(s, 2*time.Second); err == nil {
		t.Fatal("400 counted as healthy")
	}
	_ = srv.Close()
	if err := HealthCheck(s, 2*time.Second); err == nil {
		t.Fatal("closed port counted as healthy")
	}
}

// -------------------------------------------------------- reasoning effort

// effortParams is the verified /api/init shape: parameters.reasoning_effort.
func effortParams(def any, options ...any) obj {
	return obj{"reasoning_effort": obj{"options": options, "default": def}}
}

func effortCatalog() obj {
	return obj{"models": obj{
		"default_model": "glm-5-3-flash",
		"models": []any{
			obj{"id": "glm-5-3-flash", "provider": "zai", "context_window": 128000, "supported": true, "supports_thinking": true, "parameters": effortParams("max", "low", "high", "max")},
			obj{"id": "claude-haiku", "provider": "anthropic", "supported": true, "supports_thinking": true, "parameters": effortParams("low", "none", "low", "medium", "high", "max")},
			obj{"id": "minimax-m3", "provider": "minimax", "supported": true, "parameters": effortParams(nil, "low", "high")},
			obj{"id": "ki_quick", "provider": "kagi", "supported": true, "supports_thinking": false, "parameters": obj{"reasoning_effort": nil}},
			obj{"id": "plain", "provider": "acme", "supported": true}, // no parameters: global set only
		},
	}}
}

func effortFake() *kagitest.Fake {
	fake := kagitest.New()
	fake.InitJSON = effortCatalog()
	return fake
}

// sentEffort returns the reasoning_effort of the last Kagi POST (present=false if omitted).
func sentEffort(t *testing.T, fake *kagitest.Fake) (effort any, present bool) {
	t.Helper()
	rec := fake.Request(kagitest.Create)
	if rec == nil {
		t.Fatal("no POST to Kagi")
	}
	var sent obj
	if err := json.Unmarshal(rec.Body, &sent); err != nil {
		t.Fatal(err)
	}
	effort, present = sent["reasoning_effort"]
	return effort, present
}

func TestReasoningEffortPrecedence(t *testing.T) {
	for _, c := range []struct {
		name, env, model, body string
		want                   string // "" = field omitted
	}{
		{"none set: Kagi default", "", "plain", "", ""},
		{"env default", "low", "plain", "", "low"},
		{"suffix beats env", "low", "plain:high", "", "high"},
		{"body beats suffix and env", "low", "plain:high:offline", "medium", "medium"},
		{"catalog default model, no effort", "", "", "", ""},
		{"catalog default model, body effort", "", "", "max", "max"},
	} {
		t.Run(c.name, func(t *testing.T) {
			fake := effortFake()
			e := newEnv(t, fake, func(s *config.Settings) { s.ReasoningEffort = c.env })
			extra := obj{}
			if c.body != "" {
				extra["reasoning_effort"] = c.body
			}
			if r := e.post(chatBody(false, c.model, extra)); r.status != 200 {
				t.Fatalf("%d %s", r.status, r.body)
			}
			effort, present := sentEffort(t, fake)
			if c.want == "" && present {
				t.Fatalf("reasoning_effort sent (%v), want omitted", effort)
			}
			if c.want != "" && effort != c.want {
				t.Fatalf("reasoning_effort = %v (present=%v), want %q", effort, present, c.want)
			}
		})
	}
}

func TestReasoningEffortValidatedAgainstModelOptions(t *testing.T) {
	fake := effortFake()
	e := newEnv(t, fake)
	for _, c := range []struct {
		model string
		extra obj
	}{
		{"glm-5-3-flash", obj{"reasoning_effort": "medium"}}, // request field
		{"glm-5-3-flash:medium", nil},                        // suffix (Kagi itself would accept it)
		{"minimax-m3:max", nil},
	} {
		r := e.post(chatBody(false, c.model, c.extra))
		if r.status != 400 || r.errorField(t, "code") != "unsupported_reasoning_effort" {
			t.Fatalf("%s %v -> %d %s", c.model, c.extra, r.status, r.body)
		}
	}
	msg := e.post(chatBody(false, "glm-5-3-flash", obj{"reasoning_effort": "medium"})).errorField(t, "message").(string)
	if !strings.Contains(msg, "supported: low, high, max") {
		t.Fatalf("message %q does not name the supported values", msg)
	}
	if fake.Has(kagitest.Create) {
		t.Fatal("rejected requests reached Kagi")
	}
	// Supported values, and models without an option list, pass through.
	for model, effort := range map[string]string{"glm-5-3-flash": "max", "claude-haiku": "none", "ki_quick": "xhigh", "plain": "xhigh", "not-in-catalog": "minimal"} {
		if r := e.post(chatBody(false, model, obj{"reasoning_effort": effort})); r.status != 200 {
			t.Fatalf("%s/%s -> %d %s", model, effort, r.status, r.body)
		}
		if got, _ := sentEffort(t, fake); got != effort {
			t.Fatalf("%s: sent %v, want %s", model, got, effort)
		}
	}
}

func TestReasoningEffortGlobalOnlyWithoutCatalog(t *testing.T) {
	fake := effortFake()
	fake.InitStatus = 500 // catalog never fetched
	e := newEnv(t, fake)
	if r := e.post(chatBody(false, "glm-5-3-flash", obj{"reasoning_effort": "medium"})); r.status != 200 {
		t.Fatalf("%d %s", r.status, r.body)
	}
	if got, _ := sentEffort(t, fake); got != "medium" {
		t.Fatalf("sent %v", got)
	}
	if r := e.post(chatBody(false, "glm-5-3-flash", obj{"reasoning_effort": "turbo"})); r.status != 400 || r.errorField(t, "code") != "invalid_reasoning_effort" {
		t.Fatalf("%d %s", r.status, r.body)
	}
}

func TestReasoningEffortGlobalOnlyWhenCatalogIsStale(t *testing.T) {
	fake := effortFake()
	e := newEnv(t, fake, func(s *config.Settings) { s.ModelsTTL = 0 }) // every request refreshes
	if r := e.post(chatBody(false, "glm-5-3-flash", obj{"reasoning_effort": "medium"})); r.status != 400 {
		t.Fatalf("fresh catalog: %d %s", r.status, r.body)
	}
	fake.SetInitStatus(500) // refresh fails -> the cached copy is served stale
	if r := e.post(chatBody(false, "glm-5-3-flash", obj{"reasoning_effort": "medium"})); r.status != 200 {
		t.Fatalf("stale catalog: %d %s", r.status, r.body)
	}
	if got, _ := sentEffort(t, fake); got != "medium" {
		t.Fatalf("sent %v", got)
	}
}

func TestEnvEffortUnsupportedByModelIsDroppedAndLoggedOnce(t *testing.T) {
	logs := captureLogs(t)
	fake := effortFake()
	e := newEnv(t, fake, func(s *config.Settings) { s.ReasoningEffort = "medium" })
	for range 2 {
		if r := e.post(chatBody(false, "glm-5-3-flash", nil)); r.status != 200 {
			t.Fatalf("%d %s", r.status, r.body)
		}
		if effort, present := sentEffort(t, fake); present {
			t.Fatalf("unsupported env default sent: %v", effort)
		}
	}
	if n := strings.Count(logs.String(), "KAGI_REASONING_EFFORT=medium is not supported by glm-5-3-flash"); n != 1 {
		t.Fatalf("logged %d times:\n%s", n, logs)
	}
	e.post(chatBody(false, "plain", nil)) // still applied where allowed
	if got, _ := sentEffort(t, fake); got != "medium" {
		t.Fatalf("plain: sent %v", got)
	}
}

func TestModelSuffixErrorsAre400BeforeKagi(t *testing.T) {
	fake := effortFake()
	e := newEnv(t, fake)
	for _, model := range []string{"glm-5-3-flash:fast", "glm-5-3-flash:low:high", "glm-5-3-flash:online:offline"} {
		if r := e.post(chatBody(false, model, nil)); r.status != 400 || r.errorField(t, "code") != "invalid_model_suffix" {
			t.Fatalf("%s -> %d %s", model, r.status, r.body)
		}
	}
	if fake.Has(kagitest.Create) {
		t.Fatal("invalid suffix reached Kagi")
	}
}

func TestModelsListShowsReasoningEffortsWithCleanIDs(t *testing.T) {
	r := newEnv(t, effortFake()).get("/v1/models", nil)
	mustJSONEqual(t, r.json(t), `{"object": "list", "data": [
		{"id": "glm-5-3-flash", "object": "model", "created": 0, "owned_by": "zai", "reasoning_efforts": ["low", "high", "max"], "reasoning_effort_default": "max"},
		{"id": "claude-haiku", "object": "model", "created": 0, "owned_by": "anthropic", "reasoning_efforts": ["none", "low", "medium", "high", "max"], "reasoning_effort_default": "low"},
		{"id": "minimax-m3", "object": "model", "created": 0, "owned_by": "minimax", "reasoning_efforts": ["low", "high"]},
		{"id": "ki_quick", "object": "model", "created": 0, "owned_by": "kagi"},
		{"id": "plain", "object": "model", "created": 0, "owned_by": "acme"}]}`)
}

func TestModelsListLiveCatalogMix(t *testing.T) {
	// Like live Kagi: 14 models with parameters.reasoning_effort, 2 with null.
	var models []any
	for i := range 14 {
		models = append(models, obj{"id": fmt.Sprintf("thinker-%d", i), "supported": true, "parameters": effortParams("high", "low", "high")})
	}
	models = append(models, obj{"id": "ki_quick", "supported": true, "parameters": obj{"reasoning_effort": nil}},
		obj{"id": "mistral-large", "supported": true, "parameters": obj{"reasoning_effort": nil}})
	fake := kagitest.New()
	fake.InitJSON = obj{"models": obj{"default_model": "thinker-0", "models": models}}
	data := newEnv(t, fake).get("/v1/models", nil).json(t)["data"].([]any)
	with := 0
	for _, raw := range data {
		m := raw.(map[string]any)
		efforts, has := m["reasoning_efforts"]
		switch {
		case strings.HasPrefix(m["id"].(string), "thinker-"):
			if !has || fmt.Sprint(efforts) != "[low high]" || m["reasoning_effort_default"] != "high" {
				t.Fatalf("%v", m)
			}
			with++
		case has || m["reasoning_effort_default"] != nil:
			t.Fatalf("null-effort model lists efforts: %v", m)
		}
	}
	if len(data) != 16 || with != 14 {
		t.Fatalf("models=%d with efforts=%d", len(data), with)
	}
}

// ---------------------------------------------------------- thread lifecycle

func TestThreadLifecycleModes(t *testing.T) {
	for _, c := range []struct {
		mode    config.ThreadLifecycle
		isSaved any // nil = field omitted
		deletes bool
	}{
		{config.LifecycleDelete, nil, true},       // unchanged default flow
		{config.LifecycleTemporary, false, false}, // is_saved:false, no DELETE
		{config.LifecycleKeep, nil, false},        // account default, no DELETE
	} {
		for _, stream := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s/stream=%v", c.mode, stream), func(t *testing.T) {
				fake := kagitest.New()
				e := newEnv(t, fake, func(s *config.Settings) { s.ThreadLifecycle = c.mode })
				r := e.post(chatBody(stream, "kagi-default", nil))
				if r.status != 200 || r.header.Get("X-Kagi-Thread") != kagitest.Conv {
					t.Fatalf("status %d X-Kagi-Thread %q", r.status, r.header.Get("X-Kagi-Thread"))
				}
				var sent obj
				_ = json.Unmarshal(fake.Request(kagitest.Create).Body, &sent)
				value, present := sent["is_saved"]
				if c.isSaved == nil && present || c.isSaved != nil && (!present || value != c.isSaved) {
					t.Fatalf("is_saved = %v (present=%v), want %v", value, present, c.isSaved)
				}
				e.waitIdle()
				if fake.Has(kagitest.Delete) != c.deletes || e.proxy.Busy() {
					t.Fatalf("DELETE called=%v want %v; calls %v", fake.Has(kagitest.Delete), c.deletes, fake.Calls())
				}
			})
		}
	}
}

func TestNonDeleteModesSkipDeleteOnFailedTurnsToo(t *testing.T) {
	for _, mode := range []config.ThreadLifecycle{config.LifecycleTemporary, config.LifecycleKeep} {
		fake := kagitest.New()
		fake.StreamStatus = 401 // the thread exists, but its stream cannot be opened
		e := newEnv(t, fake, func(s *config.Settings) { s.ThreadLifecycle = mode })
		if r := e.post(chatBody(false, "kagi-default", nil)); r.status != 401 || r.header.Get("X-Kagi-Thread") != "" {
			t.Fatalf("%s: %d header %q", mode, r.status, r.header.Get("X-Kagi-Thread"))
		}
		if fake.Has(kagitest.Delete) || e.proxy.Busy() {
			t.Fatalf("%s: calls %v", mode, fake.Calls())
		}
	}
}
