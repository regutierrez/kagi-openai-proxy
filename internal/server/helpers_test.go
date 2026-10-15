package server

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"kagiopenaiproxy/internal/config"
	"kagiopenaiproxy/internal/kagitest"
	"kagiopenaiproxy/internal/logx"
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

// env is one proxy under test with its fake Kagi.
type env struct {
	t     *testing.T
	fake  *kagitest.Fake
	proxy *Proxy
	srv   *httptest.Server
}

func newEnv(t *testing.T, fake *kagitest.Fake, mods ...func(*config.Settings)) *env {
	t.Helper()
	s := config.Defaults(kagitest.Token, kagitest.Key)
	s.KagiBaseURL = fake.URL()
	s.Keepalive = 60 * time.Second
	for _, mod := range mods {
		mod(&s)
	}
	p := New(s)
	srv := httptest.NewServer(p)
	e := &env{t: t, fake: fake, proxy: p, srv: srv}
	t.Cleanup(func() {
		srv.Close()
		if !p.WaitIdle(10 * time.Second) {
			t.Errorf("turn runners still active at cleanup")
		}
		p.Close()
		fake.Close()
	})
	return e
}

func withDrain(d time.Duration) func(*config.Settings) {
	return func(s *config.Settings) { s.DrainTimeout = d }
}

func chatBody(stream bool, model string, extra map[string]any) map[string]any {
	body := map[string]any{"messages": []any{map[string]any{"role": "user", "content": "What is the capital of France?"}}, "stream": stream}
	if model != "" {
		body["model"] = model
	}
	for k, v := range extra {
		body[k] = v
	}
	return body
}

type result struct {
	status int
	header http.Header
	body   string
}

func (r result) json(t *testing.T) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal([]byte(r.body), &m); err != nil {
		t.Fatalf("not JSON (%v): %s", err, r.body)
	}
	return m
}

func (r result) errorField(t *testing.T, field string) any {
	t.Helper()
	return r.json(t)["error"].(map[string]any)[field]
}

// do sends a request to the proxy (with the bearer key unless headers override it).
func (e *env) do(ctx context.Context, method, path string, body any, headers map[string]string) (result, error) {
	var reader io.Reader
	switch b := body.(type) {
	case nil:
	case []byte:
		reader = bytes.NewReader(b)
	default:
		raw, _ := json.Marshal(b)
		reader = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(ctx, method, e.srv.URL+path, reader)
	if err != nil {
		return result{}, err
	}
	req.Header.Set("Authorization", "Bearer "+kagitest.Key)
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		if k == "Host" {
			req.Host = v
		} else if v == "" {
			req.Header.Del(k)
		} else {
			req.Header.Set(k, v)
		}
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return result{}, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	return result{resp.StatusCode, resp.Header, string(data)}, err
}

func (e *env) post(body any) result {
	e.t.Helper()
	r, err := e.do(context.Background(), "POST", "/v1/chat/completions", body, nil)
	if err != nil {
		e.t.Fatal(err)
	}
	return r
}

func (e *env) get(path string, headers map[string]string) result {
	e.t.Helper()
	r, err := e.do(context.Background(), "GET", path, nil, headers)
	if err != nil {
		e.t.Fatal(err)
	}
	return r
}

func (e *env) waitIdle() {
	e.t.Helper()
	if !e.proxy.WaitIdle(10 * time.Second) {
		e.t.Fatal("turn runners did not finish")
	}
}

// parseSSE splits the proxy's SSE output, asserting exact "data: ...\n\n" framing.
func parseSSE(t *testing.T, text string) []any {
	t.Helper()
	if strings.Contains(text, "\r") || !strings.HasSuffix(text, "\n\n") {
		t.Fatalf("bad framing: %q", text)
	}
	var frames []any
	for _, raw := range strings.Split(strings.TrimSuffix(text, "\n\n"), "\n\n") {
		if !strings.HasPrefix(raw, "data: ") || strings.Contains(raw, "\n") {
			t.Fatalf("bad frame: %q", raw)
		}
		payload := strings.TrimPrefix(raw, "data: ")
		if payload == "[DONE]" {
			frames = append(frames, "[DONE]")
			continue
		}
		var v map[string]any
		if err := json.Unmarshal([]byte(payload), &v); err != nil {
			t.Fatalf("bad JSON frame %q: %v", payload, err)
		}
		frames = append(frames, v)
	}
	return frames
}

func choice(f any) map[string]any {
	m, ok := f.(map[string]any)
	if !ok {
		return nil
	}
	choices, _ := m["choices"].([]any)
	if len(choices) == 0 {
		return nil
	}
	return choices[0].(map[string]any)
}

// contents returns every delta.content, in order.
func contents(frames []any) []string {
	var out []string
	for _, f := range frames {
		if c := choice(f); c != nil {
			if text, ok := c["delta"].(map[string]any)["content"].(string); ok {
				out = append(out, text)
			}
		}
	}
	return out
}

func hasDone(frames []any) bool { return slices.Contains(frames, any("[DONE]")) }

func last(frames []any) any { return frames[len(frames)-1] }

func lastError(t *testing.T, frames []any) map[string]any {
	t.Helper()
	m, ok := last(frames).(map[string]any)
	if !ok || m["error"] == nil {
		t.Fatalf("last frame is not an error: %v", last(frames))
	}
	return m["error"].(map[string]any)
}

func mustJSONEqual(t *testing.T, got any, wantJSON string) {
	t.Helper()
	var want any
	if err := json.Unmarshal([]byte(wantJSON), &want); err != nil {
		t.Fatal(err)
	}
	g, _ := json.Marshal(got)
	var gotNorm any
	_ = json.Unmarshal(g, &gotNorm)
	gn, _ := json.Marshal(gotNorm)
	wn, _ := json.Marshal(want)
	if !bytes.Equal(gn, wn) {
		t.Fatalf("got  %s\nwant %s", gn, wn)
	}
}

// streamUntilFirstFrame opens a streaming request, reads the first SSE frame, then
// hangs up (cancels the request).
func (e *env) streamThenHangUp() {
	e.t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	raw, _ := json.Marshal(chatBody(true, "kagi-default", nil))
	req, _ := http.NewRequestWithContext(ctx, "POST", e.srv.URL+"/v1/chat/completions", bytes.NewReader(raw))
	req.Header.Set("Authorization", "Bearer "+kagitest.Key)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		e.t.Fatal(err)
	}
	if resp.StatusCode != 200 {
		e.t.Fatalf("status %d", resp.StatusCode)
	}
	line, err := bufio.NewReader(resp.Body).ReadString('\n')
	if err != nil || !strings.HasPrefix(line, "data: ") {
		e.t.Fatalf("first line %q, %v", line, err)
	}
	cancel()
	resp.Body.Close()
}

func eventually(t *testing.T, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}
