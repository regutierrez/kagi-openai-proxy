// Package kagitest is an offline fake of the Kagi Assistant endpoints for tests.
package kagitest

import (
	_ "embed"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync"
	"time"
)

// RealisticStream is a recorded-shape turn: thinking node before the text node, activity
// deltas, comment/id/event lines, a reference split over two data: lines, a citation
// marker split across deltas, a #-suffixed ref, usage, is_final, mixed LF/CRLF.
//
//go:embed testdata/realistic_turn.sse
var RealisticStream []byte

// RealisticAnswer is what the proxy must produce from RealisticStream.
const RealisticAnswer = "The capital of France is Paris [^1]. About 2.1 million people live there[^2], and it hosts the national government[^1]."

// Fixed identifiers and secrets used across tests.
const (
	Token      = "sekrit-session-token-1234567890"
	Key        = "proxy-key-abcdef"
	Conv       = "conv-1"
	Turn       = "turn-1"
	StreamPath = "/api/v2/turns/" + Turn + "/stream"
	DeletePath = "/api/conversations/" + Conv
	Create     = "POST /api/v2/conversations"
	Stream     = "GET " + StreamPath
	Delete     = "DELETE " + DeletePath
	Init       = "GET /api/init"
)

// Catalog is the default /api/init body.
var Catalog = map[string]any{
	"models": map[string]any{
		"default_model": "kagi-default",
		"models": []any{
			map[string]any{"id": "kagi-default", "provider": "kagi", "display_name": "Default", "context_window": 128000, "supported": true, "deprecated": false, "retired": false},
			map[string]any{"id": "tiny", "provider": "acme", "display_name": "Tiny", "context_window": 100, "supported": true, "deprecated": false, "retired": false},
			map[string]any{"id": "old", "provider": "acme", "display_name": "Old", "context_window": 8000, "supported": true, "deprecated": true, "retired": false},
			map[string]any{"id": "gone", "provider": "acme", "display_name": "Gone", "context_window": 8000, "supported": true, "deprecated": false, "retired": true},
			map[string]any{"id": "nope", "provider": "acme", "display_name": "Nope", "context_window": 8000, "supported": false, "deprecated": false, "retired": false},
		},
	},
}

// Created is the default create-conversation body.
var Created = map[string]any{
	"conversation":        map[string]any{"uuid": Conv, "title": "", "created_at": "", "updated_at": "", "is_saved": false, "is_shared": false, "model_name": "kagi-default", "folder_uuid": nil},
	"branch_uuid":         "branch-1",
	"assistant_turn_uuid": Turn,
}

// Ev builds a Kagi event frame object. nodeID may be nil.
func Ev(typ string, nodeID any, payload map[string]any) map[string]any {
	if payload == nil {
		payload = map[string]any{}
	}
	return map[string]any{"event": map[string]any{"type": typ, "node_id": nodeID, "turn_id": Turn, "timestamp": "2026-10-14T10:00:00Z", "payload": payload}}
}

// SSE encodes objects as LF-terminated data: frames.
func SSE(objs ...any) []byte {
	var out []byte
	for _, o := range objs {
		b, err := json.Marshal(o)
		if err != nil {
			panic(err)
		}
		out = append(out, "data: "...)
		out = append(out, b...)
		out = append(out, "\n\n"...)
	}
	return out
}

// Usage is a small usage payload.
func Usage() map[string]any {
	return map[string]any{"usage": map[string]any{"input_tokens": 10, "output_tokens": 2, "cost_usd": 0.001, "model_version": "v1"}}
}

// SimpleStream is started, a text node, the deltas, then optionally completed and final.
func SimpleStream(completed, final bool, deltas ...string) []byte {
	frames := []any{Ev("turn.started", nil, nil), Ev("node.created", "t", map[string]any{"kind": "text"})}
	for _, d := range deltas {
		frames = append(frames, Ev("node.delta", "t", map[string]any{"delta": d}))
	}
	if completed {
		frames = append(frames, Ev("turn.completed", nil, Usage()))
	}
	if final {
		frames = append(frames, map[string]any{"is_final": true})
	}
	return SSE(frames...)
}

// Partial is started + text node + delta "Hi" (no terminal).
func Partial() []any {
	return []any{Ev("turn.started", nil, nil), Ev("node.created", "t", map[string]any{"kind": "text"}), Ev("node.delta", "t", map[string]any{"delta": "Hi"})}
}

// HeadCompleted is Partial plus turn.completed.
func HeadCompleted() []any {
	return append(Partial(), Ev("turn.completed", nil, map[string]any{"usage": map[string]any{"input_tokens": 1, "output_tokens": 1, "cost_usd": 0.0, "model_version": "v"}}))
}

// Gate blocks a scripted stream until opened.
type Gate struct {
	ch   chan struct{}
	once sync.Once
}

// NewGate returns a closed gate.
func NewGate() *Gate { return &Gate{ch: make(chan struct{})} }

// Open releases the gate.
func (g *Gate) Open() { g.once.Do(func() { close(g.ch) }) }

// Step is one action of a scripted stream: []byte, *Gate, or time.Duration.
type Step any

// Script is an upstream stream body. With HeartbeatEvery > 0 it then sends ": ping"
// comments forever (until the client goes away).
type Script struct {
	Steps          []Step
	HeartbeatEvery time.Duration
}

// Fake is the fake Kagi server. Configure fields before the first request.
type Fake struct {
	Server *httptest.Server

	InitStatus    int
	InitJSON      any
	CreateStatus  int
	CreateBody    []byte
	CreateHeaders map[string]string
	StreamStatus  int
	StreamHeaders map[string]string
	StreamBody    []byte
	ChunkSize     int
	Gate          *Gate // pause the default stream before chunk PauseAfter
	PauseAfter    int
	Delay         time.Duration

	mu       sync.Mutex
	calls    []string
	requests map[string]*Recorded
	scripts  []Script
}

// Recorded is a captured request.
type Recorded struct {
	Header http.Header
	Body   []byte
}

// New starts a fake with the default (happy path) configuration.
func New() *Fake {
	created, _ := json.Marshal(Created)
	f := &Fake{
		InitStatus: 200, InitJSON: Catalog,
		CreateStatus: 200, CreateBody: created, CreateHeaders: map[string]string{"Content-Type": "application/json"},
		StreamStatus: 200, StreamHeaders: map[string]string{"Content-Type": "text/event-stream"},
		StreamBody: RealisticStream, ChunkSize: 7,
		requests: map[string]*Recorded{},
	}
	f.Server = httptest.NewServer(http.HandlerFunc(f.handle))
	return f
}

// Close stops the server.
func (f *Fake) Close() { f.Server.Close() }

// URL is the fake's base URL.
func (f *Fake) URL() string { return f.Server.URL }

// AddScript queues a scripted body for the next turn-stream request.
func (f *Fake) AddScript(s Script) {
	f.mu.Lock()
	f.scripts = append(f.scripts, s)
	f.mu.Unlock()
}

// SetInitStatus changes /api/init's status at runtime.
func (f *Fake) SetInitStatus(status int) {
	f.mu.Lock()
	f.InitStatus = status
	f.mu.Unlock()
}

// Calls returns the recorded calls in order.
func (f *Fake) Calls() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.calls)
}

// Has reports whether call was recorded.
func (f *Fake) Has(call string) bool { return slices.Contains(f.Calls(), call) }

// Index returns the position of the first call, or -1.
func (f *Fake) Index(call string) int { return slices.Index(f.Calls(), call) }

// Count counts a call.
func (f *Fake) Count(call string) int {
	n := 0
	for _, c := range f.Calls() {
		if c == call {
			n++
		}
	}
	return n
}

// Request returns the last captured request for a call key such as kagitest.Create.
func (f *Fake) Request(call string) *Recorded {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.requests[call]
}

func (f *Fake) record(call string) {
	f.mu.Lock()
	f.calls = append(f.calls, call)
	f.mu.Unlock()
}

func (f *Fake) handle(w http.ResponseWriter, r *http.Request) {
	key := r.Method + " " + r.URL.Path
	body, _ := io.ReadAll(r.Body)
	f.mu.Lock()
	f.calls = append(f.calls, key)
	f.requests[key] = &Recorded{Header: r.Header.Clone(), Body: body}
	initStatus := f.InitStatus
	var script *Script
	if key == Stream && len(f.scripts) > 0 {
		s := f.scripts[0]
		f.scripts = f.scripts[1:]
		script = &s
	}
	f.mu.Unlock()

	switch key {
	case Init:
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(initStatus)
		_ = json.NewEncoder(w).Encode(f.InitJSON)
	case Create:
		for k, v := range f.CreateHeaders {
			w.Header().Set(k, v)
		}
		w.WriteHeader(f.CreateStatus)
		_, _ = w.Write(f.CreateBody)
	case Stream:
		if f.StreamStatus != 200 {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(f.StreamStatus)
			_, _ = io.WriteString(w, `{"error": "nope"}`)
			return
		}
		for k, v := range f.StreamHeaders {
			w.Header().Set(k, v)
		}
		w.WriteHeader(200)
		if script != nil {
			f.serveScript(w, r, *script)
		} else {
			f.serveChunked(w, r)
		}
	case Delete:
		w.WriteHeader(http.StatusNoContent)
	default:
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(404)
		_, _ = io.WriteString(w, `{"error": "unexpected `+key+`"}`)
	}
}

func flush(w http.ResponseWriter) {
	if fl, ok := w.(http.Flusher); ok {
		fl.Flush()
	}
}

func (f *Fake) serveChunked(w http.ResponseWriter, r *http.Request) {
	body := f.StreamBody
	var chunks [][]byte
	for i := 0; i < len(body); i += f.ChunkSize {
		chunks = append(chunks, body[i:min(i+f.ChunkSize, len(body))])
	}
	for i, chunk := range chunks {
		if i == f.PauseAfter && i > 0 {
			if f.Gate != nil {
				select {
				case <-f.Gate.ch:
				case <-r.Context().Done():
					return
				}
			}
			if f.Delay > 0 {
				time.Sleep(f.Delay)
			}
		}
		if i == len(chunks)-1 {
			f.record("stream:drained") // last upstream byte handed out
		}
		if _, err := w.Write(chunk); err != nil {
			return
		}
		flush(w)
	}
}

func (f *Fake) serveScript(w http.ResponseWriter, r *http.Request, s Script) {
	last := -1
	for i, step := range s.Steps {
		if _, ok := step.([]byte); ok {
			last = i
		}
	}
	for i, step := range s.Steps {
		switch v := step.(type) {
		case *Gate:
			select {
			case <-v.ch:
			case <-r.Context().Done():
				return
			}
		case time.Duration:
			select {
			case <-time.After(v):
			case <-r.Context().Done():
				return
			}
		case []byte:
			if i == last && s.HeartbeatEvery == 0 {
				f.record("stream:drained")
			}
			if _, err := w.Write(v); err != nil {
				return
			}
			flush(w)
		}
	}
	if s.HeartbeatEvery == 0 {
		return
	}
	f.record("stream:heartbeating")
	ticker := time.NewTicker(s.HeartbeatEvery)
	defer ticker.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case <-ticker.C:
			if _, err := io.WriteString(w, ": ping\n\n"); err != nil {
				return
			}
			flush(w)
		}
	}
}
