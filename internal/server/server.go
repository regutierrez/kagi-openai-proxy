// Package server is the HTTP surface: host allow-list, bearer auth, model catalog, and
// the Kagi turn lifecycle.
//
// Lifecycle of a chat request:
//  1. Acquire the turn slot (bounded wait -> 429).
//  2. POST the prompt and open the turn stream, checking status, BEFORE responding, so
//     upstream failures become real HTTP errors.
//  3. Hand the open stream to a turnRunner goroutine. The runner -- not the response --
//     owns the upstream: it reads to EOF/is_final (bounded once the client is gone),
//     deletes the thread, and releases the slot in a defer.
package server

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"kagiopenaiproxy/internal/config"
	"kagiopenaiproxy/internal/events"
	"kagiopenaiproxy/internal/kagi"
	"kagiopenaiproxy/internal/logx"
	"kagiopenaiproxy/internal/redact"
	"kagiopenaiproxy/internal/translate"
)

// Version is reported by /health.
const Version = "0.1.0"

var kagiErrors = map[kagi.Kind]struct {
	status    int
	typ, code string
}{
	kagi.KindAuth:        {401, "authentication_error", "kagi_session_expired"},
	kagi.KindRateLimit:   {429, "rate_limit_error", "kagi_rate_limited"},
	kagi.KindProtocol:    {502, "api_error", "kagi_protocol_drift"},
	kagi.KindBadRequest:  {400, "invalid_request_error", "kagi_rejected_request"},
	kagi.KindUpstream:    {502, "api_error", "kagi_upstream_error"},
	kagi.KindUnreachable: {502, "api_error", "kagi_unreachable"},
	kagi.KindTimeout:     {504, "api_error", "kagi_timeout"},
}

func kagiErrorToOpenAI(err *kagi.Error) *translate.TurnError {
	m := kagiErrors[err.Kind]
	return translate.NewTurnError(m.status, m.typ, m.code, err.Message)
}

// ------------------------------------------------------------------- model catalog

// catalogCache is the lazily fetched /api/init catalog with a TTL; it serves stale
// data if a refresh fails.
type catalogCache struct {
	kagi      *kagi.Client
	ttl       time.Duration
	mu        sync.Mutex
	catalog   *kagi.Catalog
	fetchedAt time.Time
	lastError string
}

func (c *catalogCache) fresh() bool {
	return c.catalog != nil && time.Since(c.fetchedAt) < c.ttl
}

func (c *catalogCache) get(ctx context.Context) (*kagi.Catalog, string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.fresh() {
		return c.catalog, ""
	}
	catalog, err := c.kagi.FetchCatalog(ctx)
	if err != nil {
		c.lastError = err.Error()
		state := "no cached copy"
		if c.catalog != nil {
			state = "serving stale copy"
		}
		logx.Warn("model catalog refresh failed (%s); %s", c.lastError, state)
		return c.catalog, c.lastError
	}
	c.catalog, c.fetchedAt, c.lastError = catalog, time.Now(), ""
	logx.Info("model catalog refreshed: %d models", len(catalog.Models))
	return c.catalog, ""
}

// ----------------------------------------------------------------------- proxy

// Proxy is the HTTP handler plus the state behind it.
type Proxy struct {
	settings config.Settings
	kagi     *kagi.Client
	catalog  *catalogCache
	slots    chan struct{}
	turns    turnTracker
	// baseCtx outlives requests: upstream turns use it (not the request context) so a
	// client disconnect never aborts the background drain. Cancelled at shutdown.
	baseCtx    context.Context
	cancelBase context.CancelFunc
	handler    http.Handler
}

// New builds a Proxy and registers its secrets for redaction.
func New(settings config.Settings) *Proxy {
	redact.RegisterSecret(settings.KagiSessionToken)
	redact.RegisterSecret(settings.ProxyAPIKey)
	client := kagi.NewClient(settings.KagiSessionToken, settings.KagiBaseURL, settings.ConnectTimeout, settings.ReadTimeout)
	ctx, cancel := context.WithCancel(context.Background())
	p := &Proxy{
		settings:   settings,
		kagi:       client,
		catalog:    &catalogCache{kagi: client, ttl: settings.ModelsTTL},
		slots:      make(chan struct{}, settings.MaxConcurrency),
		baseCtx:    ctx,
		cancelBase: cancel,
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", p.health)
	mux.HandleFunc("GET /v1/models", p.listModels)
	mux.HandleFunc("POST /v1/chat/completions", p.chatCompletions)
	mux.HandleFunc("/", p.notFound)
	p.handler = hostAllowList(settings.AllowedHosts, mux)
	return p
}

// ServeHTTP implements http.Handler.
func (p *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) { p.handler.ServeHTTP(w, r) }

// WaitIdle waits for all turn runners; it reports false on timeout.
func (p *Proxy) WaitIdle(timeout time.Duration) bool { return p.turns.wait(timeout) }

// turnTracker counts running turns. Unlike sync.WaitGroup, starting a turn may race
// with a concurrent wait (a request arriving during shutdown or a test's WaitIdle).
type turnTracker struct {
	mu   sync.Mutex
	n    int
	idle chan struct{} // closed when n drops to zero
}

func (t *turnTracker) add() {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.n == 0 {
		t.idle = make(chan struct{})
	}
	t.n++
}

func (t *turnTracker) done() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.n--
	if t.n == 0 {
		close(t.idle)
	}
}

func (t *turnTracker) wait(timeout time.Duration) bool {
	t.mu.Lock()
	if t.n == 0 {
		t.mu.Unlock()
		return true
	}
	idle := t.idle
	t.mu.Unlock()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-idle:
		return true
	case <-timer.C:
		return false
	}
}

// Busy reports whether any turn slot is taken.
func (p *Proxy) Busy() bool { return len(p.slots) > 0 }

// Close lets in-flight turns finish (10s), then cancels them so they wind down through
// their bounded cleanup (5s more), then releases connections.
func (p *Proxy) Close() {
	if !p.WaitIdle(10 * time.Second) {
		p.cancelBase()
		p.WaitIdle(5 * time.Second)
	}
	p.cancelBase()
	p.kagi.Close()
}

// ------------------------------------------------------------------ middleware

// hostAllowList rejects any Host (port stripped) not in allowed -- by default only
// 127.0.0.1 / localhost (DNS-rebinding defence). Matching is exact. There is
// deliberately no CORS and no gzip middleware.
func hostAllowList(allowed []string, next http.Handler) http.Handler {
	set := make(map[string]bool, len(allowed))
	for _, h := range allowed {
		set[h] = true
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		host, ok := config.HostOnly(r.Host)
		if !ok || !set[host] {
			w.Header().Set("Content-Type", "text/plain; charset=utf-8")
			w.WriteHeader(http.StatusBadRequest)
			_, _ = io.WriteString(w, "Invalid host header")
			return
		}
		next.ServeHTTP(w, r)
	})
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(translate.Marshal(body))
}

func writeError(w http.ResponseWriter, err *translate.TurnError) {
	writeJSON(w, err.Status(), err.Body())
}

func (p *Proxy) authorized(r *http.Request) bool {
	scheme, token, _ := strings.Cut(r.Header.Get("Authorization"), " ")
	return strings.EqualFold(scheme, "bearer") &&
		subtle.ConstantTimeCompare([]byte(strings.TrimSpace(token)), []byte(p.settings.ProxyAPIKey)) == 1
}

func (p *Proxy) requireAuth(w http.ResponseWriter, r *http.Request) bool {
	if p.authorized(r) {
		return true
	}
	writeError(w, translate.NewTurnError(401, "invalid_request_error", "invalid_api_key", "Missing or invalid proxy API key: send PROXY_API_KEY in the Authorization header"))
	return false
}

var knownRoutes = map[string]string{"/health": "GET, HEAD", "/v1/models": "GET, HEAD", "/v1/chat/completions": "POST"}

// notFound answers unrouted requests the way the reference does: 405 for a known path
// with the wrong method, a redirect for a known path with a trailing slash, else 404.
func (p *Proxy) notFound(w http.ResponseWriter, r *http.Request) {
	if allow, ok := knownRoutes[r.URL.Path]; ok {
		w.Header().Set("Allow", allow)
		writeError(w, translate.NewTurnError(405, "invalid_request_error", "http_405", "Method Not Allowed"))
		return
	}
	if trimmed := strings.TrimSuffix(r.URL.Path, "/"); trimmed != r.URL.Path {
		if _, ok := knownRoutes[trimmed]; ok {
			target := "http://" + r.Host + trimmed
			if r.URL.RawQuery != "" {
				target += "?" + r.URL.RawQuery
			}
			http.Redirect(w, r, target, http.StatusTemporaryRedirect)
			return
		}
	}
	writeError(w, translate.NewTurnError(404, "invalid_request_error", "http_404", "Not Found"))
}

// ---------------------------------------------------------------------- routes

func (p *Proxy) health(w http.ResponseWriter, r *http.Request) {
	values := r.URL.Query()["deep"]
	deep := ""
	if len(values) > 0 {
		deep = strings.ToLower(values[len(values)-1]) // last value wins, like the reference
	}
	if deep != "1" && deep != "true" && deep != "yes" {
		writeJSON(w, 200, map[string]string{"status": "ok", "version": Version})
		return
	}
	if !p.requireAuth(w, r) { // the deep check spends an upstream call
		return
	}
	if _, err := p.kagi.FetchInit(context.WithoutCancel(r.Context())); err != nil {
		var kerr *kagi.Error
		errors.As(err, &kerr)
		writeJSON(w, 503, struct {
			Status  string `json:"status"`
			Version string `json:"version"`
			Kagi    string `json:"kagi"`
			Message string `json:"message"`
		}{"error", Version, string(kerr.Kind), kerr.Message})
		return
	}
	writeJSON(w, 200, struct {
		Status  string `json:"status"`
		Version string `json:"version"`
		Kagi    string `json:"kagi"`
	}{"ok", Version, "ok"})
}

type modelEntry struct {
	ID      string `json:"id"`
	Object  string `json:"object"`
	Created int    `json:"created"`
	OwnedBy string `json:"owned_by"`
}

func (p *Proxy) listModels(w http.ResponseWriter, r *http.Request) {
	if !p.requireAuth(w, r) {
		return
	}
	catalog, lastErr := p.catalog.get(context.WithoutCancel(r.Context()))
	if catalog == nil {
		writeError(w, translate.NewTurnError(503, "api_error", "kagi_catalog_unavailable", "Kagi model catalog unavailable: "+orNone(lastErr)))
		return
	}
	data := make([]modelEntry, 0, len(catalog.Models))
	for _, m := range catalog.Models {
		owner := m.Provider
		if owner == "" {
			owner = "kagi"
		}
		data = append(data, modelEntry{m.ID, "model", 0, owner})
	}
	writeJSON(w, 200, struct {
		Object string       `json:"object"`
		Data   []modelEntry `json:"data"`
	}{"list", data})
}

func orNone(s string) string {
	if s == "" {
		return "None"
	}
	return s
}

func newCompletionID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = b[6]&0x0f | 0x40 // UUID v4 layout, like uuid.uuid4().hex
	b[8] = b[8]&0x3f | 0x80
	return "chatcmpl-" + hex.EncodeToString(b[:])
}

type slotResult int

const (
	slotAcquired slotResult = iota
	slotTimeout
	slotClientGone
)

// acquireSlot waits up to QueueWait for a turn slot. It gives up as soon as the client
// disconnects, so a request nobody is waiting for never spends a Kagi turn.
func (p *Proxy) acquireSlot(ctx context.Context) slotResult {
	if ctx.Err() != nil {
		return slotClientGone
	}
	select {
	case p.slots <- struct{}{}:
		return slotAcquired
	default:
	}
	timer := time.NewTimer(p.settings.QueueWait)
	defer timer.Stop()
	select {
	case p.slots <- struct{}{}:
		return slotAcquired
	case <-ctx.Done():
		return slotClientGone
	case <-timer.C:
		return slotTimeout
	}
}

func (p *Proxy) releaseSlot() { <-p.slots }

func (p *Proxy) chatCompletions(w http.ResponseWriter, r *http.Request) {
	if !p.requireAuth(w, r) {
		return
	}
	settings := p.settings
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		return // client gone while sending the body
	}
	body, err := translate.DecodeBody(raw)
	if err != nil {
		writeError(w, translate.InvalidRequest("request body is not valid JSON"))
		return
	}
	chat, terr := translate.ParseChatRequest(body)
	if terr != nil {
		writeError(w, terr)
		return
	}

	catalog, _ := p.catalog.get(context.WithoutCancel(r.Context()))
	var responseModel, modelID string
	var enableSearch bool
	if chat.Model == "" {
		if catalog == nil || catalog.DefaultID == "" {
			writeError(w, translate.NewTurnError(503, "api_error", "kagi_catalog_unavailable", "no model given and the Kagi model catalog is unavailable"))
			return
		}
		responseModel, modelID, enableSearch = catalog.DefaultID, catalog.DefaultID, settings.SearchDefaultOnline
	} else {
		responseModel = chat.Model
		modelID, enableSearch = translate.ResolveModel(chat.Model, settings.SearchDefaultOnline)
	}
	contextWindow := 0
	if catalog != nil {
		if info, ok := catalog.Get(modelID); ok {
			contextWindow = info.ContextWindow
		}
	}
	content := translate.Flatten(chat.Messages, contextWindow)

	switch p.acquireSlot(r.Context()) {
	case slotClientGone:
		logx.Info("client disconnected while queued for a Kagi turn slot; nothing was sent to Kagi")
		return // the client is gone: no status is written
	case slotTimeout:
		writeError(w, translate.NewTurnError(429, "rate_limit_error", "proxy_busy", fmt.Sprintf(
			"another Kagi turn is in progress; waited %ss for a free slot", strconv.FormatFloat(settings.QueueWait.Seconds(), 'g', -1, 64))))
		return
	}
	runner, terr := p.startTurn(content, modelID, enableSearch)
	if terr != nil {
		writeError(w, terr)
		return
	}

	formatter := translate.ChunkFormatter{ID: newCompletionID(), Created: time.Now().Unix(), Model: responseModel}
	if chat.Stream {
		p.stream(w, r, runner, formatter, chat.IncludeUsage)
		return
	}
	p.complete(w, r, runner, formatter)
}

// startTurn creates the Kagi turn and opens its stream while holding the slot. On
// success the slot belongs to the returned runner; on failure it is released here.
func (p *Proxy) startTurn(content, modelID string, enableSearch bool) (*turnRunner, *translate.TurnError) {
	settings := p.settings
	turnCtx, cancelTurn := context.WithCancel(p.baseCtx)
	cleanupCtx := context.WithoutCancel(p.baseCtx)
	fail := func(kerr *kagi.Error) *translate.TurnError {
		cancelTurn()
		p.releaseSlot()
		return kagiErrorToOpenAI(kerr)
	}
	created, err := p.kagi.CreateTurn(turnCtx, content, modelID, enableSearch, settings.Personalization)
	if err != nil {
		var kerr *kagi.Error
		errors.As(err, &kerr)
		logx.Warn("Kagi request failed: %s", kerr.Message)
		if kerr.ConversationUUID != "" && settings.DeleteThreads {
			deleteThreadQuietly(cleanupCtx, p.kagi, kerr.ConversationUUID)
		}
		return nil, fail(kerr)
	}
	stream, err := p.kagi.OpenTurnStream(turnCtx, created.AssistantTurnUUID)
	if err != nil {
		var kerr *kagi.Error
		errors.As(err, &kerr)
		if settings.DeleteThreads {
			deleteThreadQuietly(cleanupCtx, p.kagi, created.ConversationUUID)
		}
		logx.Warn("Kagi request failed: %s", kerr.Message)
		return nil, fail(kerr)
	}
	runner := &turnRunner{
		queue:            newEventQueue(),
		kagi:             p.kagi,
		stream:           stream,
		cancelUpstream:   cancelTurn,
		deleteCtx:        cleanupCtx,
		conversationUUID: created.ConversationUUID,
		model:            modelID,
		deleteThread:     settings.DeleteThreads,
		drainTimeout:     settings.DrainTimeout,
		release:          p.releaseSlot,
		consumerPresent:  true,
		reading:          true,
	}
	p.turns.add()
	go func() {
		defer p.turns.done()
		runner.run()
	}()
	return runner, nil
}

// stream writes the SSE response. The role chunk goes out immediately; turn.completed
// or a failure ends the answer while the runner keeps draining upstream.
func (p *Proxy) stream(w http.ResponseWriter, r *http.Request, runner *turnRunner, f translate.ChunkFormatter, includeUsage bool) {
	defer runner.consumerGone()
	flusher, _ := w.(http.Flusher)
	h := w.Header()
	h.Set("Content-Type", "text/event-stream; charset=utf-8")
	h.Set("Cache-Control", "no-cache")
	h.Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	write := func(frame string) bool {
		if _, err := io.WriteString(w, frame); err != nil {
			return false // client gone
		}
		if flusher != nil {
			flusher.Flush()
		}
		return true
	}

	assembler := translate.NewAssembler()
	var interrupted *translate.TurnError
	if !write(f.Role()) {
		return
	}
	lastEmit := time.Now()
	for {
		timer := time.NewTimer(time.Until(lastEmit.Add(p.settings.Keepalive)))
		it, err := runner.queue.pop(r.Context(), timer.C)
		timer.Stop()
		if errors.Is(err, errQueueTimeout) {
			if !write(f.Keepalive()) {
				return
			}
			lastEmit = time.Now()
			continue
		}
		if err != nil {
			return // client disconnected: the deferred consumerGone arms the bounded drain
		}
		if end, ok := it.(streamEnd); ok {
			interrupted = end.err
			break
		}
		if text := assembler.Feed(it.(events.Event)); text != "" {
			if !write(f.Content(text)) {
				return
			}
			lastEmit = time.Now()
		}
		if assembler.Done() {
			break
		}
	}
	tail, terr := assembler.Finish(interrupted)
	if tail != "" && !write(f.Content(tail)) {
		return
	}
	if terr != nil {
		logx.Warn("turn ended with error mid-stream: %s (%s)", terr.Code(), terr.Message())
		write(f.Error(terr))
		return
	}
	if !write(f.Finish()) {
		return
	}
	if includeUsage && !write(f.Usage(assembler.Usage)) {
		return
	}
	write(translate.DoneFrame)
}

// complete waits for the whole answer. If the client disconnects meanwhile, it returns
// without writing anything (the client is gone) and the bounded drain takes over.
func (p *Proxy) complete(w http.ResponseWriter, r *http.Request, runner *turnRunner, f translate.ChunkFormatter) {
	assembler := translate.NewAssembler()
	var parts []string
	var interrupted *translate.TurnError
	for {
		it, err := runner.queue.pop(r.Context(), nil)
		if err != nil {
			runner.consumerGone()
			return
		}
		if end, ok := it.(streamEnd); ok {
			interrupted = end.err
			break
		}
		parts = append(parts, assembler.Feed(it.(events.Event)))
		if assembler.Done() {
			break
		}
	}
	tail, terr := assembler.Finish(interrupted)
	runner.consumerGone()
	if terr != nil {
		logx.Warn("turn failed: %s (%s)", terr.Code(), terr.Message())
		writeError(w, terr)
		return
	}
	parts = append(parts, tail)
	writeJSON(w, 200, translate.CompletionBody(f.ID, f.Created, f.Model, strings.Join(parts, ""), assembler.Usage))
}

// IsLoopback reports whether host is a loopback name or address.
func IsLoopback(host string) bool {
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}
