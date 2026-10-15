// Package kagi is the Kagi Assistant wire protocol: the ONLY package that knows Kagi
// URLs, headers and JSON. Callers get internal events, small structs, and *Error.
package kagi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"kagiopenaiproxy/internal/events"
	"kagiopenaiproxy/internal/redact"
	"kagiopenaiproxy/internal/sse"
	"kagiopenaiproxy/internal/textutil"
)

// UserAgent identifies the proxy honestly.
const UserAgent = "kagi-openai-proxy/0.1 (personal)"

// DefaultBaseURL is the Kagi Assistant origin.
const DefaultBaseURL = "https://assistant.kagi.com"

const detailLimit = 300

// Kind classifies a Kagi failure.
type Kind string

const (
	KindAuth        Kind = "auth"        // session expired / invalid (401/403/redirect/HTML)
	KindRateLimit   Kind = "rate_limit"  // Kagi 429
	KindProtocol    Kind = "protocol"    // unexpected response shape
	KindBadRequest  Kind = "bad_request" // other Kagi 4xx (e.g. unknown model)
	KindUpstream    Kind = "upstream"    // Kagi 5xx / unexpected status
	KindUnreachable Kind = "unreachable" // connection-level failure
	KindTimeout     Kind = "timeout"     // connect / read timeout
)

// Error is a Kagi failure. Message is redacted at construction.
type Error struct {
	Kind           Kind
	Message        string
	UpstreamStatus int
	// ConversationUUID is set when a thread was created before the failure, so the
	// caller can delete it.
	ConversationUUID string
}

func (e *Error) Error() string { return e.Message }

func newError(kind Kind, message string) *Error {
	return &Error{Kind: kind, Message: redact.String(message)}
}

// CreatedTurn identifies the thread and the assistant turn of a new conversation.
type CreatedTurn struct {
	ConversationUUID  string
	AssistantTurnUUID string
}

// Client talks to Kagi Assistant. There is no total request timeout: only connect,
// time-to-headers, and per-read idle timeouts, so long completions can stream for minutes.
type Client struct {
	token       string
	baseURL     string
	readTimeout time.Duration
	http        *http.Client
}

// NewClient builds a Client.
func NewClient(token, baseURL string, connectTimeout, readTimeout time.Duration) *Client {
	transport := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           (&net.Dialer{Timeout: connectTimeout, KeepAlive: 30 * time.Second}).DialContext,
		TLSHandshakeTimeout:   connectTimeout,
		ResponseHeaderTimeout: readTimeout,
		MaxIdleConns:          10,
		IdleConnTimeout:       90 * time.Second,
	}
	return &Client{
		token:       token,
		baseURL:     strings.TrimRight(baseURL, "/"),
		readTimeout: readTimeout,
		http: &http.Client{
			Transport: transport,
			// Never follow redirects: a redirect where JSON is expected means a dead session.
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
	}
}

// Close releases idle connections.
func (c *Client) Close() { c.http.CloseIdleConnections() }

// response is an upstream response whose body enforces the per-read idle timeout.
type response struct {
	*http.Response
	body *idleBody
}

// send performs one request. The returned body must be closed (response.close).
func (c *Client) send(ctx context.Context, method, path string, body any, accept string) (*response, error) {
	var reader io.Reader
	if body != nil {
		var buf bytes.Buffer
		enc := json.NewEncoder(&buf)
		enc.SetEscapeHTML(false)
		if err := enc.Encode(body); err != nil {
			return nil, newError(KindProtocol, fmt.Sprintf("cannot encode %s %s body: %v", method, path, err))
		}
		reader = bytes.NewReader(bytes.TrimSuffix(buf.Bytes(), []byte("\n")))
	}
	reqCtx, cancel := context.WithCancel(ctx)
	req, err := http.NewRequestWithContext(reqCtx, method, c.baseURL+path, reader)
	if err != nil {
		cancel()
		return nil, newError(KindUnreachable, fmt.Sprintf("Kagi %s %s failed: %v", method, path, err))
	}
	req.Header.Set("Cookie", "kagi_session="+c.token)
	req.Header.Set("Accept", accept)
	req.Header.Set("User-Agent", UserAgent)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		cancel()
		if isTimeout(err) {
			return nil, newError(KindTimeout, fmt.Sprintf("Kagi %s %s timed out (%s)", method, path, timeoutName(err)))
		}
		return nil, newError(KindUnreachable, fmt.Sprintf("Kagi %s %s failed: %T: %v", method, path, errors.Unwrap(err), err))
	}
	ib := newIdleBody(resp.Body, c.readTimeout, cancel)
	resp.Body = ib
	return &response{Response: resp, body: ib}, nil
}

func (r *response) close() { _ = r.body.Close() }

// readAll reads the whole body, mapping a read failure to a Kagi error.
func (r *response) readAll(what string) ([]byte, error) {
	data, err := io.ReadAll(r.Body)
	if err != nil {
		if r.body.timedOut.Load() {
			return nil, newError(KindTimeout, fmt.Sprintf("Kagi %s read timed out (ReadTimeout)", what))
		}
		return nil, newError(KindUnreachable, fmt.Sprintf("Kagi %s read failed: %v", what, err))
	}
	return data, nil
}

func isTimeout(err error) bool {
	var ne net.Error
	return errors.Is(err, context.DeadlineExceeded) || (errors.As(err, &ne) && ne.Timeout())
}

func timeoutName(err error) string {
	if strings.Contains(err.Error(), "awaiting headers") {
		return "ReadTimeout"
	}
	return "ConnectTimeout"
}

// idleBody cancels the request if no Read completes within timeout.
type idleBody struct {
	body     io.ReadCloser
	timer    *time.Timer
	timeout  time.Duration
	cancel   context.CancelFunc
	timedOut atomic.Bool
	once     sync.Once
}

func newIdleBody(body io.ReadCloser, timeout time.Duration, cancel context.CancelFunc) *idleBody {
	ib := &idleBody{body: body, timeout: timeout, cancel: cancel}
	ib.timer = time.AfterFunc(timeout, func() {
		ib.timedOut.Store(true)
		cancel()
	})
	return ib
}

func (b *idleBody) Read(p []byte) (int, error) {
	n, err := b.body.Read(p)
	if err == nil {
		b.timer.Reset(b.timeout)
	}
	return n, err
}

func (b *idleBody) Close() error {
	var err error
	b.once.Do(func() {
		b.timer.Stop()
		err = b.body.Close()
		b.cancel()
	})
	return err
}

// upstreamDetail extracts a short, redacted error description from a response body.
func upstreamDetail(body []byte) string {
	if parsed, err := DecodeJSON(body); err == nil {
		if m, ok := asMap(parsed); ok {
			for _, key := range []string{"error", "message", "detail"} {
				value := m[key]
				if inner, ok := asMap(value); ok {
					if msg := inner["message"]; Truthy(msg) {
						value = msg
					} else {
						dumped, _ := json.Marshal(inner)
						value = string(dumped)
					}
				}
				if s, ok := nonEmptyString(value); ok {
					return redact.Bounded(s, detailLimit)
				}
			}
		}
	}
	text := strings.ReplaceAll(textutil.Strip(string(body)), "\n", " ")
	if text == "" {
		return "(empty body)"
	}
	return redact.Bounded(text, detailLimit)
}

// statusError maps any non-2xx status to a Kagi error (nil for 2xx).
func statusError(status int, body []byte, what string) *Error {
	e := func(kind Kind, format string, args ...any) *Error {
		err := newError(kind, fmt.Sprintf(format, args...))
		err.UpstreamStatus = status
		return err
	}
	switch {
	case status >= 200 && status < 300:
		return nil
	case status == 401 || status == 403:
		return e(KindAuth, "Kagi rejected the session (%s: HTTP %d)", what, status)
	case status >= 300 && status < 400:
		return e(KindAuth, "Kagi redirected %s (HTTP %d); session is likely expired", what, status)
	case status == 429:
		return e(KindRateLimit, "Kagi rate limit (%s): %s", what, upstreamDetail(body))
	case status >= 400 && status < 500:
		return e(KindBadRequest, "Kagi rejected %s (HTTP %d): %s", what, status, upstreamDetail(body))
	}
	return e(KindUpstream, "Kagi error on %s (HTTP %d): %s", what, status, upstreamDetail(body))
}

func isHTML(h http.Header) bool {
	return strings.Contains(strings.ToLower(h.Get("Content-Type")), "html")
}

// jsonBody returns the decoded JSON body, or a Kagi error for bad status, HTML, or non-JSON.
func (c *Client) jsonBody(resp *response, what string) (any, error) {
	defer resp.close()
	body, err := resp.readAll(what)
	if err != nil {
		return nil, err
	}
	if serr := statusError(resp.StatusCode, body, what); serr != nil {
		return nil, serr
	}
	if isHTML(resp.Header) || bytes.HasPrefix(bytes.TrimLeft(body, " \t\n\r\x0b\x0c"), []byte("<")) {
		err := newError(KindAuth, fmt.Sprintf("Kagi returned HTML instead of JSON for %s; session is likely expired", what))
		err.UpstreamStatus = resp.StatusCode
		return nil, err
	}
	parsed, jerr := DecodeJSON(body)
	if jerr != nil {
		err := newError(KindProtocol, fmt.Sprintf("Kagi returned non-JSON for %s", what))
		err.UpstreamStatus = resp.StatusCode
		return nil, err
	}
	return parsed, nil
}

// FetchInit is GET /api/init; it returns the JSON body (error unless 2xx JSON).
func (c *Client) FetchInit(ctx context.Context) (any, error) {
	resp, err := c.send(ctx, http.MethodGet, "/api/init", nil, "application/json")
	if err != nil {
		return nil, err
	}
	return c.jsonBody(resp, "/api/init")
}

// FetchCatalog fetches and parses the model catalog.
func (c *Client) FetchCatalog(ctx context.Context) (*Catalog, error) {
	obj, err := c.FetchInit(ctx)
	if err != nil {
		return nil, err
	}
	return ParseCatalog(obj)
}

type createBody struct {
	Content         string `json:"content"`
	Model           string `json:"model"`
	EnableSearch    bool   `json:"enable_search"`
	Personalization bool   `json:"personalization"`
}

// CreateTurn is POST /api/v2/conversations: create a thread and submit the prompt.
func (c *Client) CreateTurn(ctx context.Context, content, model string, enableSearch, personalization bool) (CreatedTurn, error) {
	const what = "POST /api/v2/conversations"
	resp, err := c.send(ctx, http.MethodPost, "/api/v2/conversations", createBody{content, model, enableSearch, personalization}, "application/json")
	if err != nil {
		return CreatedTurn{}, err
	}
	obj, err := c.jsonBody(resp, what)
	if err != nil {
		return CreatedTurn{}, err
	}
	root, _ := asMap(obj)
	conversation, _ := asMap(root["conversation"])
	uuid, ok := nonEmptyString(conversation["uuid"])
	if !ok {
		return CreatedTurn{}, newError(KindProtocol, "Kagi create-conversation response has no conversation.uuid")
	}
	turn, ok := nonEmptyString(root["assistant_turn_uuid"])
	if !ok {
		e := newError(KindProtocol, "Kagi create-conversation response has no assistant_turn_uuid")
		e.ConversationUUID = uuid
		return CreatedTurn{}, e
	}
	return CreatedTurn{ConversationUUID: uuid, AssistantTurnUUID: turn}, nil
}

// OpenTurnStream starts GET /api/v2/turns/{uuid}/stream and verifies its status. The
// stream lives as long as ctx; Close it when done.
func (c *Client) OpenTurnStream(ctx context.Context, assistantTurnUUID string) (*TurnStream, error) {
	resp, err := c.send(ctx, http.MethodGet, "/api/v2/turns/"+quote(assistantTurnUUID)+"/stream", nil, "text/event-stream")
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		defer resp.close()
		body, rerr := resp.readAll("turn stream")
		if rerr != nil {
			return nil, rerr
		}
		return nil, statusError(resp.StatusCode, body, "turn stream")
	}
	if isHTML(resp.Header) {
		resp.close()
		e := newError(KindAuth, "Kagi returned HTML instead of an event stream; session is likely expired")
		e.UpstreamStatus = resp.StatusCode
		return nil, e
	}
	return &TurnStream{resp: resp, reader: sse.NewReader(resp.Body)}, nil
}

// DeleteConversation is DELETE /api/conversations/{uuid}.
func (c *Client) DeleteConversation(ctx context.Context, conversationUUID string) error {
	path := "/api/conversations/" + quote(conversationUUID)
	what := "DELETE " + path
	resp, err := c.send(ctx, http.MethodDelete, path, nil, "application/json")
	if err != nil {
		return err
	}
	defer resp.close()
	body, err := resp.readAll(what)
	if err != nil {
		return err
	}
	if serr := statusError(resp.StatusCode, body, what); serr != nil {
		return serr
	}
	return nil
}

// quote is Python's urllib.parse.quote(s, safe=""): everything but unreserved
// characters is percent-encoded.
func quote(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		ch := s[i]
		if ch >= 'A' && ch <= 'Z' || ch >= 'a' && ch <= 'z' || ch >= '0' && ch <= '9' || strings.IndexByte("_.-~", ch) >= 0 {
			b.WriteByte(ch)
		} else {
			fmt.Fprintf(&b, "%%%02X", ch)
		}
	}
	return b.String()
}

// TurnStream is an open turn stream.
type TurnStream struct {
	resp   *response
	reader *sse.Reader
	done   bool
}

// NextFrame returns the next SSE frame's data payload; io.EOF at upstream EOF.
func (s *TurnStream) NextFrame() (string, error) {
	data, err := s.reader.Next()
	if err == nil || errors.Is(err, io.EOF) || errors.Is(err, sse.ErrFrameTooLarge) {
		return data, err
	}
	if s.resp.body.timedOut.Load() {
		return "", newError(KindTimeout, "Kagi stream read timed out (ReadTimeout)")
	}
	return "", newError(KindUnreachable, fmt.Sprintf("Kagi stream interrupted: %v", err))
}

// Next returns the events of the next frame; io.EOF at upstream EOF. An oversized frame
// is protocol drift and ends the stream.
func (s *TurnStream) Next() ([]events.Event, error) {
	if s.done {
		return nil, io.EOF
	}
	data, err := s.NextFrame()
	if errors.Is(err, sse.ErrFrameTooLarge) {
		s.done = true
		return []events.Event{drift(fmt.Sprintf("frame larger than %d bytes", sse.MaxFrameBytes))}, nil
	}
	if err != nil {
		return nil, err
	}
	return ParseFrame(data), nil
}

// Close closes the stream and cancels its request.
func (s *TurnStream) Close() { s.resp.close() }
