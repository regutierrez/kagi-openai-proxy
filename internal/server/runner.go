package server

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"kagiopenaiproxy/internal/events"
	"kagiopenaiproxy/internal/kagi"
	"kagiopenaiproxy/internal/logx"
	"kagiopenaiproxy/internal/textutil"
	"kagiopenaiproxy/internal/translate"
)

// streamEnd is the queue sentinel: the upstream stream is over (err set if it broke).
type streamEnd struct{ err *translate.TurnError }

// item is an events.Event or a streamEnd.
type item any

// eventQueue is an unbounded FIFO between the runner (producer) and the response
// (consumer). Pushing never blocks; once the consumer is gone, pushes are dropped.
type eventQueue struct {
	mu     sync.Mutex
	items  []item
	closed bool
	notify chan struct{}
}

func newEventQueue() *eventQueue { return &eventQueue{notify: make(chan struct{}, 1)} }

func (q *eventQueue) push(it item) {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return
	}
	q.items = append(q.items, it)
	q.mu.Unlock()
	select {
	case q.notify <- struct{}{}:
	default:
	}
}

func (q *eventQueue) close() {
	q.mu.Lock()
	q.closed = true
	q.items = nil
	q.mu.Unlock()
}

func (q *eventQueue) tryPop() (item, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.items) == 0 {
		return nil, false
	}
	it := q.items[0]
	q.items[0] = nil
	q.items = q.items[1:]
	return it, true
}

var errQueueTimeout = errors.New("queue wait timed out")

// pop waits for the next item until ctx is done or the timer channel fires
// (timeout may be nil for no timeout).
func (q *eventQueue) pop(ctx context.Context, timeout <-chan time.Time) (item, error) {
	for {
		if it, ok := q.tryPop(); ok {
			return it, nil
		}
		select {
		case <-q.notify:
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timeout:
			return nil, errQueueTimeout
		}
	}
}

// turnRunner owns one Kagi turn from an open stream to thread deletion and slot
// release. It runs in its own goroutine; the response only reads its queue, so a
// client disconnect can neither leak the slot nor abort the upstream read.
type turnRunner struct {
	queue            *eventQueue
	kagi             *kagi.Client
	stream           *kagi.TurnStream
	cancelUpstream   context.CancelFunc
	deleteCtx        context.Context
	conversationUUID string
	model            string
	deleteThread     bool
	drainTimeout     time.Duration
	release          func()

	mu              sync.Mutex
	consumerPresent bool
	reading         bool
	turnCompleted   bool
	drainTimer      *time.Timer
	drainExpired    atomic.Bool
}

// consumerGone means the response is finished or abandoned: bound the remaining drain.
// Idempotent.
func (r *turnRunner) consumerGone() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.consumerPresent {
		return
	}
	r.consumerPresent = false
	r.queue.close()
	if r.reading && !r.turnCompleted {
		// Normal completions end the response at turn.completed; only log early departures.
		logx.Info("conversation %s: response ended before the Kagi turn did; draining upstream in background (max %.0fs)", r.conversationUUID, r.drainTimeout.Seconds())
	}
	if r.reading {
		r.drainTimer = time.AfterFunc(r.drainTimeout, func() {
			r.drainExpired.Store(true)
			r.cancelUpstream()
		})
	}
}

func (r *turnRunner) run() {
	defer r.release()
	endErr := r.read()

	r.mu.Lock()
	r.reading = false
	if r.drainTimer != nil {
		r.drainTimer.Stop()
	}
	r.mu.Unlock()

	r.queue.push(streamEnd{endErr})
	r.stream.Close()
	r.cancelUpstream()
	if r.deleteThread {
		deleteThreadQuietly(r.deleteCtx, r.kagi, r.conversationUUID)
	}
}

// read consumes the upstream until is_final, EOF, an error, or the drain bound.
func (r *turnRunner) read() *translate.TurnError {
	for {
		evs, err := r.stream.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			if r.drainExpired.Load() {
				logx.Warn("conversation %s: client gone and drain exceeded %.0fs; abandoning upstream stream", r.conversationUUID, r.drainTimeout.Seconds())
				return translate.NewTurnError(504, "api_error", "kagi_drain_timeout", "upstream drain timed out")
			}
			var kerr *kagi.Error
			if errors.As(err, &kerr) {
				logx.Warn("conversation %s: upstream stream broke: %s", r.conversationUUID, kerr.Message)
				code := "kagi_stream_interrupted"
				if kerr.Kind == kagi.KindTimeout {
					code = "kagi_stream_timeout"
				}
				return translate.NewTurnError(502, "api_error", code, kerr.Message)
			}
			logx.Error("conversation %s: unexpected error reading stream: %v", r.conversationUUID, err)
			return translate.NewTurnError(500, "api_error", "proxy_internal_error", "internal error while reading the Kagi stream")
		}
		for _, ev := range evs {
			if done, ok := ev.(events.TurnCompleted); ok {
				r.mu.Lock()
				r.turnCompleted = true
				r.mu.Unlock()
				r.logUsage(done.Usage)
			}
			r.queue.push(ev)
			if _, final := ev.(events.Final); final {
				return nil
			}
		}
	}
}

func (r *turnRunner) logUsage(u *events.Usage) {
	if u == nil {
		logx.Info("turn completed: model=%s (no usage reported)", r.model)
		return
	}
	version, cost := "None", "None"
	if u.ModelVersion != nil {
		version = *u.ModelVersion
	}
	if u.CostUSD != nil {
		cost = textutil.FormatFloat(*u.CostUSD)
	}
	logx.Info("turn completed: model=%s model_version=%s input_tokens=%d output_tokens=%d cost_usd=%s",
		r.model, version, u.InputTokens, u.OutputTokens, cost)
}

func deleteThreadQuietly(ctx context.Context, client *kagi.Client, conversationUUID string) {
	if err := client.DeleteConversation(ctx, conversationUUID); err != nil {
		logx.Warn("failed to delete Kagi thread %s: %v", conversationUUID, err)
		return
	}
	logx.Info("deleted Kagi thread %s", conversationUUID)
}
