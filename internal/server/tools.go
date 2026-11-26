package server

import (
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"kagiopenaiproxy/internal/events"
	"kagiopenaiproxy/internal/logx"
	"kagiopenaiproxy/internal/translate"
)

// respondWithTools answers a chat request that carries tools. A TOOL_CALL marker is
// only recognizable in the complete answer, so the answer is buffered (streaming
// clients get keepalives meanwhile) and then emitted either as OpenAI tool_calls or
// as plain content.
func (p *Proxy) respondWithTools(w http.ResponseWriter, r *http.Request, runner *turnRunner, f translate.ChunkFormatter, stream, includeUsage bool, tools []translate.ToolSpec) {
	defer runner.consumerGone()
	flusher, _ := w.(http.Flusher)
	write := func(frame string) bool {
		if _, err := io.WriteString(w, frame); err != nil {
			return false // client gone
		}
		if flusher != nil {
			flusher.Flush()
		}
		return true
	}
	if stream {
		h := w.Header()
		h.Set("Content-Type", "text/event-stream; charset=utf-8")
		h.Set("Cache-Control", "no-cache")
		h.Set("X-Accel-Buffering", "no")
		w.WriteHeader(http.StatusOK)
		if !write(f.Role()) {
			return
		}
	}

	assembler := translate.NewAssembler()
	var parts []string
	var interrupted *translate.TurnError
	lastEmit := time.Now()
	for {
		var tick <-chan time.Time
		var timer *time.Timer
		if stream {
			timer = time.NewTimer(time.Until(lastEmit.Add(p.settings.Keepalive)))
			tick = timer.C
		}
		it, err := runner.queue.pop(r.Context(), tick)
		if timer != nil {
			timer.Stop()
		}
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
		parts = append(parts, assembler.Feed(it.(events.Event)))
		if assembler.Done() {
			break
		}
	}
	tail, terr := assembler.Finish(interrupted)
	parts = append(parts, tail)
	if terr != nil {
		logx.Warn("turn ended with error: %s (%s)", terr.Code(), terr.Message())
		if stream {
			write(f.Error(terr))
		} else {
			writeError(w, terr)
		}
		return
	}

	full := strings.Join(parts, "")
	res := translate.ParseToolCalls(full, tools)
	if res.Found && res.Err != nil {
		logx.Warn("tool-call marker present but unusable: %v; returning plain content", res.Err)
	}
	asCalls := res.Found && res.Err == nil
	if !stream {
		if asCalls {
			writeJSON(w, 200, translate.ToolCallsBody(f.ID, f.Created, f.Model, res.Preamble, res.Calls, assembler.Usage))
			return
		}
		writeJSON(w, 200, translate.CompletionBody(f.ID, f.Created, f.Model, full, assembler.Sources(), assembler.Usage))
		return
	}
	if asCalls {
		if res.Preamble != "" && !write(f.Content(res.Preamble)) {
			return
		}
		for i, c := range res.Calls {
			if !write(f.ToolCallChunk(i, c)) {
				return
			}
		}
		if !write(f.FinishToolCalls()) {
			return
		}
	} else {
		if full != "" && !write(f.Content(full)) {
			return
		}
		if !write(f.Finish()) {
			return
		}
	}
	if includeUsage && !write(f.Usage(assembler.Usage)) {
		return
	}
	write(translate.DoneFrame)
}
