// Package probe is the MANUAL live check of the Kagi Assistant wire protocol (see
// cmd/probe). It spends three cheap turns, reports the event sequences the proxy relies
// on, and always deletes the thread. Never run it automatically.
package probe

import (
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"regexp"
	"strings"
	"time"

	"kagiopenaiproxy/internal/events"
	"kagiopenaiproxy/internal/kagi"
	"kagiopenaiproxy/internal/redact"
)

// Prompt is the probe's fixed question.
const Prompt = "Reply with exactly: PONG"

// cheapHints match whole id tokens (split on - _ .), so "mini" does not match "gemini".
// Catalog entries carry no prices; these ids usually denote small/cheap models.
var cheapHints = [][]string{{"nano"}, {"mini"}, {"flash", "lite"}, {"haiku"}, {"lite"}, {"flash"}, {"small"}}

var tokenSplit = regexp.MustCompile(`[-_.]`)

// PickModel returns the cheapest-looking model, else the default ("" if none).
func PickModel(catalog *kagi.Catalog) string {
	for _, hint := range cheapHints {
		for _, m := range catalog.Models {
			tokens := map[string]bool{}
			for _, t := range tokenSplit.Split(strings.ToLower(m.ID), -1) {
				tokens[t] = true
			}
			all := true
			for _, h := range hint {
				all = all && tokens[h]
			}
			if all {
				return m.ID
			}
		}
	}
	return catalog.DefaultID
}

func eventOf(frame map[string]any) map[string]any {
	e, _ := frame["event"].(map[string]any)
	return e
}

func payloadOf(frame map[string]any) map[string]any {
	p, _ := eventOf(frame)["payload"].(map[string]any)
	return p
}

// str renders a JSON value for the probe's output.
func str(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return fmt.Sprint(v)
}

// AnswerText rebuilds the answer from the proxy's validated events only (malformed
// frames never reach here).
func AnswerText(evs []events.Event) string {
	textNodes := map[string]bool{}
	var b strings.Builder
	for _, e := range evs {
		switch ev := e.(type) {
		case events.NodeCreated:
			if ev.Kind == "text" {
				textNodes[ev.NodeID] = true
			}
		case events.TextDelta:
			if textNodes[ev.NodeID] {
				b.WriteString(ev.Text)
			}
		}
	}
	return b.String()
}

// MalformedFrames lists the problems the proxy itself would report as protocol drift.
func MalformedFrames(evs []events.Event) []string {
	var out []string
	for _, e := range evs {
		if f, ok := e.(events.TurnFailed); ok && f.Code == kagi.ProtocolDriftCode {
			out = append(out, f.Message)
		}
	}
	return out
}

type label struct {
	typ    string
	nodeID any
	kind   any
}

// CheckSequence returns the problems with the event order (empty = OK):
// turn.started -> node.created(text) -> node.delta+ -> turn.completed -> is_final.
func CheckSequence(frames []map[string]any) []string {
	var problems []string
	var labels []label
	for _, frame := range frames {
		if final, _ := frame["is_final"].(bool); final {
			labels = append(labels, label{typ: "is_final"})
			continue
		}
		event := eventOf(frame)
		labels = append(labels, label{str(event["type"]), event["node_id"], payloadOf(frame)["kind"]})
		if kagi.Truthy(frame["error"]) {
			problems = append(problems, "frame-level error: "+str(frame["error"]))
		}
	}
	first := func(pred func(label) bool, start int) int {
		for i := start; i < len(labels); i++ {
			if pred(labels[i]) {
				return i
			}
		}
		return -1
	}
	started := first(func(l label) bool { return l.typ == "turn.started" }, 0)
	if started < 0 {
		return append(problems, "no turn.started")
	}
	created := first(func(l label) bool { return l.typ == "node.created" && l.kind == "text" }, started)
	if created < 0 {
		return append(problems, "no node.created(kind=text) after turn.started")
	}
	textNode := labels[created].nodeID
	completed := first(func(l label) bool { return l.typ == "turn.completed" }, created)
	if completed < 0 {
		return append(problems, "no turn.completed after the text node was created")
	}
	deltas := 0
	for i := created; i < completed; i++ {
		if labels[i].typ == "node.delta" && reflect.DeepEqual(labels[i].nodeID, textNode) {
			deltas++
		}
	}
	if deltas == 0 {
		problems = append(problems, "no node.delta for the text node before turn.completed")
	}
	switch {
	case len(labels) == 0 || labels[len(labels)-1].typ != "is_final":
		problems = append(problems, "stream did not end with is_final")
	case first(func(l label) bool { return l.typ == "is_final" }, 0) != len(labels)-1:
		problems = append(problems, "is_final arrived before the end of the stream")
	}
	return problems
}

// CancelPrompt asks for a long answer, so the probe can cancel it mid-stream.
const CancelPrompt = "Count from 1 to 400, one number per line."

// turn is one read turn stream.
type turn struct {
	raw    []string
	frames []map[string]any
}

// readTurn reads a turn stream to its end. onFrame (may be nil) sees each frame.
func readTurn(ctx context.Context, client *kagi.Client, turnUUID string, onFrame func(map[string]any), say func(string, ...any)) (turn, error) {
	stream, err := client.OpenTurnStream(ctx, turnUUID)
	if err != nil {
		return turn{}, err
	}
	defer stream.Close()
	var t turn
	for {
		data, err := stream.NextFrame()
		if errors.Is(err, io.EOF) {
			return t, nil
		}
		if err != nil {
			return t, err
		}
		t.raw = append(t.raw, data)
		obj, jerr := kagi.DecodeJSON([]byte(data))
		if jerr != nil {
			// Redact first, then truncate, so a secret on the boundary leaves no prefix.
			say("  ! unparseable frame: %q", redact.Bounded(data, 120))
			continue
		}
		if m, ok := obj.(map[string]any); ok {
			t.frames = append(t.frames, m)
			if onFrame != nil {
				onFrame(m)
			}
		}
	}
}

// Run spends three cheap turns in one thread and always deletes the thread:
//  1. a new thread: the event sequence the proxy relies on;
//  2. a follow-up (POST /api/v2/branches/{uuid}/respond): the same sequence;
//  3. a long follow-up cancelled mid-answer (POST /api/v2/turns/{uuid}/cancel): the
//     stream must end with turn.cancelled and is_final.
//
// It returns the process exit status: 0 = protocol as expected, 1 = drift or error.
// Every line printed is redacted.
func Run(ctx context.Context, client *kagi.Client, model string, out io.Writer) (status int) {
	say := func(format string, args ...any) {
		fmt.Fprintln(out, redact.String(fmt.Sprintf(format, args...)))
	}
	var conversationUUID string
	defer func() {
		if conversationUUID == "" {
			return
		}
		if err := client.DeleteConversation(context.WithoutCancel(ctx), conversationUUID); err != nil {
			say("WARNING: failed to delete thread %s: %v", conversationUUID, err)
			return
		}
		say("deleted thread %s", conversationUUID)
	}()
	kagiError := func(err error) int {
		var kerr *kagi.Error
		if errors.As(err, &kerr) {
			say("Kagi error (%s): %s", kerr.Kind, kerr.Message)
		} else {
			say("error: %v", err)
		}
		return 1
	}
	drift := func(problems []string) int {
		say("PROTOCOL DRIFT:")
		for _, p := range problems {
			say("  - %s", p)
		}
		return 1
	}

	catalog, err := client.FetchCatalog(ctx)
	if err != nil {
		return kagiError(err)
	}
	if model == "" {
		model = PickModel(catalog)
	}
	if model == "" {
		say("probe: catalog has no usable models")
		return 1
	}
	say("catalog: %d usable models, default=%q; probing with %s", len(catalog.Models), catalog.DefaultID, model)

	created, err := client.CreateTurn(ctx, Prompt, model, kagi.TurnOptions{}) // search off, Kagi's default effort
	if err != nil {
		// A partially successful create still names the thread it made: delete it too.
		var kerr *kagi.Error
		if errors.As(err, &kerr) {
			conversationUUID = kerr.ConversationUUID
		}
		return kagiError(err)
	}
	conversationUUID = created.ConversationUUID
	say("conversation=%s branch=%s turn=%s", conversationUUID, created.BranchUUID, created.AssistantTurnUUID)

	// checkTurn reads a completed turn and reports its sequence problems.
	checkTurn := func(label, turnUUID string) ([]string, error) {
		t, err := readTurn(ctx, client, turnUUID, nil, say)
		if err != nil {
			return nil, err
		}
		var evs []events.Event
		for _, data := range t.raw {
			evs = append(evs, kagi.ParseFrame(data)...)
		}
		say("%s:", label)
		for _, f := range t.frames {
			line := "is_final"
			if _, isTitle := f["title"].(string); isTitle && eventOf(f) == nil {
				line = "title"
			} else if !kagi.Truthy(f["is_final"]) {
				line = str(eventOf(f)["type"])
				if kind := payloadOf(f)["kind"]; kagi.Truthy(kind) {
					line += "(" + str(kind) + ")"
				}
			}
			say("  %s", line)
		}
		answer := AnswerText(evs)
		say("  answer: %q", answer)
		if strings.TrimSpace(answer) != "PONG" {
			say("  note: answer is not exactly 'PONG' (model behaviour, not necessarily protocol drift)")
		}
		return append(MalformedFrames(evs), CheckSequence(t.frames)...), nil
	}

	problems, err := checkTurn("new thread", created.AssistantTurnUUID)
	if err != nil {
		return kagiError(err)
	}
	if len(problems) > 0 {
		return drift(problems)
	}
	if created.BranchUUID == "" {
		return drift([]string{"create-conversation response has no branch_uuid: follow-ups cannot work"})
	}

	followUp, err := client.Respond(ctx, created.BranchUUID, Prompt, model, kagi.TurnOptions{})
	if err != nil {
		return kagiError(err)
	}
	if problems, err = checkTurn("follow-up", followUp.AssistantTurnUUID); err != nil {
		return kagiError(err)
	}
	if len(problems) > 0 {
		return drift(problems)
	}

	long, err := client.Respond(ctx, created.BranchUUID, CancelPrompt, model, kagi.TurnOptions{})
	if err != nil {
		return kagiError(err)
	}
	var cancelledAt time.Time
	var cancelErr error
	t, err := readTurn(ctx, client, long.AssistantTurnUUID, func(f map[string]any) {
		if cancelledAt.IsZero() && eventOf(f)["type"] == "node.delta" {
			cancelledAt = time.Now()
			cancelErr = client.CancelTurn(ctx, long.AssistantTurnUUID)
		}
	}, say)
	if err != nil {
		return kagiError(err)
	}
	if cancelErr != nil {
		return kagiError(cancelErr)
	}
	sawCancelled := false
	for _, f := range t.frames {
		sawCancelled = sawCancelled || eventOf(f)["type"] == "turn.cancelled"
	}
	switch {
	case cancelledAt.IsZero():
		return drift([]string{"cancel check: the long answer produced no node.delta"})
	case !sawCancelled || !kagi.Truthy(t.frames[len(t.frames)-1]["is_final"]):
		return drift([]string{"cancel check: stream did not end with turn.cancelled then is_final"})
	}
	say("cancel: stream ended %s after POST .../cancel", time.Since(cancelledAt).Round(time.Millisecond))
	say("OK: new thread, follow-up and cancel match the proxy's expectations")
	return 0
}
