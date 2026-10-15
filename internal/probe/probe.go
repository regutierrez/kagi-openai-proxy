// Package probe is the MANUAL live check of the Kagi Assistant wire protocol (see
// cmd/probe). It spends one cheap turn, reports the event sequence the proxy relies on,
// and always deletes the thread. Never run it automatically.
package probe

import (
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"regexp"
	"strings"

	"kagiopenaiproxy/internal/events"
	"kagiopenaiproxy/internal/kagi"
	"kagiopenaiproxy/internal/redact"
	"kagiopenaiproxy/internal/textutil"
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

// pyStr is Python's str() for a JSON value.
func pyStr(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return textutil.Repr(v)
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
		labels = append(labels, label{pyStr(event["type"]), event["node_id"], payloadOf(frame)["kind"]})
		if kagi.Truthy(frame["error"]) {
			problems = append(problems, "frame-level error: "+textutil.Repr(frame["error"]))
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

// Run spends one turn, prints the event sequence to out, and always deletes the thread.
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
	defaultID := catalog.DefaultID
	if defaultID == "" {
		defaultID = "None"
	}
	say("catalog: %d usable models, default=%s; probing with %s", len(catalog.Models), defaultID, model)

	created, err := client.CreateTurn(ctx, Prompt, model, false, false)
	if err != nil {
		// A partially successful create still names the thread it made: delete it too.
		var kerr *kagi.Error
		if errors.As(err, &kerr) {
			conversationUUID = kerr.ConversationUUID
		}
		return kagiError(err)
	}
	conversationUUID = created.ConversationUUID
	say("conversation=%s turn=%s", conversationUUID, created.AssistantTurnUUID)

	stream, err := client.OpenTurnStream(ctx, created.AssistantTurnUUID)
	if err != nil {
		return kagiError(err)
	}
	var raw []string
	var frames []map[string]any
	for {
		data, err := stream.NextFrame()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			stream.Close()
			return kagiError(err)
		}
		raw = append(raw, data)
		obj, jerr := kagi.DecodeJSON([]byte(data))
		if jerr != nil {
			// Redact first, then truncate, so a secret on the boundary leaves no prefix.
			say("  ! unparseable frame: %s", textutil.Repr(redact.Bounded(data, 120)))
			continue
		}
		if m, ok := obj.(map[string]any); ok {
			frames = append(frames, m)
		}
	}
	stream.Close()

	var evs []events.Event
	for _, data := range raw {
		evs = append(evs, kagi.ParseFrame(data)...)
	}
	answer := AnswerText(evs)
	for _, f := range frames {
		line := "is_final"
		if !kagi.Truthy(f["is_final"]) {
			line = pyStr(eventOf(f)["type"])
			if kind := payloadOf(f)["kind"]; kagi.Truthy(kind) {
				line += "(" + pyStr(kind) + ")"
			}
		}
		say("  %s", line)
	}
	var usage any
	for _, f := range frames {
		if eventOf(f)["type"] == "turn.completed" {
			usage = payloadOf(f)["usage"]
			break
		}
	}
	say("answer: %s", textutil.Repr(answer))
	say("usage: %s", textutil.Repr(usage))
	if textutil.Strip(answer) != "PONG" {
		say("note: answer is not exactly 'PONG' (model behaviour, not necessarily protocol drift)")
	}

	problems := append(MalformedFrames(evs), CheckSequence(frames)...)
	if len(problems) > 0 {
		say("PROTOCOL DRIFT:")
		for _, p := range problems {
			say("  - %s", p)
		}
		return 1
	}
	say("OK: event sequence matches the proxy's expectations")
	return 0
}
