package kagi

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"

	"kagiopenaiproxy/internal/events"
	"kagiopenaiproxy/internal/logx"
	"kagiopenaiproxy/internal/redact"
	"kagiopenaiproxy/internal/textutil"
)

// ProtocolDriftCode marks malformed protocol data.
const ProtocolDriftCode = "kagi_protocol_drift"

// Substrings of a turn.failed error_code (lower-cased) that indicate quota / rate
// limiting. The real code values are undocumented; this list is a best guess.
var quotaHints = []string{"quota", "rate_limit", "rate-limit", "ratelimit", "rate_limited", "too_many", "limit_exceeded", "usage_limit"}

// Known event types the proxy deliberately ignores (not logged as unknown).
var ignoredEventTypes = map[string]bool{"turn.started": true}

var (
	loggedUnknownMu sync.Mutex
	loggedUnknown   = map[string]bool{}
)

// IsQuotaCode reports whether a failure code (or message) looks like a quota condition.
func IsQuotaCode(code string) bool {
	lowered := strings.ToLower(code)
	for _, hint := range quotaHints {
		if strings.Contains(lowered, hint) {
			return true
		}
	}
	return false
}

// DecodeJSON parses one JSON value, keeping numbers as json.Number so ints and floats
// stay distinguishable. Trailing data is an error.
func DecodeJSON(data []byte) (any, error) {
	dec := json.NewDecoder(strings.NewReader(string(data)))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, err
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return nil, errors.New("trailing data after JSON value")
	}
	return v, nil
}

// pyType names a JSON value's Python type, for diagnostics.
func pyType(v any) string {
	switch x := v.(type) {
	case nil:
		return "NoneType"
	case bool:
		return "bool"
	case string:
		return "str"
	case json.Number:
		if isIntLiteral(x) {
			return "int"
		}
		return "float"
	case []any:
		return "list"
	case map[string]any:
		return "dict"
	}
	return fmt.Sprintf("%T", v)
}

func isIntLiteral(n json.Number) bool { return !strings.ContainsAny(n.String(), ".eE") }

// Truthy is Python truthiness for JSON values.
func Truthy(v any) bool {
	switch x := v.(type) {
	case nil:
		return false
	case bool:
		return x
	case string:
		return x != ""
	case json.Number:
		f, err := x.Float64()
		return err != nil || f != 0
	case []any:
		return len(x) > 0
	case map[string]any:
		return len(x) > 0
	}
	return true
}

func asMap(v any) (map[string]any, bool) {
	m, ok := v.(map[string]any)
	return m, ok
}

func nonEmptyString(v any) (string, bool) {
	s, ok := v.(string)
	return s, ok && s != ""
}

// ParseFrame translates one SSE frame's data payload into internal events.
func ParseFrame(data string) []events.Event {
	parsed, err := DecodeJSON([]byte(data))
	if err != nil {
		return []events.Event{drift(fmt.Sprintf("unparseable frame (%d chars)", textutil.Len(data)))}
	}
	obj, ok := asMap(parsed)
	if !ok {
		return []events.Event{drift(fmt.Sprintf("frame is a JSON %s, not an object", pyType(parsed)))}
	}
	var out []events.Event
	if event, present := obj["event"]; present { // absent key = e.g. a title-only frame
		if m, isObject := asMap(event); isObject {
			out = append(out, parseEvent(m))
		} else {
			kind := pyType(event)
			if event == nil {
				kind = "null"
			}
			out = append(out, drift(fmt.Sprintf("'event' is %s, not an object", kind)))
		}
	}
	if frameErr := obj["error"]; Truthy(frameErr) {
		// A same-frame error wins over completion: never expose success for a failed turn.
		kept := out[:0]
		for _, e := range out {
			if _, completed := e.(events.TurnCompleted); !completed {
				kept = append(kept, e)
			}
		}
		out = append(kept, frameError(frameErr))
	}
	if final, _ := obj["is_final"].(bool); final {
		out = append(out, events.Final{})
	}
	if len(out) == 0 {
		// Well-formed object with no event (e.g. a bare title update): ignore.
		out = append(out, unknown("frame_without_event"))
	}
	return out
}

// drift is malformed protocol data: a kagi_protocol_drift failure, never ignored.
func drift(reason string) events.TurnFailed {
	message := "Kagi stream protocol drift: " + reason
	logx.Warn("%s", message)
	return events.TurnFailed{Code: ProtocolDriftCode, Message: message}
}

// unknown is well-formed but unrecognized: ignored, logged once per process per type.
func unknown(kind string) events.Unknown {
	if !ignoredEventTypes[kind] {
		loggedUnknownMu.Lock()
		first := !loggedUnknown[kind]
		loggedUnknown[kind] = true
		loggedUnknownMu.Unlock()
		if first {
			logx.Info("Kagi stream: ignoring unknown event type %s (logged once)", textutil.Repr(kind))
		}
	}
	return events.Unknown{Type: kind}
}

func nodeID(event map[string]any) (string, bool) {
	switch v := event["node_id"].(type) {
	case json.Number:
		if isIntLiteral(v) {
			return v.String(), true
		}
	case string:
		return v, v != ""
	}
	return "", false
}

func parseEvent(event map[string]any) events.Event {
	kind, ok := nonEmptyString(event["type"])
	if !ok {
		return drift("event without a string 'type'")
	}
	payload, ok := asMap(event["payload"])
	if !ok {
		payload = map[string]any{}
	}
	id, hasID := nodeID(event)
	switch kind {
	case "node.created":
		nodeKind, isString := payload["kind"].(string)
		if !hasID || !isString {
			return drift("node.created without node_id or payload.kind")
		}
		return events.NodeCreated{NodeID: id, Kind: nodeKind}
	case "node.delta":
		delta, isString := payload["delta"].(string)
		if !hasID || !isString {
			return drift("node.delta without node_id or payload.delta")
		}
		return events.TextDelta{NodeID: id, Text: delta}
	case "reference.added":
		refID, ok := nonEmptyString(payload["ref_id"])
		if !ok {
			return drift("reference.added without payload.ref_id")
		}
		copied := make(map[string]any, len(payload))
		for k, v := range payload {
			copied[k] = v
		}
		return events.ReferenceAdded{RefID: refID, Payload: copied}
	case "turn.completed":
		// Usage is informational; a missing/odd usage block does not invalidate the answer.
		return events.TurnCompleted{Usage: parseUsage(payload["usage"])}
	case "turn.failed", "turn.cancelled":
		code, ok := nonEmptyString(payload["error_code"])
		if !ok {
			return drift(kind + " without payload.error_code")
		}
		raw := payload["message"]
		if !Truthy(raw) {
			raw = payload["error"]
		}
		message, ok := nonEmptyString(raw)
		if !ok {
			message = fmt.Sprintf("Kagi %s (%s)", kind, code)
		}
		return events.TurnFailed{Code: code, Message: redact.String(message), RateLimited: IsQuotaCode(code)}
	}
	return unknown(kind)
}

func frameError(raw any) events.TurnFailed {
	code := "kagi_stream_error"
	var message string
	switch v := raw.(type) {
	case map[string]any:
		for _, key := range []string{"error_code", "code"} {
			if c, ok := nonEmptyString(v[key]); ok {
				code = c
				break
			}
		}
		message, _ = v["message"].(string)
	case string:
		message = v
	}
	if message == "" {
		dumped, _ := json.Marshal(raw)
		message = "Kagi stream error: " + redact.Bounded(string(dumped), detailLimit)
	}
	return events.TurnFailed{Code: code, Message: redact.String(message), RateLimited: IsQuotaCode(code) || IsQuotaCode(message)}
}

func parseUsage(raw any) *events.Usage {
	m, ok := asMap(raw)
	if !ok {
		return nil
	}
	asInt := func(v any) int {
		if n, ok := v.(json.Number); ok && isIntLiteral(n) {
			if i, err := strconv.Atoi(n.String()); err == nil {
				return i
			}
		}
		return 0
	}
	usage := &events.Usage{InputTokens: asInt(m["input_tokens"]), OutputTokens: asInt(m["output_tokens"])}
	if n, ok := m["cost_usd"].(json.Number); ok {
		if f, err := n.Float64(); err == nil {
			usage.CostUSD = &f
		}
	}
	if s, ok := m["model_version"].(string); ok {
		usage.ModelVersion = &s
	}
	return usage
}

// ModelInfo is one usable catalog entry.
type ModelInfo struct {
	ID            string
	Provider      string // "" when absent
	DisplayName   string
	ContextWindow int // 0 when unknown
}

// Catalog is the filtered /api/init model list.
type Catalog struct {
	Models    []ModelInfo
	DefaultID string // "" when none
}

// Get returns the model with the given id.
func (c *Catalog) Get(id string) (ModelInfo, bool) {
	for _, m := range c.Models {
		if m.ID == id {
			return m, true
		}
	}
	return ModelInfo{}, false
}

// ParseCatalog filters /api/init's models.models to supported, non-deprecated,
// non-retired entries and finds the default model id.
func ParseCatalog(obj any) (*Catalog, error) {
	root, _ := asMap(obj)
	section, _ := asMap(root["models"])
	items, ok := section["models"].([]any)
	if !ok {
		return nil, newError(KindProtocol, "Kagi /api/init response has no models.models list")
	}
	catalog := &Catalog{}
	for _, raw := range items {
		item, ok := asMap(raw)
		if !ok {
			continue
		}
		id, ok := item["id"].(string)
		if !ok {
			continue
		}
		if supported, _ := item["supported"].(bool); !supported || Truthy(item["deprecated"]) || Truthy(item["retired"]) {
			continue
		}
		info := ModelInfo{ID: id}
		info.Provider, _ = item["provider"].(string)
		info.DisplayName, _ = item["display_name"].(string)
		if n, ok := item["context_window"].(json.Number); ok && isIntLiteral(n) {
			if cw, err := strconv.Atoi(n.String()); err == nil && cw > 0 {
				info.ContextWindow = cw
			}
		}
		catalog.Models = append(catalog.Models, info)
	}
	catalog.DefaultID = findDefaultModel(root, section)
	if catalog.DefaultID == "" && len(catalog.Models) > 0 {
		catalog.DefaultID = catalog.Models[0].ID
		logx.Warn("Kagi catalog has no recognizable default model id; using %s", catalog.DefaultID)
	}
	return catalog, nil
}

// findDefaultModel tries the plausible spellings in the models section, then the top level.
func findDefaultModel(root, section map[string]any) string {
	for _, container := range []map[string]any{section, root} {
		for _, key := range []string{"default", "default_model", "default_model_id", "default_id"} {
			value := container[key]
			if m, ok := asMap(value); ok {
				value = m["id"]
			}
			if s, ok := nonEmptyString(value); ok {
				return s
			}
		}
	}
	return ""
}
