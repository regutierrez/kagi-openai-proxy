// Package events defines the internal turn events. Package kagi produces them; nothing
// else ever sees Kagi JSON.
package events

// Event is one of NodeCreated, NodeUpdated, TextDelta, ReferenceAdded, Title,
// TurnCompleted, TurnFailed, Final, Unknown.
type Event interface {
	// Name is the event's type name, used in diagnostics (e.g. the empty-answer guard).
	Name() string
}

// Usage is the token accounting reported by turn.completed.
type Usage struct {
	InputTokens  int
	OutputTokens int
	CostUSD      *float64
	ModelVersion *string
}

// NodeCreated announces a node. Kind is "text" (the answer), "activity" (search,
// thinking, and other steps), "assistant_message" (the turn's root), or anything new.
// For activity nodes, Type is "tool_call", "thinking", "config", ...; ToolName is
// e.g. "search"; Content is the node's text so far (a thinking summary).
type NodeCreated struct{ NodeID, Kind, Type, ToolName, Content string }

// NodeUpdated is node.updated or node.completed: new status, the search query once
// known, and the node's full text on completion. Empty fields did not change.
type NodeUpdated struct{ NodeID, Status, ToolName, Query, Content string }

// TextDelta is incremental markdown for one node of any kind (the consumer filters by kind).
type TextDelta struct{ NodeID, Text string }

// ReferenceAdded registers a citation source. Fields other than RefID may be "".
type ReferenceAdded struct {
	RefID, URL, Title, Snippet, Favicon, PublishedAt string
}

// Title is Kagi's generated title for the thread.
type Title struct{ Text string }

// TurnCompleted ends the answer; Usage is nil when Kagi reported none.
type TurnCompleted struct{ Usage *Usage }

// TurnFailed is a failed or cancelled turn, a frame-level error, or protocol drift.
type TurnFailed struct {
	Code    string
	Message string
	// RateLimited is true for quota / rate-limit conditions (decided by package kagi).
	RateLimited bool
}

// Final is the terminal is_final frame.
type Final struct{}

// Unknown is a well-formed event the proxy does not use.
type Unknown struct{ Type string }

func (NodeCreated) Name() string    { return "NodeCreated" }
func (NodeUpdated) Name() string    { return "NodeUpdated" }
func (Title) Name() string          { return "Title" }
func (TextDelta) Name() string      { return "TextDelta" }
func (ReferenceAdded) Name() string { return "ReferenceAdded" }
func (TurnCompleted) Name() string  { return "TurnCompleted" }
func (TurnFailed) Name() string     { return "TurnFailed" }
func (Final) Name() string          { return "Final" }
func (u Unknown) Name() string      { return "Unknown(" + u.Type + ")" }
