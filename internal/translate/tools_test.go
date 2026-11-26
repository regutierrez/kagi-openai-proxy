package translate

import (
	"encoding/json"
	"strings"
	"testing"
)

var calcTools = []ToolSpec{{Name: "calculator", Description: "Evaluate an arithmetic expression"}}

func TestParseToolCalls(t *testing.T) {
	cases := []struct {
		name    string
		text    string
		wantErr bool
		wantN   int
	}{
		{"plain", "Just an answer.", false, 0},
		{"single", "TOOL_CALL:{\"name\":\"calculator\",\"arguments\":{\"expression\":\"6*7\"}}", false, 1},
		{"two lines", "TOOL_CALL:{\"name\":\"calculator\",\"arguments\":{}}\nTOOL_CALL:{\"name\":\"calculator\",\"arguments\":{}}", false, 2},
		{"multi", "TOOL_CALLS:[{\"name\":\"calculator\",\"arguments\":{}},{\"name\":\"calculator\",\"arguments\":{}}]", false, 2},
		{"unknown tool", "TOOL_CALL:{\"name\":\"nope\",\"arguments\":{}}", true, 0},
		{"bad json", "TOOL_CALL:{not json", true, 0},
		{"args not object", "TOOL_CALL:{\"name\":\"calculator\",\"arguments\":\"6*7\"}", true, 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			res := ParseToolCalls(c.text, calcTools)
			if (res.Err != nil) != c.wantErr {
				t.Fatalf("err = %v, wantErr %v", res.Err, c.wantErr)
			}
			if len(res.Calls) != c.wantN {
				t.Fatalf("got %d calls, want %d", len(res.Calls), c.wantN)
			}
			for _, call := range res.Calls {
				var args map[string]any
				if err := json.Unmarshal([]byte(call.Function.Arguments), &args); err != nil {
					t.Fatalf("arguments not valid JSON: %v", err)
				}
			}
		})
	}
}

func TestAssistantToolCallRendering(t *testing.T) {
	raw := `{"messages":[{"role":"user","content":"hi"},{"role":"assistant","content":null,"tool_calls":[{"id":"c1","type":"function","function":{"name":"calculator","arguments":"{\"expression\":\"6*7\"}"}}]},{"role":"tool","content":"42"}]}`
	body, err := DecodeBody([]byte(raw))
	if err != nil {
		t.Fatal(err)
	}
	req, terr := ParseChatRequest(body)
	if terr != nil {
		t.Fatal(terr)
	}
	if got := req.Messages[1].Content; !strings.Contains(got, "called tool \"calculator\"") {
		t.Fatalf("assistant tool call not rendered: %q", got)
	}
}

func TestToolPrompt(t *testing.T) {
	p := ToolPrompt(calcTools)
	if !strings.Contains(p, "TOOL_CALL:") || !strings.Contains(p, "calculator") {
		t.Fatalf("prompt missing expected pieces:\n%s", p)
	}
}
