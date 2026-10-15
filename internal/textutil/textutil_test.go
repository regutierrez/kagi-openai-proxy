package textutil

import (
	"encoding/json"
	"testing"
)

func TestPythonWhitespaceAndLengths(t *testing.T) {
	if got := Strip("\x1c  hi 　\x1f"); got != "hi" {
		t.Fatalf("Strip = %q", got)
	}
	if got := LStrip("  a  "); got != "a  " {
		t.Fatalf("LStrip = %q", got)
	}
	if Len("héllo") != 5 || TruncateRunes("héllo", 2) != "hé" || TruncateRunes("hé", 9) != "hé" || TruncateRunes("x", 0) != "" {
		t.Fatal("character counting is byte-based")
	}
}

func TestRepr(t *testing.T) {
	for v, want := range map[any]string{nil: "None", true: "True", "it's": `"it's"`, "a'b\"c": `'a\'b"c'`, json.Number("7"): "7"} {
		if got := Repr(v); got != want {
			t.Fatalf("Repr(%#v) = %s, want %s", v, got, want)
		}
	}
	if got := Repr(map[string]any{"b": []any{1.5, nil}, "a": "x"}); got != "{'a': 'x', 'b': [1.5, None]}" {
		t.Fatalf("Repr(map) = %s", got)
	}
	if FormatFloat(1) != "1.0" || FormatFloat(0.00184) != "0.00184" {
		t.Fatal("FormatFloat differs from Python str(float)")
	}
}
