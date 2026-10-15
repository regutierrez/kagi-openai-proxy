package citations

import (
	"slices"
	"strings"
	"testing"
)

func rewriter(refs ...string) *Rewriter {
	r := New()
	for _, ref := range refs {
		r.AddReference(ref, map[string]any{"url": "https://example.org/" + ref})
	}
	return r
}

// run feeds each delta, then flushes; it returns every output in order.
func run(r *Rewriter, deltas ...string) []string {
	var out []string
	for _, d := range deltas {
		out = append(out, r.Feed(d))
	}
	return append(out, r.Flush())
}

func eq(t *testing.T, got, want []string) {
	t.Helper()
	if !slices.Equal(got, want) {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestRenumberedByFirstCitationOrder(t *testing.T) {
	r := rewriter("a", "b", "c")
	if got := r.Feed("x[^b] y[^a] z[^b] w[^c]"); got != "x[^1] y[^2] z[^1] w[^3]" {
		t.Fatalf("got %q", got)
	}
	eq(t, r.Cited(), []string{"b", "a", "c"})
}

func TestMarkerSplitAcrossDeltas(t *testing.T) {
	eq(t, run(rewriter("r_b2"), "Paris [^r", "_b", "2]. Next"), []string{"Paris ", "", "[^1]. Next", ""})
}

func TestMarkerSplitBetweenBracketAndCaret(t *testing.T) {
	eq(t, run(rewriter("r1"), "Paris [", "^r1] ok"), []string{"Paris ", "[^1] ok", ""})
	// a trailing "[" that turns out not to be a marker is released unchanged
	eq(t, run(rewriter("r1"), "array[", "0]"), []string{"array", "[0]", ""})
}

func TestUnknownRefPassesThroughUnchanged(t *testing.T) {
	r := rewriter("known")
	if got := r.Feed("a[^mystery] b[^known] c[^mystery#x]"); got != "a[^mystery] b[^1] c[^mystery#x]" {
		t.Fatalf("got %q", got)
	}
	eq(t, r.Cited(), []string{"known"})
}

func TestHashSuffixedRefIDsUsePartBeforeHash(t *testing.T) {
	r := rewriter("r1", "r2")
	if got := r.Feed("[^r2#chunk-3] and [^r1#p1] and [^r2#chunk-9]"); got != "[^1] and [^2] and [^1]" {
		t.Fatalf("got %q", got)
	}
}

func TestHeldTailCappedAt64CharsThenFlushedRaw(t *testing.T) {
	r := rewriter("r1")
	held := "[^" + strings.Repeat("x", MaxHeld-2) // exactly 64 chars: still held
	if got := r.Feed("a " + held); got != "a " {
		t.Fatalf("got %q", got)
	}
	if got := r.Feed("y"); got != held+"y" { // 65 chars: released raw
		t.Fatalf("got %q", got)
	}
	if got := r.Flush(); got != "" {
		t.Fatalf("flush = %q", got)
	}
	// a real marker after an over-long non-marker is still rewritten
	out := strings.Join(run(rewriter("r1"), "[^"+strings.Repeat("z", 70)+" then [^r1]"), "")
	if want := "[^" + strings.Repeat("z", 70) + " then [^1]"; out != want {
		t.Fatalf("got %q, want %q", out, want)
	}
}

func TestHeldTailCapCountsCharactersNotBytes(t *testing.T) {
	r := rewriter("r1")
	held := "[^" + strings.Repeat("é", MaxHeld-2) // 64 chars, 126 bytes: still held
	if got := r.Feed(held); got != "" {
		t.Fatalf("got %q", got)
	}
}

func TestUnclosedMarkerFlushedRawAtEndOfStream(t *testing.T) {
	eq(t, run(rewriter("r1"), "trailing [^r1"), []string{"trailing ", "[^r1"})
}

func TestStrayOpenerBeforeRealMarker(t *testing.T) {
	if got := strings.Join(run(rewriter("r1"), "x [^not ", "a marker [^r1] y"), ""); got != "x [^not a marker [^1] y" {
		t.Fatalf("got %q", got)
	}
}
