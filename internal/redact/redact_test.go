package redact

import (
	"strings"
	"testing"
)

func TestRegisteredSecretsAndCredentialPatterns(t *testing.T) {
	RegisterSecret("abc-secret")
	RegisterSecret("abc-secret-longer") // longest first: no partial leftover
	RegisterSecret("")                  // ignored
	got := String("x abc-secret-longer y abc-secret kagi_session=zzz; Bearer qqq bearer   rrr")
	want := "x [REDACTED] y [REDACTED] kagi_session=[REDACTED]; Bearer [REDACTED] bearer   [REDACTED]"
	if got != want {
		t.Fatalf("got  %q\nwant %q", got, want)
	}
}

func TestBoundedRedactsBeforeTruncating(t *testing.T) {
	RegisterSecret("straddling-secret")
	got := Bounded(strings.Repeat("x", 8)+"straddling-secret", 12)
	if got != "xxxxxxxx[RED" || strings.Contains(got, "stra") {
		t.Fatalf("got %q", got)
	}
}
