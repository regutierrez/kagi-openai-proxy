// Package redact is the single choke point that removes secrets from any string
// leaving the process: log lines, error bodies, error chunks, probe output.
package redact

import (
	"regexp"
	"sort"
	"strings"
	"sync"

	"kagiopenaiproxy/internal/textutil"
)

// Marker replaces every removed secret.
const Marker = "[REDACTED]"

var (
	mu      sync.RWMutex
	secrets = map[string]struct{}{}

	// Belt and braces: scrub anything shaped like a credential even if never registered.
	credentialPatterns = []*regexp.Regexp{
		regexp.MustCompile(`(kagi_session=)[^;\s"']+`),
		regexp.MustCompile(`(?i)(Bearer\s+)[^\s"']+`),
	}
)

// RegisterSecret adds a value that must never appear in logs or error messages.
func RegisterSecret(value string) {
	if value == "" {
		return
	}
	mu.Lock()
	secrets[value] = struct{}{}
	mu.Unlock()
}

// String returns s with every registered secret and credential pattern removed.
func String(s string) string {
	mu.RLock()
	ordered := make([]string, 0, len(secrets))
	for secret := range secrets {
		ordered = append(ordered, secret)
	}
	mu.RUnlock()
	// Longest first, so a secret that contains another secret is fully removed.
	sort.Slice(ordered, func(i, j int) bool { return len(ordered[i]) > len(ordered[j]) })
	out := s
	for _, secret := range ordered {
		out = strings.ReplaceAll(out, secret, Marker)
	}
	for _, pattern := range credentialPatterns {
		out = pattern.ReplaceAllString(out, "${1}"+Marker)
	}
	return out
}

// Bounded redacts first, then truncates to limit characters: truncating first could cut
// a secret in half and leak its prefix.
func Bounded(s string, limit int) string {
	return textutil.TruncateRunes(String(s), limit)
}
