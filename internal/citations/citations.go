// Package citations rewrites Kagi citation markers [^ref_id] to [^n] while streaming.
//
// n is the 1-based order in which a known reference is first cited. Unknown ref_ids pass
// through unchanged. An opened-but-unclosed marker is held back (up to MaxHeld
// characters) so markers split across deltas are rewritten correctly.
package citations

import (
	"strconv"
	"strings"

	"kagiopenaiproxy/internal/textutil"
)

// MaxHeld caps the held tail of an unclosed marker; longer tails are released raw.
const MaxHeld = 64

const open = "[^"

// Rewriter holds the citation state of one answer.
type Rewriter struct {
	references map[string]map[string]any
	cited      []string
	held       string
}

// New returns an empty Rewriter.
func New() *Rewriter { return &Rewriter{references: map[string]map[string]any{}} }

// AddReference registers a citation source.
func (r *Rewriter) AddReference(refID string, payload map[string]any) {
	r.references[baseID(refID)] = payload
}

// Cited returns the ref_ids in order of first citation.
func (r *Rewriter) Cited() []string { return append([]string(nil), r.cited...) }

// Feed returns the text that can be emitted now and holds back any incomplete marker.
func (r *Rewriter) Feed(text string) string {
	s := r.held + text
	r.held = ""
	var out strings.Builder
	i := 0
	for {
		j := strings.Index(s[i:], open)
		if j == -1 {
			rest := s[i:]
			// A lone trailing "[" may be the first half of a split "[^".
			if strings.HasSuffix(rest, "[") {
				out.WriteString(rest[:len(rest)-1])
				r.held = "["
			} else {
				out.WriteString(rest)
			}
			break
		}
		j += i
		out.WriteString(s[i:j])
		closeIdx := strings.Index(s[j+len(open):], "]")
		if closeIdx == -1 {
			tail := s[j:]
			if textutil.Len(tail) > MaxHeld {
				// Too long to be a marker: release the opener raw and keep scanning
				// (a later "[^" in the tail is still eligible).
				out.WriteString(open)
				i = j + len(open)
				continue
			}
			r.held = tail
			break
		}
		closeIdx += j + len(open)
		marker := s[j : closeIdx+1]
		if strings.Contains(marker[len(open):], "[") {
			// Another "[" opens before this one closes: this opener is not a marker.
			out.WriteString(open)
			i = j + len(open)
			continue
		}
		out.WriteString(r.rewrite(marker))
		i = closeIdx + 1
	}
	return out.String()
}

// Flush releases any held text raw (end of stream).
func (r *Rewriter) Flush() string {
	held := r.held
	r.held = ""
	return held
}

func (r *Rewriter) rewrite(marker string) string {
	ref := baseID(marker[len(open) : len(marker)-1])
	if ref == "" {
		return marker
	}
	if _, known := r.references[ref]; !known {
		return marker
	}
	for n, cited := range r.cited {
		if cited == ref {
			return "[^" + strconv.Itoa(n+1) + "]"
		}
	}
	r.cited = append(r.cited, ref)
	return "[^" + strconv.Itoa(len(r.cited)) + "]"
}

func baseID(refID string) string {
	base, _, _ := strings.Cut(refID, "#")
	return base
}
