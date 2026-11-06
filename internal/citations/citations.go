// Package citations numbers citation sources and rewrites Kagi citation markers
// [^ref_id] while streaming.
//
// Sources are numbered 1, 2, ... in the order Kagi announces them (all of a turn's
// search results arrive before its answer text, verified live), so the numbers in the
// text match a source list shown next to it. A marker for a source with a URL becomes
// the Markdown link [[n]](url); one without a URL becomes [^n]. Unknown ref_ids pass
// through unchanged. An opened-but-unclosed marker is held back (up to MaxHeld
// characters) so markers split across deltas are rewritten correctly.
package citations

import (
	"strconv"
	"strings"
	"unicode/utf8"

	"kagiopenaiproxy/internal/events"
)

// MaxHeld caps the held tail of an unclosed marker; longer tails are released raw.
const MaxHeld = 64

const open = "[^"

// Source is one citation source.
type Source struct {
	N                                             int
	ID, URL, Title, Snippet, Favicon, PublishedAt string
}

// Rewriter holds the citation state of one answer.
type Rewriter struct {
	byID    map[string]int // base ref_id -> index in sources
	sources []Source
	held    string
}

// New returns an empty Rewriter.
func New() *Rewriter { return &Rewriter{byID: map[string]int{}} }

// AddReference registers a source. It returns the numbered source and whether it is
// new; a repeated ref_id keeps its first number.
func (r *Rewriter) AddReference(ref events.ReferenceAdded) (Source, bool) {
	id := baseID(ref.RefID)
	if i, ok := r.byID[id]; ok {
		return r.sources[i], false
	}
	source := Source{len(r.sources) + 1, id, ref.URL, ref.Title, ref.Snippet, ref.Favicon, ref.PublishedAt}
	r.byID[id] = len(r.sources)
	r.sources = append(r.sources, source)
	return source, true
}

// Sources returns the registered sources in number order.
func (r *Rewriter) Sources() []Source { return append([]Source(nil), r.sources...) }

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
			if utf8.RuneCountInString(tail) > MaxHeld {
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
	i, known := r.byID[baseID(marker[len(open):len(marker)-1])]
	if !known {
		return marker
	}
	source := r.sources[i]
	if source.URL == "" {
		return "[^" + strconv.Itoa(source.N) + "]"
	}
	return Link(source)
}

// Link is the inline Markdown citation for a source: [[n]](url).
func Link(s Source) string {
	return "[[" + strconv.Itoa(s.N) + "]](" + linkEscaper.Replace(s.URL) + ")"
}

// linkEscaper keeps a URL inside a Markdown link destination.
var linkEscaper = strings.NewReplacer(" ", "%20", "(", "%28", ")", "%29", "<", "%3C", ">", "%3E")

func baseID(refID string) string {
	base, _, _ := strings.Cut(refID, "#")
	return base
}
