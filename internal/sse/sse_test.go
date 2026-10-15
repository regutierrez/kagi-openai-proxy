package sse

import (
	"bytes"
	"errors"
	"io"
	"os"
	"slices"
	"strings"
	"testing"
)

// chunkReader returns one chunk per Read call, like a network stream.
type chunkReader struct{ chunks [][]byte }

func (c *chunkReader) Read(p []byte) (int, error) {
	if len(c.chunks) == 0 {
		return 0, io.EOF
	}
	n := copy(p, c.chunks[0])
	if n < len(c.chunks[0]) {
		c.chunks[0] = c.chunks[0][n:]
	} else {
		c.chunks = c.chunks[1:]
	}
	return n, nil
}

func feedAll(t *testing.T, chunks ...[]byte) []string {
	t.Helper()
	r := NewReader(&chunkReader{chunks: chunks})
	var frames []string
	for {
		f, err := r.Next()
		if errors.Is(err, io.EOF) {
			return frames
		}
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		frames = append(frames, f)
	}
}

func splitEvery(b []byte, n int) [][]byte {
	var out [][]byte
	for i := 0; i < len(b); i += n {
		out = append(out, b[i:min(i+n, len(b))])
	}
	return out
}

func assertFrames(t *testing.T, got []string, want ...string) {
	t.Helper()
	if !slices.Equal(got, want) {
		t.Fatalf("frames = %q, want %q", got, want)
	}
}

func TestLFFrames(t *testing.T) {
	assertFrames(t, feedAll(t, []byte("data: {\"a\":1}\n\ndata: {\"b\":2}\n\n")), `{"a":1}`, `{"b":2}`)
}

func TestCRLFFrames(t *testing.T) {
	assertFrames(t, feedAll(t, []byte("data: {\"a\":1}\r\n\r\ndata: {\"b\":2}\r\n\r\n")), `{"a":1}`, `{"b":2}`)
}

func TestFrameSplitAcrossReadBoundariesIncludingMidCRLF(t *testing.T) {
	body := []byte("data: {\"a\":1}\r\n\r\ndata: {\"text\":\"caf\xc3\xa9\"}\n\n")
	whole := feedAll(t, body)
	assertFrames(t, whole, `{"a":1}`, `{"text":"café"}`)
	for cut := 1; cut < len(body); cut++ {
		assertFrames(t, feedAll(t, body[:cut], body[cut:]), whole...)
	}
	assertFrames(t, feedAll(t, splitEvery(body, 1)...), whole...)
}

func TestMultipleDataLinesJoinedWithNewline(t *testing.T) {
	assertFrames(t, feedAll(t, []byte("data: line one\ndata: line two\n\n")), "line one\nline two")
	assertFrames(t, feedAll(t, []byte("data: {\"a\":\r\ndata: 1}\r\n\r\n")), "{\"a\":\n1}")
}

func TestCommentIDAndEventLinesIgnored(t *testing.T) {
	body := []byte(": keep-alive\nid: 42\nevent: message\nretry: 10\ndata: x\n\n: lonely comment\n\n")
	assertFrames(t, feedAll(t, body), "x")
}

func TestUnterminatedLastFrameDispatchedAtEOF(t *testing.T) {
	assertFrames(t, feedAll(t, []byte(`data: {"is_final": true}`)), `{"is_final": true}`)
}

func TestRealisticFixtureParsesIdenticallyRegardlessOfChunking(t *testing.T) {
	fixture, err := os.ReadFile("../kagitest/testdata/realistic_turn.sse")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(fixture, []byte("\r\n")) || !bytes.Contains(fixture, []byte(": ping")) {
		t.Fatal("fixture lost its CRLF lines or comment (check .gitattributes)")
	}
	whole := feedAll(t, fixture)
	if len(whole) != 15 {
		t.Fatalf("got %d frames, want 15", len(whole))
	}
	assertFrames(t, feedAll(t, splitEvery(fixture, 5)...), whole...)
	assertFrames(t, feedAll(t, splitEvery(fixture, 1)...), whole...)
}

func TestLongFrameBeyondBufferSize(t *testing.T) {
	long := strings.Repeat("x", 300<<10) // larger than the 64 KiB read buffer and Scanner's default
	assertFrames(t, feedAll(t, []byte("data: "+long+"\n\n")), long)
}

func TestFrameTooLarge(t *testing.T) {
	r := NewReader(strings.NewReader("data: " + strings.Repeat("x", MaxFrameBytes) + "\n\n"))
	if _, err := r.Next(); !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("err = %v, want ErrFrameTooLarge", err)
	}
}
