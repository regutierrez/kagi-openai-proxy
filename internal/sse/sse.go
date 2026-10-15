// Package sse reads Server-Sent Events line by line.
//
// Bytes are split into lines on LF (a trailing CR is stripped, so CRLF works); a blank
// line dispatches the frame. Multiple data: lines in one frame are joined with "\n".
// id:, event:, retry: and comment (:) lines are ignored. Raw bytes are never split on
// "\n\n", so frames split at any read boundary (even mid-CRLF or mid-UTF-8) parse
// correctly. Lines are accumulated from bufio.Reader.ReadSlice rather than a
// bufio.Scanner, so long frames are not limited by a token size; instead one frame is
// capped at MaxFrameBytes to bound memory.
package sse

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"strings"
	"unicode/utf8"
)

// MaxFrameBytes bounds one frame (all of its lines together).
const MaxFrameBytes = 16 << 20

// ErrFrameTooLarge is returned when a frame exceeds MaxFrameBytes.
var ErrFrameTooLarge = errors.New("sse: frame exceeds 16 MiB")

// Reader yields the data payload of each frame.
type Reader struct {
	br        *bufio.Reader
	data      []string
	dataBytes int
	eof       bool
}

// NewReader wraps r.
func NewReader(r io.Reader) *Reader {
	return &Reader{br: bufio.NewReaderSize(r, 64<<10)}
}

// Next returns the next frame's data payload, or io.EOF once the stream has ended.
// A frame missing only its trailing blank line is still dispatched at EOF.
func (r *Reader) Next() (string, error) {
	for !r.eof {
		line, err := r.readLine()
		if errors.Is(err, io.EOF) {
			r.eof = true
			if len(line) > 0 {
				if frame, ok := r.line(line); ok {
					return frame, nil
				}
			}
			if frame, ok := r.line(nil); ok {
				return frame, nil
			}
			return "", io.EOF
		}
		if err != nil {
			return "", err
		}
		if frame, ok := r.line(line); ok {
			return frame, nil
		}
	}
	return "", io.EOF
}

// readLine returns one line without its LF. At EOF it returns the partial last line
// together with io.EOF.
func (r *Reader) readLine() ([]byte, error) {
	var line []byte
	for {
		chunk, err := r.br.ReadSlice('\n')
		if r.dataBytes+len(line)+len(chunk) > MaxFrameBytes {
			return nil, ErrFrameTooLarge
		}
		line = append(line, chunk...)
		switch {
		case err == nil:
			return line[:len(line)-1], nil
		case errors.Is(err, bufio.ErrBufferFull):
			continue
		default:
			return line, err
		}
	}
}

func (r *Reader) line(raw []byte) (string, bool) {
	raw = bytes.TrimSuffix(raw, []byte("\r"))
	if len(raw) == 0 {
		if len(r.data) == 0 {
			return "", false
		}
		payload := strings.Join(r.data, "\n")
		r.data, r.dataBytes = nil, 0
		return payload, true
	}
	line := decode(raw)
	if strings.HasPrefix(line, ":") {
		return "", false
	}
	name, value, found := strings.Cut(line, ":")
	if found {
		value = strings.TrimPrefix(value, " ")
	}
	if name == "data" {
		r.data = append(r.data, value)
		r.dataBytes += len(value) + 1
	}
	// id / event / retry / unknown fields: ignored.
	return "", false
}

// decode turns bytes into text, replacing each invalid UTF-8 byte with U+FFFD.
func decode(b []byte) string {
	if utf8.Valid(b) {
		return string(b)
	}
	var sb strings.Builder
	for len(b) > 0 {
		r, size := utf8.DecodeRune(b)
		sb.WriteRune(r) // RuneError for an invalid byte (size 1)
		b = b[size:]
	}
	return sb.String()
}
