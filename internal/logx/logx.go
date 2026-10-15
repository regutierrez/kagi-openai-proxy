// Package logx is the proxy's only logger. Every line is rendered, then redacted, then
// written, so no log line can contain the Kagi session token or the proxy key.
package logx

import (
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	"kagiopenaiproxy/internal/redact"
)

var (
	mu  sync.Mutex
	out io.Writer = os.Stderr
)

// SetOutput replaces the destination and returns a function that restores the previous one.
func SetOutput(w io.Writer) (restore func()) {
	mu.Lock()
	prev := out
	out = w
	mu.Unlock()
	return func() {
		mu.Lock()
		out = prev
		mu.Unlock()
	}
}

func logf(level, format string, args ...any) {
	msg := redact.String(fmt.Sprintf(format, args...))
	now := time.Now()
	stamp := now.Format("2006-01-02 15:04:05") + fmt.Sprintf(",%03d", now.Nanosecond()/int(time.Millisecond))
	line := fmt.Sprintf("%s %s kagi_proxy: %s\n", stamp, level, strings.TrimRight(msg, "\n"))
	mu.Lock()
	defer mu.Unlock()
	_, _ = io.WriteString(out, line)
}

// Info logs an informational line.
func Info(format string, args ...any) { logf("INFO", format, args...) }

// Warn logs a warning.
func Warn(format string, args ...any) { logf("WARNING", format, args...) }

// Error logs an error.
func Error(format string, args ...any) { logf("ERROR", format, args...) }
