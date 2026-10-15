// Command probe is a MANUAL live probe of the Kagi Assistant wire protocol. Never run it
// automatically or on a schedule.
//
// It spends one real (cheap) Kagi Assistant turn:
//  1. GET /api/init and pick the cheapest-looking model (or -model).
//  2. POST /api/v2/conversations with "Reply with exactly: PONG" (web search off).
//  3. Stream the turn and check the event sequence
//     turn.started -> node.created(text) -> node.delta+ -> turn.completed -> is_final
//     (other events such as thinking nodes or references may appear in between).
//  4. DELETE the thread, always.
//
// Usage:
//
//	KAGI_SESSION_TOKEN=... go run ./cmd/probe [-model MODEL_ID]
//
// Exit status 0 = protocol matches what the proxy expects; 1 = drift (details printed).
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"kagiopenaiproxy/internal/kagi"
	"kagiopenaiproxy/internal/probe"
	"kagiopenaiproxy/internal/redact"
)

func main() {
	model := flag.String("model", "", "Kagi model id (default: cheapest-looking model in the catalog)")
	flag.Parse()

	token := strings.TrimSpace(os.Getenv("KAGI_SESSION_TOKEN"))
	if token == "" {
		fmt.Fprintln(os.Stderr, "probe: KAGI_SESSION_TOKEN is required")
		os.Exit(1)
	}
	redact.RegisterSecret(token)
	baseURL := os.Getenv("KAGI_BASE_URL")
	if baseURL == "" {
		baseURL = kagi.DefaultBaseURL
	}
	client := kagi.NewClient(token, strings.TrimRight(baseURL, "/"), 20*time.Second, 120*time.Second)
	status := probe.Run(context.Background(), client, *model, os.Stdout)
	client.Close()
	os.Exit(status)
}
