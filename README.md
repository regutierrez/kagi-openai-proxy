# kagi-openai-proxy (Go)

A personal, single-user, **localhost-only** proxy that exposes Kagi Assistant
(`https://assistant.kagi.com`) as an OpenAI-compatible Chat Completions API. Point
Raycast's Custom Providers (or any client with a configurable OpenAI `base_url`) at it
and chat with the models in your paid Kagi plan.

This is the Go port of the Python `kagi-openai-proxy` (reference commit `2527cd9`): one
static binary, standard library only, no runtime or virtualenv. Behaviour is the same;
the one intentional difference is described under [Disconnects](#disconnects).

Kagi has **no official Assistant API**. This proxy wraps the undocumented web API the
Assistant web app uses. See [Risks and terms](#risks-and-terms) before using it.

The authoritative behaviour spec is [SPEC.md](SPEC.md).

## What it does

| Endpoint | Behaviour |
| --- | --- |
| `GET /health` | `{"status": "ok", "version": ...}`, no upstream call, no auth |
| `GET /health?deep=1` | Also calls Kagi `GET /api/init`; 200 only if Kagi answers 2xx JSON, else 503 (requires auth) |
| `GET /v1/models` | Kagi's model catalog (supported, not deprecated/retired), cached for `KAGI_MODELS_TTL`; stale copy served if a refresh fails; 503 if never fetched |
| `POST /v1/chat/completions` | Streaming and non-streaming |

Every request is turned into **one new Kagi thread**: the whole OpenAI message list is
flattened into a single prompt (system instructions + transcript), sent, streamed back,
and then the thread is deleted (unless `KAGI_DELETE_THREADS=false`). There is no
conversation cache in v0.1, so long chats resend their history each turn. When the
flattened prompt exceeds ~70% of the model's context window, the oldest messages are
dropped and replaced by `[...earlier messages omitted...]`.

Other behaviour:

- **Web search:** append `:online` or `:offline` to a model id (`some-model:offline`) to
  turn Kagi's web search on or off. With no suffix, `KAGI_SEARCH_DEFAULT` applies.
- **Citations:** Kagi's `[^ref]` markers are renumbered `[^1]`, `[^2]`, … in order of
  first citation.
- **Ignored inputs:** `tools`, `tool_choice`, `n`, `stop`, `seed`, `top_p`,
  `response_format`, penalties, and `user` are ignored. `temperature` is validated but not
  forwarded, because Kagi has no per-request temperature. Images become `[image omitted]`.
- **Concurrency:** one Kagi turn at a time by default. Extra requests wait up to
  `KAGI_QUEUE_WAIT_SECONDS`, then get 429. Turns are never retried automatically.
- **End of answer:** the response ends at Kagi's `turn.completed`; anything Kagi sends
  after it is not part of the answer.
- **Disconnects:** if the client hangs up (streaming or not), or after the answer ends,
  the proxy keeps reading the Kagi turn in the background (bounded, 600s) before deleting
  the thread and freeing the slot.

### Disconnects

The Python version answered a non-streaming request whose client had already gone with
status 499. That was an artefact of its ASGI server. In Go the handler watches
`r.Context().Done()`: when the client disconnects it marks the turn's consumer as gone
(which starts the bounded drain) and returns **without writing a status**, because the
connection is closed and any write would fail. Nothing reaches a client either way.

## Build

Requires Go ≥ 1.27. Standard library only; there are no module dependencies.

```sh
cd kagi-openai-proxy-go
go build -o kagi-openai-proxy ./cmd/kagi-openai-proxy

# For a Mac (Apple silicon) from any OS:
GOOS=darwin GOARCH=arm64 go build -o kagi-openai-proxy ./cmd/kagi-openai-proxy
# Intel Mac: GOARCH=amd64
```

The result is one self-contained binary.

## Setup

1. **Get your Kagi session token.** In Kagi, open *Settings → Account → Session Link*.
   The link looks like `https://kagi.com/search?token=XXXXXXXX…`; the token is the value
   after `token=`. Anyone holding it is logged in as you, so treat it like a password.
   Resetting the session link in Kagi revokes it.

2. **Configure.** Put the variables in your shell or in a `.env` file (git-ignored),
   then load it with `set -a; . ./.env; set +a`:

   ```sh
   KAGI_SESSION_TOKEN=your-session-link-token
   PROXY_API_KEY=$(openssl rand -base64 32)
   ```

3. **Run:**

   ```sh
   ./kagi-openai-proxy            # binds PROXY_HOST:PROXY_PORT (127.0.0.1:8000)
   ```

   The process starts without contacting Kagi. A bad token only shows up on the first
   request, as a 401 `kagi_session_expired`. To check the token up front:

   ```sh
   curl -s -H "Authorization: Bearer $PROXY_API_KEY" 'http://127.0.0.1:8000/health?deep=1'
   curl -s -H "Authorization: Bearer $PROXY_API_KEY" http://127.0.0.1:8000/v1/models
   ```

   SIGINT/SIGTERM shut it down gracefully. Open responses get 10s to finish. In-flight
   turns get 10s more to drain, then they are cancelled; each cancelled turn still
   tries to delete its thread.

4. **Try it:**

   ```sh
   curl -sN http://127.0.0.1:8000/v1/chat/completions \
     -H "Authorization: Bearer $PROXY_API_KEY" -H 'Content-Type: application/json' \
     -d '{"model": "<id from /v1/models>", "stream": true,
          "messages": [{"role": "user", "content": "Hello"}]}'
   ```

### Run at login with launchd (macOS)

Copy the binary to e.g. `~/bin/kagi-openai-proxy`, then create
`~/Library/LaunchAgents/com.example.kagi-openai-proxy.plist`. The plist holds your
session token, so make it readable only by you: `chmod 600` it.

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>com.example.kagi-openai-proxy</string>
  <key>ProgramArguments</key><array><string>/Users/YOU/bin/kagi-openai-proxy</string></array>
  <key>EnvironmentVariables</key>
  <dict>
    <key>KAGI_SESSION_TOKEN</key><string>your-session-link-token</string>
    <key>PROXY_API_KEY</key><string>your-proxy-key-at-least-16-chars</string>
  </dict>
  <key>RunAtLoad</key><true/>
  <key>KeepAlive</key><true/>
  <key>StandardErrorPath</key><string>/Users/YOU/Library/Logs/kagi-openai-proxy.log</string>
</dict>
</plist>
```

```sh
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.example.kagi-openai-proxy.plist
launchctl kickstart -k gui/$(id -u)/com.example.kagi-openai-proxy   # restart after changes
```

### Configuration

| Variable | Default | Meaning |
| --- | --- | --- |
| `KAGI_SESSION_TOKEN` | *(required)* | Kagi session-link token, sent as the `kagi_session` cookie |
| `PROXY_API_KEY` | *(required, ≥ 16 chars)* | Clients must send `Authorization: Bearer <PROXY_API_KEY>` |
| `PROXY_HOST` | `127.0.0.1` | Bind address. Keep it loopback |
| `PROXY_PORT` | `8000` | Bind port |
| `KAGI_BASE_URL` | `https://assistant.kagi.com` | Kagi Assistant origin |
| `KAGI_DELETE_THREADS` | `true` | Delete each Kagi thread after its turn ends |
| `KAGI_PERSONALIZATION` | `false` | Pass Kagi's `personalization` flag |
| `KAGI_SEARCH_DEFAULT` | `online` | Web search for model ids without a suffix (`online`/`offline`) |
| `KAGI_MAX_CONCURRENCY` | `1` | Max concurrent Kagi turns |
| `KAGI_QUEUE_WAIT_SECONDS` | `30` | How long a request waits for a free slot before 429 |
| `KAGI_CONNECT_TIMEOUT` | `20` | Upstream connect timeout (seconds) |
| `KAGI_READ_TIMEOUT` | `120` | Upstream time-to-headers and per-read timeout (seconds); there is no total timeout |
| `KAGI_MODELS_TTL` | `21600` | Model catalog cache lifetime (seconds) |

Logs go to stderr in the format `2026-10-15 07:02:56,423 INFO kagi_proxy: ...`. The Go
binary writes no per-request access log; under the Python version, uvicorn wrote those.

## Raycast

Raycast's [Custom Providers](https://manual.raycast.com/ai/custom-providers) feature
reads `~/.config/raycast/ai/providers.yaml`. Enable it under *Settings → AI → Models &
Providers → Custom Providers*. Then copy the entry from
[providers.yaml.example](providers.yaml.example), replacing:

- the API key with your `PROXY_API_KEY`, not the Kagi token;
- the model ids with ids from `GET /v1/models`, plus `:offline` variants if you want
  them. Raycast does not read `/v1/models` itself, so the ids must match exactly.

Leave `tools` and `vision` unsupported and do not add a top-level `tools:` block. The
proxy ignores tools and replaces images with a placeholder.

## Errors

All errors use the OpenAI shape `{"error": {"message", "type", "param", "code"}}`.

| Situation | HTTP | `type` / `code` |
| --- | --- | --- |
| Missing or wrong proxy key | 401 | `invalid_request_error` / `invalid_api_key` |
| Host header other than `127.0.0.1` / `localhost` | 400 | plain text `Invalid host header` |
| Kagi 401/403, redirect, or HTML instead of JSON | 401 | `authentication_error` / `kagi_session_expired` |
| Kagi 429 or a quota-like turn failure | 429 | `rate_limit_error` |
| Proxy busy longer than `KAGI_QUEUE_WAIT_SECONDS` | 429 | `rate_limit_error` / `proxy_busy` |
| Kagi rejected the request (e.g. unknown model) | 400 | `invalid_request_error` / `kagi_rejected_request` |
| Unexpected Kagi response shape, or a malformed stream frame | 502 | `api_error` / `kagi_protocol_drift` |
| Turn finished with no answer text | 502 | `api_error` / `kagi_empty_answer` |

Once a stream has started, failures (`turn.failed`, malformed frames, truncated streams,
empty answers) arrive as a final `data: {"error": {...}}` frame, with no `data: [DONE]`
after it.

## Tests

```sh
go vet ./...
go test -race ./...
```

The suite runs offline. Kagi is simulated by `internal/kagitest`, an `httptest` server
on 127.0.0.1 that records call order. Every test points the client at that fake. The
realistic fixture stream (`internal/kagitest/testdata/realistic_turn.sse`) is
byte-identical to the Python one; `.gitattributes` keeps its CRLF lines intact.

`cmd/probe` is a **manual** live check against the real Kagi API. It spends one small
turn, verifies the SSE event sequence the proxy relies on, then deletes the thread. Run
it by hand when the proxy starts misbehaving. Never schedule or automate it:

```sh
KAGI_SESSION_TOKEN=... go run ./cmd/probe [-model MODEL_ID]
```

## Layout

| Path | Role |
| --- | --- |
| `cmd/kagi-openai-proxy` | Server binary: config, HTTP server, graceful shutdown |
| `cmd/probe` | Manual live probe |
| `internal/kagi` | The only code that knows Kagi URLs, headers and JSON; produces internal events |
| `internal/events` | Internal turn events |
| `internal/sse` | Line-based SSE reader (`bufio.Reader`, 16 MiB per-frame cap) |
| `internal/citations` | `[^ref]` → `[^n]` streaming rewrite |
| `internal/translate` | OpenAI request parsing, flattening, answer assembly, chunk formatting, redacted errors |
| `internal/server` | HTTP surface, catalog cache, turn slot, per-turn runner goroutine |
| `internal/redact`, `internal/logx` | Secret redaction choke point; redacting logger |
| `internal/kagitest` | Offline fake Kagi for tests |

## Risks and terms

- **Personal use only.** This automates your own Kagi account through an undocumented
  interface. Kagi has not sanctioned it, and it may conflict with Kagi's terms of
  service. Using it is your own decision and your own risk, including to your account.
- **It will break.** The wire protocol was reverse-engineered and can change without
  notice. The proxy fails loudly (`kagi_protocol_drift`, `kagi_empty_answer`) instead of
  returning silently wrong output. `cmd/probe` shows what changed.
- **Keep usage human-scale.** One turn at a time by default and no automatic retries.
  Don't script bulk traffic through it, share it, or raise concurrency far.
- **Never expose it beyond localhost.** The proxy is effectively your logged-in Kagi
  session. It binds `127.0.0.1`, rejects other `Host` headers, requires a bearer key,
  and sends no CORS headers. Do not put it behind a tunnel, a reverse proxy, or
  `0.0.0.0`.
- **Secrets:** the session token and proxy key are redacted from logs, error messages,
  and probe output. Logs record token counts and `cost_usd` per turn, never the prompt.
