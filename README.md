# kagi-openai-proxy (Go)

> [!WARNING]
> **this works perfectly but this is pure vibeslop. you have been warned.**

A personal, single-user, **localhost-first** proxy that exposes Kagi Assistant
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
- **Reasoning effort:** `reasoning_effort` in the request, an effort suffix such as
  `glm-5-3-flash:low`, or `KAGI_REASONING_EFFORT`. See
  [Reasoning effort](#reasoning-effort).
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

### Reasoning effort

Kagi's v2 conversation API takes a `reasoning_effort` field: `none`, `minimal`, `low`,
`medium`, `high`, `xhigh`, or `max`. Each model supports a subset. This field and its
values were verified against live Kagi. It is a real cost and latency lever: with no
effort set, a GLM model defaulted to `max` and spent 4,109 output tokens and 88s on an
ordinary question. (`thinking_preset` was the legacy v1 field; it does not apply to v2
and the proxy never sends it.)

The proxy picks one effort per request, first match wins:

1. `reasoning_effort` in the Chat Completions request body (OpenAI's field name);
2. an effort suffix on the model id;
3. `KAGI_REASONING_EFFORT`;
4. nothing: the field is omitted and Kagi uses the model's own default.

**Model id grammar.** Suffixes are a client-side convention; `/v1/models` lists plain ids.

```
requested = model-id *( ":" suffix )
suffix    = "online" / "offline" / effort
effort    = "none" / "minimal" / "low" / "medium" / "high" / "xhigh" / "max"
```

Suffixes form a set: any order, repeats allowed. So `glm-5-3-flash:low:offline` and
`glm-5-3-flash:offline:low` are the same. These are 400 `invalid_model_suffix`: an
unknown or uppercase token, `:online` together with `:offline`, and two different
efforts. The model id is everything before the first `:`, so a Kagi id must not
contain `:`.

**Validation.** Every value must be in the set above (400 `invalid_reasoning_effort`).
Kagi's `/api/init` catalog also lists each model's options (`supports_thinking` and a
`reasoning_effort` list; for example GLM has low/high/max and `ki_quick` has none).
`GET /v1/models` shows them as a non-standard `reasoning_efforts` array per model. When
the catalog lists a model's options, a request or suffix effort outside them is a 400
`unsupported_reasoning_effort` that names the supported values, and nothing reaches
Kagi. An unsupported `KAGI_REASONING_EFFORT` default is not an error: for that model the
proxy omits the field (Kagi's default) and logs it once. If the catalog is unavailable,
is being served stale after a failed refresh, doesn't list the model, or has no option
list for it, only the global set is checked; Kagi then rejects anything else
(`kagi_rejected_request`).

Thinking output stays dropped: only text nodes reach the answer. Passing reasoning
through (e.g. as `reasoning_content`) is a possible future option.

### Disconnects

The Python version answered a non-streaming request whose client had already gone with
status 499. That was an artefact of its ASGI server. In Go the handler watches
`r.Context().Done()`: when the client disconnects it marks the turn's consumer as gone
(which starts the bounded drain) and returns **without writing a status**, because the
connection is closed and any write would fail. Nothing reaches a client either way.

A request still waiting for the turn slot gives up as soon as its client disconnects, so
it never starts a Kagi turn nobody will read. (The Python version would still run it.)

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
   tries to delete its thread. To run it permanently, see [Deployment](#deployment).

4. **Try it:**

   ```sh
   curl -sN http://127.0.0.1:8000/v1/chat/completions \
     -H "Authorization: Bearer $PROXY_API_KEY" -H 'Content-Type: application/json' \
     -d '{"model": "<id from /v1/models>", "stream": true,
          "messages": [{"role": "user", "content": "Hello"}]}'
   ```

### Configuration

| Variable | Default | Meaning |
| --- | --- | --- |
| `KAGI_SESSION_TOKEN` | *(required)* | Kagi session-link token, sent as the `kagi_session` cookie |
| `PROXY_API_KEY` | *(required, ≥ 16 chars)* | Clients must send `Authorization: Bearer <PROXY_API_KEY>` |
| `PROXY_HOST` | `127.0.0.1` | Bind address. Keep it loopback unless you deploy on a tailnet (see [Deployment](#deployment)) |
| `PROXY_PORT` | `8000` | Bind port |
| `PROXY_ALLOWED_HOSTS` | `127.0.0.1,localhost` | Comma-separated `Host` header values to accept (DNS-rebinding defence). Ports and whitespace are stripped before matching; matching is exact. Setting it **replaces** the default. Empty/unset = localhost only |
| `KAGI_BASE_URL` | `https://assistant.kagi.com` | Kagi Assistant origin |
| `KAGI_DELETE_THREADS` | `true` | Delete each Kagi thread after its turn ends |
| `KAGI_PERSONALIZATION` | `false` | Pass Kagi's `personalization` flag |
| `KAGI_SEARCH_DEFAULT` | `online` | Web search for model ids without a suffix (`online`/`offline`) |
| `KAGI_REASONING_EFFORT` | *(unset)* | Default `reasoning_effort` when neither the request nor a model suffix sets one: `none`, `minimal`, `low`, `medium`, `high`, `xhigh`, `max`. Validated at startup. Unset = Kagi's per-model default. Skipped (logged once) for a model whose catalog options don't include it |
| `KAGI_MAX_CONCURRENCY` | `1` | Max concurrent Kagi turns |
| `KAGI_QUEUE_WAIT_SECONDS` | `30` | How long a request waits for a free slot before 429 |
| `KAGI_CONNECT_TIMEOUT` | `20` | Upstream connect timeout (seconds) |
| `KAGI_READ_TIMEOUT` | `120` | Upstream time-to-headers and per-read timeout (seconds); there is no total timeout |
| `KAGI_MODELS_TTL` | `21600` | Model catalog cache lifetime (seconds) |

The binary also has a `-healthcheck` flag: instead of serving, it sends `GET /health`
to `PROXY_HOST:PROXY_PORT` (a `0.0.0.0`/`::` bind is reached through loopback), using
the first `PROXY_ALLOWED_HOSTS` entry as the `Host` header so the allow-list accepts it.
It exits 0 if the answer is `200 {"status":"ok"}`, else 1. It never calls Kagi. It
exists for container `HEALTHCHECK`s on images without curl.

Logs go to stderr in the format `2026-10-15 07:02:56,423 INFO kagi_proxy: ...`. The Go
binary writes no per-request access log; under the Python version, uvicorn wrote those.

## Deployment

Pick one. In every case the proxy is reachable only from places you choose: your own
machine, or your private tailnet.

### Bare binary

Run the binary as yourself. It binds `127.0.0.1:8000` by default, so only local apps
such as Raycast can reach it.

**macOS, launchd (optional).** Copy the binary to e.g. `~/bin/kagi-openai-proxy`, then
create `~/Library/LaunchAgents/com.example.kagi-openai-proxy.plist`. The plist holds
your session token, so `chmod 600` it.

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

**Linux, systemd user service (optional).** Put the variables in
`~/.config/kagi-openai-proxy.env` (`chmod 600`) and create
`~/.config/systemd/user/kagi-openai-proxy.service`:

```ini
[Unit]
Description=kagi-openai-proxy

[Service]
EnvironmentFile=%h/.config/kagi-openai-proxy.env
ExecStart=%h/bin/kagi-openai-proxy
Restart=on-failure

[Install]
WantedBy=default.target
```

```sh
systemctl --user daemon-reload && systemctl --user enable --now kagi-openai-proxy
```

### Docker + Tailscale

Use this to run the proxy on one machine, such as a home server, and reach it from your
other devices over your tailnet. The image is `FROM scratch` (binary + CA bundle, about
3.5 MB), runs as `65534:65534` (nobody), and has a `HEALTHCHECK` using `-healthcheck`.
Inside the container it listens on `0.0.0.0:8000`. You limit exposure in two places.

1. **Publish on the Tailscale IP only.** In
   [docker-compose.yml](docker-compose.yml), replace `100.101.102.103` with your
   machine's Tailscale IP (`tailscale ip -4`). Never publish with plain `-p 8000:8000`
   or `"8000:8000"`: that binds `0.0.0.0` on the host, every interface including your
   LAN.
2. **Allow your tailnet name/IP as Host.** Create `.env` next to the compose file. It
   is git- and docker-ignored, so keep it that way, and `chmod 600` it:

   ```sh
   KAGI_SESSION_TOKEN=your-session-link-token
   PROXY_API_KEY=your-proxy-key-at-least-16-chars
   PROXY_ALLOWED_HOSTS=100.101.102.103,yourhost.your-tailnet.ts.net
   ```

```sh
docker compose up -d --build
docker compose ps        # STATUS shows (healthy) once /health answers
```

From another device on the tailnet, use `http://100.101.102.103:8000/v1` (or your
MagicDNS name) as the `base_url`, for example in Raycast's `providers.yaml`.

> [!CAUTION]
> **Never use Tailscale Funnel** (or Serve with Funnel, port forwarding, a public reverse
> proxy, or a cloud tunnel) for this. Funnel publishes the service to the whole
> internet, and this proxy is your logged-in Kagi session. Keep it on your private
> tailnet only.

Plain HTTP is fine here. Tailscale traffic between your devices is already encrypted
end-to-end by WireGuard, so the bearer key never crosses a network in the clear. Don't
reuse the same setup over a LAN without Tailscale.

## Raycast

Raycast's [Custom Providers](https://manual.raycast.com/ai/custom-providers) feature
reads `~/.config/raycast/ai/providers.yaml`. Enable it under *Settings → AI → Models &
Providers → Custom Providers*. Then copy the entry from
[providers.yaml.example](providers.yaml.example), replacing:

- the API key with your `PROXY_API_KEY`, not the Kagi token;
- the model ids with ids from `GET /v1/models`, plus suffixed variants if you want
  them: `:offline`, an effort such as `:low`, or both (`glm-5-3-flash:low:offline`).
  Raycast does not read `/v1/models` itself, so the ids must match exactly.

If you enable Raycast's `reasoning_effort` ability for a model, every value Raycast
sends must be in that model's `reasoning_efforts` list from `/v1/models`, or the proxy
answers 400. A request-body effort overrides the model's suffix.

Leave `tools` and `vision` unsupported and do not add a top-level `tools:` block. The
proxy ignores tools and replaces images with a placeholder.

## Errors

All errors use the OpenAI shape `{"error": {"message", "type", "param", "code"}}`.

| Situation | HTTP | `type` / `code` |
| --- | --- | --- |
| Missing or wrong proxy key | 401 | `invalid_request_error` / `invalid_api_key` |
| Host header not in `PROXY_ALLOWED_HOSTS` (default `127.0.0.1` / `localhost`) | 400 | plain text `Invalid host header` |
| Kagi 401/403, redirect, or HTML instead of JSON | 401 | `authentication_error` / `kagi_session_expired` |
| Kagi 429 or a quota-like turn failure | 429 | `rate_limit_error` |
| Proxy busy longer than `KAGI_QUEUE_WAIT_SECONDS` | 429 | `rate_limit_error` / `proxy_busy` |
| Kagi rejected the request (e.g. unknown model) | 400 | `invalid_request_error` / `kagi_rejected_request` |
| Unknown or conflicting model suffix | 400 | `invalid_request_error` / `invalid_model_suffix` |
| `reasoning_effort` not in the global set | 400 | `invalid_request_error` / `invalid_reasoning_effort` |
| Effort not in the model's catalog options | 400 | `invalid_request_error` / `unsupported_reasoning_effort` |
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

## License

[MIT](LICENSE).

## Layout

| Path | Role |
| --- | --- |
| `cmd/kagi-openai-proxy` | Server binary: config, HTTP server, graceful shutdown, `-healthcheck` |
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
- **Never expose it to the internet.** The proxy is effectively your logged-in Kagi
  session. By default it binds `127.0.0.1`, rejects other `Host` headers, requires a
  bearer key, and sends no CORS headers. The only remote setup this README supports is
  a private tailnet ([Deployment](#deployment)). No Tailscale Funnel, public reverse
  proxy, port forward, or `0.0.0.0` publish on a LAN.
- **Secrets:** the session token and proxy key are redacted from logs, error messages,
  and probe output. Logs record token counts and `cost_usd` per turn, never the prompt.
