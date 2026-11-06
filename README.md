# kagi-openai-proxy

> [!WARNING]
> **this works perfectly but this is pure vibeslop. you have been warned.**

A personal, single-user proxy for Kagi Assistant (`https://assistant.kagi.com`). It
offers two APIs over your logged-in Kagi session:

- **`POST /v1/chat/completions`**: OpenAI-compatible, for Raycast Custom Providers and
  any client with a configurable OpenAI `base_url`.
- **`POST /v1/ask`**: a typed event stream for web-search clients you write yourself:
  search steps, sources, answer text, and follow-ups in the same Kagi thread.

Kagi has **no official Assistant API**. The proxy wraps the undocumented web API the
Assistant web app uses; [PROTOCOL.md](PROTOCOL.md) lists the wire facts it relies on.
Read [Risks](#risks) before using it. One static Go binary, standard library only.

## Endpoints

| Endpoint | Auth | Behaviour |
| --- | --- | --- |
| `GET /health` | no | `{"status": "ok", "version": ...}`, no Kagi call |
| `GET /health?deep=1` | yes | Also calls Kagi `GET /api/init`; 503 unless Kagi answers 2xx JSON |
| `GET /v1/models` | yes | Kagi's usable models (cached `KAGI_MODELS_TTL`, stale copy on refresh failure, 503 if never fetched), with non-standard `reasoning_efforts` and `reasoning_effort_default` per model |
| `POST /v1/chat/completions` | yes | OpenAI Chat Completions, streaming or not |
| `POST /v1/ask` | yes | Typed event stream, see [/v1/ask](#v1ask) |

Auth is `Authorization: Bearer <PROXY_API_KEY>`. Every successful chat or ask response
carries `X-Kagi-Thread: <conversation uuid>`.

Shared behaviour:

- **One Kagi turn at a time** by default (`KAGI_MAX_CONCURRENCY`). Extra requests wait
  up to `KAGI_QUEUE_WAIT_SECONDS`, then get 429 `proxy_busy`. Turns are never retried.
- **Cancel on disconnect:** if the client leaves before the answer ends, the proxy
  cancels the Kagi turn (`POST /api/v2/turns/{uuid}/cancel`). The stream then ends in
  well under a second and the slot frees. If Kagi keeps streaming anyway, the proxy
  stops reading after 600 s.
- **Citations:** sources are numbered 1, 2, ... in the order Kagi sends them. A
  citation becomes the Markdown link `[[n]](url)`. Footnote definition lines that the
  model sometimes writes (`[^ref]: url`) are dropped.
- **Web search** is on unless `KAGI_SEARCH_DEFAULT=offline`, a `:offline` model suffix,
  or `"search": false` on `/v1/ask`.

### Model ids and reasoning effort

```
requested = model-id *( ":" suffix )
suffix    = "online" / "offline" / effort
effort    = "none" / "minimal" / "low" / "medium" / "high" / "xhigh" / "max"
```

Suffixes form a set (any order, repeats allowed): `glm-5-3-flash:low:offline`.
`:online` with `:offline`, two different efforts, or an unknown token is 400
`invalid_model_suffix`. `/v1/models` lists plain ids.

Effort precedence: the request's `reasoning_effort`, then the suffix, then
`KAGI_REASONING_EFFORT`, then the model's own default (Kagi's; GLM defaults to `max`,
which is slow and expensive). An effort outside the model's `reasoning_efforts` is 400
`unsupported_reasoning_effort` before any Kagi call; Kagi itself would silently accept
it. An unsupported `KAGI_REASONING_EFFORT` is skipped for that model (logged once).

## /v1/ask

```json
POST /v1/ask
{"query": "How many people live in Lyon?", "model": "glm-5-3-flash:low",
 "search": true, "reasoning_effort": "low", "branch": "<from an earlier thread event>"}
```

Only `query` is required. `model` defaults to Kagi's default model. Unknown fields are
400. Without `branch` the proxy starts a new Kagi thread; with it, the question is a
follow-up in that thread and Kagi keeps the earlier turns and search results as
context. The query is sent as-is. Threads are never deleted (under
`KAGI_THREAD_LIFECYCLE=delete` they are created as Temporary instead).

The response is Server-Sent Events, each frame `event: <name>` plus `data: <json>`,
and `: keepalive` comments while Kagi is quiet:

| Event | Data | Notes |
| --- | --- | --- |
| `thread` | `{conversation, branch, turn, url}` | Always first. Send `branch` back for a follow-up. `url` opens the thread in Kagi |
| `title` | `{title}` | Kagi's title for the thread |
| `activity` | `{id, type, text, status}` | Upsert by `id`. `type` is `search` (`text` = the query), `thinking` (`text` = the summary), or another tool name. `status`: `running`, `completed`, `failed`, `cancelled` |
| `source` | `{n, url, title, snippet, favicon?, published_at?}` | `n` matches the `[[n]](url)` links in the text |
| `text` | `{delta}` | Answer Markdown |
| `done` | `{model, model_version?, input_tokens, output_tokens, cost_usd?}` | Success, always last |
| `error` | `{type, code, message}` | Failure after the stream started, always last |

Errors before the stream starts are HTTP errors with the body shape in [Errors](#errors).

## /v1/chat/completions

Every request becomes one new Kagi thread. The OpenAI message list is flattened into
one prompt (system instructions plus a `User:` / `Assistant:` transcript). If the prompt
exceeds about 70% of the model's context window, the oldest messages are replaced by
`[...earlier messages omitted...]`. Long chats resend their history each turn.

- Images become `[image omitted]`. `tools`, `tool_choice`, `n`, `stop`, `seed`,
  `top_p`, `response_format`, penalties, and `user` are ignored. `temperature` is
  validated but not forwarded.
- Thinking and search steps are not part of the answer.
- Non-streaming responses carry OpenAI `message.annotations` of type `url_citation`,
  one per citation link, with `start_index` / `end_index` in characters (code points).
- `stream_options.include_usage` adds the usage chunk.
- The answer ends at Kagi's `turn.completed`. A failure after the stream started
  arrives as a final `data: {"error": {...}}` frame with no `[DONE]`.

## Kagi threads

`KAGI_THREAD_LIFECYCLE` decides what happens to each thread a chat request creates:

| Mode | Created with | After the turn | In Kagi |
| --- | --- | --- | --- |
| `temporary` (default) | `"is_saved": false` | nothing | Sidebar with a *Temporary* badge; Kagi deletes it after the account's *Temporary Duration* (24h, 7d or 30d) |
| `delete` | account default | `DELETE` | Moved to Kagi's trash (restorable), not purged |
| `keep` | account default | nothing | Whatever the account's thread saving mode says |

The proxy never changes account settings. Kagi applies the account's custom
instructions to every turn; `KAGI_PERSONALIZATION` only controls personalized search
results.

## Configuration

| Variable | Default | Meaning |
| --- | --- | --- |
| `KAGI_SESSION_TOKEN` | *(required)* | Kagi *Settings → Account → Session Link*: the value after `token=`, without any `&q=%s`. Anyone holding it is logged in as you; resetting the session link revokes it |
| `PROXY_API_KEY` | *(required, ≥ 16 chars)* | Bearer key clients must send, e.g. `openssl rand -hex 32` |
| `PROXY_HOST` / `PROXY_PORT` | `127.0.0.1` / `8000` | Bind address |
| `PROXY_ALLOWED_HOSTS` | `127.0.0.1,localhost` | Accepted `Host` header values, comma-separated, ports ignored (DNS-rebinding defence). Setting it replaces the default |
| `KAGI_BASE_URL` | `https://assistant.kagi.com` | Kagi Assistant origin |
| `KAGI_THREAD_LIFECYCLE` | `temporary` | `temporary`, `delete` or `keep`, see [Kagi threads](#kagi-threads) |
| `KAGI_PERSONALIZATION` | `false` | Kagi's personalized search results |
| `KAGI_SEARCH_DEFAULT` | `online` | Web search for model ids without `:online` / `:offline` |
| `KAGI_REASONING_EFFORT` | *(unset)* | Default effort; unset = the model's own default |
| `KAGI_MAX_CONCURRENCY` | `1` | Concurrent Kagi turns |
| `KAGI_QUEUE_WAIT_SECONDS` | `30` | Wait for a free slot before 429 |
| `KAGI_CONNECT_TIMEOUT` | `20` | Upstream connect timeout (seconds) |
| `KAGI_READ_TIMEOUT` | `120` | Upstream time-to-headers and per-read idle timeout (seconds); there is no total timeout |
| `KAGI_MODELS_TTL` | `21600` | Model catalog cache lifetime (seconds) |

The process starts without contacting Kagi; a bad token shows up on the first request
as 401 `kagi_session_expired`, or on `GET /health?deep=1`.

## Run

Requires Go ≥ 1.27.

```sh
go build -o kagi-openai-proxy ./cmd/kagi-openai-proxy
GOOS=darwin GOARCH=arm64 go build -o kagi-openai-proxy ./cmd/kagi-openai-proxy   # for a Mac
KAGI_SESSION_TOKEN=... PROXY_API_KEY=... ./kagi-openai-proxy
```

SIGINT/SIGTERM shut it down gracefully: open responses get 10 s, and in-flight turns
get 10 s more before the proxy stops reading them.

**Docker:** the image is `FROM scratch` (binary plus CA bundle), runs as `65534:65534`,
listens on `0.0.0.0:8000` inside the container, and has a `HEALTHCHECK` through the
binary's `-healthcheck` flag (it calls `/health` with the first allowed host; no Kagi
call). [docker-compose.yml](docker-compose.yml) publishes the port on a Tailscale IP
only. Behind a reverse proxy on a private network, do not publish a port at all and
set `PROXY_ALLOWED_HOSTS` to the proxy's host name.

Logs go to stderr and record token counts and `cost_usd` per turn, never prompts. The
session token and proxy key are redacted from every log line, error, and probe output.

## Raycast

Raycast's [Custom Providers](https://manual.raycast.com/ai/custom-providers) read
`~/.config/raycast/ai/providers.yaml`; enable them under *Settings → AI → Models &
Providers*. Copy the entry from [providers.yaml.example](providers.yaml.example), with
your `PROXY_API_KEY` (not the Kagi token) and exact ids from `/v1/models`, plus
suffixed variants if you want them. Leave `tools` and `vision` unsupported. If you
enable the `reasoning_effort` ability, every value Raycast sends must be in that
model's `reasoning_efforts`.

## Errors

All HTTP errors use the OpenAI shape `{"error": {"message", "type", "param", "code"}}`.

| Situation | HTTP | `type` / `code` |
| --- | --- | --- |
| Missing or wrong proxy key | 401 | `invalid_request_error` / `invalid_api_key` |
| Host header not allowed | 400 | plain text `Invalid host header` |
| Bad request body or field | 400 | `invalid_request_error` / `invalid_request` |
| Unknown or conflicting model suffix | 400 | `invalid_request_error` / `invalid_model_suffix` |
| Effort not in the global set / not supported by the model | 400 | `invalid_request_error` / `invalid_reasoning_effort` / `unsupported_reasoning_effort` |
| Kagi rejected the request (e.g. unknown model or branch) | 400 | `invalid_request_error` / `kagi_rejected_request` |
| Kagi 401/403, redirect, or HTML instead of JSON | 401 | `authentication_error` / `kagi_session_expired` |
| Kagi 429 or a quota-like turn failure | 429 | `rate_limit_error` |
| Proxy busy longer than `KAGI_QUEUE_WAIT_SECONDS` | 429 | `rate_limit_error` / `proxy_busy` |
| Unexpected Kagi response shape or malformed stream frame | 502 | `api_error` / `kagi_protocol_drift` |
| Turn finished with no answer text | 502 | `api_error` / `kagi_empty_answer` |
| Kagi cancelled the turn | 502 | `api_error` / `kagi_turn_cancelled` |

## Tests and the live probe

```sh
go vet ./...
go test -race ./...
```

The suite runs offline against `internal/kagitest`, an `httptest` fake of Kagi that
records call order. Its fixtures follow live event shapes.

`cmd/probe` is a **manual** live check. It spends three cheap turns in one thread (a new
thread, a follow-up, and a cancelled long answer), checks the event sequences the proxy
relies on, then deletes the thread. Run it by hand when the proxy misbehaves; never
schedule it:

```sh
KAGI_SESSION_TOKEN=... go run ./cmd/probe [-model MODEL_ID]
```

## Layout

| Path | Role |
| --- | --- |
| `cmd/kagi-openai-proxy` | Server binary, graceful shutdown, `-healthcheck` |
| `cmd/probe`, `internal/probe` | Manual live probe |
| `internal/kagi` | The only code that knows Kagi URLs, headers and JSON; produces internal events |
| `internal/events` | Internal turn events |
| `internal/sse` | Line-based SSE reader (16 MiB per-frame cap) |
| `internal/citations` | Source numbering and streaming `[^ref]` → `[[n]](url)` rewrite |
| `internal/translate` | OpenAI request parsing, flattening, answer assembly, chunk formatting |
| `internal/server` | HTTP surface, `/v1/ask`, catalog cache, turn slot, per-turn runner |
| `internal/redact`, `internal/logx` | Secret redaction and the redacting logger |
| `internal/kagitest` | Offline fake Kagi for tests |

## Risks

- **Personal use only.** This automates your own Kagi account through an undocumented
  interface that Kagi has not sanctioned; it may conflict with Kagi's terms. Using it
  is your decision and your risk.
- **It will break** when Kagi changes its wire protocol. The proxy fails loudly
  (`kagi_protocol_drift`, `kagi_empty_answer`) instead of returning wrong output; the
  probe shows what changed.
- **Keep usage human-scale.** Kagi meters Assistant use per account. Don't script bulk
  traffic, share the proxy, or raise concurrency far.
- **Never expose it to the internet.** It is your logged-in Kagi session. No Tailscale
  Funnel, public reverse proxy, port forward, or `0.0.0.0` publish on a LAN.

## License

[MIT](LICENSE).
