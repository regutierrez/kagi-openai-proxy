# Implement `kagi-openai-proxy` v0.1

> **Go port of kagi-openai-proxy 2527cd9; behavioral requirements identical.**
> This is the Python project's SPEC.md with only the stack sections changed (environment,
> deliverables layout, test tooling). Behavioral requirements are unchanged and are
> refined by the Python fix rounds (`efdf32b`, `2527cd9`), whose 121 tests are the
> behavioral reference: `turn.completed` ends the answer, a same-frame `error` wins over
> `turn.completed`, malformed frames are `kagi_protocol_drift`, only the very first text
> delta is whitespace-stripped, every error body/chunk is redacted. Approved Go deviation:
> a non-streaming client that has disconnected gets no status (the Python 499 was an ASGI
> artefact); see README.
>
> **Go-only additions after the port** (documented in README): `PROXY_ALLOWED_HOSTS`
> (comma-separated Host allow-list, ports stripped; empty/unset = the localhost-only list
> below), a request queued for the turn slot abandons the wait when its client
> disconnects (never spends a Kagi turn), a `-healthcheck` flag, a Docker image, and
> reasoning-effort control (section "Reasoning effort" below).

Implement exactly this spec. It has already been through an architecture review; deviations are allowed only where the spec is internally impossible — otherwise implement as written and record concerns in your final report.

## Background

The user has a paid Kagi subscription including Kagi Assistant (web app at `https://assistant.kagi.com`). Kagi has **no official Assistant API**. Goal: a personal, single-user, localhost **OpenAI-compatible proxy** in Python that wraps the undocumented Assistant web API, so it can be used from Raycast's "Custom Providers" feature (any OpenAI-compatible `base_url`) and similar clients.

## Environment constraints (Go port)

- Go ≥ 1.27, **standard library only**: `net/http` (Go 1.22+ method/path patterns), `encoding/json`, `testing`/`httptest`. No external modules. `go build` produces one self-contained static binary (target: macOS via `GOOS=darwin GOARCH=arm64`, run as a launchd agent).
- Project at `kagi-openai-proxy-go/` in the workspace root, its own git repository, module path `kagiopenaiproxy`.
- You have **no Kagi credentials**. Do NOT make network calls to kagi.com / assistant.kagi.com. All tests must run fully offline against an `httptest` fake Kagi and fixture data. The server must build and start without a valid token (token supplied via env at runtime).
- `go vet ./...` and `go test -race ./...` must pass with no access to Kagi.
- This SPEC.md is the statement of intended behavior for reviewers.

## The Kagi Assistant wire protocol (verified; undocumented, treat as facts)

Auth: a single cookie `kagi_session=<token>`, where the token is the user's long-lived "session link" token from Kagi settings. No CSRF/OAuth/signing. A dead session surfaces as HTTP 401/403 **or** an HTML body where JSON was expected.

Base URL: `https://assistant.kagi.com`

Endpoints used by v0.1:

- `GET /api/init` → JSON model catalog: `models.models[]` each `{id, provider, display_name, context_window, supported, deprecated, retired}` plus a default model id. Filter to `supported && !deprecated && !retired`.
- `POST /api/v2/conversations` — creates a new thread AND sends the first prompt in one call. JSON body: `{content: string, model: string, enable_search: bool, personalization: bool}`. Response: `{conversation: {uuid, title, created_at, updated_at, is_saved, is_shared, model_name, folder_uuid}, branch_uuid: string, assistant_turn_uuid: string}`.
- `GET /api/v2/turns/{assistant_turn_uuid}/stream` with header `Accept: text/event-stream` — chunked SSE stream of the turn.
- `DELETE /api/conversations/{conversation_uuid}` — delete the thread. *(Go port, verified live: this is a soft delete — the thread goes to the trash, restorable via `POST .../restore`; `DELETE .../permanent` and `/api/conversations/deleted/purge` also exist and are unused.)*

SSE stream format:

- Frames separated by a blank line; both `\n\n` and `\r\n\r\n` occur. Lines beginning `data:` carry JSON; multiple `data:` lines in one frame are joined with `\n`. Ignore `id:`/`event:`/comment (`:`) lines.
- Normal frame: `{"event": {"type": "...", "node_id": "...", "turn_id": "...", "timestamp": "...", "payload": {...}}, "title": "...", "error": "..."}` (`title`/`error` optional).
- Terminal frame: `data: {"is_final": true}` (no `event` key).
- Event types (`event.type`):
  - `node.created` — `payload.kind` is `"text"` (the answer), `"thinking"` (reasoning), or `"activity"` (search/tool steps). Answer text comes ONLY from `kind === "text"` nodes, keyed by `node_id`.
  - `node.delta` — `payload.delta` is incremental markdown **per node**; the event carries the `node_id` it belongs to. Thinking nodes stream their own deltas (must be skipped for the answer).
  - `reference.added` — `payload` contains `ref_id` plus source metadata (a citation source). Answer text contains citation markers `[^ref_id]` (ref_id may contain a `#` suffix — use the part before `#`); markers can be split across deltas. Some models emit a leading space in the first text delta.
  - `turn.started` — `timestamp`.
  - `turn.completed` — `payload.usage: {input_tokens: int, output_tokens: int, cost_usd: number, model_version: string}`.
  - `turn.failed` / `turn.cancelled` — `payload.error_code`.
- Operational: connect timeout 20s, per-read timeout 120s, **no total request timeout** (long completions stream for minutes).

## v0.1 design (final)

### Surface

- `GET /health` — no upstream call; `{status: "ok", version}`. `GET /health?deep=1` — calls `GET /api/init`, ok only on 2xx JSON.
- `GET /v1/models` — OpenAI list format from the `/api/init` catalog (filtered as above), lazily cached with TTL (default 6h); serve stale on refresh failure; 503 if never fetched successfully.
- `POST /v1/chat/completions` — streaming and non-streaming, OpenAI Chat Completions shapes.
- Bind `127.0.0.1` by default. A Host allow-list (Starlette `TrustedHostMiddleware` semantics) allowing only `127.0.0.1[:port]` and `localhost[:port]`. No CORS middleware, ever.

### Reasoning effort (Go addition; wire facts verified live)

- Kagi wire: `POST /api/v2/conversations` accepts an optional `reasoning_effort` string. Global value set: `none`, `minimal`, `low`, `medium`, `high`, `xhigh`, `max`; each model supports a subset. The legacy v1 field `thinking_preset` does not apply to v2 and is never sent.
- `/api/init` (verified live): each catalog model carries `supports_thinking` (bool) and `parameters.reasoning_effort`, either `{"options": [...], "default": "<effort>" | null}` or `null` (no list; e.g. `ki_quick`, `mistral-large`). Examples: GLM options low/high/max, default max; an Opus-class model low…max, default high; Haiku none…max, default low; `minimax-m3` default null. When `parameters.reasoning_effort` is present it decides (null = no list); only when it is absent are older shape guesses tried (a top-level `reasoning_effort` list of strings or `{value|id|name}` objects, optionally wrapped in `{options|values: [...]}`). Absent/unrecognized = unknown. `GET /v1/models` exposes non-standard `reasoning_efforts` and `reasoning_effort_default` fields per model (omitted when unknown/null); model ids stay plain. Kagi itself accepts efforts outside a model's list (verified), so the proxy's per-model check is the only guard.
- Sources, first match wins: request-body `reasoning_effort` (string or null) > model-id effort suffix > `KAGI_REASONING_EFFORT` (validated at startup against the global set; unset = none) > none. None = omit the field (Kagi's per-model default).
- Model-id grammar: `requested = model-id *(":" suffix)`, `suffix = "online" / "offline" / effort`. The model id is everything before the first `:`. Suffixes are a set (order-insensitive, repeats allowed); unknown tokens, `online`+`offline`, or two different efforts → 400 `invalid_model_suffix`. `online`/`offline` keep their meaning; neither = `KAGI_SEARCH_DEFAULT`.
- Validation: a value outside the global set → 400 `invalid_reasoning_effort`. If a fresh catalog (not one being served stale after a failed refresh) lists the model's options: a request/suffix value outside them → 400 `unsupported_reasoning_effort` naming the supported values, before any Kagi call; an env default outside them is omitted for that model and logged once. Otherwise global-only validation, and Kagi's own error is mapped as usual.
- Thinking output stays dropped (text nodes only); reasoning passthrough is a possible future option.

### Thread lifecycle (Go addition; wire facts verified live)

- `POST /api/v2/conversations` accepts optional `is_saved` (bool). `false` = the UI's "Temporary" state: listed in the sidebar with a Temporary badge, `folder_uuid: null`, `temporary_duration` from account settings (e.g. `"24h"`), auto-expired by Kagi after the account's window. Omitted = the account default (`settings.thread_saving_mode`).
- `PATCH /api/conversations/{uuid}` with `{"is_saved": bool}` (also `folder_uuid`, `title`, `is_pinned`, `is_shared`) is the UI's Mark temporary/permanent toggle. Not used by the proxy. Account settings `thread_saving_mode` and `temporary_duration` are never touched.
- `KAGI_THREAD_LIFECYCLE` = `delete` (default; unchanged flow: no `is_saved`, DELETE after the turn → trash) | `temporary` (`is_saved: false`, no DELETE) | `keep` (no `is_saved`, no DELETE). Non-delete modes also skip the cleanup DELETE after failed turns.
- Deprecated alias `KAGI_DELETE_THREADS`: `true` → `delete`, `false` → `keep`; used alone it logs a deprecation warning at startup; set together with `KAGI_THREAD_LIFECYCLE` and disagreeing → startup error. Invalid values of either → startup error.
- Every successful chat response carries `X-Kagi-Thread: <conversation-uuid>` (set before the body starts, streaming or not).

### Proxy auth

Require `Authorization: Bearer <PROXY_API_KEY>` (config); missing/wrong → 401 with an OpenAI-style error body. (Defends against browser CSRF/DNS-rebinding to the localhost port.)

### Request normalization

- `content` may be a string, an array of parts, or `null`. Parts: `{type: "text"}` → join text; `{type: "image_url"}` → literal `[image omitted]` in v0.1 (vision deferred); other types → skip. `null` → empty string.
- Role mapping: `developer` → `system`; `tool` → flattened into the transcript as `Tool result: {content}`; `system` collected separately.
- Silently ignore (log once per process per key): `tools`, `tool_choice`, `n`, `logprobs`, `response_format`, `stop`, `seed`, `top_p`, `presence_penalty`, `frequency_penalty`, `user`. Accept `temperature` (validated as number, not forwarded — Kagi has no per-request temperature).
- `stream` (default false); `stream_options.include_usage: true` → emit the final usage chunk (with `choices: []`) — ONLY in that case.

### Flattening (v0.1: every request is flattened — no conversation caching yet)

Build one `content` string:

```
[System instructions]
{merged system content, or "None"}

[Conversation so far]
User: {message}
Assistant: {message}
Tool result: {message}

[Respond as the Assistant to the last message, following the system instructions above.]
```

- If the flattened text exceeds ~70% of the model's `context_window` (from the catalog; default 128000 if unknown), drop the OLDEST non-system turns and insert a line `[...earlier messages omitted...]`. Never drop the system block or the last message.

### Model resolution

- Requested model id may carry a `:online` / `:offline` suffix → `enable_search` true/false. Default (no suffix): true. Strip the suffix; the remainder is the Kagi model id passed through as `model`.
- If `model` is missing → use the catalog default; if no catalog → 503.
- Unknown/invalid model ids: pass through and let Kagi error (the catalog may be stale); map the error.

### Per-request flow

1. Acquire a global semaphore (max concurrent Kagi turns, default 1) with a bounded wait (default 30s). Timeout → 429 OpenAI-style error. The slot is held until the upstream stream is fully drained — even if the client disconnects.
2. `POST /api/v2/conversations` `{content, model, enable_search, personalization}` with headers `Cookie: kagi_session=…`, `Accept: application/json`, `Content-Type: application/json`, `User-Agent: kagi-openai-proxy/0.1 (personal)`. **Perform this POST (and start the stream GET, verifying status) before returning the StreamingResponse**, so failures get real HTTP status codes instead of mid-stream errors.
3. Error mapping (OpenAI error bodies): 401/403 or HTML-where-JSON-expected → `{type: "authentication_error", code: "kagi_session_expired"}` (401). Kagi 429 or turn-failure quota codes → 429 `rate_limit_error`, **never auto-retry a turn**. Unexpected shapes → 502 `{code: "kagi_protocol_drift"}`. Redact the session token and proxy key from every error message and log line (central redact helper).
4. `GET /api/v2/turns/{assistant_turn_uuid}/stream` (`Accept: text/event-stream`), same cookie. Parse SSE **line-based** (never `split(b"\n\n")` on raw bytes): accumulate lines, dispatch a frame on a blank line, handle `\r\n\r\n`, join multi-`data:` lines, ignore comments.

### Protocol isolation

All Kagi wire code lives in one module producing internal events only: `NodeCreated(node_id, kind)`, `TextDelta(node_id, text)`, `ReferenceAdded(ref_id, payload)`, `TurnCompleted(usage)`, `TurnFailed(code)`, `Final()`, `Unknown(type)`. The OpenAI translation layer never sees Kagi JSON.

### Streaming translation

- Emit the first chunk `{"delta": {"role": "assistant", "content": ""}}` immediately, before any Kagi text arrives.
- Forward `TextDelta` of `kind=text` nodes as content chunks. Skip thinking/activity nodes. A delta for an unknown node_id: treat as text, log once (warning).
- Strip leading whitespace of the very first text delta only.
- Citation rendering: maintain `references` (from `reference.added`, keyed by ref_id) and `cited` (ref_ids in order of first citation). Scan emitted text for `[^… ]` markers: a complete marker whose ref_id (before any `#`) is known → rewrite to `[^n]` where n = index in `cited` + 1 (append on first sight). Unknown ref_id → pass through unchanged. A marker opened (`[^`) but not yet closed → hold that tail in a buffer until it completes or the stream ends (then flush raw). Cap the held tail at 64 chars — beyond that, flush raw. This must work when a marker is split across deltas.
- Keep-alive: if ~15s pass with no text emitted, emit a chunk with `delta: {"content": ""}` (NOT SSE comment lines).
- Termination — treat ALL of these as terminal: `is_final` frame, `turn.completed` (finish emitting after it; keep consuming until EOF/`is_final`), or EOF. EOF with neither `is_final` nor `turn.completed` → truncated: mid-stream error chunk, no `[DONE]`.
- `turn.failed` / `turn.cancelled` / frame-level `error` → mid-stream error chunk `{"error": {message, type: "api_error", code: <error_code>}}`, close WITHOUT `[DONE]`.
- Empty-answer guard: if the turn completed but ZERO text-node bytes were emitted → error (pre-first-byte: 502; mid-stream: error chunk) whose message names the event types and node kinds actually seen. This converts silent protocol drift into loud failure.
- Final chunk: `delta: {}`, `finish_reason: "stop"`, then `data: [DONE]\n\n`.
- Chunk formatting: every frame exactly `data: {json}\n\n` (LF only); same `id` and `created` on all chunks; `object: "chat.completion.chunk"`; `choices[0].index: 0`. No gzip middleware (it buffers streams).

### Usage and cleanup

- On `turn.completed`: capture `{input_tokens, output_tokens, cost_usd, model_version}`. Non-streaming → fill the OpenAI `usage` field. Streaming → emit the usage chunk only when `include_usage`. Always log tokens + `cost_usd` (never the token/key values).
- After the turn ends (success, failure, or client disconnect), if the thread lifecycle is `delete` (Go port: `KAGI_THREAD_LIFECYCLE`, default `delete`; see "Thread lifecycle" below) → `DELETE /api/conversations/{conversation_uuid}`; log failures, never fail the response because of it. On client disconnect: the response generator is cancelled — spawn a background task that keeps reading the upstream stream to completion (bounded), then deletes the thread and releases the semaphore. Try/finally around the whole lifecycle so the semaphore can never leak.

### Config (env vars; document all in README)

`KAGI_SESSION_TOKEN` (required), `PROXY_API_KEY` (required), `PROXY_HOST=127.0.0.1`, `PROXY_PORT=8000`, `KAGI_BASE_URL=https://assistant.kagi.com`, `KAGI_THREAD_LIFECYCLE=delete` (Go port; replaces `KAGI_DELETE_THREADS=true`, kept as a deprecated alias), `KAGI_PERSONALIZATION=false`, `KAGI_SEARCH_DEFAULT=online`, `KAGI_MAX_CONCURRENCY=1`, `KAGI_QUEUE_WAIT_SECONDS=30`, `KAGI_CONNECT_TIMEOUT=20`, `KAGI_READ_TIMEOUT=120`, `KAGI_MODELS_TTL=21600`.

### Deliverables (Go port)

```
kagi-openai-proxy-go/
  README.md            # what it is, build (go build, cross-compile), setup (session link, env,
                       # run, launchd), Raycast providers.yaml instructions, honest ToS/risk
                       # note, the 499 → no-status deviation
  SPEC.md              # this spec
  go.mod               # module kagiopenaiproxy, no requirements
  cmd/kagi-openai-proxy/  # server binary
  cmd/probe/           # MANUAL live probe only (go run ./cmd/probe): cheapest model (whole-token
                       # hints), prompt "Reply with exactly: PONG", assert event sequence
                       # turn.started→node.created(text)→node.delta+→turn.completed→is_final,
                       # then DELETE the thread (also after a partial create); requires
                       # KAGI_SESSION_TOKEN; never automated/scheduled
  internal/            # config, kagi (protocol client), events, sse, citations, translate,
                       # server, probe, redact, logx, textutil, kagitest (offline fake + fixture)
  providers.yaml.example
  .gitignore, .gitattributes   # .gitattributes keeps the CRLF fixture byte-identical
```

### Tests (offline; an `httptest` fake for the Kagi endpoints)

- SSE parser: LF and CRLF frame endings; a frame split across read boundaries; multi-`data:` lines; comment/`id:` lines ignored; `{"is_final": true}` terminal.
- Citations: renumbering by first-citation order; marker split across deltas; unknown ref passthrough; 64-char cap flush; `#`-suffixed ref_ids.
- Translation: leading-space strip (first delta only); thinking/activity nodes skipped; first-chunk role delta; final chunk + `[DONE]`; error chunk without `[DONE]`.
- Flattening: system/developer merge; tool flattening; content parts (text join, image_url → `[image omitted]`); null content; truncation inserts the omission marker and keeps system + last message.
- Server end-to-end (httptest fake Kagi): streaming happy path (assert exact chunk sequence); non-streaming happy path with usage; include_usage chunk; Kagi 401 → 401 authentication_error; HTML body → session-expired; turn.failed mid-stream; empty-answer guard; EOF without terminal → truncated error; client disconnect → upstream drained + thread deleted + semaphore released (handler can record call order); queue timeout → 429; wrong bearer → 401; wrong Host header → rejected.
- Fixtures must include a realistic stream: a thinking node before the text node, several text deltas, a split citation marker, reference.added events, turn.completed with usage, is_final.

### Git

`git init`, commit everything (respecting .gitignore). Reviewers will review the entire tree as the change.

### Report back

Files created; build and run instructions; `go vet` and `go test -race` summary; mutation checks of the most safety-critical behaviors; deviations from the Python behavior; concerns.
