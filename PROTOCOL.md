# Kagi Assistant wire protocol

What the proxy relies on. Kagi does not document this API. Every fact below was seen
against live Kagi (latest check 2026-11-06, model `glm-5-3-flash`) or read from the
Assistant web app's JavaScript, as marked. `cmd/probe` re-checks the turn, follow-up,
and cancel flows.

## Auth

One cookie: `kagi_session=<token>`, the *Session Link* token. No CSRF token or signing.
A dead session shows up as HTTP 401/403, a redirect, or HTML where JSON was expected.

## Endpoints

| Call | Used for |
| --- | --- |
| `GET /api/init` | Model catalog and account state |
| `POST /api/v2/conversations` | New thread plus its first turn |
| `POST /api/v2/branches/{branch_uuid}/respond` | Follow-up turn in a thread |
| `GET /api/v2/turns/{assistant_turn_uuid}/stream` | The turn's SSE stream (`Accept: text/event-stream`) |
| `POST /api/v2/turns/{assistant_turn_uuid}/cancel` | Stop a running turn |
| `DELETE /api/conversations/{conversation_uuid}` | Move a thread to the trash |

### `GET /api/init`

- `models.models[]`: `{id, provider, display_name, context_window, supported,
  deprecated, retired, parameters}`. Usable = `supported && !deprecated && !retired`.
- `models.default`: the default model id (a string, e.g. `ki_quick`).
- `parameters.reasoning_effort`: `{"options": [...], "default": "<effort>" | null}`, or
  `null` for models without a list. Global effort values: `none`, `minimal`, `low`,
  `medium`, `high`, `xhigh`, `max`. Kagi accepts an effort outside a model's options
  without error.
- `billing.usage`: the account's Assistant meter (`interaction_count`,
  `interaction_limit`, `included_cost_usd`, `included_cost_limit_usd`).

### New thread and follow-up

Create body:

```json
{"content": "...", "model": "glm-5-3-flash", "enable_search": true,
 "personalization": false, "reasoning_effort": "low", "is_saved": false}
```

`reasoning_effort` and `is_saved` are optional. Without `reasoning_effort` the model's
default applies. `is_saved: false` makes a *Temporary* thread that Kagi deletes after
the account's temporary duration; without it the account's thread saving mode applies.

The create response has `conversation_uuid`, `branch_uuid`, `assistant_turn_uuid`,
`user_turn_uuid`, and a `conversation` object (whose `uuid` is the same thread).

Follow-up body: the same fields without `is_saved`, plus `"operation": "new_turn"`.
The response has the same ids at the top level; `branch_uuid` stays the same. The
follow-up sees the earlier turns, including their search results.

### Cancel and delete

- `POST .../cancel` answers 204. The turn stream then ends within about 0.5 s with
  `node.completed {status: cancelled}`, `turn.cancelled {reason: "cancelled"}` (no
  `error_code`), and `{"is_final": true, "error": "Cancelled", "persisted": true}`.
- `DELETE /api/conversations/{uuid}` is a soft delete: the thread goes to the trash
  and can be restored. From the web app's JavaScript (unused): `.../restore`,
  `.../permanent`, `/api/conversations/deleted/purge`, and
  `PATCH /api/conversations/{uuid}` with `{"is_saved": bool}`.
- A thread opens in the web app at `https://assistant.kagi.com/chat/{conversation_uuid}`
  (from the JavaScript).

## Turn stream

SSE. Frames end with a blank line (`\n\n` or `\r\n\r\n`); `data:` lines carry JSON and
several in one frame join with `\n`; `id:`, `event:` and `:` comment lines are ignored.

A normal frame is `{"event": {"type", "node_id", "parent_node_id", "turn_id",
"timestamp", "payload"}}`, sometimes with a frame-level `error`. Other frames:
`{"title": "..."}` (the thread title) and the last frame
`{"is_final": true, "billing": {...}}`.

| Event | Payload (fields the proxy reads) |
| --- | --- |
| `turn.started` | `root_node_id`, `branch_uuid` |
| `node.created` | `kind`: `assistant_message` (root), `text` (the answer), or `activity`. Activity `type`: `config`, `tool_call` (with `tool_name`, e.g. `search`), or `thinking` (with `content`) |
| `node.updated` | `status`; for a search, `arguments.query` |
| `node.delta` | `delta`: incremental Markdown for `node_id` (text and thinking nodes) |
| `node.completed` | `status` (`completed` or `cancelled`) and, for text and thinking, the full `content` |
| `reference.added` | `ref_id`, `url`, `title`, `snippet`, `favicon`, `reference_kind` (`search`), `extra.published_at`; `node_id` is null |
| `turn.completed` | `usage`: `input_tokens`, `output_tokens`, `cost_usd`, `model_version`, `duration_ms`, `tokens_per_second` |
| `turn.failed` | `error_code` |
| `turn.cancelled` | `reason` (a user cancel has no `error_code`; one is used if present) |
| `source.read` | Which sources the model read (ignored) |

Answer text is only the `node.delta` of `text` nodes. It cites sources as `[^ref_id]`;
a `#suffix` may follow the id (`[^r_b2#p3]`, also `#start-end` ranges), and a marker
can be split across deltas. A search's `reference.added` events all arrive before the
answer's text node starts. The model sometimes ends with footnote definitions
(`[^ref_id]: url`) or its own `Sources:` list.

The web app also handles `node.replaced`, `node.failed`, `node.cancelled`,
`turn.awaiting_input`, `citation.bound`, `citation.passages`, and `artifact.citations`.
These were not seen in search turns; the proxy logs them once as unknown.
