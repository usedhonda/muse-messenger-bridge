# Handover protocol

Two handover channels run over one existing, authenticated SSH route to
the hub host. No new accounts, keys, or public endpoints are created for
the bridge.

## 1. Status snapshots

- The producer rebuilds the status DB every minute (deep collection
  hourly) in a single transaction per run: index rebuild, snapshot
  append, and update log commit together, so a backup never catches a
  half-written state.
- `deposit_snapshot.py` hashes the *stable* content (status counts,
  thread index, per-thread stats, contacts, calls, stable meta —
  capture timestamps and history tables excluded). Only when that hash
  changes does it deposit:
  - a consistent snapshot made through the SQLite backup API, named
    `messenger-status-bridge-<UTC stamp>.sqlite3`, transferred to a
    temporary name in the hub's incoming directory, verified by size
    and SHA-256, then renamed to its final name (mode 0600);
  - an atomically replaced pointer manifest,
    `messenger-status-latest.json`, with file name, bytes, SHA-256,
    backup UTC, schema version, content hash, and previous file.
- Existing versions are never overwritten or deleted by the producer.
  A failed deposit changes nothing on the receiving side; the latest
  successful version simply stays in place.

## 2. Content batches (JSONL stream)

When the status DB's thread index shows a conversation's activity time
ahead of its handed position, `stream_content_batch.py`:

1. Refreshes that one conversation at the source and reads it live.
2. Builds one UTF-8 JSONL batch **in memory** and pipes it through the
   SSH connection's stdin to `<name>.part` in the hub's content
   directory, then renames it to `.jsonl` after a clean close and a
   byte-count check. The producer writes no body file locally.
3. When a receiver endpoint is configured, calls it over the same SSH
   route after the file is final. Only an exit-0 response whose status
   is `batch_received` for the exact batch id advances the
   conversation's position to the batch's newest message timestamp.
   The SHA-256 of the bytes actually streamed is passed to the
   receiver, which verifies the file against it and echoes it in the
   response; a missing or different echoed SHA-256 is not
   acceptance. An unclear, timed-out, or
   non-zero result is not treated as success; the same batch id, file,
   and hash are preserved.
4. If the receiver is not configured or its acceptance does not
   complete, records the batch as `awaiting_batch_received` in its
   state file (metadata only) — the record is persisted before the
   notice is sent, so a failed notice cannot strand a finalized
   file — and sends a completion notice — metadata
   only: batch id, file name, bytes, SHA-256, fetch time, new/context
   counts, and shortfall flags. When the hub later replies
   `batch_received` for the exact batch id, the producer advances the
   position (`--confirm <batch_id>`). Until acceptance either way, the
   conversation is not re-batched, and later diffs are never skipped.
   On later runs, awaiting batches are first re-verified against the
   receiver with the same file and hash rather than rebuilt.

### Batch format

First line, a header:

```json
{"type": "batch", "batch_id": "muse-c-<thread_key[:8]>-<newest_ts_ms>",
 "source": "muse", "thread_key": "<sha256 hex>",
 "fetched_at_utc": "<ISO-8601 UTC>",
 "coverage": {"from_utc": "...", "to_utc": "...",
              "new_count": 0, "context_count": 0},
 "has_more": false, "shortfall": false}
```

Then one line per message, oldest first:

```json
{"type": "message", "message_ref": "<sha256 hex>",
 "sent_at_utc": "<ISO-8601 UTC>", "sender": "<name as at the source>",
 "sender_key": "<sha256 hex or null>", "from_self": null,
 "text": "<original text>", "is_context": false,
 "has_attachment": false, "reaction_count": 0}
```

- `message_ref` is a hash of (thread_key, timestamp, sender, text) —
  stable across fetches, and no raw source ID is exposed.
- `sender_key` is the SHA-256 of the source's stable sender ID when one
  is returned, otherwise null; no raw sender ID is exposed. `from_self`
  is null unless the source explicitly provides a self flag or an
  authenticated-account value to collate against — it is never inferred
  from names or text. (The reference producer collates `sender_id`
  against the authenticated account ID returned by the connector's
  own `check` function; when that basis is unavailable, `from_self`
  is null.)
- New messages and preceding context are distinguished by `is_context`.
- A batch holds at most 500 message lines. If more new messages exist,
  the batch takes the oldest slice and sets `has_more`; the rest forms
  the next batch after confirmation. If the fetch window cannot reach
  back to the handed position, `shortfall` is set — a gap is flagged,
  never silently absorbed.

### Initial context

A conversation's first batch may be an `initial_context` batch
(`"batch_kind": "initial_context"`, `new_count: 0`, every message marked
as context, up to the 500-line cap of currently retained messages).
It seeds the hub with the existing context. Receiving it means only
that the retained range arrived — it does not mark anything read,
answered, or reminded, and it never becomes a reason to skip later
diffs.

### Health signal

Each stream run also atomically replaces
`messenger-acquisition-health.json` in the hub's incoming status
directory (mode 0600, no bodies): `source`, `schema_version`,
`last_attempt_at`, `last_success_at`, `last_attempt_status`
(`ok`/`error`), `last_content_change_at`, and `last_transfer_at`,
with unknown values as null and all times in UTC ISO-8601. Source
message times and observation times are kept distinct.

## 3. On-request fetch

`fetch_thread_context.py --thread-key <key>` resolves the key against
the live listing, optionally refreshes that one conversation, and
prints its recent flow (sender, local time, body, attachment/reaction
markers) as JSON. Default window: the most recent 100 messages and
everything within 7 days, whichever is larger, capped at 500, paged
with explicit truncation reporting. It writes nothing anywhere.

## Identities and retries

Batch ids derive from (thread, newest included message) and completion
notices use deterministic submission ids, so a retry after an unknown
outcome re-presents the same identity instead of duplicating the
handover; the outcome is diagnosed by that id, never resent under a
new one.
