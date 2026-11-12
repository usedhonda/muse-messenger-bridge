# Handover protocol

Two handover channels run over one existing, authenticated SSH route to
the hub host. No new accounts, keys, or public endpoints are created for
the bridge.

## 1. Status snapshots

- The producer rebuilds the status DB every minute (deep collection
  hourly) in a single transaction per run: index rebuild, snapshot
  append, and update log commit together, so a backup never catches a
  half-written state. A failed source command, malformed collection, or failed
  deep refresh rolls back the run and preserves the previous status. Failure
  health is recorded in the selected database with a sanitized error; each
  source command has a 120-second timeout. Every successful run creates the
  complete schema; shallow runs leave uncollected deep tables empty so the
  receiver can query them without inventing message statistics.
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
2. Builds one UTF-8 JSONL batch **in memory** and durably records its
   exact batch id, file name, byte count, SHA-256, and coverage metadata
   locally before starting any remote operation. No bodies enter this
   state file. It then pipes the batch through the SSH connection's
   stdin to a unique private temporary file in the hub's content directory.
   The configured remote Python verifies byte count and SHA-256, then
   publishes the `.jsonl` name with an atomic hard link that refuses any
   existing destination. The producer writes no body file locally.
3. When a receiver endpoint is configured, calls it over the same SSH
   route after the file is final. Only an exit-0 response whose status
   is `batch_received` for the exact batch id advances the
   conversation's position to the batch's newest message timestamp.
   The SHA-256 of the bytes actually streamed is passed to the
   receiver, which verifies the file against it and echoes it in the
   response; a missing or different echoed SHA-256 is not
   acceptance. An unclear, timed-out, or
   non-zero result is not treated as success; the same batch id, file,
   and hash are preserved. A transfer timeout also preserves this
   record, with `transfer_unknown` status: the file may already be
   final, so later runs verify the recorded identity rather than fetch
   a replacement or stream it again. An interrupted process may leave
   `transferring`, which is handled with the same caution.
4. If the receiver is not configured or its acceptance does not
   complete, records the batch as `awaiting_batch_received` in its
   state file (metadata only) and sends a completion notice — metadata
   only: batch id, file name, bytes, SHA-256, fetch time, new/context
   counts, and shortfall flags. When the hub later replies
   `batch_received` for the exact batch id, the producer advances the
   position (`--confirm <batch_id>`). Confirmation takes the same
   process lock as a stream run, so it cannot overwrite concurrent
   pending-state changes. Until acceptance either way, the
   conversation is not re-batched, and later diffs are never skipped.
   On later runs, awaiting batches are first re-verified against the
   receiver with the same file and hash rather than rebuilt.
   The notice submission id is persisted before the notice is sent;
   a lost notice response is diagnosed with that same id on later
   runs and is never blindly submitted again. Unknown transfers whose
   files cannot be accepted remain pending for operator investigation.

### Batch format

First line, a header:

```json
{"type": "batch", "batch_id": "muse-c-<thread_key[:8]>-<newest_ts_ms>-<selection_sha256>",
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
- `text` is always a string. Missing or null source bodies (for example,
  attachment-only messages) are represented by an empty string; other
  non-string body shapes fail before transfer. Original string bodies
  and their existing reference hashes are preserved.
- Repeated rows with an identical published message identity and identical
  normalized fields are emitted once, before applying batch limits. If
  different content or sender metadata produces the same existing ref,
  collection fails without echoing the content or choosing a row to drop.
  The bridge's published identity remains content based; distinct source
  ids alone do not create distinct references for otherwise identical rows.
- `sender_key` is the SHA-256 of the source's stable sender ID when one
  is returned, otherwise null; no raw sender ID is exposed. `from_self`
  is null unless the source explicitly provides a self flag or an
  authenticated-account value to collate against — it is never inferred
  from names or text. (The reference producer collates `sender_id`
  against the authenticated account ID returned by the connector's
  own `check` function; when that basis is unavailable, `from_self`
  is null.)
- New messages and preceding context are distinguished by `is_context`.
- The local acknowledged position also records the hashed `message_ref`
  values at its newest timestamp. When a capped batch divides messages
  from the same millisecond, only the acknowledged refs are excluded from
  the next batch. The known `has_more` continuation is checked even when
  the thread activity timestamp has not advanced. These fields are
  metadata only and advance through the same receiver-confirmation path.
- Legacy positions without boundary refs retain their timestamp-only
  meaning until a newer batch is acknowledged. Previously lost boundary
  information cannot be reconstructed automatically from old state.
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
`--initial-context` therefore rejects a thread that already has an
acknowledged position; use normal diff delivery for that conversation.
Its `has_more` flag reports unavailable older context, not a request to
advance over subsequent new messages.

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

Batch ids include a SHA-256 over the full thread key and the ordered refs
of the selected messages, alongside the newest timestamp. This distinguishes
two capped slices ending in the same millisecond and threads whose short
key prefixes match. Existing pending ids keep their original identities.
Completion notices use deterministic submission ids, so a retry after an unknown
outcome re-presents the same identity instead of duplicating the
handover; the outcome is diagnosed by that id, never resent under a
new one.
