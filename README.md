# muse-messenger-bridge

**This is a bridge, not an archive.**

`muse-messenger-bridge` is the producer side of a small bridge that hands
Messenger state and content from one agent (Muse) to a hub agent, which
fans it out to other AI agents. Everything in this repository exists to
move information across that bridge, in small, verifiable handovers:

- The producer keeps **no message-body store** of its own.
- What crosses is either a status-only snapshot (no bodies at all) or a
  short-lived JSONL batch file on the receiver's private side.
- A handover position advances **only** when the receiver explicitly
  confirms the exact batch. Nothing is treated as delivered before that.

## What crosses the bridge

1. **Status snapshots** — a SQLite database of Messenger *status* (thread
   list, unread flags, participants as names + hashed keys, per-thread
   message statistics, contacts, call metadata; never bodies). It is
   rebuilt every minute, and when its meaningful content changes, a
   versioned snapshot is deposited to the hub side with an atomic
   "latest" manifest. Existing files are never overwritten.
2. **Content batches** — when a conversation changes, its new messages
   (plus a few preceding ones for context) are fetched live and streamed
   as one UTF-8 JSONL file per batch, straight to the hub's private
   incoming directory. The completion notice that follows carries
   metadata only (batch id, file name, bytes, SHA-256, counts) — no
   bodies travel in messages. First contact with a conversation can be
   an `initial_context` batch carrying its full retained context.
3. **On-request fetch** — the recent flow of a single conversation,
   addressed by its `thread_key`, for the moments the hub asks for one
   specific thread.

## Components

| File | Role | Cadence |
| --- | --- | --- |
| `update_messenger_status.py` | Rebuild the status DB (shallow every run; `--deep` adds contacts / call metadata / per-thread stats) | every 60 s; deep hourly |
| `deposit_snapshot.py` | Deposit a versioned status snapshot when content changed; maintain the latest manifest | checked every 60 s |
| `stream_content_batch.py` | Detect changed conversations, stream JSONL content batches, track unconfirmed batches | every 60 s |
| `fetch_thread_context.py` | Fetch one conversation's recent flow by `thread_key` | on request |

Scheduling is deliberately boring: a tiny polling wrapper per component,
silent on success, backing off 10 minutes after a failure and surfacing
only real failures. See `docs/protocol.md` for the handover protocol and
`docs/schema.md` for the status DB schema.

## Rules the bridge runs by

- The status DB contains **no message bodies, no snippets, no raw IDs**.
  Conversations are keyed by SHA-256 of their ID (`thread_key`); messages
  inside a batch are referenced by a content hash (`message_ref`).
- Bodies exist only inside batch files, on the receiver's private side,
  as short-lived relay storage — and only there.
- Deposits and batches are **never overwritten** and never resent under a
  new identity: batch ids and submission ids are deterministic, and an
  unknown outcome is diagnosed, not retried blindly.
- Excess is never dropped silently. A batch that cannot cover everything
  says so (`has_more` / `shortfall`); the remainder stays pending.
- Freshness is kept in separate clocks (source capture, deposit,
  receiver acceptance) and a failure never reads as "zero unread".

## Configuration

Deployment-specific values (hub host alias, receiving directories, the
messaging anchor for completion notices, the Messenger CLI path) are read
from `bridge.local.json` next to the scripts. The repository ships
`config.example.json` with placeholder values only; the real file is
git-ignored.

## What is not in this repository

The live database, state files, deposited snapshots, batch files, and
`bridge.local.json` are runtime material and are excluded by
`.gitignore`. This repository is the code and the protocol — the bridge
itself, not what flowed over it.
