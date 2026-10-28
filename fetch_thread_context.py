#!/usr/bin/env python3
"""Fetch the recent flow of ONE Messenger conversation for bridge handover.

The shared status DB identifies conversations by thread_key (sha256 of the
conversation ID) and holds no message bodies. When the bridge side (the hub,
for Kai/Chi) needs the actual content of a specific conversation, this tool
resolves the thread_key back to the conversation through the live listing,
reads that conversation only, and prints its recent flow as JSON (sender
name, local time, body, attachment/reaction markers). Nothing is written to
the shared DB or any file by this tool; the caller decides what to do with
the output. No raw IDs appear in the output.

Window rule: the most recent --limit messages AND everything within the
last --days days, whichever is larger, up to --cap messages. A fixed small
count is not enough for busy groups (measured 2026-10-28: in the busiest
group the latest 30 messages covered only about 24 hours).
"""

import argparse
import hashlib
import json
import subprocess
import sys
from pathlib import Path
import time
from datetime import datetime, timezone


def _local_config():
    try:
        return json.loads(
            (Path(__file__).resolve().parent / "bridge.local.json").read_text(encoding="utf-8")
        )
    except Exception:
        return {}


_LOCAL = _local_config()

CLI = _LOCAL.get("messenger_cli", "/path/to/messenger_cli")
PAGE = 200


def run_cli(args):
    proc = subprocess.run(
        [CLI, *args], text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    if proc.returncode != 0:
        raise RuntimeError(f"messenger cli failed: {' '.join(args)}")
    return json.loads(proc.stdout)


def resolve_conversation(thread_key):
    first = run_cli(["threads", "--limit", "1"])
    cache = first.get("data", {}).get("_cache", {})
    count = cache.get("cached_thread_count") or 1
    listing = run_cli(["threads", "--limit", str(max(1, min(int(count), 10000)))])
    for item in listing.get("data", {}).get("threads", []):
        cid = item.get("conversation_id")
        if cid and hashlib.sha256(str(cid).encode()).hexdigest() == thread_key:
            return cid, item
    return None, None


def fetch_window(cid, min_count, days, cap):
    cutoff = (time.time() - days * 86400) * 1000 if days > 0 else 0
    seen = {}
    before = None
    thread_meta = {}
    while len(seen) < cap:
        args = ["messages", "--cid", cid, "--limit", str(PAGE)]
        if before is not None:
            args += ["--before", str(int(before))]
        result = run_cli(args)
        data = result.get("data", {})
        if not thread_meta:
            thread_meta = data
        rows = data.get("messages", [])
        if not rows:
            break
        new = 0
        for row in rows:
            mid = row.get("message_id")
            key = mid if mid else (row.get("timestamp"), row.get("sender_name"), row.get("body"))
            if key not in seen:
                seen[key] = row
                new += 1
        oldest = min(
            (r.get("timestamp") for r in rows if isinstance(r.get("timestamp"), (int, float))),
            default=None,
        )
        if oldest is None or new == 0:
            break
        before = oldest
        if len(seen) >= min_count and oldest <= cutoff:
            break
        if len(rows) < PAGE:
            break
    rows = sorted(
        seen.values(),
        key=lambda r: r.get("timestamp") if isinstance(r.get("timestamp"), (int, float)) else 0,
    )
    in_window = [r for r in rows if isinstance(r.get("timestamp"), (int, float)) and r["timestamp"] >= cutoff]
    chosen = in_window if len(in_window) >= min_count else rows[-min_count:]
    return thread_meta, chosen[-cap:]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--thread-key", required=True)
    parser.add_argument("--limit", type=int, default=100,
                        help="minimum number of most-recent messages to include")
    parser.add_argument("--days", type=int, default=7,
                        help="also include everything within this many days")
    parser.add_argument("--cap", type=int, default=500)
    parser.add_argument(
        "--sync", action="store_true", help="refresh this one thread before reading"
    )
    args = parser.parse_args()

    cid, thread = resolve_conversation(args.thread_key)
    if not cid:
        print("thread_key not found in the current listing", file=sys.stderr)
        return 2

    if args.sync:
        run_cli(["sync", "messages", "--cid", cid])

    data, rows = fetch_window(cid, args.limit, args.days, args.cap)
    messages = []
    for row in rows:
        ts = row.get("timestamp")
        sent_local = None
        msa = row.get("message_sent_at")
        if isinstance(msa, dict) and msa.get("user_local"):
            sent_local = msa["user_local"]
        elif isinstance(ts, (int, float)):
            sent_local = datetime.fromtimestamp(ts / 1000, tz=timezone.utc).isoformat()
        messages.append(
            {
                "sent_at": sent_local,
                "sender_name": row.get("sender_name"),
                "body": row.get("body"),
                "has_attachment": bool(row.get("attachments")),
                "reaction_count": len(row.get("reactions") or []),
            }
        )

    out = {
        "thread_key": args.thread_key,
        "display_name": data.get("thread_name") or thread.get("name"),
        "thread_type": data.get("thread_type") or thread.get("thread_type"),
        "is_unread": data.get("is_unread"),
        "message_count": len(messages),
        "covered_from": messages[0]["sent_at"] if messages else None,
        "covered_to": messages[-1]["sent_at"] if messages else None,
        "messages": messages,
    }
    print(json.dumps(out, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
