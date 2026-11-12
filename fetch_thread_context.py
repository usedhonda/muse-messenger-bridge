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
CLI_TIMEOUT_SECONDS = 120


class ContextFetchError(RuntimeError):
    """A source failure whose message contains no source IDs or content."""


def run_cli(args):
    operation = args[0] if args and args[0] in {"check", "threads", "messages", "sync"} else "request"
    try:
        proc = subprocess.run(
            [CLI, *args], text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            timeout=CLI_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired:
        raise ContextFetchError(f"messenger cli {operation} timed out") from None
    except (OSError, UnicodeError):
        raise ContextFetchError(f"messenger cli {operation} could not be read") from None
    if proc.returncode != 0:
        raise ContextFetchError(f"messenger cli {operation} failed")
    try:
        result = json.loads(proc.stdout)
    except (ValueError, TypeError):
        raise ContextFetchError(f"messenger cli {operation} returned invalid JSON") from None
    if not isinstance(result, dict) or result.get("success") is False:
        raise ContextFetchError(f"messenger cli {operation} returned an unsuccessful response")
    return result


def response_data(result, collection):
    data = result.get("data") if isinstance(result, dict) else None
    rows = data.get(collection) if isinstance(data, dict) else None
    if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
        raise ContextFetchError(f"messenger cli returned invalid {collection} data")
    return data


def resolve_conversation(thread_key):
    first = response_data(run_cli(["threads", "--limit", "1"]), "threads")
    cache = first.get("_cache", {})
    if not isinstance(cache, dict):
        raise ContextFetchError("messenger cli returned invalid thread cache metadata")
    count = cache.get("cached_thread_count")
    if count is None:
        count = 1
    if isinstance(count, str) and count.isascii() and count.isdecimal():
        try:
            count = int(count)
        except ValueError:
            raise ContextFetchError("messenger cli returned an invalid thread count") from None
    if type(count) is not int or count < 0:
        raise ContextFetchError("messenger cli returned an invalid thread count")
    listing = run_cli(["threads", "--limit", str(max(1, min(count, 10000)))])
    for item in response_data(listing, "threads")["threads"]:
        cid = item.get("conversation_id")
        if not isinstance(cid, (str, int)) or isinstance(cid, bool) or cid == "":
            raise ContextFetchError("messenger cli returned an invalid conversation identifier")
        if hashlib.sha256(str(cid).encode()).hexdigest() == thread_key:
            return str(cid), item
    return None, None


def validate_window(min_count, days, cap):
    if any(type(value) is not int for value in (min_count, days, cap)):
        raise ValueError("limit, days, and cap must be integers")
    if min_count < 1 or cap < 1 or min_count > cap or days < 0:
        raise ValueError("require 1 <= limit <= cap and days >= 0")


def validate_timestamp(row):
    ts = row.get("timestamp")
    if not isinstance(ts, (int, float)) or isinstance(ts, bool):
        raise ContextFetchError("messenger cli returned an invalid message timestamp")
    try:
        datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
    except (ValueError, OverflowError, OSError):
        raise ContextFetchError("messenger cli returned an invalid message timestamp") from None


def fetch_window(cid, min_count, days, cap):
    validate_window(min_count, days, cap)
    cutoff = (time.time() - days * 86400) * 1000 if days > 0 else None
    seen = {}
    before = None
    thread_meta = {}
    truncation_reason = None
    while len(seen) < cap:
        args = ["messages", "--cid", cid, "--limit", str(PAGE)]
        if before is not None:
            args += ["--before", str(int(before))]
        data = response_data(run_cli(args), "messages")
        if not thread_meta:
            thread_meta = dict(data)
        rows = data["messages"]
        if not rows:
            break
        new = 0
        for row in rows:
            validate_timestamp(row)
            mid = row.get("message_id")
            key = json.dumps(
                ["id", mid] if mid else
                ["content", row["timestamp"], row.get("sender_name"), row.get("body")],
                ensure_ascii=False, sort_keys=True,
            )
            if key not in seen:
                seen[key] = row
                new += 1
        oldest = min(row["timestamp"] for row in rows)
        if new == 0 or (before is not None and oldest >= before):
            truncation_reason = "pagination_stalled"
            break
        if cutoff is not None and len(seen) >= min_count and oldest < cutoff:
            break
        if len(rows) < PAGE:
            break
        if len(seen) >= cap:
            truncation_reason = "cap"
            break
        before = oldest
    rows = sorted(seen.values(), key=lambda row: row["timestamp"])
    in_window = rows if cutoff is None else [row for row in rows if row["timestamp"] >= cutoff]
    chosen = in_window if len(in_window) >= min_count else rows[-min_count:]
    if len(chosen) > cap:
        truncation_reason = "cap"
    thread_meta["context_truncation"] = {
        "truncated": truncation_reason is not None,
        "reason": truncation_reason,
    }
    return thread_meta, chosen[-cap:]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--thread-key", required=True)
    parser.add_argument("--limit", type=int, default=100,
                        help="minimum number of most-recent messages to include")
    parser.add_argument("--days", type=int, default=7,
                        help="also include everything within this many days; 0 reads retained context up to cap")
    parser.add_argument("--cap", type=int, default=500)
    parser.add_argument(
        "--sync", action="store_true", help="refresh this one thread before reading"
    )
    args = parser.parse_args()
    try:
        validate_window(args.limit, args.days, args.cap)
    except ValueError as exc:
        parser.error(str(exc))
    try:
        return print_context(args)
    except Exception as exc:
        # Unexpected library/shape errors must not echo source values either.
        detail = str(exc) if isinstance(exc, ContextFetchError) else type(exc).__name__
        print(f"error: context fetch failed: {detail}", file=sys.stderr)
        return 1


def print_context(args):
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
        if isinstance(msa, dict) and isinstance(msa.get("user_local"), str) and msa["user_local"]:
            sent_local = msa["user_local"]
        elif isinstance(ts, (int, float)):
            sent_local = datetime.fromtimestamp(ts / 1000, tz=timezone.utc).isoformat()
        messages.append(
            {
                "sent_at": sent_local,
                "sender_name": row.get("sender_name"),
                "body": row.get("body"),
                "has_attachment": bool(row.get("attachments")),
                "reaction_count": len(row["reactions"]) if isinstance(row.get("reactions"), (list, dict)) else 0,
            }
        )

    out = {
        "thread_key": args.thread_key,
        "display_name": data.get("thread_name") or thread.get("name"),
        "thread_type": data.get("thread_type") or thread.get("thread_type"),
        "is_unread": data.get("is_unread"),
        "message_count": len(messages),
        "context_truncation": data["context_truncation"],
        "covered_from": messages[0]["sent_at"] if messages else None,
        "covered_to": messages[-1]["sent_at"] if messages else None,
        "messages": messages,
    }
    print(json.dumps(out, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
