#!/usr/bin/env python3
"""Content handover to the hub bridge by direct stream (JSONL batches).

Form (adopted 2026-10-28 at the owner's direction): when the Messenger
status DB shows a conversation changed, fetch that conversation live and
stream a UTF-8 JSONL batch straight to the hub private incoming
directory on the hub host over the existing SSH route. The payload is
built in memory and piped through ssh stdin — Muse writes no body file
or DB on its own side. The local state file holds metadata only (thread
keys, timestamps, batch records), never bodies.

Discipline:
- A conversation's handed position advances ONLY when the hub replies
  batch_received for the exact batch_id (see --confirm). The completion
  MSG (metadata only, no bodies) never advances it.
- Excess is never dropped: a batch holds at most CAP message lines;
  anything remaining stays above the handed position and forms the next
  batch (has_more marks this). A coverage shortfall is flagged, never
  silent.
- One batch per conversation per run at most; other changed
  conversations are handled in the same run, oldest change first.
"""

import hashlib
import json
import sqlite3
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import fetch_thread_context as ftc  # noqa: E402


def _local_config():
    try:
        return json.loads(
            (Path(__file__).resolve().parent / "bridge.local.json").read_text(encoding="utf-8")
        )
    except Exception:
        return {}


_LOCAL = _local_config()

BASE = Path(__file__).resolve().parent
DB = BASE / "messenger_status.sqlite"
STATE = BASE / "content-push-state.json"
CONN_DIR = Path(_LOCAL.get("tproj_connector_dir", str(Path.home() / "workspace" / "custom-connectors" / "custom.tproj")))
VM_CLIENT = CONN_DIR / ".venv" / "bin" / "python"
CLIENT = CONN_DIR / "bin" / "vm_client.py"
CONFIG = CONN_DIR / "client.json"
PARENT_MESSAGE = _LOCAL.get("notify_parent_message", "")  # hub thread anchor for completion notices
REMOTE_DIR = _LOCAL.get("remote_content_dir", "/path/to/incoming/muse/content")
SSH_HOST = _LOCAL.get("ssh_host", "bridge-host")
CONTEXT = 5
CAP = 500


def now_iso():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def iso_to_ms(value):
    if not value:
        return 0
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return 0
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def ms_to_iso(ms):
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat().replace("+00:00", "Z")


def load_state():
    if STATE.exists():
        state = json.loads(STATE.read_text(encoding="utf-8"))
    else:
        state = {"threads": {}, "pending": {}}
    state.setdefault("threads", {})
    state.setdefault("pending", {})
    state.setdefault("batches", {})
    return state


def save_state(state):
    tmp = STATE.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(state, ensure_ascii=False, indent=1), encoding="utf-8")
    tmp.replace(STATE)


def call_tool(name, args):
    args_file = CONN_DIR / f"stream-batch-{args['submission_id']}.json"
    args_file.write_text(json.dumps(args, ensure_ascii=False), encoding="utf-8")
    proc = subprocess.run(
        [str(VM_CLIENT), str(CLIENT), "--config", str(CONFIG),
         "call-tool", "--name", name, "--arguments-file", str(args_file)],
        text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=120,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"{name} failed: {proc.stderr[:200]}")
    outer = json.loads(proc.stdout)
    return json.loads(outer["content"][0]["text"])


def diagnose(submission_id):
    diag_file = CONN_DIR / f"stream-batch-diag-{submission_id}.json"
    diag_file.write_text(json.dumps({"submission_id": submission_id}), encoding="utf-8")
    proc = subprocess.run(
        [str(VM_CLIENT), str(CLIENT), "--config", str(CONFIG),
         "call-tool", "--name", "tproj_diagnose", "--arguments-file", str(diag_file)],
        text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=120,
    )
    if proc.returncode != 0:
        return {"hub_acceptance": "unknown"}
    outer = json.loads(proc.stdout)
    return json.loads(outer["content"][0]["text"])


def changed_threads(state):
    # Detection uses the thread index activity time, refreshed every minute
    # by the status updater. (Per-thread message stats refresh only on the
    # hourly deep run, so they lag real changes.) The batch builder re-reads
    # the conversation live and splits precisely by message timestamp, so an
    # activity blip without new messages simply produces no batch.
    con = sqlite3.connect(f"file:{DB}?mode=ro", uri=True, timeout=60)
    rows = con.execute(
        "SELECT thread_key, updated_at_utc FROM messenger_thread_index"
    ).fetchall()
    con.close()
    out = []
    for key, updated_iso in rows:
        updated_ms = iso_to_ms(updated_iso)
        handed = state["threads"].get(key, {}).get("last_handed_ts", 0)
        if updated_ms > handed:
            out.append((handed, key, updated_ms))
    out.sort()  # oldest handed position first
    return out


def normalize_rows(rows):
    msgs = []
    for row in rows:
        ts = row.get("timestamp")
        if not isinstance(ts, (int, float)):
            continue
        msgs.append({
            "ts_ms": int(ts),
            "sender": row.get("sender_name"),
            "text": row.get("body"),
            "has_attachment": bool(row.get("attachments")),
            "reaction_count": len(row.get("reactions") or []),
        })
    msgs.sort(key=lambda m: m["ts_ms"])
    return msgs


def build_batch(thread_key, handed_ms):
    cid, _thread = ftc.resolve_conversation(thread_key)
    if not cid:
        raise RuntimeError(f"thread_key not in current listing: {thread_key[:8]}")
    ftc.run_cli(["sync", "messages", "--cid", cid])
    # Latest CAP messages, no day window: the split below is by timestamp.
    _meta, rows = ftc.fetch_window(cid, 1, 0, CAP)
    msgs = normalize_rows(rows)
    new = [m for m in msgs if m["ts_ms"] > handed_ms]
    if not new:
        return None
    room = CAP - 1  # header line
    take_new = new[: max(0, room - CONTEXT)]
    first_new_idx = msgs.index(take_new[0])
    context = msgs[max(0, first_new_idx - CONTEXT):first_new_idx]
    has_more = len(new) > len(take_new)
    shortfall = bool(msgs) and handed_ms > 0 and msgs[0]["ts_ms"] > handed_ms and len(msgs) >= CAP
    upto_ms = take_new[-1]["ts_ms"]
    batch_id = f"muse-c-{thread_key[:8]}-{upto_ms}"
    fetched_at = now_iso()
    lines = [{
        "type": "batch",
        "batch_id": batch_id,
        "source": "muse",
        "thread_key": thread_key,
        "fetched_at_utc": fetched_at,
        "coverage": {
            "from_utc": ms_to_iso(take_new[0]["ts_ms"]),
            "to_utc": ms_to_iso(upto_ms),
            "new_count": len(take_new),
            "context_count": len(context),
        },
        "has_more": has_more,
        "shortfall": shortfall,
    }]
    for m in context:
        lines.append(message_line(thread_key, m, True))
    for m in take_new:
        lines.append(message_line(thread_key, m, False))
    payload = ("\n".join(json.dumps(x, ensure_ascii=False) for x in lines) + "\n").encode("utf-8")
    return {
        "batch_id": batch_id,
        "thread_key": thread_key,
        "upto_ms": upto_ms,
        "fetched_at_utc": fetched_at,
        "new_count": len(take_new),
        "context_count": len(context),
        "has_more": has_more,
        "shortfall": shortfall,
        "payload": payload,
    }


def build_initial_batch(thread_key):
    con = sqlite3.connect(f"file:{DB}?mode=ro", uri=True, timeout=60)
    row = con.execute(
        "SELECT message_count FROM messenger_thread_stats WHERE thread_key=?",
        (thread_key,),
    ).fetchone()
    con.close()
    retained = row[0] if row else None
    cid, _thread = ftc.resolve_conversation(thread_key)
    if not cid:
        raise RuntimeError(f"thread_key not in current listing: {thread_key[:8]}")
    ftc.run_cli(["sync", "messages", "--cid", cid])
    _meta, rows = ftc.fetch_window(cid, 1, 0, CAP)
    msgs = normalize_rows(rows)
    if not msgs:
        return None
    upto_ms = msgs[-1]["ts_ms"]
    batch_id = f"muse-c-{thread_key[:8]}-initial-{upto_ms}"
    fetched_at = now_iso()
    shortfall = bool(retained is not None and retained > len(msgs))
    lines = [{
        "type": "batch",
        "batch_kind": "initial_context",
        "initial_context": True,
        "batch_id": batch_id,
        "source": "muse",
        "thread_key": thread_key,
        "fetched_at_utc": fetched_at,
        "coverage": {
            "from_utc": ms_to_iso(msgs[0]["ts_ms"]),
            "to_utc": ms_to_iso(upto_ms),
            "new_count": 0,
            "context_count": len(msgs),
        },
        "retained_count": retained,
        "fetch_cap": CAP,
        "has_more": shortfall,
        "shortfall": shortfall,
    }]
    for m in msgs:
        lines.append(message_line(thread_key, m, True))
    payload = ("\n".join(json.dumps(x, ensure_ascii=False) for x in lines) + "\n").encode("utf-8")
    return {
        "batch_id": batch_id,
        "thread_key": thread_key,
        "upto_ms": upto_ms,
        "fetched_at_utc": fetched_at,
        "new_count": 0,
        "context_count": len(msgs),
        "has_more": shortfall,
        "shortfall": shortfall,
        "payload": payload,
    }


def message_line(thread_key, m, is_context):
    ref = hashlib.sha256(
        f"{thread_key}|{m['ts_ms']}|{m['sender']}|{m['text']}".encode("utf-8")
    ).hexdigest()
    return {
        "type": "message",
        "message_ref": ref,
        "sent_at_utc": ms_to_iso(m["ts_ms"]),
        "sender": m["sender"],
        "text": m["text"],
        "is_context": is_context,
        "has_attachment": m["has_attachment"],
        "reaction_count": m["reaction_count"],
    }


def stream_to_mac(batch):
    filename = f"muse-content-{batch['batch_id']}.jsonl"
    part = f"{REMOTE_DIR}/{filename}.part"
    final = f"{REMOTE_DIR}/{filename}"
    remote_cmd = (
        f"umask 077 && mkdir -p '{REMOTE_DIR}' && cat > '{part}' && "
        f"mv '{part}' '{final}' && wc -c < '{final}'"
    )
    proc = subprocess.run(
        ["ssh", SSH_HOST, remote_cmd],
        input=batch["payload"], stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        timeout=300,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"ssh stream failed: {proc.stderr.decode('utf-8', 'replace')[:200]}")
    remote_bytes = int(proc.stdout.decode().strip().split()[0])
    if remote_bytes != len(batch["payload"]):
        raise RuntimeError(f"byte mismatch: local {len(batch['payload'])} remote {remote_bytes}")
    return filename


def send_completion_msg(batch, filename):
    sha = hashlib.sha256(batch["payload"]).hexdigest()
    body = (
        "batch受渡しの完了連絡です（本文は含みません）。\n"
        f"batch_id: {batch['batch_id']}\n"
        f"ファイル名: {filename}\n"
        f"bytes: {len(batch['payload'])}\n"
        f"SHA256: {sha}\n"
        f"取得時刻: {batch['fetched_at_utc']}\n"
        f"新着: {batch['new_count']}件 / 文脈: {batch['context_count']}件\n"
        f"不足: {'あり（継続あり）' if (batch['has_more'] or batch['shortfall']) else 'なし'}\n"
        "受入確認は batch_received でお願いします。確認が来るまで、こちらの渡し済み位置は進めません。"
    )
    submission_id = f"muse-dh-stream-{batch['batch_id']}"
    result = call_tool("tproj_reply", {
        "message_id": PARENT_MESSAGE,
        "body": body,
        "submission_id": submission_id,
    })
    if result.get("hub_acceptance") != "committed" and result.get("delivery_pending") is not False:
        result = diagnose(submission_id)
    return submission_id, result.get("hub_acceptance")


def run():
    state = load_state()
    # A thread with an unconfirmed batch is not re-batched this run.
    busy = {b["thread_key"] for b in state["batches"].values()}
    sent = 0
    for _handed, key, _newest in changed_threads(state):
        if key in busy:
            continue
        handed = state["threads"].get(key, {}).get("last_handed_ts", 0)
        batch = build_batch(key, handed)
        if batch is None:
            # Stats said changed but nothing newer was fetched; leave as is.
            continue
        filename = stream_to_mac(batch)
        submission_id, acceptance = send_completion_msg(batch, filename)
        state["batches"][batch["batch_id"]] = {
            "thread_key": key,
            "upto_ts": batch["upto_ms"],
            "filename": filename,
            "bytes": len(batch["payload"]),
            "sha256": hashlib.sha256(batch["payload"]).hexdigest(),
            "fetched_at_utc": batch["fetched_at_utc"],
            "new_count": batch["new_count"],
            "context_count": batch["context_count"],
            "has_more": batch["has_more"],
            "shortfall": batch["shortfall"],
            "msg_submission_id": submission_id,
            "msg_hub_acceptance": acceptance,
            "state": "awaiting_batch_received",
        }
        save_state(state)
        sent += 1
    print(f"batches sent: {sent}")


def initial_context(thread_key):
    state = load_state()
    if any(b["thread_key"] == thread_key for b in state["batches"].values()):
        print("a batch is already awaiting confirmation for this thread")
        return 2
    batch = build_initial_batch(thread_key)
    if batch is None:
        print("no messages fetched")
        return 2
    filename = stream_to_mac(batch)
    sha = hashlib.sha256(batch["payload"]).hexdigest()
    state["batches"][batch["batch_id"]] = {
        "thread_key": thread_key,
        "kind": "initial_context",
        "upto_ts": batch["upto_ms"],
        "filename": filename,
        "bytes": len(batch["payload"]),
        "sha256": sha,
        "fetched_at_utc": batch["fetched_at_utc"],
        "new_count": 0,
        "context_count": batch["context_count"],
        "has_more": batch["has_more"],
        "shortfall": batch["shortfall"],
        "state": "awaiting_batch_received",
    }
    save_state(state)
    print(json.dumps({
        "batch_id": batch["batch_id"],
        "filename": filename,
        "bytes": len(batch["payload"]),
        "sha256": sha,
        "fetched_at_utc": batch["fetched_at_utc"],
        "new_count": 0,
        "context_count": batch["context_count"],
        "has_more": batch["has_more"],
        "shortfall": batch["shortfall"],
    }, ensure_ascii=False))
    return 0


def confirm(batch_id):
    state = load_state()
    batch = state["batches"].get(batch_id)
    if not batch:
        print(f"unknown batch: {batch_id}")
        return 2
    key = batch["thread_key"]
    entry = state["threads"].setdefault(key, {"last_handed_ts": 0})
    if batch["upto_ts"] > entry.get("last_handed_ts", 0):
        entry["last_handed_ts"] = batch["upto_ts"]
    del state["batches"][batch_id]
    save_state(state)
    print(f"confirmed {batch_id}; position advanced to {batch['upto_ts']}")
    return 0


def list_pending():
    state = load_state()
    print(json.dumps(state["batches"], ensure_ascii=False, indent=1))


if __name__ == "__main__":
    if len(sys.argv) >= 3 and sys.argv[1] == "--initial-context":
        raise SystemExit(initial_context(sys.argv[2]))
    if len(sys.argv) >= 3 and sys.argv[1] == "--confirm":
        raise SystemExit(confirm(sys.argv[2]))
    if len(sys.argv) >= 2 and sys.argv[1] == "--list-pending":
        list_pending()
        raise SystemExit(0)
    run()
