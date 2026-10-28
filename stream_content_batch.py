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
- A conversation's handed position advances ONLY when the receiver
  returns batch_received for the exact batch_id and SHA-256, either
  through the receiver CLI wired into this script or through the hub's
  MSG reply followed by --confirm. A completion MSG by itself (metadata
  only, no bodies) never advances it.
- Excess is never dropped: a batch holds at most CAP message lines;
  anything remaining stays above the handed position and forms the next
  batch (has_more marks this). A coverage shortfall is flagged, never
  silent.
- One batch per conversation per run at most; other changed
  conversations are handled in the same run, oldest change first.
"""

import fcntl
import hashlib
import json
import shlex
import sqlite3
import subprocess
import sys
from contextlib import contextmanager
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
REMOTE_STATUS_DIR = _LOCAL.get("remote_status_dir", "/path/to/incoming/muse")
SSH_HOST = _LOCAL.get("ssh_host", "bridge-host")
RECEIVER = _LOCAL.get("receiver") if isinstance(_LOCAL.get("receiver"), dict) else {}
RECEIVER_CWD = RECEIVER.get("cwd")
RECEIVER_ROOT = RECEIVER.get("root")
RECEIVER_PYTHON = RECEIVER.get("python", "/usr/bin/python3")
RECEIVER_MODULE = RECEIVER.get("module", "hub.messenger_relay")
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
    state.setdefault("health", {
        "last_attempt_at": None,
        "last_success_at": None,
        "last_attempt_status": None,
        "last_content_change_at": None,
        "last_transfer_at": None,
    })
    return state


def save_state(state):
    tmp = STATE.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(state, ensure_ascii=False, indent=1), encoding="utf-8")
    tmp.replace(STATE)


def payload_sha(batch):
    return hashlib.sha256(batch["payload"]).hexdigest()


def sender_key(value):
    if value is None or value == "":
        return None
    return hashlib.sha256(str(value).encode("utf-8")).hexdigest()


def health_state(state):
    return state.setdefault("health", {
        "last_attempt_at": None,
        "last_success_at": None,
        "last_attempt_status": None,
        "last_content_change_at": None,
        "last_transfer_at": None,
    })


def latest_content_change_iso():
    if not DB.exists():
        return None
    con = sqlite3.connect(f"file:{DB}?mode=ro", uri=True, timeout=60)
    try:
        row = con.execute("SELECT MAX(updated_at_utc) FROM messenger_thread_index").fetchone()
    finally:
        con.close()
    return row[0] if row and row[0] else None


def refresh_health_content_change(state):
    health = health_state(state)
    latest = latest_content_change_iso()
    if latest and iso_to_ms(latest) > iso_to_ms(health.get("last_content_change_at")):
        health["last_content_change_at"] = latest


def write_health_file(state):
    health = health_state(state)
    obj = {
        "source": "muse",
        "schema_version": 1,
        "last_attempt_at": health.get("last_attempt_at"),
        "last_success_at": health.get("last_success_at"),
        "last_attempt_status": health.get("last_attempt_status"),
        "last_content_change_at": health.get("last_content_change_at"),
        "last_transfer_at": health.get("last_transfer_at"),
    }
    payload = (json.dumps(obj, ensure_ascii=False) + "\n").encode("utf-8")
    filename = "messenger-acquisition-health.json"
    part = f"{REMOTE_STATUS_DIR}/{filename}.part"
    final = f"{REMOTE_STATUS_DIR}/{filename}"
    remote_cmd = (
        f"umask 077 && mkdir -p {shlex.quote(REMOTE_STATUS_DIR)} && "
        f"cat > {shlex.quote(part)} && chmod 0600 {shlex.quote(part)} && "
        f"mv {shlex.quote(part)} {shlex.quote(final)} && wc -c < {shlex.quote(final)}"
    )
    proc = subprocess.run(
        ["ssh", SSH_HOST, remote_cmd],
        input=payload, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=120,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"health file write failed: {proc.stderr.decode('utf-8', 'replace')[:200]}")
    remote_bytes = int(proc.stdout.decode().strip().split()[0])
    if remote_bytes != len(payload):
        raise RuntimeError(f"health byte mismatch: local {len(payload)} remote {remote_bytes}")
    return filename


def try_receiver_accept(batch_id, sha, filename):
    if not RECEIVER_CWD or not RECEIVER_ROOT:
        return False, {"status": "not_configured"}
    remote_cmd = " && ".join([
        f"cd {shlex.quote(str(RECEIVER_CWD))}",
        " ".join([
            shlex.quote(str(RECEIVER_PYTHON)),
            "-m",
            shlex.quote(str(RECEIVER_MODULE)),
            "--root",
            shlex.quote(str(RECEIVER_ROOT)),
            "accept",
            shlex.quote(filename),
            "--sha256",
            sha,
        ]),
    ])
    try:
        proc = subprocess.run(
            ["ssh", SSH_HOST, remote_cmd],
            text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=180,
        )
    except Exception as exc:
        return False, {"status": "receiver_unknown", "error": type(exc).__name__}
    if proc.returncode != 0:
        return False, {"status": "receiver_error", "returncode": proc.returncode}
    result = None
    stdout = (proc.stdout or "").strip()
    try:
        result = json.loads(stdout)
    except Exception:
        for line in reversed(stdout.splitlines()):
            try:
                result = json.loads(line)
                break
            except Exception:
                continue
    if not isinstance(result, dict):
        return False, {"status": "receiver_unknown"}
    returned_sha = result.get("sha256")
    accepted = (
        result.get("status") == "batch_received"
        and result.get("batch_id") == batch_id
        and returned_sha is not None
        and returned_sha == sha
    )
    return accepted, result


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


_SELF_ID = False  # False = unresolved, None = unavailable


def self_id():
    """The authenticated account's stable ID, from the connector's own
    check function. Used only as a collation basis for from_self; the
    raw value never leaves this side."""
    global _SELF_ID
    if _SELF_ID is False:
        try:
            data = ftc.run_cli(["check"])
            value = (data.get("data") or {}).get("user_id")
            _SELF_ID = str(value) if value is not None else None
        except Exception:
            _SELF_ID = None
    return _SELF_ID


def normalize_rows(rows):
    owner = self_id()
    msgs = []
    for row in rows:
        ts = row.get("timestamp")
        if not isinstance(ts, (int, float)):
            continue
        msgs.append({
            "ts_ms": int(ts),
            "sender": row.get("sender_name"),
            "sender_key": sender_key(row.get("sender_id")),
            "from_self": (str(row.get("sender_id")) == owner) if (owner is not None and row.get("sender_id") is not None) else None,
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
        "sender_key": m.get("sender_key"),
        "from_self": m.get("from_self"),
        "text": m["text"],
        "is_context": is_context,
        "has_attachment": m["has_attachment"],
        "reaction_count": m["reaction_count"],
    }


def stream_to_mac(batch):
    filename = f"muse-content-{batch['batch_id']}.jsonl"
    part = f"{REMOTE_DIR}/{filename}.part"
    final = f"{REMOTE_DIR}/{filename}"
    exists = subprocess.run(
        ["ssh", SSH_HOST, f"test -e '{final}'"],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=60,
    )
    if exists.returncode == 0:
        raise RuntimeError(
            f"remote file already exists for {batch['batch_id']}; refusing to overwrite"
        )
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


def retry_pending_accepts(state):
    """Re-verify awaiting batches against the receiver with their
    recorded file/hash; advance positions for any now accepted. A batch
    is never rebuilt while its finalized file exists."""
    advanced = 0
    for batch_id, record in list(state["batches"].items()):
        accepted, _result = try_receiver_accept(
            batch_id, record.get("sha256"), record.get("filename")
        )
        if not accepted:
            continue
        entry = state["threads"].setdefault(record["thread_key"], {"last_handed_ts": 0})
        if record["upto_ts"] > entry.get("last_handed_ts", 0):
            entry["last_handed_ts"] = record["upto_ts"]
        del state["batches"][batch_id]
        advanced += 1
        print(f"auto-accepted pending {batch_id}; position advanced to {record['upto_ts']}")
    if advanced:
        save_state(state)
    return advanced


def _run(state=None):
    if state is None:
        state = load_state()
    retry_pending_accepts(state)
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
        health_state(state)["last_transfer_at"] = now_iso()
        accepted, receiver_result = try_receiver_accept(batch["batch_id"], payload_sha(batch), filename)
        if accepted:
            entry = state["threads"].setdefault(key, {"last_handed_ts": 0})
            if batch["upto_ms"] > entry.get("last_handed_ts", 0):
                entry["last_handed_ts"] = batch["upto_ms"]
            save_state(state)
            print(f"auto-accepted {batch['batch_id']}; position advanced to {batch['upto_ms']}")
            sent += 1
            continue
        if receiver_result.get("status") != "not_configured":
            print(
                f"receiver auto-accept not completed for {batch['batch_id']}: "
                f"{receiver_result.get('status')}"
            )
        # Persist the pending record BEFORE the completion notice: if
        # the notice fails, the finalized file's metadata survives and
        # later runs re-verify the receiver against the same file/hash
        # instead of rebuilding (which the no-overwrite guard rejects).
        state["batches"][batch["batch_id"]] = {
            "thread_key": key,
            "upto_ts": batch["upto_ms"],
            "filename": filename,
            "bytes": len(batch["payload"]),
            "sha256": payload_sha(batch),
            "fetched_at_utc": batch["fetched_at_utc"],
            "new_count": batch["new_count"],
            "context_count": batch["context_count"],
            "has_more": batch["has_more"],
            "shortfall": batch["shortfall"],
            "msg_submission_id": None,
            "msg_hub_acceptance": None,
            "state": "awaiting_batch_received",
        }
        save_state(state)
        submission_id, acceptance = send_completion_msg(batch, filename)
        state["batches"][batch["batch_id"]]["msg_submission_id"] = submission_id
        state["batches"][batch["batch_id"]]["msg_hub_acceptance"] = acceptance
        save_state(state)
        sent += 1
    print(f"batches sent: {sent}")
    return sent


def _initial_context(thread_key, state=None):
    if state is None:
        state = load_state()
    if any(b["thread_key"] == thread_key for b in state["batches"].values()):
        print("a batch is already awaiting confirmation for this thread")
        return 2
    batch = build_initial_batch(thread_key)
    if batch is None:
        print("no messages fetched")
        return 2
    filename = stream_to_mac(batch)
    sha = payload_sha(batch)
    health_state(state)["last_transfer_at"] = now_iso()
    accepted, receiver_result = try_receiver_accept(batch["batch_id"], sha, filename)
    if accepted:
        entry = state["threads"].setdefault(thread_key, {"last_handed_ts": 0})
        if batch["upto_ms"] > entry.get("last_handed_ts", 0):
            entry["last_handed_ts"] = batch["upto_ms"]
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
            "receiver_status": receiver_result.get("status"),
            "position_advanced": True,
        }, ensure_ascii=False))
        return 0
    if receiver_result.get("status") != "not_configured":
        print(
            f"receiver auto-accept not completed for {batch['batch_id']}: "
            f"{receiver_result.get('status')}"
        )
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


@contextmanager
def single_run():
    lock_path = BASE / "stream_content_batch.lock"
    with open(lock_path, "w") as fh:
        try:
            fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            yield False
            return
        yield True


def run():
    with single_run() as acquired:
        if not acquired:
            print("another run in progress; skipped")
            return
        state = load_state()
        health = health_state(state)
        health["last_attempt_at"] = now_iso()
        refresh_health_content_change(state)
        save_state(state)
        try:
            _run(state)
        except Exception:
            health = health_state(state)
            health["last_attempt_status"] = "error"
            refresh_health_content_change(state)
            save_state(state)
            try:
                write_health_file(state)
            except Exception as exc:
                print(f"health file update failed: {exc}")
            raise
        health = health_state(state)
        health["last_attempt_status"] = "ok"
        health["last_success_at"] = now_iso()
        refresh_health_content_change(state)
        save_state(state)
        try:
            write_health_file(state)
        except Exception as exc:
            print(f"health file update failed: {exc}")


def initial_context(thread_key):
    with single_run() as acquired:
        if not acquired:
            print("another run in progress; skipped")
            return 2
        return _initial_context(thread_key)


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
