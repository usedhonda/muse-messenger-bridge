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
import os
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
    descriptor = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8") as fh:
        os.fchmod(fh.fileno(), 0o600)
        json.dump(state, fh, ensure_ascii=False, indent=1)
        fh.flush()
        os.fsync(fh.fileno())
    tmp.replace(STATE)
    directory = os.open(STATE.parent, os.O_RDONLY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


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
        entry = state["threads"].get(key, {})
        handed = entry.get("last_handed_ts", 0)
        if updated_ms > handed or entry.get("has_more", False):
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


def normalize_rows(rows, thread_key=None):
    owner = self_id()
    msgs = []
    seen = {}
    for row in rows:
        ts = row.get("timestamp")
        if not isinstance(ts, (int, float)):
            continue
        body = row.get("body")
        if body is None:
            body = ""
        elif not isinstance(body, str):
            raise RuntimeError("message body must be a string or null")
        message = {
            "ts_ms": int(ts),
            "sender": row.get("sender_name"),
            "sender_key": sender_key(row.get("sender_id")),
            "from_self": (str(row.get("sender_id")) == owner) if (owner is not None and row.get("sender_id") is not None) else None,
            "text": body,
            "has_attachment": bool(row.get("attachments")),
            "reaction_count": len(row.get("reactions") or []),
        }
        if thread_key is not None:
            ref = message_line(thread_key, message, False)["message_ref"]
            previous = seen.get(ref)
            if previous is not None:
                if previous != message:
                    raise RuntimeError("conflicting messages share a message_ref")
                continue
            seen[ref] = message
        msgs.append(message)
    msgs.sort(key=lambda m: m["ts_ms"])
    return msgs


def build_batch(thread_key, handed_ms, handed_refs=None):
    cid, _thread = ftc.resolve_conversation(thread_key)
    if not cid:
        raise RuntimeError(f"thread_key not in current listing: {thread_key[:8]}")
    ftc.run_cli(["sync", "messages", "--cid", cid])
    # Latest CAP messages, no day window: the split below is by timestamp.
    _meta, rows = ftc.fetch_window(cid, 1, 0, CAP)
    msgs = normalize_rows(rows, thread_key)
    msgs.sort(key=lambda m: (m["ts_ms"], message_ref(thread_key, m)))
    known = set(handed_refs or [])
    new = [m for m in msgs if m["ts_ms"] > handed_ms or (
        handed_refs is not None and m["ts_ms"] == handed_ms
        and message_ref(thread_key, m) not in known
    )]
    if not new:
        return None
    room = CAP - 1  # header line
    take_new = new[: max(0, room - CONTEXT)]
    first_new_idx = msgs.index(take_new[0])
    context = msgs[max(0, first_new_idx - CONTEXT):first_new_idx]
    has_more = len(new) > len(take_new)
    truncated = (_meta.get("context_truncation") or {}).get("truncated") is True
    shortfall = bool(msgs) and msgs[0]["ts_ms"] > handed_ms and (
        truncated or (handed_ms > 0 and len(rows) >= CAP)
    )
    upto_ms = take_new[-1]["ts_ms"]
    batch_id = batch_identity(thread_key, upto_ms, take_new)
    upto_refs = sorted({message_ref(thread_key, m) for m in take_new if m["ts_ms"] == upto_ms})
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
        "upto_refs": upto_refs,
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
    msgs = normalize_rows(rows, thread_key)
    msgs.sort(key=lambda m: (m["ts_ms"], message_ref(thread_key, m)))
    if not msgs:
        return None
    upto_ms = msgs[-1]["ts_ms"]
    batch_id = batch_identity(thread_key, upto_ms, msgs, initial=True)
    fetched_at = now_iso()
    truncated = (_meta.get("context_truncation") or {}).get("truncated") is True
    shortfall = bool(retained is not None and retained > len(msgs)) or truncated
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
        "upto_refs": sorted({message_ref(thread_key, m) for m in msgs if m["ts_ms"] == upto_ms}),
        "fetched_at_utc": fetched_at,
        "new_count": 0,
        "context_count": len(msgs),
        "has_more": shortfall,
        "shortfall": shortfall,
        "payload": payload,
    }


def message_ref(thread_key, m):
    return hashlib.sha256(
        f"{thread_key}|{m['ts_ms']}|{m['sender']}|{m['text']}".encode("utf-8")
    ).hexdigest()


def batch_identity(thread_key, upto_ms, messages, initial=False):
    # The timestamp alone cannot distinguish capped slices that end at
    # the same millisecond. Include the full thread key and selected refs
    # without changing the existing message_ref format.
    refs = [message_ref(thread_key, m) for m in messages]
    digest = hashlib.sha256("\n".join([thread_key, *refs]).encode("utf-8")).hexdigest()
    kind = "initial-" if initial else ""
    return f"muse-c-{thread_key[:8]}-{kind}{upto_ms}-{digest}"


def message_line(thread_key, m, is_context):
    return {
        "type": "message",
        "message_ref": message_ref(thread_key, m),
        "sent_at_utc": ms_to_iso(m["ts_ms"]),
        "sender": m["sender"],
        "sender_key": m.get("sender_key"),
        "from_self": m.get("from_self"),
        "text": m["text"],
        "is_context": is_context,
        "has_attachment": m["has_attachment"],
        "reaction_count": m["reaction_count"],
    }


# Executed on the receiver through the configured Python interpreter. The
# producer still sends bodies only through SSH stdin, never a local body file.
REMOTE_CONTENT_WRITER = r'''
import hashlib
import os
from pathlib import Path
import sys
import tempfile

def publish():
    directory, filename, expected_size, expected_sha = sys.argv[1:]
    if Path(filename).name != filename or filename in {".", ".."}:
        raise ValueError("invalid batch filename")
    expected_size = int(expected_size)
    os.makedirs(directory, mode=0o700, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=".muse-content-", suffix=".part", dir=directory)
    try:
        total = 0
        digest = hashlib.sha256()
        with os.fdopen(fd, "wb") as output:
            while True:
                chunk = sys.stdin.buffer.read(65536)
                if not chunk:
                    break
                total += len(chunk)
                if total > expected_size:
                    raise ValueError("batch size mismatch")
                output.write(chunk)
                digest.update(chunk)
            if total != expected_size or digest.hexdigest() != expected_sha:
                raise ValueError("batch integrity mismatch")
            output.flush()
            os.fsync(output.fileno())
        # link creates the final name atomically and fails if any entry exists.
        # An existence check followed by rename would overwrite a racing writer.
        os.link(temporary, os.path.join(directory, filename))
        directory_fd = os.open(directory, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
        print(total)
    finally:
        os.unlink(temporary)

try:
    publish()
except Exception as error:
    # No payload bytes or deployment-specific path appears in diagnostics.
    print("content publication failed: " + type(error).__name__, file=sys.stderr)
    raise SystemExit(1)
'''


def stream_to_mac(batch):
    filename = f"muse-content-{batch['batch_id']}.jsonl"
    if Path(filename).name != filename or "\0" in filename:
        raise ValueError("invalid batch filename")
    remote_cmd = " ".join(shlex.quote(str(value)) for value in (
        RECEIVER_PYTHON, "-c", REMOTE_CONTENT_WRITER, REMOTE_DIR,
        filename, len(batch["payload"]), payload_sha(batch),
    ))
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


def transfer_pending_batch(state, batch, kind=None):
    """Reserve the exact identity durably before any remote side effect.

    A timeout can occur after the receiver-side file is finalized. Keep
    its metadata even then: subsequent runs verify this identity rather
    than rebuild a new payload or replay an uncertain transfer.
    """
    filename = f"muse-content-{batch['batch_id']}.jsonl"
    record = {
        "thread_key": batch["thread_key"],
        "upto_ts": batch["upto_ms"],
        "upto_refs": batch.get("upto_refs"),
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
        "state": "transferring",
    }
    if kind is not None:
        record["kind"] = kind
    state["batches"][batch["batch_id"]] = record
    save_state(state)
    try:
        stream_to_mac(batch)
    except Exception:
        record["state"] = "transfer_unknown"
        save_state(state)
        raise
    record["state"] = "awaiting_batch_received"
    health_state(state)["last_transfer_at"] = now_iso()
    save_state(state)
    return filename


def advance_checkpoint(state, key, upto_ts, upto_refs=None, has_more=False):
    """Advance only an acknowledged position, retaining ties at its edge.

    Older state has no boundary refs. Preserve its timestamp-only
    semantics until a newer timestamp is accepted, rather than replaying
    messages that an earlier version already handed over.
    """
    entry = state["threads"].setdefault(key, {"last_handed_ts": 0})
    previous = entry.get("last_handed_ts", 0)
    if upto_ts < previous:
        return
    if upto_ts > previous:
        entry["last_handed_ts"] = upto_ts
        entry.pop("last_handed_refs", None)
        if upto_refs is not None:
            entry["last_handed_refs"] = sorted(set(upto_refs))
    elif upto_refs is not None and ("last_handed_refs" in entry or previous == 0):
        entry["last_handed_refs"] = sorted(set(entry.get("last_handed_refs", [])) | set(upto_refs))
    entry["has_more"] = bool(has_more)


def retry_pending_accepts(state):
    """Re-verify awaiting batches against the receiver with their
    recorded file/hash; advance positions for any now accepted. A batch
    is never rebuilt while its finalized file exists."""
    advanced = 0
    changed = False
    for batch_id, record in list(state["batches"].items()):
        accepted, _result = try_receiver_accept(
            batch_id, record.get("sha256"), record.get("filename")
        )
        if not accepted:
            # An interrupted notice may already be committed. Diagnose
            # its original submission ID; never send it a second time.
            submission_id = record.get("msg_submission_id")
            if submission_id and record.get("msg_hub_acceptance") != "committed":
                try:
                    result = diagnose(submission_id)
                except Exception:
                    result = {"hub_acceptance": "unknown"}
                acceptance = result.get("hub_acceptance", "unknown")
                if acceptance != record.get("msg_hub_acceptance"):
                    record["msg_hub_acceptance"] = acceptance
                    changed = True
            continue
        advance_checkpoint(
            state, record["thread_key"], record["upto_ts"], record.get("upto_refs"),
            record.get("has_more", False) and record.get("kind") != "initial_context",
        )
        del state["batches"][batch_id]
        advanced += 1
        print(f"auto-accepted pending {batch_id}; position advanced to {record['upto_ts']}")
    if advanced or changed:
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
        entry = state["threads"].get(key, {})
        handed = entry.get("last_handed_ts", 0)
        batch = build_batch(key, handed, entry.get("last_handed_refs"))
        if batch is None:
            # Stats said changed but nothing newer was fetched; leave as is.
            continue
        filename = transfer_pending_batch(state, batch)
        accepted, receiver_result = try_receiver_accept(batch["batch_id"], payload_sha(batch), filename)
        if accepted:
            advance_checkpoint(
                state, key, batch["upto_ms"], batch.get("upto_refs"), batch["has_more"]
            )
            del state["batches"][batch["batch_id"]]
            save_state(state)
            print(f"auto-accepted {batch['batch_id']}; position advanced to {batch['upto_ms']}")
            sent += 1
            continue
        if receiver_result.get("status") != "not_configured":
            print(
                f"receiver auto-accept not completed for {batch['batch_id']}: "
                f"{receiver_result.get('status')}"
            )
        # Persist the notice identity before attempting submission too.
        # A lost response is diagnosed by this ID on the next run.
        state["batches"][batch["batch_id"]]["msg_submission_id"] = (
            f"muse-dh-stream-{batch['batch_id']}"
        )
        state["batches"][batch["batch_id"]]["msg_hub_acceptance"] = "unknown"
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
    entry = state["threads"].get(thread_key, {})
    if entry.get("last_handed_ts", 0) > 0 or "last_handed_refs" in entry:
        print("initial context is only available before the first acknowledged batch")
        return 2
    batch = build_initial_batch(thread_key)
    if batch is None:
        print("no messages fetched")
        return 2
    filename = transfer_pending_batch(state, batch, kind="initial_context")
    sha = payload_sha(batch)
    accepted, receiver_result = try_receiver_accept(batch["batch_id"], sha, filename)
    if accepted:
        advance_checkpoint(state, thread_key, batch["upto_ms"], batch.get("upto_refs"))
        del state["batches"][batch["batch_id"]]
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


def _confirm(batch_id):
    state = load_state()
    batch = state["batches"].get(batch_id)
    if not batch:
        print(f"unknown batch: {batch_id}")
        return 2
    key = batch["thread_key"]
    advance_checkpoint(
        state, key, batch["upto_ts"], batch.get("upto_refs"),
        batch.get("has_more", False) and batch.get("kind") != "initial_context",
    )
    del state["batches"][batch_id]
    save_state(state)
    print(f"confirmed {batch_id}; position advanced to {batch['upto_ts']}")
    return 0


def confirm(batch_id):
    with single_run() as acquired:
        if not acquired:
            print("another run in progress; confirmation not applied")
            return 2
        return _confirm(batch_id)


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
