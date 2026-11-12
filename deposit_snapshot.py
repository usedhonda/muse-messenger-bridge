#!/usr/bin/env python3
"""Deposit versioned, consistent snapshots of the Messenger status DB to the
hub bridge incoming folder on the hub host.

Rules (agreed with the hub agent, extended to a bridge cadence):
- Each deposit is a NEW file: messenger-status-bridge-<UTC stamp>.sqlite3.
  Existing files are never overwritten; transfer goes to a unique private
  temporary file, then an atomic hard link publishes it after size/hash verify.
- Files land 0600 in the 0700 incoming directory, over the existing SSH route.
- Verification or filename conflicts leave the latest pointer unchanged.
  An interrupted publication may leave a verified, unreferenced version;
  unknown SSH outcomes keep a pending identity until a read-only diagnosis
  verifies both the exact remote manifest and the deposited bytes.
- messenger-status-latest.json is the pointer manifest (file name, bytes,
  sha256, backup UTC, schema version), replaced atomically by rename.
- A deposit happens only when the meaningful content changed (status counts,
  thread index, thread stats, contacts, calls, stable meta). History tables
  (snapshots, update log) and volatile meta keys are excluded from the hash.
"""

from contextlib import closing
import fcntl
import hashlib
import json
import os
import shlex
import sqlite3
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path


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
STATE = BASE / "deposit-state.json"
EXPORTS = BASE / "exports"
REMOTE_DIR = _LOCAL.get("remote_status_dir", "/path/to/incoming/muse")
SSH_HOST = _LOCAL.get("ssh_host", "bridge-host")
_RECEIVER = _LOCAL.get("receiver")
_RECEIVER = _RECEIVER if isinstance(_RECEIVER, dict) else {}
REMOTE_PYTHON = _RECEIVER.get("python") or "/usr/bin/python3"
VOLATILE_META = {"last_written_at_utc", "deep_last_run_at_utc", "deep_last_failure_count"}


def content_hash(con) -> str:
    h = hashlib.sha256()

    def feed(obj) -> None:
        h.update(json.dumps(obj, ensure_ascii=False).encode("utf-8"))
        h.update(b"\n")

    meta = {
        k: v
        for k, v in con.execute("SELECT key, value FROM messenger_meta")
        if k not in VOLATILE_META
    }
    feed(sorted(meta.items()))
    # Explicit stable columns only: per-run capture timestamps are excluded so
    # a mere re-read of unchanged data does not look like a change.
    stable_columns = {
        "messenger_thread_index": (
            "thread_key, display_name, thread_type, folder, is_unread,"
            " participant_count, participant_names, participant_keys,"
            " is_e2ee, provisional, read_timestamp_utc, updated_at_utc"
        ),
        "messenger_thread_stats": (
            "thread_key, message_count, newest_message_at_utc,"
            " oldest_message_at_utc, last_sender_name, last_message_at_utc,"
            " last_has_attachment, last_reaction_count"
        ),
        "messenger_contacts": "contact_key, name",
        "messenger_calls": (
            "call_key, started_at_utc, direction, call_type,"
            " duration_seconds, missed, counterparty_name"
        ),
    }
    tables = {row[0] for row in con.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}
    optional_tables = {"messenger_thread_stats", "messenger_contacts", "messenger_calls"}
    for table, columns in stable_columns.items():
        feed(table)
        if table in optional_tables and table not in tables:
            # Shallow updates are valid before the first deep collection.
            feed(None)
            continue
        for row in con.execute(f"SELECT {columns} FROM {table} ORDER BY 1"):
            feed([None if v is None else str(v) for v in row])
    latest = con.execute(
        "SELECT connector_connected, returned_thread_count, group_thread_count,"
        " one_to_one_thread_count, inbox_thread_count, pending_thread_count,"
        " unread_thread_count, newest_activity_at_utc, oldest_activity_at_utc"
        " FROM messenger_status_snapshot ORDER BY id DESC LIMIT 1"
    ).fetchone()
    feed(list(latest) if latest else None)
    return h.hexdigest()


# This uses the same configured remote Python as the receiver. All staging
# files are unique and private; publication never follows a final-name symlink.
REMOTE_PUBLISH = r"""
import fcntl
import hashlib
import json
import os
import sys
import tempfile

root, name, expected_size, expected_sha, encoded_manifest = sys.argv[1:]
expected_size = int(expected_size)
manifest = json.loads(encoded_manifest)
if os.path.basename(name) != name or name in ("", ".", ".."):
    raise RuntimeError("invalid snapshot name")
if (manifest.get("file") != name or manifest.get("bytes") != expected_size
        or manifest.get("sha256") != expected_sha):
    raise RuntimeError("snapshot metadata mismatch")
os.umask(0o077)
os.makedirs(root, mode=0o700, exist_ok=True)
os.chmod(root, 0o700)
staged = []
try:
    fd, snapshot_tmp = tempfile.mkstemp(prefix="." + name + ".", suffix=".partial", dir=root)
    staged.append(snapshot_tmp)
    digest, received = hashlib.sha256(), 0
    with os.fdopen(fd, "wb") as stream:
        while True:
            chunk = sys.stdin.buffer.read(65536)
            if not chunk:
                break
            received += len(chunk)
            if received > expected_size:
                raise RuntimeError("snapshot byte count mismatch")
            digest.update(chunk)
            stream.write(chunk)
        stream.flush()
        os.fsync(stream.fileno())
    if received != expected_size or digest.hexdigest() != expected_sha:
        raise RuntimeError("snapshot verification failed")

    lock_fd = os.open(os.path.join(root, ".messenger-status-publish.lock"), os.O_CREAT | os.O_RDWR, 0o600)
    with os.fdopen(lock_fd, "a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        latest = os.path.join(root, "messenger-status-latest.json")
        try:
            with open(latest, encoding="utf-8") as stream:
                previous = json.load(stream)
        except FileNotFoundError:
            previous = {}
        manifest["previous_file"] = previous.get("file")
        fd, manifest_tmp = tempfile.mkstemp(prefix=".messenger-status-latest.", suffix=".partial", dir=root)
        staged.append(manifest_tmp)
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(manifest, stream, ensure_ascii=False, indent=2)
            stream.flush()
            os.fsync(stream.fileno())
        # link() fails atomically for every existing destination type. Both
        # names are on the same filesystem; the final inode is already 0600.
        os.link(snapshot_tmp, os.path.join(root, name))
        os.replace(manifest_tmp, latest)
    print(json.dumps(manifest, ensure_ascii=False))
finally:
    for path in staged:
        try:
            os.unlink(path)
        except FileNotFoundError:
            pass
"""


# Diagnosis opens existing files read-only and never retransmits or republishes.
REMOTE_DIAGNOSE = r"""
import fcntl
import hashlib
import json
import os
import stat
import sys

root, encoded_expected = sys.argv[1:]
expected = json.loads(encoded_expected)
name = expected["file"]
if os.path.basename(name) != name or name in ("", ".", ".."):
    raise RuntimeError("invalid snapshot name")
lock_fd = os.open(os.path.join(root, ".messenger-status-publish.lock"), os.O_RDONLY)
with os.fdopen(lock_fd, "r") as lock:
    fcntl.flock(lock, fcntl.LOCK_SH)
    with open(os.path.join(root, "messenger-status-latest.json"), encoding="utf-8") as stream:
        current = json.load(stream)
    fields = ("file", "bytes", "sha256", "backup_utc", "schema_version", "content_hash")
    if not isinstance(current, dict) or any(current.get(key) != expected[key] for key in fields):
        raise RuntimeError("latest manifest does not match pending snapshot")
    fd = os.open(os.path.join(root, name), os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(fd, "rb") as stream:
        info = os.fstat(stream.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_size != expected["bytes"]:
            raise RuntimeError("pending snapshot size or type mismatch")
        digest, size = hashlib.sha256(), 0
        while True:
            chunk = stream.read(65536)
            if not chunk:
                break
            size += len(chunk)
            if size > expected["bytes"]:
                raise RuntimeError("pending snapshot byte count mismatch")
            digest.update(chunk)
        if size != expected["bytes"] or digest.hexdigest() != expected["sha256"]:
            raise RuntimeError("pending snapshot verification failed")
print(json.dumps(current, ensure_ascii=False))
"""


def run(cmd: list[str], *, input=None):
    try:
        return subprocess.run(cmd, input=input, check=True, capture_output=True, timeout=300)
    except subprocess.TimeoutExpired:
        raise RuntimeError("snapshot transfer command timed out") from None
    except (subprocess.CalledProcessError, OSError):
        raise RuntimeError("snapshot transfer command failed") from None


def atomic_json(path, value):
    fd, temporary = tempfile.mkstemp(prefix="." + path.name + ".", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(value, stream, ensure_ascii=False, indent=2)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        Path(temporary).unlink(missing_ok=True)


def publish_snapshot(data, manifest):
    command = shlex.join([
        REMOTE_PYTHON, "-c", REMOTE_PUBLISH, REMOTE_DIR, manifest["file"],
        str(manifest["bytes"]), manifest["sha256"], json.dumps(manifest, ensure_ascii=False),
    ])
    result = run(["ssh", "--", SSH_HOST, command], input=data)
    return matching_manifest(result, manifest)


def matching_manifest(result, expected):
    try:
        published = json.loads(result.stdout)
    except (ValueError, TypeError):
        raise RuntimeError("snapshot publication response was invalid") from None
    if not isinstance(published, dict) or any(
        published.get(key) != expected[key]
        for key in ("file", "bytes", "sha256", "backup_utc", "schema_version", "content_hash")
    ):
        raise RuntimeError("snapshot publication response did not match the snapshot")
    return published


def reconcile_pending(pending):
    if not isinstance(pending, dict):
        raise RuntimeError("pending snapshot metadata is invalid; reconciliation required")
    name = pending.get("file")
    valid = (
        isinstance(name, str) and Path(name).name == name
        and name.startswith("messenger-status-bridge-") and name.endswith(".sqlite3")
        and type(pending.get("bytes")) is int and pending["bytes"] > 0
        and all(isinstance(pending.get(key), str) and pending[key]
                for key in ("backup_utc", "schema_version"))
        and all(isinstance(pending.get(key), str) and len(pending[key]) == 64
                and all(char in "0123456789abcdef" for char in pending[key])
                for key in ("sha256", "content_hash"))
    )
    if not valid:
        raise RuntimeError("pending snapshot metadata is invalid; reconciliation required")
    command = shlex.join([
        REMOTE_PYTHON, "-c", REMOTE_DIAGNOSE, REMOTE_DIR,
        json.dumps(pending, ensure_ascii=False),
    ])
    try:
        published = matching_manifest(run(["ssh", "--", SSH_HOST, command]), pending)
    except Exception:
        raise RuntimeError(
            "pending snapshot reconciliation required: exact remote manifest and bytes were not verified"
        ) from None
    return complete_deposit(published, reconciled=True)


def complete_deposit(manifest, *, reconciled=False):
    EXPORTS.mkdir(mode=0o700, parents=True, exist_ok=True)
    atomic_json(EXPORTS / "messenger-status-latest.json", manifest)
    # Preserve the legacy successful-state shape; pending exists only while
    # transmission/publication has not been confirmed.
    atomic_json(STATE, {key: manifest[key] for key in ("content_hash", "file", "backup_utc")})
    result = {key: manifest[key] for key in ("file", "bytes", "sha256", "backup_utc")}
    result["deposited"] = not reconciled
    if reconciled:
        result["reason"] = "reconciled"
    print(json.dumps(result))
    return 0


def deposit() -> int:
    state = {}
    if STATE.exists():
        state = json.loads(STATE.read_text(encoding="utf-8"))
    if "pending" in state:
        return reconcile_pending(state["pending"])

    # Hash the completed backup, never independent reads of the live database:
    # the updater may commit between any two reads or before the backup starts.
    EXPORTS.mkdir(mode=0o700, parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=".snapshot-", dir=EXPORTS) as staging:
        candidate = Path(staging) / "status.sqlite3"
        with closing(sqlite3.connect(DB.resolve().as_uri() + "?mode=ro", uri=True, timeout=120)) as src:
            with closing(sqlite3.connect(candidate, timeout=120)) as dst:
                src.backup(dst, pages=1024, sleep=0.25)
        backup_utc = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        with closing(sqlite3.connect(candidate.resolve().as_uri() + "?mode=ro", uri=True)) as chk:
            integrity = chk.execute("PRAGMA integrity_check").fetchone()[0]
            if integrity != "ok":
                raise RuntimeError("snapshot integrity check failed")
            schema_version = chk.execute(
                "SELECT value FROM messenger_meta WHERE key = 'schema_version'"
            ).fetchone()[0]
            current_hash = content_hash(chk)

        if state.get("content_hash") == current_hash:
            print(json.dumps({"deposited": False, "reason": "unchanged"}))
            return 0

        stamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%SZ")
        fname = f"messenger-status-bridge-{stamp}.sqlite3"
        local = EXPORTS / fname
        candidate.chmod(0o600)
        # Refuse a same-name local version without deleting or replacing it.
        os.link(candidate, local)

    data = local.read_bytes()
    sha = hashlib.sha256(data).hexdigest()
    manifest = {
        "file": fname,
        "bytes": len(data),
        "sha256": sha,
        "backup_utc": backup_utc,
        "schema_version": schema_version,
        "content_hash": current_hash,
        "previous_file": state.get("file"),
    }
    # Persist the exact identity before SSH can create a remote version. Any
    # interruption from here is diagnosed on the next run, never re-batched.
    atomic_json(STATE, {**state, "pending": manifest})
    manifest = publish_snapshot(data, manifest)
    return complete_deposit(manifest)


def main() -> int:
    STATE.parent.mkdir(parents=True, exist_ok=True)
    lock_fd = os.open(STATE.with_suffix(".lock"), os.O_CREAT | os.O_RDWR, 0o600)
    with os.fdopen(lock_fd, "a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            print(json.dumps({"deposited": False, "reason": "already_running"}))
            return 0
        return deposit()


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:  # surfaced to the hook for a throttled wake
        print(f"deposit failed: {exc}", file=sys.stderr)
        raise SystemExit(1)
