#!/usr/bin/env python3
"""Deposit versioned, consistent snapshots of the Messenger status DB to the
hub bridge incoming folder on the hub host.

Rules (agreed with the hub agent, extended to a bridge cadence):
- Each deposit is a NEW file: messenger-status-bridge-<UTC stamp>.sqlite3.
  Existing remote files are never overwritten; transfer goes to a temporary
  name in the same directory and is renamed only after size/hash verify.
- Files land 0600 in the 0700 incoming directory, over the existing SSH route.
- A failed deposit changes nothing on the receiving side; the latest
  successful deposit stays in place.
- messenger-status-latest.json is the pointer manifest (file name, bytes,
  sha256, backup UTC, schema version), replaced atomically by rename.
- A deposit happens only when the meaningful content changed (status counts,
  thread index, thread stats, contacts, calls, stable meta). History tables
  (snapshots, update log) and volatile meta keys are excluded from the hash.
"""

import hashlib
import json
import sqlite3
import subprocess
import sys
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
            " is_e2ee, provisional, updated_at_utc"
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
    for table, columns in stable_columns.items():
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


def run(cmd: list[str]) -> None:
    subprocess.run(cmd, check=True, capture_output=True, text=True, timeout=300)


def main() -> int:
    con = sqlite3.connect(f"file:{DB}?mode=ro", uri=True, timeout=120)
    current_hash = content_hash(con)
    con.close()

    state = {}
    if STATE.exists():
        state = json.loads(STATE.read_text(encoding="utf-8"))
    if state.get("content_hash") == current_hash:
        print(json.dumps({"deposited": False, "reason": "unchanged"}))
        return 0

    stamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%SZ")
    fname = f"messenger-status-bridge-{stamp}.sqlite3"
    EXPORTS.mkdir(parents=True, exist_ok=True)
    local = EXPORTS / fname
    if local.exists():
        local.unlink()

    src = sqlite3.connect(f"file:{DB}?mode=ro", uri=True, timeout=120)
    dst = sqlite3.connect(str(local), timeout=120)
    with dst:
        src.backup(dst, pages=1024, sleep=0.25)
    backup_utc = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    dst.close()
    src.close()

    chk = sqlite3.connect(f"file:{local}?mode=ro", uri=True)
    integrity = chk.execute("PRAGMA integrity_check").fetchone()[0]
    schema_version = chk.execute(
        "SELECT value FROM messenger_meta WHERE key = 'schema_version'"
    ).fetchone()[0]
    chk.close()
    if integrity != "ok":
        raise RuntimeError("snapshot integrity check failed")

    data = local.read_bytes()
    sha = hashlib.sha256(data).hexdigest()
    tmp_name = fname + ".partial"

    run(["scp", str(local), f"{SSH_HOST}:{REMOTE_DIR}/{tmp_name}"])
    remote = (
        f"set -e; D={REMOTE_DIR}; "
        f'chmod 600 "$D/{tmp_name}"; '
        f'echo "{sha}  $D/{tmp_name}" | shasum -a 256 -c - > /dev/null; '
        f'test ! -e "$D/{fname}"; '
        f'mv "$D/{tmp_name}" "$D/{fname}"'
    )
    run(["ssh", SSH_HOST, remote])

    manifest = {
        "file": fname,
        "bytes": len(data),
        "sha256": sha,
        "backup_utc": backup_utc,
        "schema_version": schema_version,
        "content_hash": current_hash,
        "previous_file": state.get("file"),
    }
    mlocal = EXPORTS / "messenger-status-latest.json"
    mlocal.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    run(["scp", str(mlocal), f"{SSH_HOST}:{REMOTE_DIR}/messenger-status-latest.json.partial"])
    run(
        [
            "ssh",
            SSH_HOST,
            f'mv "{REMOTE_DIR}/messenger-status-latest.json.partial" '
            f'"{REMOTE_DIR}/messenger-status-latest.json" && '
            f'chmod 600 "{REMOTE_DIR}/messenger-status-latest.json"',
        ]
    )

    STATE.write_text(
        json.dumps(
            {"content_hash": current_hash, "file": fname, "backup_utc": backup_utc},
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    print(
        json.dumps(
            {
                "deposited": True,
                "file": fname,
                "bytes": len(data),
                "sha256": sha,
                "backup_utc": backup_utc,
            }
        )
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:  # surfaced to the hook for a throttled wake
        print(f"deposit failed: {exc}", file=sys.stderr)
        raise SystemExit(1)
