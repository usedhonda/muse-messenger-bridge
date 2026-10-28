#!/usr/bin/env python3
"""Create a status-only SQLite snapshot for Messenger.

This intentionally stores aggregate status only: no message bodies, snippets,
participant lists, names, or raw conversation/user IDs.
"""
import argparse
import hashlib
import json
import sqlite3
import subprocess
import sys
import time
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

CLI = _LOCAL.get("messenger_cli", "/path/to/messenger_cli")
DEFAULT_DB = Path(__file__).resolve().parent / "messenger_status.sqlite"
SCHEMA_VERSION = "3"


def run_cli(args):
    proc = subprocess.run([CLI, *args], text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if proc.returncode != 0:
        raise RuntimeError(f"messenger cli failed: {' '.join(args)}")
    return json.loads(proc.stdout)


def ms_to_iso(ms):
    if not isinstance(ms, (int, float)):
        return None
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat().replace("+00:00", "Z")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default=str(DEFAULT_DB))
    parser.add_argument("--sync", action="store_true", help="refresh Messenger before aggregating")
    parser.add_argument("--deep", action="store_true", help="also collect contacts, call history metadata, and per-thread message stats")
    args = parser.parse_args()
    started = time.time()
    mode = ("sync" if args.sync else "retained") + ("+deep" if args.deep else "")

    check = run_cli(["check"])
    check_data = check.get("data", {}) if isinstance(check, dict) else {}
    connected = bool(check.get("success") and check_data.get("has_keys"))

    if args.sync:
        run_cli(["sync", "both"])

    first = run_cli(["threads", "--limit", "1"])
    cache = first.get("data", {}).get("_cache", {})
    cached_thread_count = cache.get("cached_thread_count") or 0
    limit = max(1, min(int(cached_thread_count or 1), 10000))
    listing = run_cli(["threads", "--limit", str(limit)])
    data = listing.get("data", {})
    cache = data.get("_cache", cache)
    threads = data.get("threads", [])

    def count_if(pred):
        return sum(1 for item in threads if isinstance(item, dict) and pred(item))

    updated = [item.get("updated_at") for item in threads if isinstance(item, dict) and isinstance(item.get("updated_at"), (int, float))]
    captured_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    snapshot = {
        "captured_at_utc": captured_at,
        "connector_connected": 1 if connected else 0,
        "returned_thread_count": len(threads),
        "group_thread_count": count_if(lambda x: x.get("thread_type") == "GROUP"),
        "one_to_one_thread_count": count_if(lambda x: x.get("thread_type") != "GROUP"),
        "inbox_thread_count": count_if(lambda x: x.get("folder") == "INBOX"),
        "pending_thread_count": count_if(lambda x: x.get("folder") == "PENDING"),
        "unread_thread_count": count_if(lambda x: x.get("is_unread") is True),
        "newest_activity_at_utc": ms_to_iso(max(updated) if updated else None),
        "oldest_activity_at_utc": ms_to_iso(min(updated) if updated else None),
        "threads_last_synced_at_utc": ms_to_iso(cache.get("threads_last_synced_at")),
        "scope_note": "status plus thread index; deep tables add contacts, call metadata, per-thread message stats; no message bodies or raw IDs anywhere",
    }

    db_path = Path(args.db)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(db_path)
    con.execute("PRAGMA busy_timeout = 120000")
    try:
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS messenger_status_snapshot (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                captured_at_utc TEXT NOT NULL,
                connector_connected INTEGER NOT NULL,
                returned_thread_count INTEGER,
                group_thread_count INTEGER,
                one_to_one_thread_count INTEGER,
                inbox_thread_count INTEGER,
                pending_thread_count INTEGER,
                unread_thread_count INTEGER,
                newest_activity_at_utc TEXT,
                oldest_activity_at_utc TEXT,
                threads_last_synced_at_utc TEXT,
                scope_note TEXT
            )
            """
        )
        con.execute(
            """
            CREATE VIEW IF NOT EXISTS latest_messenger_status AS
            SELECT * FROM messenger_status_snapshot ORDER BY id DESC LIMIT 1
            """
        )
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS messenger_update_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_at_utc TEXT NOT NULL,
                ok INTEGER NOT NULL,
                mode TEXT,
                duration_ms INTEGER,
                error TEXT
            )
            """
        )
        con.execute(
            """
            CREATE VIEW IF NOT EXISTS messenger_update_health AS
            SELECT * FROM messenger_update_log ORDER BY id DESC LIMIT 1
            """
        )
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS messenger_thread_index (
                thread_key TEXT PRIMARY KEY,
                display_name TEXT,
                thread_type TEXT,
                folder TEXT,
                is_unread INTEGER,
                participant_count INTEGER,
                participant_names TEXT,
                updated_at_utc TEXT,
                captured_at_utc TEXT
            )
            """
        )
        con.execute(
            """
            INSERT INTO messenger_status_snapshot (
                captured_at_utc, connector_connected,
                returned_thread_count, group_thread_count, one_to_one_thread_count,
                inbox_thread_count, pending_thread_count,
                unread_thread_count, newest_activity_at_utc, oldest_activity_at_utc,
                threads_last_synced_at_utc, scope_note
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            tuple(snapshot[k] for k in [
                "captured_at_utc", "connector_connected",
                "returned_thread_count", "group_thread_count", "one_to_one_thread_count",
                "inbox_thread_count", "pending_thread_count",
                "unread_thread_count", "newest_activity_at_utc", "oldest_activity_at_utc",
                "threads_last_synced_at_utc", "scope_note",
            ]),
        )
        existing_cols = {row[1] for row in con.execute("PRAGMA table_info(messenger_thread_index)")}
        for col_name, col_type in [
            ("is_e2ee", "INTEGER"),
            ("read_timestamp_utc", "TEXT"),
            ("provisional", "INTEGER"),
            ("participant_keys", "TEXT"),
        ]:
            if col_name not in existing_cols:
                con.execute(f"ALTER TABLE messenger_thread_index ADD COLUMN {col_name} {col_type}")
        con.execute("DELETE FROM messenger_thread_index")
        thread_ids = {}
        for item in threads:
            if not isinstance(item, dict):
                continue
            cid = item.get("conversation_id")
            if not isinstance(cid, str) or not cid:
                continue
            thread_key = hashlib.sha256(cid.encode("utf-8")).hexdigest()
            thread_ids[thread_key] = cid
            names = []
            keys = []
            participants = item.get("participants")
            if isinstance(participants, list):
                for person in participants:
                    if isinstance(person, dict):
                        if isinstance(person.get("name"), str):
                            names.append(person["name"])
                        pid = person.get("id")
                        keys.append(hashlib.sha256(pid.encode("utf-8")).hexdigest() if isinstance(pid, str) else None)
            con.execute(
                """
                INSERT OR REPLACE INTO messenger_thread_index (
                    thread_key, display_name, thread_type, folder, is_unread,
                    participant_count, participant_names, participant_keys,
                    is_e2ee, read_timestamp_utc, provisional,
                    updated_at_utc, captured_at_utc
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    thread_key,
                    item.get("name"),
                    item.get("thread_type"),
                    item.get("folder"),
                    1 if item.get("is_unread") is True else 0,
                    len(names),
                    json.dumps(names, ensure_ascii=False),
                    json.dumps(keys, ensure_ascii=False),
                    1 if item.get("is_e2ee") is True else 0,
                    ms_to_iso(item.get("read_timestamp")),
                    1 if item.get("provisional") is True else 0,
                    ms_to_iso(item.get("updated_at")),
                    captured_at,
                ),
            )
        deep_failures = 0
        if args.deep:
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS messenger_contacts (
                    contact_key TEXT PRIMARY KEY,
                    name TEXT,
                    captured_at_utc TEXT
                )
                """
            )
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS messenger_calls (
                    call_key TEXT PRIMARY KEY,
                    started_at_utc TEXT,
                    direction TEXT,
                    call_type TEXT,
                    duration_seconds INTEGER,
                    missed INTEGER,
                    counterparty_name TEXT,
                    captured_at_utc TEXT
                )
                """
            )
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS messenger_thread_stats (
                    thread_key TEXT PRIMARY KEY,
                    message_count INTEGER,
                    newest_message_at_utc TEXT,
                    oldest_message_at_utc TEXT,
                    last_sender_name TEXT,
                    last_message_at_utc TEXT,
                    last_has_attachment INTEGER,
                    last_reaction_count INTEGER,
                    captured_at_utc TEXT
                )
                """
            )
            try:
                run_cli(["sync", "contacts", "--limit", "2000"])
                contacts_resp = run_cli(["contacts", "--limit", "10000"])
                contacts = contacts_resp.get("data", {}).get("contacts", [])
                con.execute("DELETE FROM messenger_contacts")
                for person in contacts:
                    if isinstance(person, dict) and isinstance(person.get("fbid"), str):
                        con.execute(
                            "INSERT OR REPLACE INTO messenger_contacts (contact_key, name, captured_at_utc) VALUES (?, ?, ?)",
                            (hashlib.sha256(person["fbid"].encode("utf-8")).hexdigest(), person.get("name"), captured_at),
                        )
            except Exception:
                deep_failures += 1
            try:
                calls_resp = run_cli(["call-logs", "--limit", "50"])
                calls = calls_resp.get("call_logs")
                if not isinstance(calls, list):
                    cdata = calls_resp.get("data")
                    calls = cdata.get("call_logs", []) if isinstance(cdata, dict) else []
                con.execute("DELETE FROM messenger_calls")
                for call in calls:
                    if not isinstance(call, dict):
                        continue
                    call_key = hashlib.sha256(json.dumps(call, sort_keys=True, ensure_ascii=False).encode("utf-8")).hexdigest()
                    started = call.get("timestamp") or call.get("started_at") or call.get("start_time") or call.get("time")
                    counterparty = call.get("counterparty_name") or call.get("caller_name") or call.get("callee_name") or call.get("name")
                    duration = call.get("duration_seconds") or call.get("duration")
                    con.execute(
                        """
                        INSERT OR REPLACE INTO messenger_calls (
                            call_key, started_at_utc, direction, call_type,
                            duration_seconds, missed, counterparty_name, captured_at_utc
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            call_key,
                            ms_to_iso(started) if isinstance(started, (int, float)) else None,
                            call.get("direction"),
                            call.get("call_type") or call.get("type"),
                            duration if isinstance(duration, (int, float)) else None,
                            1 if call.get("missed") is True else 0,
                            counterparty if isinstance(counterparty, str) else None,
                            captured_at,
                        ),
                    )
            except Exception:
                deep_failures += 1
            con.execute("DELETE FROM messenger_thread_stats")
            for thread_key, cid in thread_ids.items():
                try:
                    msg_resp = run_cli(["messages", "--cid", cid, "--limit", "1"])
                    mdata = msg_resp.get("data", {})
                    mcache = mdata.get("_cache", {})
                    msgs = mdata.get("messages", [])
                    last = msgs[0] if msgs else {}
                    attachments = last.get("attachments")
                    reactions = last.get("reactions")
                    con.execute(
                        """
                        INSERT OR REPLACE INTO messenger_thread_stats (
                            thread_key, message_count, newest_message_at_utc, oldest_message_at_utc,
                            last_sender_name, last_message_at_utc, last_has_attachment,
                            last_reaction_count, captured_at_utc
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            thread_key,
                            mcache.get("cached_message_count"),
                            ms_to_iso(mcache.get("newest_cached_ts")),
                            ms_to_iso(mcache.get("oldest_cached_ts")),
                            last.get("sender_name") if isinstance(last.get("sender_name"), str) else None,
                            ms_to_iso(last.get("message_sent_at") or last.get("timestamp")),
                            1 if isinstance(attachments, list) and attachments else 0,
                            len(reactions) if isinstance(reactions, (list, dict)) else 0,
                            captured_at,
                        ),
                    )
                except Exception:
                    deep_failures += 1
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS messenger_meta (
                key TEXT PRIMARY KEY,
                value TEXT
            )
            """
        )
        existing_created = con.execute(
            "SELECT value FROM messenger_meta WHERE key = 'database_created_at_utc'"
        ).fetchone()
        if not existing_created:
            earliest = con.execute(
                "SELECT MIN(captured_at_utc) FROM messenger_status_snapshot"
            ).fetchone()[0]
            con.execute(
                "INSERT OR IGNORE INTO messenger_meta (key, value) VALUES ('database_created_at_utc', ?)",
                (earliest or captured_at,),
            )
        meta_values = {
            "schema_version": SCHEMA_VERSION,
            "generator": "update_messenger_status.py (muse-messenger-bridge)",
            "source": "Messenger connector, read-only",
            "update_cadence": "every 60s; failure backoff 600s",
            "audience": "the owner's own agents",
            "scope": snapshot["scope_note"],
            "contents": "messenger_status_snapshot, messenger_thread_index, messenger_update_log, messenger_meta; deep runs add messenger_contacts, messenger_calls, messenger_thread_stats",
            "distribution": "versioned backup snapshots deposited to the hub bridge incoming folder, with a latest manifest; no overwrite of existing files",
            "last_written_at_utc": captured_at,
        }
        if args.deep:
            meta_values["deep_cadence"] = "every 1h"
            meta_values["deep_last_run_at_utc"] = captured_at
            meta_values["deep_last_failure_count"] = str(deep_failures)
        for meta_key, meta_value in meta_values.items():
            con.execute(
                "INSERT OR REPLACE INTO messenger_meta (key, value) VALUES (?, ?)",
                (meta_key, meta_value),
            )
        con.execute(
            """
            INSERT INTO messenger_update_log (run_at_utc, ok, mode, duration_ms, error)
            VALUES (?, 1, ?, ?, NULL)
            """,
            (captured_at, mode, int((time.time() - started) * 1000)),
        )
        con.commit()
    finally:
        con.close()

    print(json.dumps({"db": str(db_path), **snapshot}, ensure_ascii=False))


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        try:
            con = sqlite3.connect(DEFAULT_DB)
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS messenger_update_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_at_utc TEXT NOT NULL,
                    ok INTEGER NOT NULL,
                    mode TEXT,
                    duration_ms INTEGER,
                    error TEXT
                )
                """
            )
            con.execute(
                "INSERT INTO messenger_update_log (run_at_utc, ok, mode, duration_ms, error) VALUES (?, 0, NULL, NULL, ?)",
                (datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"), str(exc)[:500]),
            )
            con.commit()
            con.close()
        except Exception:
            pass
        print(f"error: {exc}", file=sys.stderr)
        raise SystemExit(1)
