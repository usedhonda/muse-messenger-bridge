#!/usr/bin/env python3
"""Create a status-only SQLite snapshot for Messenger.

This stores status and permitted display names only: no message bodies,
snippets, or raw conversation/user IDs.
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
CLI_TIMEOUT_SECONDS = 120


class StatusCollectionError(RuntimeError):
    """A safe, value-free source failure suitable for the status log."""


def run_cli(args):
    operation = args[0] if args[0] in {
        "check", "sync", "threads", "contacts", "call-logs", "messages"
    } else "command"
    try:
        proc = subprocess.run(
            [CLI, *args], text=True, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, timeout=CLI_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired:
        raise StatusCollectionError(f"messenger {operation} command timed out") from None
    except OSError:
        raise StatusCollectionError(f"messenger {operation} command could not start") from None
    if proc.returncode != 0:
        raise StatusCollectionError(f"messenger {operation} command failed")
    try:
        response = json.loads(proc.stdout)
    except (ValueError, TypeError):
        raise StatusCollectionError(f"messenger {operation} returned invalid JSON") from None
    if not isinstance(response, dict) or response.get("success") is False:
        raise StatusCollectionError(f"messenger {operation} response failed")
    return response


def collection(response, key, *, top_level=False):
    data = response if top_level else response.get("data")
    items = data.get(key) if isinstance(data, dict) else None
    if not isinstance(items, list) or any(not isinstance(item, dict) for item in items):
        raise StatusCollectionError(f"messenger response has invalid {key} collection")
    return items


def ms_to_iso(ms):
    if not isinstance(ms, (int, float)):
        return None
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat().replace("+00:00", "Z")


def update(args, started):
    mode = ("sync" if args.sync else "retained") + ("+deep" if args.deep else "")

    check = run_cli(["check"])
    check_data = check.get("data", {}) if isinstance(check, dict) else {}
    connected = bool(check.get("success") and check_data.get("has_keys"))

    if args.sync:
        run_cli(["sync", "both"])

    first = run_cli(["threads", "--limit", "1"])
    collection(first, "threads")
    cache = first["data"].get("_cache")
    cached_thread_count = cache.get("cached_thread_count") if isinstance(cache, dict) else None
    if type(cached_thread_count) is not int or cached_thread_count < 0:
        raise StatusCollectionError("messenger response has invalid thread count")
    limit = max(1, min(cached_thread_count, 10000))
    listing = run_cli(["threads", "--limit", str(limit)])
    threads = collection(listing, "threads")
    data = listing["data"]
    cache = data.get("_cache", cache)
    if not isinstance(cache, dict):
        raise StatusCollectionError("messenger response has invalid thread cache")
    if any(not isinstance(item.get("conversation_id"), str) or not item["conversation_id"]
           for item in threads):
        raise StatusCollectionError("messenger thread collection has missing identifiers")

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
    con = sqlite3.connect(db_path, timeout=120)
    try:
        con.execute("BEGIN IMMEDIATE")
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
                    if isinstance(person, dict) and isinstance(person.get("name"), str):
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
        if args.deep:
            run_cli(["sync", "contacts", "--limit", "2000"])
            contacts_resp = run_cli(["contacts", "--limit", "10000"])
            contacts = collection(contacts_resp, "contacts")
            if any(not isinstance(person.get("fbid"), str) or not person["fbid"]
                   for person in contacts):
                raise StatusCollectionError("messenger contact collection has missing identifiers")
            con.execute("DELETE FROM messenger_contacts")
            for person in contacts:
                if isinstance(person, dict) and isinstance(person.get("fbid"), str):
                    con.execute(
                        "INSERT OR REPLACE INTO messenger_contacts (contact_key, name, captured_at_utc) VALUES (?, ?, ?)",
                        (hashlib.sha256(person["fbid"].encode("utf-8")).hexdigest(), person.get("name"), captured_at),
                    )
            calls_resp = run_cli(["call-logs", "--limit", "50"])
            calls = collection(calls_resp, "call_logs", top_level="call_logs" in calls_resp)
            con.execute("DELETE FROM messenger_calls")
            for call in calls:
                if not isinstance(call, dict):
                    continue
                call_key = hashlib.sha256(json.dumps(call, sort_keys=True, ensure_ascii=False).encode("utf-8")).hexdigest()
                raw_started = call.get("call_started_at") or call.get("timestamp") or call.get("started_at") or call.get("start_time") or call.get("time")
                if isinstance(raw_started, dict):
                    call_started = raw_started.get("utc") or raw_started.get("user_local")
                else:
                    call_started = raw_started
                counterparty = call.get("counterparty_name") or call.get("caller_name") or call.get("callee_name") or call.get("name")
                duration = call.get("call_duration_seconds") or call.get("duration_seconds") or call.get("duration")
                con.execute(
                    """
                    INSERT OR REPLACE INTO messenger_calls (
                        call_key, started_at_utc, direction, call_type,
                        duration_seconds, missed, counterparty_name, captured_at_utc
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        call_key,
                        ms_to_iso(call_started) if isinstance(call_started, (int, float)) else (call_started if isinstance(call_started, str) else None),
                        call.get("direction"),
                        call.get("call_type") or call.get("type") or call.get("event"),
                        duration if isinstance(duration, (int, float)) else None,
                        1 if call.get("missed") is True else 0,
                        counterparty if isinstance(counterparty, str) else None,
                        captured_at,
                    ),
                )
            con.execute("DELETE FROM messenger_thread_stats")
            for thread_key, cid in thread_ids.items():
                msg_resp = run_cli(["messages", "--cid", cid, "--limit", "1"])
                msgs = collection(msg_resp, "messages")
                mdata = msg_resp["data"]
                mcache = mdata.get("_cache", {})
                if not isinstance(mcache, dict):
                    raise StatusCollectionError("messenger response has invalid message cache")
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
            meta_values["deep_last_failure_count"] = "0"
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


def record_failure(db_path, mode, duration_ms, error):
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(db_path, timeout=120)
    try:
        with con:
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
                "INSERT INTO messenger_update_log (run_at_utc, ok, mode, duration_ms, error) VALUES (?, 0, ?, ?, ?)",
                (datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"), mode, duration_ms, error),
            )
    finally:
        con.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default=str(DEFAULT_DB))
    parser.add_argument("--sync", action="store_true", help="refresh Messenger before aggregating")
    parser.add_argument("--deep", action="store_true", help="also collect contacts, call history metadata, and per-thread message stats")
    args = parser.parse_args()
    started = time.time()
    mode = ("sync" if args.sync else "retained") + ("+deep" if args.deep else "")
    try:
        update(args, started)
    except Exception as exc:
        # Exception strings from subprocess/SQLite may contain source IDs or data.
        error = str(exc) if isinstance(exc, StatusCollectionError) else f"{type(exc).__name__} while updating Messenger status"
        try:
            record_failure(Path(args.db), mode, int((time.time() - started) * 1000), error)
        except Exception:
            pass
        print(f"error: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
