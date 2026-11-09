"""Offline regressions for failure-safe status collection (stdlib only)."""
import contextlib
import hashlib
import io
import json
import os
from pathlib import Path
import runpy
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from unittest import mock


SOURCE_ROOT = Path(os.environ.get("MUSE_SOURCE_ROOT", Path(__file__).resolve().parents[1]))


class StatusUpdateTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.script = self.root / "update_messenger_status.py"
        shutil.copyfile(SOURCE_ROOT / self.script.name, self.script)
        self.db = self.root / "chosen" / "status.sqlite"
        self.responses = {}
        self.threads = [
            {"conversation_id": "synthetic-thread-1", "name": "One", "is_unread": True,
             "thread_type": "GROUP", "updated_at": 1000, "participants": []},
            {"conversation_id": "synthetic-thread-2", "name": "Two", "is_unread": False,
             "thread_type": "ONE_TO_ONE", "updated_at": 2000, "participants": []},
        ]
        self.contacts = [{"fbid": "synthetic-person-1", "name": "Alpha"},
                         {"fbid": "synthetic-person-2", "name": "Beta"}]

    def cli(self, cmd, **kwargs):
        args = tuple(cmd[1:])
        if args in self.responses:
            value = self.responses[args]
            if isinstance(value, BaseException):
                raise value
        elif args[0] == "check":
            value = {"success": True, "data": {"has_keys": True}}
        elif args[0] == "sync":
            value = {"success": True}
        elif args[0] == "threads":
            value = {"success": True, "data": {"threads": self.threads[:int(args[-1])],
                                                "_cache": {"cached_thread_count": 2}}}
        elif args[0] == "contacts":
            value = {"success": True, "data": {"contacts": self.contacts}}
        elif args[0] == "call-logs":
            value = {"success": True, "call_logs": []}
        elif args[0] == "messages":
            value = {"success": True, "data": {"messages": [{"sender_name": "Alpha",
                "message_sent_at": 1000, "text": "PRIVATE_MESSAGE_BODY"}],
                "_cache": {"cached_message_count": 7}}}
        else:
            raise AssertionError(f"Unexpected command: {args[0]}")
        return subprocess.CompletedProcess(cmd, 0, json.dumps(value), "")

    def invoke(self, *, deep=False):
        argv = [str(self.script), "--db", str(self.db)] + (["--deep"] if deep else [])
        out, err = io.StringIO(), io.StringIO()
        with mock.patch.object(sys, "argv", argv), mock.patch("subprocess.run", side_effect=self.cli) as runner:
            with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
                try:
                    runpy.run_path(str(self.script), run_name="__main__")
                    status = 0
                except SystemExit as exc:
                    status = exc.code
        return status, out.getvalue(), err.getvalue(), runner.call_args_list

    def rows(self, table):
        with contextlib.closing(sqlite3.connect(self.db)) as con:
            return con.execute(f"SELECT * FROM {table} ORDER BY 1").fetchall()

    def assert_failed_preserving(self, before, *, deep=False):
        status, _, _, _ = self.invoke(deep=deep)
        self.assertEqual(status, 1)
        for table, rows in before.items():
            self.assertEqual(self.rows(table), rows, table)
        self.assertEqual(self.rows("messenger_update_health")[0][2], 0)
        self.assertFalse((self.root / "messenger_status.sqlite").exists())

    def test_logical_listing_failure_preserves_status_and_logs_to_requested_db(self):
        self.assertEqual(self.invoke()[0], 0)
        before = {table: self.rows(table) for table in (
            "messenger_thread_index", "messenger_status_snapshot")}
        self.responses[("threads", "--limit", "2")] = {"success": False, "error": "PRIVATE_MESSAGE_BODY"}
        self.assert_failed_preserving(before)

    def test_missing_thread_collection_is_failure_not_empty_inbox(self):
        self.assertEqual(self.invoke()[0], 0)
        before = {"messenger_thread_index": self.rows("messenger_thread_index")}
        self.responses[("threads", "--limit", "2")] = {"success": True, "data": {}}
        self.assert_failed_preserving(before)

    def test_contact_insert_failure_rolls_back_entire_deep_refresh(self):
        self.assertEqual(self.invoke(deep=True)[0], 0)
        before = {table: self.rows(table) for table in (
            "messenger_contacts", "messenger_thread_index", "messenger_status_snapshot")}
        self.threads[0]["name"] = "Changed"
        self.contacts[0]["name"] = "Changed"
        self.contacts[1]["name"] = {"invalid": "PRIVATE_MESSAGE_BODY"}
        self.assert_failed_preserving(before, deep=True)

    def test_failed_thread_stats_preserve_prior_stats_and_capture(self):
        self.assertEqual(self.invoke(deep=True)[0], 0)
        before = {table: self.rows(table) for table in (
            "messenger_thread_stats", "messenger_status_snapshot")}
        self.responses[("messages", "--cid", "synthetic-thread-2", "--limit", "1")] = {
            "success": False, "error": "PRIVATE_MESSAGE_BODY"}
        self.assert_failed_preserving(before, deep=True)

    def test_timeout_records_sanitized_failure_on_first_run(self):
        self.responses[("check",)] = subprocess.TimeoutExpired(
            ["messenger", "PRIVATE_RAW_ID"], 120, output="PRIVATE_MESSAGE_BODY")
        status, _, stderr, _ = self.invoke()
        self.assertEqual(status, 1)
        with contextlib.closing(sqlite3.connect(self.db)) as con:
            stored = "\n".join(con.iterdump())
            self.assertEqual(con.execute("SELECT ok FROM messenger_update_health").fetchone(), (0,))
        for private in ("PRIVATE_RAW_ID", "PRIVATE_MESSAGE_BODY"):
            self.assertNotIn(private, stored + stderr)
        self.assertIn("timed out", stderr)
        self.assertFalse((self.root / "messenger_status.sqlite").exists())

    def test_first_shallow_run_creates_empty_deep_schema_without_collecting_it(self):
        status, _, _, calls = self.invoke()
        self.assertEqual(status, 0)
        for table in ("messenger_thread_stats", "messenger_contacts", "messenger_calls"):
            self.assertEqual(self.rows(table), [])
        self.assertEqual({call.args[0][1] for call in calls}, {"check", "threads"})

    def test_participant_names_and_keys_stay_paired_when_a_name_is_missing(self):
        self.threads[0]["participants"] = [
            {"id": "synthetic-unnamed"},
            {"id": "synthetic-named", "name": "Present"},
            {"name": "No key"},
        ]
        self.assertEqual(self.invoke()[0], 0)
        with contextlib.closing(sqlite3.connect(self.db)) as con:
            count, names, keys = con.execute(
                "SELECT participant_count, participant_names, participant_keys "
                "FROM messenger_thread_index WHERE display_name = 'One'"
            ).fetchone()
        self.assertEqual(count, 2)
        self.assertEqual(json.loads(names), ["Present", "No key"])
        self.assertEqual(json.loads(keys), [hashlib.sha256(b"synthetic-named").hexdigest(), None])

    def test_successful_calls_have_timeout_and_database_contains_only_status(self):
        status, _, _, calls = self.invoke(deep=True)
        self.assertEqual(status, 0)
        for call in calls:
            self.assertEqual(call.kwargs.get("timeout"), 120)
        with contextlib.closing(sqlite3.connect(self.db)) as con:
            stored = "\n".join(con.iterdump())
        self.assertNotIn("PRIVATE_MESSAGE_BODY", stored)
        self.assertNotIn("synthetic-thread-", stored)
        self.assertNotIn("synthetic-person-", stored)
        self.assertIn(hashlib.sha256(b"synthetic-thread-1").hexdigest(), stored)


if __name__ == "__main__":
    unittest.main()
