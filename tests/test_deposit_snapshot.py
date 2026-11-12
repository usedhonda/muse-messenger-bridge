"""Snapshot consistency tests using temporary SQLite databases and no transport."""
import contextlib
import importlib.util
import io
import json
import os
from pathlib import Path
import shlex
import sqlite3
import subprocess
import tempfile
import unittest
from unittest import mock


SOURCE_ROOT = Path(os.environ.get("MUSE_SOURCE_ROOT", Path(__file__).resolve().parents[1]))


def load_module(config=None):
    spec = importlib.util.spec_from_file_location("deposit_under_test", SOURCE_ROOT / "deposit_snapshot.py")
    module = importlib.util.module_from_spec(spec)
    # Import only source code, even if this suite is run in a deployment tree.
    config_read = {"side_effect": FileNotFoundError} if config is None else {"return_value": json.dumps(config)}
    with mock.patch.object(Path, "read_text", **config_read):
        spec.loader.exec_module(module)
    return module


class DepositSnapshotTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.module = load_module()
        self.module.DB = self.root / "status.sqlite"
        self.module.STATE = self.root / "state.json"
        self.module.EXPORTS = self.root / "exports"
        schema = (SOURCE_ROOT / "docs/schema.md").read_text().split("```sql\n", 1)[1].split("```", 1)[0]
        with contextlib.closing(sqlite3.connect(self.module.DB)) as con:
            con.executescript(schema)
            con.execute("INSERT INTO messenger_meta VALUES ('schema_version', '3')")
            con.execute("INSERT INTO messenger_meta VALUES ('last_written_at_utc', 'old')")
            con.execute("INSERT INTO messenger_thread_index (thread_key, display_name, read_timestamp_utc, captured_at_utc) VALUES ('synthetic-key', 'Before', 'read-old', 'old')")
            con.execute("INSERT INTO messenger_status_snapshot (captured_at_utc, connector_connected) VALUES ('old', 1)")
            con.commit()

    def change(self, sql, params=()):
        with contextlib.closing(sqlite3.connect(self.module.DB)) as con:
            con.execute(sql, params)
            con.commit()

    def hash(self):
        with contextlib.closing(sqlite3.connect(self.module.DB)) as con:
            return self.module.content_hash(con)

    def invoke(self):
        def acknowledge(cmd, **kwargs):
            return subprocess.CompletedProcess(cmd, 0, shlex.split(cmd[-1])[-1].encode("utf-8"), b"")
        with mock.patch.object(self.module, "run", side_effect=acknowledge) as transport, contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(self.module.main(), 0)
        return transport

    def test_non_object_receiver_config_uses_default_interpreter(self):
        for receiver in (None, [], ["invalid"], "invalid"):
            self.assertEqual(load_module({"receiver": receiver}).REMOTE_PYTHON, "/usr/bin/python3")
        self.assertEqual(load_module({"receiver": {"python": "/configured/python3"}}).REMOTE_PYTHON,
                         "/configured/python3")

    def test_first_shallow_database_can_be_deposited(self):
        for table in ("messenger_thread_stats", "messenger_contacts", "messenger_calls"):
            self.change(f"DROP TABLE {table}")
        transport = self.invoke()
        self.assertTrue(transport.called)
        state = json.loads(self.module.STATE.read_text())
        self.assertEqual(state["content_hash"], self.hash())

    def test_manifest_hash_matches_backup_during_concurrent_source_update(self):
        original = self.module.content_hash

        def hash_then_source_changes(con):
            result = original(con)
            self.change("UPDATE messenger_thread_index SET display_name = 'After'")
            return result

        with mock.patch.object(self.module, "content_hash", side_effect=hash_then_source_changes):
            self.invoke()
        manifest = json.loads((self.module.EXPORTS / "messenger-status-latest.json").read_text())
        with contextlib.closing(sqlite3.connect(self.module.EXPORTS / manifest["file"])) as con:
            deposited_hash = original(con)
        self.assertEqual(manifest["content_hash"], deposited_hash)
        self.assertNotEqual(self.hash(), deposited_hash)

    def test_read_timestamp_changes_hash_without_unread_or_activity_change(self):
        before = self.hash()
        self.change("UPDATE messenger_thread_index SET read_timestamp_utc = 'read-new'")
        self.assertNotEqual(self.hash(), before)

    def test_capture_and_history_updates_do_not_trigger_transport(self):
        before = self.hash()
        self.module.STATE.write_text(json.dumps({"content_hash": before, "file": "previous.sqlite3"}))
        self.change("UPDATE messenger_thread_index SET captured_at_utc = 'new'")
        self.change("UPDATE messenger_meta SET value = 'new' WHERE key = 'last_written_at_utc'")
        self.change("INSERT INTO messenger_status_snapshot (captured_at_utc, connector_connected) VALUES ('new', 1)")
        self.change("INSERT INTO messenger_update_log (run_at_utc, ok) VALUES ('new', 1)")
        self.assertEqual(self.hash(), before)
        self.assertFalse(self.invoke().called)
        self.assertEqual(list(self.module.EXPORTS.iterdir()) if self.module.EXPORTS.exists() else [], [])

    def test_backup_failure_closes_both_connections_and_cleans_temporary_file(self):
        connections = []
        real_connect = sqlite3.connect

        class TrackedConnection(sqlite3.Connection):
            closed = False

            def backup(self, *args, **kwargs):
                raise sqlite3.OperationalError("synthetic backup failure")

            def close(self):
                self.closed = True
                super().close()

        def connect(*args, **kwargs):
            connection = real_connect(*args, factory=TrackedConnection, **kwargs)
            connections.append(connection)
            return connection

        with mock.patch.object(self.module.sqlite3, "connect", side_effect=connect), mock.patch.object(self.module, "run") as transport:
            with self.assertRaisesRegex(sqlite3.OperationalError, "synthetic backup failure"):
                self.module.main()
        self.assertTrue(connections)
        self.assertTrue(all(connection.closed for connection in connections))
        self.assertFalse(transport.called)
        self.assertEqual(list(self.module.EXPORTS.iterdir()), [])


if __name__ == "__main__":
    unittest.main()
