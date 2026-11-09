"""Execute the SSH command locally against disposable files; never call SSH/SCP."""
import contextlib
from datetime import datetime, timezone
import hashlib
import io
import json
import os
from pathlib import Path
import shutil
import sqlite3
import stat
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

from test_deposit_snapshot import SOURCE_ROOT, load_module


class SnapshotPublicationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.module = load_module()
        self.module.DB = self.root / "status.sqlite"
        self.module.STATE = self.root / "state.json"
        self.module.EXPORTS = self.root / "exports"
        self.module.REMOTE_DIR = str(self.root / "remote")
        self.module.SSH_HOST = "offline-fixture"
        self.module.REMOTE_PYTHON = sys.executable
        self.remote = Path(self.module.REMOTE_DIR)
        self.remote.mkdir()
        self.latest = self.remote / "messenger-status-latest.json"
        self.old_manifest = {"file": "previous.sqlite3", "sha256": "previous"}
        self.latest.write_text(json.dumps(self.old_manifest))
        schema = (SOURCE_ROOT / "docs/schema.md").read_text().split("```sql\n", 1)[1].split("```", 1)[0]
        with contextlib.closing(sqlite3.connect(self.module.DB)) as con:
            con.executescript(schema)
            con.execute("INSERT INTO messenger_meta VALUES ('schema_version', '3')")
            con.execute("INSERT INTO messenger_thread_index (thread_key, display_name) VALUES ('synthetic-key', 'Before')")
            con.execute("INSERT INTO messenger_status_snapshot (captured_at_utc, connector_connected) VALUES ('old', 1)")
            con.commit()
        self.corrupt = False
        self.lose_response = False
        self.transfers = []
        self.env = os.environ.copy()
        self.hooks = self.root / "hooks"
        self.hooks.mkdir()
        # Child-process hooks introduce a competing creator exactly at the
        # publication syscall, or fail pointer replacement before it happens.
        (self.hooks / "sitecustomize.py").write_text('''import os, stat
_original_link, _original_replace = os.link, os.replace

def before(src, dst):
    name = os.path.basename(dst)
    if name.endswith(".sqlite3") and os.environ.get("FIXTURE_RACE"):
        with open(dst, "xb") as stream:
            stream.write(b"existing immutable version")
    if name == "messenger-status-latest.json":
        with open(os.environ["FIXTURE_MODE_FILE"], "w") as stream:
            stream.write(str(stat.S_IMODE(os.stat(src).st_mode)))
        if os.environ.get("FIXTURE_FAIL_MANIFEST"):
            raise OSError("synthetic manifest failure")

def link(src, dst, *args, **kwargs):
    before(src, dst)
    return _original_link(src, dst, *args, **kwargs)

def replace(src, dst, *args, **kwargs):
    before(src, dst)
    return _original_replace(src, dst, *args, **kwargs)

os.link, os.replace = link, replace
''')
        self.bin = self.root / "bin"
        self.bin.mkdir()
        mv = self.bin / "mv"
        mv.write_text(f"#!{sys.executable}\nimport os, sys\nos.replace(sys.argv[-2], sys.argv[-1])\n")
        mv.chmod(0o700)
        self.mode_file = self.root / "manifest-mode"
        self.env.update({"PYTHONPATH": str(self.hooks), "PATH": str(self.bin) + os.pathsep + self.env.get("PATH", ""),
                         "FIXTURE_MODE_FILE": str(self.mode_file)})

    def transport(self, cmd, *, input=None):
        self.transfers.append((cmd, input is not None))
        if cmd[0] == "scp":
            target = Path(cmd[-1].split(":", 1)[1])
            shutil.copyfile(cmd[1], target)
            if self.corrupt and target.name.endswith(".sqlite3.partial"):
                payload = target.read_bytes()
                target.write_bytes(bytes([payload[0] ^ 1]) + payload[1:])
            return subprocess.CompletedProcess(cmd, 0, b"", b"")
        if cmd[0] != "ssh":
            raise AssertionError("Unexpected transport executable")
        if self.corrupt and input:
            input = bytes([input[0] ^ 1]) + input[1:]
        result = subprocess.run(["/bin/sh", "-c", cmd[-1]], input=input,
                                check=True, capture_output=True, env=self.env, timeout=15)
        legacy_manifest_install = "messenger-status-latest.json.partial" in cmd[-1] and "mv " in cmd[-1]
        if self.lose_response and (input is not None or legacy_manifest_install):
            self.lose_response = False
            raise subprocess.TimeoutExpired(cmd, 300)
        return result

    def invoke(self):
        old_umask = os.umask(0o022)
        try:
            with mock.patch.object(self.module, "run", side_effect=self.transport), contextlib.redirect_stdout(io.StringIO()):
                return self.module.main()
        finally:
            os.umask(old_umask)

    def test_verified_snapshot_and_manifest_are_private_before_publication(self):
        self.assertEqual(self.invoke(), 0)
        manifest = json.loads(self.latest.read_text())
        payload = (self.remote / manifest["file"]).read_bytes()
        self.assertEqual(manifest["bytes"], len(payload))
        self.assertEqual(manifest["sha256"], hashlib.sha256(payload).hexdigest())
        self.assertEqual(stat.S_IMODE((self.remote / manifest["file"]).stat().st_mode), 0o600)
        self.assertEqual(int(self.mode_file.read_text()), 0o600)
        self.assertEqual(manifest["previous_file"], "previous.sqlite3")
        self.assertFalse(list(self.remote.glob("*.partial")))
        self.assertEqual(stat.S_IMODE(self.module.STATE.stat().st_mode), 0o600)

    def test_competing_final_name_cannot_be_replaced(self):
        self.env["FIXTURE_RACE"] = "1"
        with self.assertRaises((subprocess.CalledProcessError, RuntimeError)):
            self.invoke()
        finals = list(self.remote.glob("messenger-status-bridge-*.sqlite3"))
        self.assertEqual(len(finals), 1)
        self.assertEqual(finals[0].read_bytes(), b"existing immutable version")
        self.assertEqual(json.loads(self.latest.read_text()), self.old_manifest)
        self.assertIn("pending", json.loads(self.module.STATE.read_text()))
        self.assertFalse(list(self.remote.glob("*.partial")))

    def test_corrupt_transfer_does_not_publish_or_advance_state(self):
        self.corrupt = True
        with self.assertRaises((subprocess.CalledProcessError, RuntimeError)):
            self.invoke()
        self.assertEqual(json.loads(self.latest.read_text()), self.old_manifest)
        self.assertFalse(list(self.remote.glob("messenger-status-bridge-*.sqlite3")))
        self.assertIn("pending", json.loads(self.module.STATE.read_text()))
        self.assertFalse(list(self.remote.glob("*.partial")))

    def test_manifest_failure_preserves_previous_pointer_and_local_state(self):
        self.env["FIXTURE_FAIL_MANIFEST"] = "1"
        with self.assertRaises((subprocess.CalledProcessError, RuntimeError)):
            self.invoke()
        self.assertEqual(json.loads(self.latest.read_text()), self.old_manifest)
        self.assertIn("pending", json.loads(self.module.STATE.read_text()))
        self.assertFalse(list(self.remote.glob("*.partial")))

    def test_existing_local_version_is_not_unlinked_on_same_name_retry(self):
        class FrozenDatetime:
            @staticmethod
            def now(tz):
                return datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
        with mock.patch.object(self.module, "datetime", FrozenDatetime):
            self.assertEqual(self.invoke(), 0)
            state = json.loads(self.module.STATE.read_text())
            local = self.module.EXPORTS / state["file"]
            original = local.read_bytes()
            with contextlib.closing(sqlite3.connect(self.module.DB)) as con:
                con.execute("UPDATE messenger_thread_index SET display_name = 'Changed'")
                con.commit()
            with self.assertRaises((FileExistsError, subprocess.CalledProcessError, RuntimeError)):
                self.invoke()
        self.assertEqual(local.read_bytes(), original)
        self.assertEqual(json.loads(self.module.STATE.read_text()), state)

    def test_lost_publication_response_is_reconciled_without_retransmission(self):
        self.lose_response = True
        with self.assertRaises((subprocess.TimeoutExpired, RuntimeError)):
            self.invoke()
        pending = json.loads(self.module.STATE.read_text())["pending"]
        published = self.latest.read_bytes()
        # Recovery must not open or rebuild the source before diagnosing pending.
        self.module.DB.unlink()
        self.assertEqual(self.invoke(), 0)
        self.assertEqual(self.latest.read_bytes(), published)
        self.assertEqual(json.loads(self.module.STATE.read_text()), {
            key: pending[key] for key in ("content_hash", "file", "backup_utc")
        })
        self.assertEqual(len(self.transfers), 2)
        self.assertEqual([has_payload for _, has_payload in self.transfers], [True, False])

    def test_unconfirmed_pending_prevents_a_fresh_timestamp_or_upload(self):
        self.env["FIXTURE_FAIL_MANIFEST"] = "1"
        with self.assertRaises((subprocess.CalledProcessError, RuntimeError)):
            self.invoke()
        pending_state = self.module.STATE.read_bytes()
        versions = list(self.remote.glob("messenger-status-bridge-*.sqlite3"))
        self.assertEqual(len(versions), 1)
        del self.env["FIXTURE_FAIL_MANIFEST"]
        with contextlib.closing(sqlite3.connect(self.module.DB)) as con:
            con.execute("UPDATE messenger_thread_index SET display_name = 'New source state'")
            con.commit()
        with self.assertRaisesRegex(RuntimeError, "reconciliation required"):
            self.invoke()
        self.assertEqual(self.module.STATE.read_bytes(), pending_state)
        self.assertEqual(list(self.remote.glob("messenger-status-bridge-*.sqlite3")), versions)
        self.assertEqual(json.loads(self.latest.read_text()), self.old_manifest)
        self.assertEqual([has_payload for _, has_payload in self.transfers], [True, False])

    def test_pending_file_existence_and_manifest_do_not_bypass_byte_verification(self):
        self.lose_response = True
        with self.assertRaises((subprocess.TimeoutExpired, RuntimeError)):
            self.invoke()
        pending_state = self.module.STATE.read_bytes()
        pending = json.loads(pending_state)["pending"]
        version = self.remote / pending["file"]
        payload = version.read_bytes()
        version.write_bytes(bytes([payload[0] ^ 1]) + payload[1:])
        with self.assertRaisesRegex(RuntimeError, "reconciliation required"):
            self.invoke()
        self.assertEqual(self.module.STATE.read_bytes(), pending_state)
        self.assertEqual([has_payload for _, has_payload in self.transfers], [True, False])

    def test_remote_path_with_spaces_and_quotes_is_literal(self):
        self.module.REMOTE_DIR = str(self.root / "incoming with 'quotes'")
        self.remote = Path(self.module.REMOTE_DIR)
        self.remote.mkdir()
        self.latest = self.remote / "messenger-status-latest.json"
        self.assertEqual(self.invoke(), 0)
        manifest = json.loads(self.latest.read_text())
        self.assertTrue((self.remote / manifest["file"]).is_file())


if __name__ == "__main__":
    unittest.main()
