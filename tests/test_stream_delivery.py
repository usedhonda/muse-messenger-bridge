"""Metadata-only recovery tests; no live connector or SSH command is run."""

import contextlib
import hashlib
import io
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest import mock

with mock.patch.object(Path, "read_text", side_effect=FileNotFoundError):
    import stream_content_batch as stream


class DeliveryStateTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.patches = contextlib.ExitStack()
        self.addCleanup(self.patches.close)
        self.patches.enter_context(mock.patch.object(stream, "BASE", self.root))
        self.patches.enter_context(mock.patch.object(stream, "STATE", self.root / "state.json"))
        self.patches.enter_context(mock.patch.object(
            stream.subprocess, "run", side_effect=AssertionError("external process forbidden")
        ))
        self.patches.enter_context(contextlib.redirect_stdout(io.StringIO()))
        self.key = "a" * 64
        self.batch = {
            "batch_id": "muse-c-aaaaaaaa-1234",
            "thread_key": self.key,
            "upto_ms": 1234,
            "fetched_at_utc": "1970-01-01T00:00:02Z",
            "new_count": 1,
            "context_count": 0,
            "has_more": False,
            "shortfall": False,
            "payload": b'{"text":"synthetic-private-body-sentinel"}\n',
        }
        self.filename = "muse-content-" + self.batch["batch_id"] + ".jsonl"
        self.state = stream.load_state()
        stream.save_state(self.state)
        self.patches.enter_context(mock.patch.object(
            stream, "changed_threads", return_value=[(0, self.key, 1234)]
        ))
        self.build = self.patches.enter_context(mock.patch.object(
            stream, "build_batch", return_value=self.batch
        ))
        self.patches.enter_context(mock.patch.object(
            stream, "build_initial_batch", return_value=self.batch
        ))
        self.transfer = self.patches.enter_context(mock.patch.object(
            stream, "stream_to_mac", return_value=self.filename
        ))
        self.accept = self.patches.enter_context(mock.patch.object(
            stream, "try_receiver_accept", return_value=(False, {"status": "not_configured"})
        ))
        self.notice = self.patches.enter_context(mock.patch.object(
            stream, "send_completion_msg", return_value=("submission", "committed")
        ))
        self.diagnose = self.patches.enter_context(mock.patch.object(
            stream, "diagnose", return_value={"hub_acceptance": "unknown"}
        ))

    def pending(self):
        return stream.load_state()["batches"][self.batch["batch_id"]]

    def assert_identity_preserved(self):
        record = self.pending()
        self.assertEqual(record["thread_key"], self.key)
        self.assertEqual(record["upto_ts"], self.batch["upto_ms"])
        self.assertEqual(record["filename"], self.filename)
        self.assertEqual(record["bytes"], len(self.batch["payload"]))
        self.assertEqual(record["sha256"], hashlib.sha256(self.batch["payload"]).hexdigest())
        self.assertNotIn("synthetic-private-body-sentinel", stream.STATE.read_text())
        self.assertNotIn("payload", record)
        self.assertEqual(stream.load_state()["threads"], {})

    def test_stream_timeout_preserves_identity_without_advancing_or_saving_bodies(self):
        self.transfer.side_effect = subprocess.TimeoutExpired("synthetic-ssh", 1)
        with self.assertRaises(subprocess.TimeoutExpired):
            stream._run(self.state)
        self.assert_identity_preserved()
        self.accept.assert_not_called()
        self.notice.assert_not_called()

    def test_initial_context_timeout_preserves_identity(self):
        self.transfer.side_effect = subprocess.TimeoutExpired("synthetic-ssh", 1)
        with self.assertRaises(subprocess.TimeoutExpired):
            stream._initial_context(self.key, self.state)
        self.assert_identity_preserved()
        self.assertEqual(self.pending()["kind"], "initial_context")

    def test_receiver_sees_durable_pending_record_before_it_can_accept(self):
        def accept(*_args):
            self.assert_identity_preserved()
            raise KeyboardInterrupt("simulated process stop after delivery")

        self.accept.side_effect = accept
        with self.assertRaises(KeyboardInterrupt):
            stream._run(self.state)
        self.assert_identity_preserved()

    def test_restart_rechecks_exact_identity_without_rebuilding_or_restreaming(self):
        self.transfer.side_effect = subprocess.TimeoutExpired("synthetic-ssh", 1)
        with self.assertRaises(subprocess.TimeoutExpired):
            stream._run(self.state)
        self.build.reset_mock()
        self.transfer.reset_mock()
        self.accept.return_value = (False, {"status": "receiver_unknown"})
        stream._run(stream.load_state())
        self.accept.assert_called_once_with(
            self.batch["batch_id"], hashlib.sha256(self.batch["payload"]).hexdigest(), self.filename
        )
        self.build.assert_not_called()
        self.transfer.assert_not_called()
        self.assert_identity_preserved()

    def test_successful_receiver_ack_removes_record_and_advances_once(self):
        self.accept.return_value = (True, {"status": "batch_received"})
        stream._run(self.state)
        saved = stream.load_state()
        self.assertEqual(saved["threads"][self.key]["last_handed_ts"], 1234)
        self.assertEqual(saved["batches"], {})
        self.notice.assert_not_called()

    def test_initial_context_success_clears_reserved_record(self):
        self.accept.return_value = (True, {"status": "batch_received"})
        self.assertEqual(stream._initial_context(self.key, self.state), 0)
        saved = stream.load_state()
        self.assertEqual(saved["threads"][self.key]["last_handed_ts"], 1234)
        self.assertEqual(saved["batches"], {})

    def test_notice_timeout_keeps_submission_id_and_is_diagnosed_without_replay(self):
        self.notice.side_effect = subprocess.TimeoutExpired("synthetic-notice", 1)
        with self.assertRaises(subprocess.TimeoutExpired):
            stream._run(self.state)
        submission = self.pending()["msg_submission_id"]
        self.assertEqual(submission, "muse-dh-stream-" + self.batch["batch_id"])
        self.diagnose.return_value = {"hub_acceptance": "committed"}
        stream._run(stream.load_state())
        self.diagnose.assert_called_once_with(submission)
        self.assertEqual(self.notice.call_count, 1)
        self.assertEqual(self.pending()["msg_hub_acceptance"], "committed")
        self.assert_identity_preserved()

    def test_confirmation_does_not_race_an_active_run(self):
        stream._run(self.state)
        before = stream.STATE.read_bytes()
        with stream.single_run() as acquired:
            self.assertTrue(acquired)
            self.assertEqual(stream.confirm(self.batch["batch_id"]), 2)
            self.assertEqual(stream.STATE.read_bytes(), before)
        self.assertEqual(stream.confirm(self.batch["batch_id"]), 0)
        self.assertEqual(stream.load_state()["batches"], {})
        self.assertEqual(stream.load_state()["threads"][self.key]["last_handed_ts"], 1234)


class ReceiverAckTests(unittest.TestCase):
    def test_only_exit_zero_exact_batch_and_hash_ack_is_accepted(self):
        batch_id, digest = "muse-c-synthetic-1", "a" * 64
        cases = [
            (0, {"status": "batch_received", "batch_id": batch_id, "sha256": digest}, True),
            (1, {"status": "batch_received", "batch_id": batch_id, "sha256": digest}, False),
            (0, {"status": "queued", "batch_id": batch_id, "sha256": digest}, False),
            (0, {"status": "batch_received", "batch_id": "other", "sha256": digest}, False),
            (0, {"status": "batch_received", "batch_id": batch_id, "sha256": "b" * 64}, False),
            (0, {"status": "batch_received", "batch_id": batch_id}, False),
        ]
        with mock.patch.object(stream, "RECEIVER_CWD", "/synthetic"), mock.patch.object(
            stream, "RECEIVER_ROOT", "/synthetic/root"
        ):
            for returncode, result, expected in cases:
                with self.subTest(returncode=returncode, result=result), mock.patch.object(
                    stream.subprocess, "run", return_value=subprocess.CompletedProcess(
                        [], returncode, stdout=json.dumps(result), stderr=""
                    )
                ):
                    accepted, _ = stream.try_receiver_accept(batch_id, digest, "synthetic.jsonl")
                    self.assertEqual(accepted, expected)


if __name__ == "__main__":
    unittest.main()
