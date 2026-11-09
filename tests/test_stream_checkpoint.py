"""Synthetic same-timestamp and ACK checkpoint regression tests."""

import contextlib
import io
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest import mock

with mock.patch.object(Path, "read_text", side_effect=FileNotFoundError):
    import stream_content_batch as stream


class CheckpointTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.patches = contextlib.ExitStack()
        self.addCleanup(self.patches.close)
        for name, value in (("BASE", self.root), ("STATE", self.root / "state.json"),
                            ("DB", self.root / "status.sqlite")):
            self.patches.enter_context(mock.patch.object(stream, name, value))
        self.patches.enter_context(contextlib.redirect_stdout(io.StringIO()))
        self.patches.enter_context(mock.patch.object(
            stream.subprocess, "run", side_effect=AssertionError("external process forbidden")
        ))
        self.patches.enter_context(mock.patch.object(stream, "self_id", return_value=None))
        self.patches.enter_context(mock.patch.object(stream, "now_iso", return_value="synthetic-clock"))
        self.key = "a" * 64
        self.rows = [
            {"timestamp": 1000, "sender_name": "Synthetic sender", "body": f"message {i}"}
            for i in range(stream.CAP)
        ]
        self.fetch = self.patches.enter_context(mock.patch.object(
            stream.ftc, "fetch_window", side_effect=lambda *_args: ({}, list(self.rows))
        ))
        self.patches.enter_context(mock.patch.object(
            stream.ftc, "resolve_conversation", return_value=("synthetic-id", {})
        ))
        self.patches.enter_context(mock.patch.object(stream.ftc, "run_cli", return_value={}))
        self.deliveries = []

        def transfer(batch):
            self.deliveries.append(batch)
            return f"muse-content-{batch['batch_id']}.jsonl"

        self.patches.enter_context(mock.patch.object(stream, "stream_to_mac", side_effect=transfer))
        self.accept = self.patches.enter_context(mock.patch.object(
            stream, "try_receiver_accept", return_value=(True, {"status": "batch_received"})
        ))
        self.patches.enter_context(mock.patch.object(
            stream, "send_completion_msg", return_value=("synthetic-submission", "committed")
        ))
        with sqlite3.connect(stream.DB) as con:
            con.execute("CREATE TABLE messenger_thread_index(thread_key TEXT, updated_at_utc TEXT)")
            con.execute("INSERT INTO messenger_thread_index VALUES (?, ?)",
                        (self.key, stream.ms_to_iso(1000)))
            con.execute("CREATE TABLE messenger_thread_stats(thread_key TEXT, message_count INTEGER)")
            con.execute("INSERT INTO messenger_thread_stats VALUES (?, ?)", (self.key, len(self.rows)))

    @staticmethod
    def new_refs(batch):
        return [line["message_ref"] for line in map(json.loads, batch["payload"].splitlines())
                if line["type"] == "message" and not line["is_context"]]

    def assert_complete_after_boundary_ack(self, mode):
        state = stream.load_state()
        if mode != "immediate":
            self.accept.return_value = (False, {"status": "not_configured"})
        stream._run(state)
        first = self.deliveries[0]
        self.assertTrue(first["has_more"])
        self.assertLess(first["new_count"], len(self.rows))
        if mode == "manual":
            self.assertEqual(stream.confirm(first["batch_id"]), 0)
        elif mode == "pending":
            self.accept.return_value = (True, {"status": "batch_received"})
        self.accept.return_value = (True, {"status": "batch_received"})
        stream._run(stream.load_state())
        self.assertEqual(len(self.deliveries), 2)
        self.assertNotEqual(self.deliveries[0]["batch_id"], self.deliveries[1]["batch_id"])
        delivered = [ref for batch in self.deliveries for ref in self.new_refs(batch)]
        self.assertEqual(len(delivered), len(self.rows))
        self.assertEqual(len(set(delivered)), len(self.rows))
        saved = stream.load_state()
        self.assertEqual(len(saved["threads"][self.key]["last_handed_refs"]), len(self.rows))
        self.assertFalse(saved["threads"][self.key]["has_more"])
        self.assertEqual(saved["batches"], {})
        stream._run(saved)
        self.assertEqual(len(self.deliveries), 2)

    def test_immediate_ack_keeps_same_timestamp_remainder(self):
        self.assert_complete_after_boundary_ack("immediate")

    def test_pending_receiver_ack_keeps_same_timestamp_remainder(self):
        self.assert_complete_after_boundary_ack("pending")

    def test_manual_ack_keeps_same_timestamp_remainder(self):
        self.assert_complete_after_boundary_ack("manual")

    def test_boundary_checkpoint_is_not_advanced_before_ack(self):
        self.accept.return_value = (False, {"status": "not_configured"})
        state = stream.load_state()
        stream._run(state)
        saved = stream.load_state()
        self.assertEqual(saved["threads"], {})
        pending = next(iter(saved["batches"].values()))
        self.assertEqual(set(pending["upto_refs"]), set(self.new_refs(self.deliveries[0])))
        stream._run(saved)
        self.assertEqual(len(self.deliveries), 1)

    def test_legacy_timestamp_checkpoint_does_not_replay_boundary_messages(self):
        self.rows.append({"timestamp": 2000, "sender_name": "Synthetic sender", "body": "new"})
        state = stream.load_state()
        state["threads"][self.key] = {"last_handed_ts": 1000}
        with sqlite3.connect(stream.DB) as con:
            con.execute("UPDATE messenger_thread_index SET updated_at_utc=?", (stream.ms_to_iso(2000),))
        stream._run(state)
        self.assertEqual(self.deliveries[0]["new_count"], 1)
        self.assertEqual(len(stream.load_state()["threads"][self.key]["last_handed_refs"]), 1)

    def test_batch_identity_distinguishes_full_thread_keys(self):
        for row in self.rows:
            row["timestamp"] = 1_800_000_000_000
        first = stream.build_batch(self.key, 0)
        other_key = self.key[:8] + "b" * 56
        second = stream.build_batch(other_key, 0)
        self.assertNotEqual(first["batch_id"], second["batch_id"])
        initial = stream.build_initial_batch(self.key)
        for batch in (first, second, initial):
            self.assertLessEqual(len("muse-dh-stream-" + batch["batch_id"]), 128)

    def test_equal_timestamp_selection_is_independent_of_source_order(self):
        first = stream.build_batch(self.key, 0)
        self.rows.reverse()
        second = stream.build_batch(self.key, 0)
        self.assertEqual(self.new_refs(first), self.new_refs(second))
        self.assertEqual(first["batch_id"], second["batch_id"])

    def test_initial_context_cannot_replace_existing_checkpoint_with_new_diffs(self):
        state = stream.load_state()
        state["threads"][self.key] = {"last_handed_ts": 500}
        stream.save_state(state)
        before = stream.STATE.read_bytes()
        self.assertEqual(stream._initial_context(self.key, state), 2)
        self.fetch.assert_not_called()
        self.assertEqual(stream.STATE.read_bytes(), before)
        self.assertEqual(self.deliveries, [])

    def test_initial_ack_records_boundary_without_chasing_older_truncated_context(self):
        with sqlite3.connect(stream.DB) as con:
            con.execute("UPDATE messenger_thread_stats SET message_count=?", (stream.CAP + 1,))
        self.assertEqual(stream._initial_context(self.key), 0)
        checkpoint = stream.load_state()["threads"][self.key]
        self.assertEqual(checkpoint["last_handed_ts"], 1000)
        self.assertEqual(len(checkpoint["last_handed_refs"]), len(self.rows))
        self.assertFalse(checkpoint["has_more"])
        self.assertTrue(self.deliveries[0]["shortfall"])

    def test_stale_ack_cannot_erase_newer_checkpoint_or_continuation(self):
        state = stream.load_state()
        checkpoint = {"last_handed_ts": 2000, "last_handed_refs": ["existing-ref"], "has_more": True}
        state["threads"][self.key] = checkpoint.copy()
        state["batches"]["old-batch"] = {
            "thread_key": self.key, "upto_ts": 1000, "upto_refs": ["old-ref"], "has_more": False
        }
        stream.save_state(state)
        self.assertEqual(stream.confirm("old-batch"), 0)
        self.assertEqual(stream.load_state()["threads"][self.key], checkpoint)

    def test_legacy_pending_record_remains_confirmable(self):
        state = stream.load_state()
        state["batches"]["legacy-batch"] = {"thread_key": self.key, "upto_ts": 1000}
        stream.save_state(state)
        self.assertEqual(stream.confirm("legacy-batch"), 0)
        checkpoint = stream.load_state()["threads"][self.key]
        self.assertEqual(checkpoint["last_handed_ts"], 1000)
        self.assertNotIn("last_handed_refs", checkpoint)

    def test_stalled_fetch_gap_is_flagged_even_below_the_cap(self):
        self.fetch.side_effect = None
        self.fetch.return_value = ({"context_truncation": {
            "truncated": True, "reason": "pagination_stalled"
        }}, self.rows[:2])
        batch = stream.build_batch(self.key, 500)
        self.assertTrue(batch["shortfall"])

    def test_first_diff_delivery_reports_known_fetch_truncation(self):
        self.fetch.side_effect = None
        self.fetch.return_value = ({"context_truncation": {
            "truncated": True, "reason": "pagination_stalled"
        }}, self.rows[:2])
        self.assertTrue(stream.build_batch(self.key, 0)["shortfall"])

    def test_truncation_before_an_already_covered_position_is_not_a_diff_gap(self):
        self.fetch.side_effect = None
        self.fetch.return_value = ({"context_truncation": {
            "truncated": True, "reason": "cap"
        }}, [self.rows[0], {"timestamp": 2000, "sender_name": "Synthetic sender", "body": "new"}])
        self.assertFalse(stream.build_batch(self.key, 1000)["shortfall"])

    def test_initial_fetch_truncation_overrides_stale_retained_count(self):
        self.fetch.side_effect = None
        self.fetch.return_value = ({"context_truncation": {
            "truncated": True, "reason": "pagination_stalled"
        }}, self.rows[:2])
        with sqlite3.connect(stream.DB) as con:
            con.execute("UPDATE messenger_thread_stats SET message_count=2")
        batch = stream.build_initial_batch(self.key)
        self.assertTrue(batch["shortfall"])
        self.assertTrue(batch["has_more"])


if __name__ == "__main__":
    unittest.main()
