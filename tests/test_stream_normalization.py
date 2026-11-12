"""Receiver message-shape regressions with synthetic source responses."""

import contextlib
import hashlib
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest import mock

with mock.patch.object(Path, "read_text", side_effect=FileNotFoundError):
    import stream_content_batch as stream


class NormalizationTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.patches = contextlib.ExitStack()
        self.addCleanup(self.patches.close)
        self.patches.enter_context(mock.patch.object(stream, "DB", self.root / "status.sqlite"))
        self.patches.enter_context(mock.patch.object(
            stream.subprocess, "run", side_effect=AssertionError("external process forbidden")
        ))
        self.patches.enter_context(mock.patch.object(stream, "self_id", return_value=None))
        self.patches.enter_context(mock.patch.object(stream.ftc, "run_cli", return_value={}))
        self.patches.enter_context(mock.patch.object(
            stream.ftc, "resolve_conversation", return_value=("synthetic-id", {})
        ))
        self.key = "a" * 64
        self.rows = []
        self.patches.enter_context(mock.patch.object(
            stream.ftc, "fetch_window", side_effect=lambda *_args: ({}, list(self.rows))
        ))
        with sqlite3.connect(stream.DB) as con:
            con.execute("CREATE TABLE messenger_thread_stats(thread_key TEXT, message_count INTEGER)")

    @staticmethod
    def messages(batch):
        return [line for line in map(json.loads, batch["payload"].splitlines())
                if line["type"] == "message"]

    def test_attachment_only_diff_uses_empty_text(self):
        self.rows = [{"timestamp": 1000, "sender_name": "Synthetic", "body": None,
                      "attachments": [{"synthetic": True}]}]
        message = self.messages(stream.build_batch(self.key, 0))[0]
        self.assertEqual(message["text"], "")
        self.assertTrue(message["has_attachment"])

    def test_attachment_only_initial_context_uses_empty_text(self):
        self.rows = [{"timestamp": 1000, "sender_name": "Synthetic",
                      "attachments": [{"synthetic": True}]}]
        message = self.messages(stream.build_initial_batch(self.key))[0]
        self.assertEqual(message["text"], "")
        self.assertTrue(message["is_context"])

    def test_repeated_published_identity_is_emitted_once_before_capping(self):
        self.rows = [
            {"message_id": f"source-{i}", "timestamp": 1000, "sender_name": "Synthetic", "body": "same"}
            for i in range(stream.CAP)
        ]
        batch = stream.build_batch(self.key, 0)
        self.assertEqual(batch["new_count"], 1)
        self.assertEqual(len(self.messages(batch)), 1)
        self.assertFalse(batch["has_more"])

    def test_initial_context_has_unique_message_refs(self):
        self.rows = [{"timestamp": 1000, "sender_name": "Synthetic", "body": "same"}] * 3
        batch = stream.build_initial_batch(self.key)
        self.assertEqual(batch["context_count"], 1)
        self.assertEqual(len(self.messages(batch)), 1)

    def test_missing_null_and_empty_bodies_share_one_empty_text_identity(self):
        self.rows = [
            {"timestamp": 1000, "sender_name": "Synthetic"},
            {"timestamp": 1000, "sender_name": "Synthetic", "body": None},
            {"timestamp": 1000, "sender_name": "Synthetic", "body": ""},
        ]
        messages = self.messages(stream.build_batch(self.key, 0))
        self.assertEqual(len(messages), 1)
        self.assertEqual(messages[0]["text"], "")

    def test_unsupported_body_shapes_fail_without_echoing_private_content(self):
        for value in ({"secret": "synthetic-private-sentinel"}, ["synthetic-private-sentinel"], 7, True):
            with self.subTest(type=type(value).__name__):
                self.rows = [{"timestamp": 1000, "sender_name": "Synthetic", "body": value}]
                with self.assertRaises(RuntimeError) as caught:
                    stream.build_batch(self.key, 0)
                self.assertNotIn("synthetic-private-sentinel", str(caught.exception))

    def test_existing_string_text_and_reference_remain_unchanged(self):
        body = "Original | text\nwith unicode \u65e5\u672c\u8a9e"
        self.rows = [{"timestamp": 1000, "sender_name": "Synthetic", "body": body}]
        message = self.messages(stream.build_batch(self.key, 0))[0]
        expected = hashlib.sha256(f"{self.key}|1000|Synthetic|{body}".encode()).hexdigest()
        self.assertEqual(message["text"], body)
        self.assertEqual(message["message_ref"], expected)

    def test_deduplication_does_not_hide_a_capped_fetch_gap(self):
        self.rows = [{"timestamp": 1000, "sender_name": "Synthetic", "body": "same"}] * stream.CAP
        batch = stream.build_batch(self.key, 500)
        self.assertTrue(batch["shortfall"])

    def test_different_content_sharing_the_existing_reference_fails_closed(self):
        self.rows = [
            {"timestamp": 1000, "sender_name": "Synthetic|private", "body": "one"},
            {"timestamp": 1000, "sender_name": "Synthetic", "body": "private|one"},
        ]
        with self.assertRaises(RuntimeError) as caught:
            stream.build_batch(self.key, 0)
        self.assertNotIn("private", str(caught.exception))

    def test_conflicting_sender_identity_is_not_silently_coalesced(self):
        self.rows = [
            {"timestamp": 1000, "sender_name": "Synthetic", "sender_id": "source-one", "body": "same"},
            {"timestamp": 1000, "sender_name": "Synthetic", "sender_id": "source-two", "body": "same"},
        ]
        with self.assertRaises(RuntimeError):
            stream.build_batch(self.key, 0)


if __name__ == "__main__":
    unittest.main()
