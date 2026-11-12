"""Synthetic fetch regressions; no live configuration or connector is read."""

import contextlib
import hashlib
import io
import json
from pathlib import Path
import subprocess
import traceback
import unittest
from unittest.mock import patch

with patch.object(Path, "read_text", side_effect=FileNotFoundError):
    import fetch_thread_context as ftc


NOW = 20 * 86400
THREAD_ID = "synthetic-private-conversation"
THREAD_KEY = hashlib.sha256(THREAD_ID.encode()).hexdigest()


def message(number, days_ago=0):
    return {
        "message_id": f"synthetic-message-{number}",
        "timestamp": (NOW - days_ago * 86400) * 1000 + number,
        "sender_name": "Synthetic Sender",
        "body": f"Synthetic text {number}",
    }


def page(rows):
    return {"success": True, "data": {"thread_name": "Synthetic thread", "messages": rows}}


class FetchWindowTests(unittest.TestCase):
    def test_capped_context_reports_omission_and_returns_latest_rows(self):
        rows = [message(n) for n in range(1, 6)]
        with patch.object(ftc, "PAGE", 3), patch.object(ftc.time, "time", return_value=NOW), patch.object(
            ftc, "run_cli", side_effect=[page(rows[2:][::-1]), page(rows[:2][::-1])]
        ) as source:
            meta, chosen = ftc.fetch_window(THREAD_ID, 2, 7, 4)
        self.assertEqual(chosen, rows[1:])
        self.assertEqual(meta.get("context_truncation"), {"truncated": True, "reason": "cap"})
        self.assertEqual(source.call_count, 2)

    def test_count_or_day_window_keeps_the_larger_context(self):
        rows = [message(n, days_ago=10 - n) for n in range(1, 10)]
        for minimum, days, expected in [(2, 4, rows[-4:]), (5, 1, rows[-5:])]:
            with self.subTest(minimum=minimum, days=days), patch.object(ftc, "PAGE", 3), patch.object(
                ftc.time, "time", return_value=NOW
            ), patch.object(ftc, "run_cli", side_effect=[page(rows[-3:][::-1]), page(rows[-6:-3][::-1])]):
                meta, chosen = ftc.fetch_window(THREAD_ID, minimum, days, 20)
            self.assertEqual(chosen, expected)
            self.assertEqual(meta.get("context_truncation"), {"truncated": False, "reason": None})

    def test_days_zero_keeps_retained_context_for_stream_caller(self):
        rows = [message(n) for n in range(1, 4)]
        with patch.object(ftc, "PAGE", 2), patch.object(
            ftc, "run_cli", side_effect=[page(rows[1:][::-1]), page(rows[:1])]
        ) as source:
            meta, chosen = ftc.fetch_window(THREAD_ID, 1, 0, 10)
        self.assertEqual(chosen, rows)
        self.assertEqual(source.call_count, 2)
        self.assertFalse(meta.get("context_truncation", {}).get("truncated", True))

    def test_exhausted_source_at_exact_cap_is_complete(self):
        rows = [message(n) for n in range(1, 4)]
        with patch.object(ftc, "PAGE", 5), patch.object(ftc, "run_cli", return_value=page(rows[::-1])):
            meta, chosen = ftc.fetch_window(THREAD_ID, 1, 0, 3)
        self.assertEqual(chosen, rows)
        self.assertEqual(meta.get("context_truncation"), {"truncated": False, "reason": None})

    def test_empty_source_is_complete(self):
        with patch.object(ftc, "run_cli", return_value=page([])):
            meta, chosen = ftc.fetch_window(THREAD_ID, 1, 0, 10)
        self.assertEqual(chosen, [])
        self.assertEqual(meta.get("context_truncation"), {"truncated": False, "reason": None})

    def test_nonadvancing_source_reports_stalled_pagination(self):
        rows = [message(2), message(3)]
        for following in (rows, [message(3), message(4)]):
            with self.subTest(following=following), patch.object(ftc, "PAGE", 2), patch.object(
                ftc, "run_cli", side_effect=[page(rows), page(following)]
            ) as source:
                meta, chosen = ftc.fetch_window(THREAD_ID, 1, 0, 10)
            self.assertEqual(source.call_count, 2)
            self.assertTrue(chosen)
            self.assertEqual(meta.get("context_truncation"), {"truncated": True, "reason": "pagination_stalled"})

    def test_invalid_window_bounds_do_not_call_source(self):
        for minimum, days, cap in [(0, 7, 5), (-1, 7, 5), (2, -1, 5), (2, 7, 0), (6, 7, 5), (True, 7, 5)]:
            with self.subTest(minimum=minimum, days=days, cap=cap), patch.object(ftc, "run_cli") as source:
                with self.assertRaises(ValueError):
                    ftc.fetch_window(THREAD_ID, minimum, days, cap)
                source.assert_not_called()

    def test_malformed_source_cannot_look_like_empty_or_complete_context(self):
        malformed = [{}, {"data": []}, {"data": {}}, {"data": {"messages": None}}, page(["bad row"])]
        malformed += [page([{**message(1), "timestamp": ts}]) for ts in (None, "123", True, float("nan"), float("inf"), 10**100)]
        for response in malformed:
            with self.subTest(response=response), patch.object(ftc, "run_cli", return_value=response):
                with self.assertRaises(RuntimeError):
                    ftc.fetch_window(THREAD_ID, 1, 0, 5)


class ConnectorFailureTests(unittest.TestCase):
    def test_cli_calls_have_a_finite_timeout(self):
        with patch.object(ftc.subprocess, "run", return_value=subprocess.CompletedProcess([], 0, '{"success": true}', "")) as run:
            self.assertEqual(ftc.run_cli(["check"]), {"success": True})
        self.assertEqual(run.call_args.kwargs.get("timeout"), 120)

    def test_cli_failures_do_not_expose_arguments_or_source_output(self):
        secret = "synthetic-private-source-output"
        failures = [
            subprocess.CompletedProcess([], 1, secret, secret),
            subprocess.TimeoutExpired(["messages", "--cid", THREAD_ID], 120, output=secret, stderr=secret),
            FileNotFoundError(secret),
            subprocess.CompletedProcess([], 0, secret, ""),
            subprocess.CompletedProcess([], 0, json.dumps([secret]), ""),
            subprocess.CompletedProcess([], 0, json.dumps({"success": False, "error": secret}), ""),
        ]
        for failure in failures:
            kwargs = {"side_effect": failure} if isinstance(failure, Exception) else {"return_value": failure}
            with self.subTest(failure=type(failure).__name__), patch.object(ftc.subprocess, "run", **kwargs):
                try:
                    ftc.run_cli(["messages", "--cid", THREAD_ID])
                except RuntimeError as exc:
                    rendered = "".join(traceback.format_exception(exc))
                    self.assertNotIn(THREAD_ID, rendered)
                    self.assertNotIn(secret, rendered)
                else:
                    self.fail("A failed connector response was accepted")

    def test_invalid_thread_listing_is_an_error_not_not_found(self):
        malformed = [{}, {"data": []}, {"data": {"threads": [], "_cache": []}}, {"data": {"threads": ["invalid"]}}, {"data": {"threads": [{}]}}]
        malformed += [{"data": {"threads": [], "_cache": {"cached_thread_count": count}}} for count in (-1, True, 1.5, "invalid")]
        for response in malformed:
            with self.subTest(response=response), patch.object(ftc, "run_cli", return_value=response):
                with self.assertRaises(RuntimeError):
                    ftc.resolve_conversation(THREAD_KEY)


class FetchCommandTests(unittest.TestCase):
    def test_json_contains_truncation_metadata_and_no_raw_source_ids(self):
        rows = [{**message(n), "sender_id": "synthetic-private-sender"} for n in range(1, 4)]
        listing = {"data": {"threads": [{"conversation_id": THREAD_ID, "name": "Synthetic thread"}], "_cache": {"cached_thread_count": "1"}}}
        output = io.StringIO()
        with patch.object(ftc.sys, "argv", ["fetch", "--thread-key", THREAD_KEY, "--limit", "1", "--days", "0", "--cap", "2"]), patch.object(
            ftc, "run_cli", side_effect=[listing, listing, page(rows)]
        ), contextlib.redirect_stdout(output):
            result = ftc.main()
        parsed = json.loads(output.getvalue())
        self.assertEqual(result, 0)
        self.assertEqual(parsed.get("context_truncation"), {"truncated": True, "reason": "cap"})
        self.assertEqual(parsed["message_count"], 2)
        self.assertNotIn(THREAD_ID, output.getvalue())
        self.assertNotIn("synthetic-private-sender", output.getvalue())
        self.assertNotIn("synthetic-message-", output.getvalue())

    def test_invalid_cli_bounds_fail_before_resolving_a_thread(self):
        with patch.object(ftc.sys, "argv", ["fetch", "--thread-key", THREAD_KEY, "--limit", "0"]), patch.object(
            ftc, "resolve_conversation"
        ) as resolve, contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit) as raised:
                ftc.main()
        self.assertEqual(raised.exception.code, 2)
        resolve.assert_not_called()

    def test_source_failure_has_nonzero_exit_and_sanitized_stderr(self):
        output, errors = io.StringIO(), io.StringIO()
        with patch.object(ftc.sys, "argv", ["fetch", "--thread-key", THREAD_KEY]), patch.object(
            ftc, "resolve_conversation", side_effect=RuntimeError(THREAD_ID)
        ), contextlib.redirect_stdout(output), contextlib.redirect_stderr(errors):
            result = ftc.main()
        self.assertEqual(result, 1)
        self.assertEqual(output.getvalue(), "")
        self.assertNotIn(THREAD_ID, errors.getvalue())
        self.assertIn("error", errors.getvalue())


if __name__ == "__main__":
    unittest.main()
