"""Exercise the generated receiver command locally with synthetic bytes only."""
from pathlib import Path
import stat
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
with patch.object(Path, "read_text", side_effect=FileNotFoundError):
    import stream_content_batch as stream


class ContentPublicationTests(unittest.TestCase):
    def run_publish(self, directory, payload, *, delivered=None, competing=None):
        batch = {"batch_id": "muse-c-fixture-123", "payload": payload}
        filename = "muse-content-muse-c-fixture-123.jsonl"
        final = directory / filename
        real_run = subprocess.run

        def local_ssh(command, **kwargs):
            self.assertEqual(command[:2], ["ssh", "fixture-host"])
            remote = command[2]
            if remote.startswith("test -e "):
                # Original implementation observed absence before a competing
                # publisher committed; the later write must still not clobber it.
                return subprocess.CompletedProcess(command, 1, b"", b"")
            if competing is not None:
                directory.mkdir(parents=True, exist_ok=True)
                final.write_bytes(competing)
            wire = kwargs.get("input") if delivered is None else delivered
            return real_run(["/bin/sh", "-c", remote], input=wire,
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=10)

        with patch.object(stream, "SSH_HOST", "fixture-host"), \
             patch.object(stream, "REMOTE_DIR", str(directory)), \
             patch.object(stream, "RECEIVER_PYTHON", sys.executable), \
             patch.object(stream.subprocess, "run", side_effect=local_ssh):
            result = stream.stream_to_mac(batch)
        self.assertEqual(result, filename)
        return final

    def test_quoted_destination_preserves_bytes_and_private_mode(self):
        with tempfile.TemporaryDirectory() as root:
            directory = Path(root) / "receiver's incoming files"
            payload = '{"text":"synthetic 日本語"}\n'.encode()
            final = self.run_publish(directory, payload)
            self.assertEqual(final.read_bytes(), payload)
            self.assertEqual(stat.S_IMODE(final.stat().st_mode), 0o600)
            self.assertEqual(list(directory.glob("*.part")), [])

    def test_corrupt_or_truncated_input_is_never_published(self):
        for wire in [b"different", b"sho"]:
            with self.subTest(wire=wire), tempfile.TemporaryDirectory() as root:
                directory = Path(root) / "incoming"
                with self.assertRaises(RuntimeError):
                    self.run_publish(directory, b"synthetic", delivered=wire)
                self.assertFalse((directory / "muse-content-muse-c-fixture-123.jsonl").exists())
                self.assertEqual(list(directory.glob("*.part")), [])

    def test_racing_existing_version_is_not_overwritten(self):
        with tempfile.TemporaryDirectory() as root:
            directory = Path(root) / "incoming"
            with self.assertRaises(RuntimeError):
                self.run_publish(directory, b"synthetic", competing=b"previous complete batch")
            self.assertEqual((directory / "muse-content-muse-c-fixture-123.jsonl").read_bytes(),
                             b"previous complete batch")
            self.assertEqual(list(directory.glob("*.part")), [])


if __name__ == "__main__":
    unittest.main()
