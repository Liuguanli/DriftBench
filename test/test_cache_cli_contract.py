from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def _run(*arguments: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "driftbench.cli", *arguments],
        cwd=REPO_ROOT,
        check=False,
        capture_output=True,
        text=True,
    )


class CacheCLIContractTests(unittest.TestCase):
    def test_off_json_is_one_document_and_does_not_parse_azure_config(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            request = root / "request.yaml"
            request.write_text(
                "schema: driftbench.artifact-request/v1\n"
                "benchmark: ycsb\nartifact_type: data\n"
                "parameters:\n  record_count: 2\n",
                encoding="utf-8",
            )
            proc = _run(
                "cache",
                "materialize",
                "--request",
                str(request),
                "--output-dir",
                str(root / "out"),
                "--cache-mode",
                "off",
                "--azure-cache-config",
                str(root / "does-not-exist.yaml"),
                "--credential-env-file",
                str(root / "also-does-not-exist.env"),
                "--json",
            )
            self.assertEqual(proc.returncode, 0, proc.stderr)
            payload = json.loads(proc.stdout)
            self.assertTrue(payload["ok"])
            self.assertEqual(payload["cache_outcome"], "disabled_generated")
            self.assertEqual(payload["materialization_source"], "generated")
            self.assertEqual(payload["remote_entry_status"], "not_checked")
            self.assertFalse(payload["force"])
            self.assertEqual(payload["warnings"], [])
            self.assertNotIn("[driftbench]", proc.stdout)
            self.assertEqual(proc.stderr, "")

    def test_strict_request_error_is_exit_three_and_redacted_single_json(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            secret = "never-print-this-value"
            request = root / "request.yaml"
            request.write_text(
                "schema: driftbench.artifact-request/v1\n"
                "benchmark: ycsb\nartifact_type: data\n"
                f"unknown_secret_field: {secret}\n",
                encoding="utf-8",
            )
            proc = _run(
                "cache", "materialize", "--request", str(request),
                "--output-dir", str(root / "out"), "--json",
            )
            self.assertEqual(proc.returncode, 3)
            payload = json.loads(proc.stdout)
            self.assertFalse(payload["ok"])
            self.assertEqual(payload["outcome"], "configuration_error")
            self.assertNotIn("output_dir", payload)
            self.assertNotIn(str((root / "out").resolve()), proc.stdout + proc.stderr)
            self.assertNotIn(secret, proc.stdout + proc.stderr)
            self.assertEqual(proc.stderr, "")

    def test_enabled_cache_requires_non_secret_config(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            request = root / "request.yaml"
            request.write_text(
                "schema: driftbench.artifact-request/v1\n"
                "benchmark: ycsb\nartifact_type: data\n"
                "parameters:\n  record_count: 2\n",
                encoding="utf-8",
            )
            proc = _run(
                "cache", "materialize", "--request", str(request),
                "--output-dir", str(root / "out"),
                "--cache-mode", "read", "--json",
            )
            self.assertEqual(proc.returncode, 3)
            self.assertEqual(json.loads(proc.stdout)["outcome"], "configuration_error")
            self.assertEqual(proc.stderr, "")

    def test_generator_failure_is_exit_four_without_exception_detail(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            request = root / "request.yaml"
            missing = root / "missing-source"
            request.write_text(
                "schema: driftbench.artifact-request/v1\n"
                "benchmark: tpch\nartifact_type: data\n"
                f"parameters:\n  mode: copy\n  source_dir: '{missing.as_posix()}'\n",
                encoding="utf-8",
            )
            proc = _run(
                "cache", "materialize", "--request", str(request),
                "--output-dir", str(root / "out"), "--json",
            )
            self.assertEqual(proc.returncode, 4)
            payload = json.loads(proc.stdout)
            self.assertEqual(payload["outcome"], "runtime_error")
            self.assertEqual(payload["error"], "local artifact generation failed")
            self.assertNotIn(str(missing), proc.stdout + proc.stderr)

    def test_json_parse_error_suppresses_argparse_usage(self) -> None:
        proc = _run("cache", "materialize", "--json")
        self.assertEqual(proc.returncode, 3)
        payload = json.loads(proc.stdout)
        self.assertEqual(payload["command"], "cache materialize")
        self.assertEqual(proc.stderr, "")


if __name__ == "__main__":
    unittest.main()
