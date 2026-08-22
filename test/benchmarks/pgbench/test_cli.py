from __future__ import annotations

import io
import json
import os
import subprocess
import sys
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest.mock import patch

from driftbench.benchmarking.pgbench import (
    _POSTGRESQL_SETTINGS,
    PairedPgBenchOutcome,
    PgBenchStdoutMetrics,
)
from driftbench.cli import main
from ..helpers import REPO_ROOT


class PgBenchCLIContractTests(unittest.TestCase):
    def _run(self, *arguments: str) -> tuple[int, str, str]:
        stdout = io.StringIO()
        stderr = io.StringIO()
        with redirect_stdout(stdout), redirect_stderr(stderr):
            exit_code = main(list(arguments))
        return exit_code, stdout.getvalue(), stderr.getvalue()

    def _arguments(self, root: Path, *extra: str) -> tuple[str, ...]:
        return (
            "benchmark",
            "pgbench",
            "--candidate-script",
            str(root / "candidate.sql"),
            "--output-dir",
            str(root / "results"),
            "--database",
            "driftbench_ci",
            "--json",
            *extra,
        )

    def test_success_is_one_json_document_and_exit_zero(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            result_root = root / "results"
            outcome = PairedPgBenchOutcome(
                ok=True,
                baseline_path=result_root / "baseline.json",
                candidate_path=result_root / "candidate.json",
                decision_path=result_root / "decision.json",
                execution_order_path=result_root / "execution_order.json",
            )
            with patch("driftbench.cli.run_paired_pgbench", return_value=outcome):
                exit_code, stdout, stderr = self._run(*self._arguments(root))

        self.assertEqual(exit_code, 0)
        self.assertEqual(stderr, "")
        payload = json.loads(stdout)
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["outcome"], "passed")
        self.assertEqual(payload["command"], "benchmark pgbench")

    def test_threshold_failure_is_one_json_document_and_exit_five(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            result_root = root / "results"
            outcome = PairedPgBenchOutcome(
                ok=False,
                baseline_path=result_root / "baseline.json",
                candidate_path=result_root / "candidate.json",
                decision_path=result_root / "decision.json",
                execution_order_path=result_root / "execution_order.json",
            )
            with patch("driftbench.cli.run_paired_pgbench", return_value=outcome):
                exit_code, stdout, stderr = self._run(*self._arguments(root))

        self.assertEqual(exit_code, 5)
        self.assertEqual(stderr, "")
        payload = json.loads(stdout)
        self.assertFalse(payload["ok"])
        self.assertEqual(payload["outcome"], "threshold_failed")

    def test_missing_candidate_is_execution_error_with_evidence_and_exit_four(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            exit_code, stdout, stderr = self._run(*self._arguments(root))
            result_root = root / "results"
            order = json.loads(
                (result_root / "execution_order.json").read_text(encoding="utf-8")
            )
            decision = json.loads(
                (result_root / "decision.json").read_text(encoding="utf-8")
            )

        self.assertEqual(exit_code, 4)
        self.assertEqual(stderr, "")
        payload = json.loads(stdout)
        self.assertFalse(payload["ok"])
        self.assertEqual(payload["outcome"], "execution_error")
        self.assertEqual(order["status"], "failed")
        self.assertEqual(len(order["pairs"]), 3)
        self.assertFalse(decision["ok"])

    def test_nonempty_output_directory_fails_without_overwriting_old_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            result_root = root / "results"
            result_root.mkdir()
            old_decision = result_root / "decision.json"
            old_decision.write_text('{"sentinel": true}\n', encoding="utf-8")
            exit_code, stdout, stderr = self._run(*self._arguments(root))
            preserved = old_decision.read_text(encoding="utf-8")

        self.assertEqual(exit_code, 4)
        self.assertEqual(stderr, "")
        self.assertEqual(json.loads(stdout)["outcome"], "execution_error")
        self.assertEqual(preserved, '{"sentinel": true}\n')

    def test_invalid_policy_and_connection_are_configuration_exit_three(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            policy = root / "invalid-policy.json"
            policy.write_text("{}\n", encoding="utf-8")
            exit_code, stdout, stderr = self._run(
                *self._arguments(root, "--policy", str(policy))
            )
            self.assertEqual(exit_code, 3)
            self.assertEqual(stderr, "")
            self.assertEqual(json.loads(stdout)["outcome"], "configuration_error")

            exit_code, stdout, stderr = self._run(
                *self._arguments(root, "--port", "70000")
            )
            self.assertEqual(exit_code, 3)
            self.assertEqual(stderr, "")
            self.assertEqual(json.loads(stdout)["outcome"], "configuration_error")

    def test_measurement_tps_mismatch_is_execution_exit_four_not_threshold_five(self) -> None:
        def phase(**kwargs) -> dict:
            is_measurement = kwargs["duration_seconds"] == 5
            return {
                "actual_seconds": 2.0,
                "parsed": PgBenchStdoutMetrics(
                    transactions_successful=20,
                    transactions_failed=0,
                    transactions_total=20,
                    reported_latency_mean_ms=1.0,
                    reported_tps=999.0 if is_measurement else 10.0,
                    scale_factor=1,
                    clients=2,
                ),
                "latencies_us": [1000] * 20,
                "failure_types": {},
                "artifacts": {
                    "stdout": {"path": "raw/stdout", "sha256": "0" * 64, "bytes": 1},
                    "stderr": {"path": "raw/stderr", "sha256": "0" * 64, "bytes": 1},
                    "transaction_logs": [
                        {"path": "raw/log", "sha256": "0" * 64, "bytes": 1}
                    ],
                },
                "returncode": 0,
            }

        postgresql = (
            {"full": "PostgreSQL 16.9", "major": 16},
            {
                "current_database": "driftbench_ci",
                "settings": {
                    name: {
                        "setting": "fixture-value",
                        "unit": "8kB" if name == "shared_buffers" else None,
                        "source": "default",
                    }
                    for name in _POSTGRESQL_SETTINGS
                },
                "initialization": {
                    "pgbench_branches": 1,
                    "pgbench_tellers": 10,
                    "pgbench_accounts": 100000,
                    "pgbench_history": 0,
                    "scale_factor_inferred": 1,
                },
            },
        )
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "candidate.sql").write_text("SELECT 1;\n", encoding="utf-8")
            with patch(
                "driftbench.benchmarking.pgbench._git_sha", return_value="a" * 40
            ), patch(
                "driftbench.benchmarking.pgbench._pgbench_version",
                return_value={"full": "pgbench (PostgreSQL) 16.9", "major": 16},
            ), patch(
                "driftbench.benchmarking.pgbench._postgresql_environment",
                return_value=postgresql,
            ), patch(
                "driftbench.benchmarking.pgbench._run_phase", side_effect=phase
            ):
                exit_code, stdout, stderr = self._run(*self._arguments(root))
            decision = json.loads(
                (root / "results" / "decision.json").read_text(encoding="utf-8")
            )
            baseline = json.loads(
                (root / "results" / "baseline.json").read_text(encoding="utf-8")
            )

        self.assertEqual(exit_code, 4)
        self.assertNotEqual(exit_code, 5)
        self.assertEqual(stderr, "")
        payload = json.loads(stdout)
        self.assertEqual(payload["outcome"], "execution_error")
        self.assertIn("TPS relative delta", payload["error"])
        self.assertFalse(decision["ok"])
        self.assertFalse(baseline["valid"])

    def test_pgbench_json_usage_and_unexpected_errors_are_single_documents(self) -> None:
        exit_code, stdout, stderr = self._run(
            "benchmark", "pgbench", "--unknown", "--json"
        )
        self.assertEqual(exit_code, 3)
        self.assertEqual(stderr, "")
        self.assertEqual(json.loads(stdout)["outcome"], "configuration_error")

        with tempfile.TemporaryDirectory() as temporary, patch(
            "driftbench.cli.run_paired_pgbench",
            side_effect=RuntimeError("unexpected pgbench failure"),
        ):
            exit_code, stdout, stderr = self._run(
                *self._arguments(Path(temporary))
            )
        self.assertEqual(exit_code, 4)
        self.assertEqual(stderr, "")
        payload = json.loads(stdout)
        self.assertEqual(payload["outcome"], "execution_error")
        self.assertIn("unexpected pgbench failure", payload["error"])

    def test_json_remains_valid_for_non_bmp_text_on_strict_cp1252(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "json_😀"
            root.mkdir()
            environment = os.environ.copy()
            environment.update(
                {"PYTHONUTF8": "0", "PYTHONIOENCODING": "cp1252:strict"}
            )
            process = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "driftbench.cli",
                    "list-outputs",
                    "--root",
                    str(root),
                    "--json",
                ],
                cwd=REPO_ROOT,
                env=environment,
                capture_output=True,
                check=False,
                text=True,
                encoding="cp1252",
            )

        self.assertEqual(process.returncode, 0, process.stderr)
        self.assertEqual(process.stderr, "")
        payload = json.loads(process.stdout)
        self.assertTrue(payload["root"].endswith("json_😀"))

if __name__ == "__main__":
    unittest.main()
