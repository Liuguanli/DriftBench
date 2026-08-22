from __future__ import annotations

import copy
import hashlib
import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from driftbench.benchmarking.metrics import BenchmarkResultError, validate_benchmark_result
from driftbench.benchmarking.pgbench import (
    _POSTGRESQL_SETTINGS,
    _base_environment,
    PgBenchConnection,
    PgBenchExecutionError,
    PgBenchStdoutMetrics,
    run_paired_pgbench,
)
from driftbench.benchmarking.policy import evaluate_regression, load_pgbench_policy
from driftbench.benchmarking.provenance import (
    DriftBenchSourceIdentity,
    SourceProvenanceError,
    canonical_pgbench_policy_payload,
    driftbench_source_sha,
    inspect_driftbench_source,
    pgbench_policy_sha256,
)
from ..helpers import REPO_ROOT


POLICY_PATH = (
    REPO_ROOT
    / "driftbench"
    / "benchmarking"
    / "policies"
    / "pgbench_ci_v1.json"
)
SOURCE_SHA = "a" * 40


def _git(root: Path, *arguments: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", *arguments],
        cwd=root,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=True,
    )


def _init_source_repo(root: Path) -> str:
    for package in ("driftbench", "driftbench_service", "driftbench_mcp"):
        directory = root / package
        directory.mkdir(parents=True, exist_ok=True)
        (directory / "runtime.py").write_text("VALUE = 1\n", encoding="utf-8")
    (root / "pyproject.toml").write_text(
        '[project]\nname = "driftbench-fixture"\nversion = "0.0.0"\n',
        encoding="utf-8",
    )
    _git(root, "init")
    _git(root, "config", "user.name", "DriftBench Tests")
    _git(root, "config", "user.email", "tests@driftbench.invalid")
    _git(root, "add", ".")
    _git(root, "commit", "-m", "fixture")
    return _git(root, "rev-parse", "HEAD").stdout.strip()


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _phase_artifacts() -> dict:
    descriptor = {"path": "raw/evidence.log", "sha256": "0" * 64, "bytes": 1}
    return {
        "stdout": dict(descriptor),
        "stderr": dict(descriptor),
        "transaction_logs": [dict(descriptor)],
    }


def _successful_phase(**_kwargs) -> dict:
    return {
        "actual_seconds": 2.0,
        "parsed": PgBenchStdoutMetrics(
            transactions_successful=2,
            transactions_failed=0,
            transactions_total=2,
            reported_latency_mean_ms=1.1,
            reported_tps=1.0,
            scale_factor=1,
            clients=2,
        ),
        "latencies_us": [1000, 1200],
        "failure_types": {},
        "artifacts": _phase_artifacts(),
        "returncode": 0,
    }


def _postgresql_evidence() -> tuple[dict, dict]:
    return (
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


class SourceProvenanceTests(unittest.TestCase):
    def test_clean_checkout_records_full_head_and_source_fields(self) -> None:
        with tempfile.TemporaryDirectory() as temporary, patch.dict(
            os.environ, {"DRIFTBENCH_GIT_SHA": ""}, clear=False
        ):
            root = Path(temporary)
            head = _init_source_repo(root)
            identity = inspect_driftbench_source(package_root=root)

            self.assertEqual(identity.source_sha, head)
            self.assertEqual(identity.source_state, "clean")
            self.assertEqual(identity.source_sha_source, "git_head")
            self.assertFalse(identity.override_asserted)
            self.assertEqual(driftbench_source_sha(package_root=root), head)

    def test_tracked_staged_and_scoped_untracked_changes_fail_closed(self) -> None:
        cases = {
            "tracked": lambda root: (root / "driftbench" / "runtime.py").write_text(
                "VALUE = 2\n", encoding="utf-8"
            ),
            "staged": lambda root: (
                (root / "driftbench_service" / "runtime.py").write_text(
                    "VALUE = 2\n", encoding="utf-8"
                ),
                _git(root, "add", "driftbench_service/runtime.py"),
            ),
            "untracked": lambda root: (root / "driftbench_mcp" / "new.py").write_text(
                "VALUE = 2\n", encoding="utf-8"
            ),
            "pyproject": lambda root: (root / "pyproject.toml").write_text(
                "[project]\nname='changed'\n", encoding="utf-8"
            ),
        }
        for name, mutate in cases.items():
            with self.subTest(name=name), tempfile.TemporaryDirectory() as temporary, patch.dict(
                os.environ, {"DRIFTBENCH_GIT_SHA": ""}, clear=False
            ):
                root = Path(temporary)
                _init_source_repo(root)
                mutate(root)
                with self.assertRaisesRegex(SourceProvenanceError, "runtime source is dirty"):
                    inspect_driftbench_source(package_root=root)

    def test_docs_tests_and_output_do_not_make_runtime_source_dirty(self) -> None:
        with tempfile.TemporaryDirectory() as temporary, patch.dict(
            os.environ, {"DRIFTBENCH_GIT_SHA": ""}, clear=False
        ):
            root = Path(temporary)
            head = _init_source_repo(root)
            for relative in ("docs/note.md", "test/new_test.py", "output/result.json"):
                path = root / relative
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text("outside runtime scope\n", encoding="utf-8")
            self.assertEqual(
                inspect_driftbench_source(package_root=root).source_sha,
                head,
            )

    def test_override_is_full_sha_assertion_and_never_bypasses_dirty(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            head = _init_source_repo(root)
            for override, message in (
                ("abc123", "full 40-character"),
                ("f" * 40, "does not match"),
            ):
                with self.subTest(override=override), patch.dict(
                    os.environ, {"DRIFTBENCH_GIT_SHA": override}, clear=False
                ), self.assertRaisesRegex(SourceProvenanceError, message):
                    inspect_driftbench_source(package_root=root)

            (root / "driftbench" / "runtime.py").write_text(
                "VALUE = 2\n", encoding="utf-8"
            )
            with patch.dict(
                os.environ, {"DRIFTBENCH_GIT_SHA": head}, clear=False
            ), self.assertRaisesRegex(SourceProvenanceError, "dirty"):
                inspect_driftbench_source(package_root=root)

    def test_matching_override_is_recorded_as_assertion(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            head = _init_source_repo(root)
            with patch.dict(
                os.environ, {"DRIFTBENCH_GIT_SHA": head.upper()}, clear=False
            ):
                identity = inspect_driftbench_source(package_root=root)
            self.assertTrue(identity.override_asserted)
            self.assertEqual(
                identity.environment_fields()["source_sha_assertion"],
                "DRIFTBENCH_GIT_SHA",
            )

    def test_wheel_does_not_borrow_caller_repository_sha(self) -> None:
        with tempfile.TemporaryDirectory() as temporary, patch.dict(
            os.environ,
            {"DRIFTBENCH_GIT_SHA": "f" * 40, "GITHUB_SHA": "e" * 40},
            clear=False,
        ), patch("driftbench.benchmarking.provenance.subprocess.run") as run:
            with self.assertRaisesRegex(SourceProvenanceError, "Git metadata is unavailable"):
                inspect_driftbench_source(package_root=temporary)
            run.assert_not_called()

    def test_source_git_is_resolved_from_package_root_not_caller_cwd(self) -> None:
        with tempfile.TemporaryDirectory() as temporary, patch.dict(
            os.environ, {"DRIFTBENCH_GIT_SHA": ""}, clear=False
        ):
            root = Path(temporary)
            source = root / "source"
            caller = root / "caller"
            source.mkdir()
            caller.mkdir()
            head = _init_source_repo(source)
            previous = Path.cwd()
            try:
                os.chdir(caller)
                self.assertEqual(
                    inspect_driftbench_source(package_root=source).source_sha,
                    head,
                )
            finally:
                os.chdir(previous)


class PgBenchBundleProvenanceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.policy = load_pgbench_policy(POLICY_PATH)

    def test_base_environment_normalizes_nonpositive_cpu_count_to_null(self) -> None:
        identity = DriftBenchSourceIdentity(
            source_sha=SOURCE_SHA,
            source_state="clean",
            source_sha_source="git_head",
            repository_root=REPO_ROOT,
        )
        with patch(
            "driftbench.benchmarking.pgbench.os.cpu_count", return_value=0
        ):
            environment = _base_environment(
                PgBenchConnection(database="driftbench_ci"), source=identity
            )
        self.assertIsNone(environment["platform"]["logical_cpu_count"])

    def _run(self, root: Path, *, postgresql_evidence=None):
        candidate = root / "source-candidate.sql"
        candidate.write_bytes(b"\\set aid random(1, 100000 * :scale)\nSELECT abalance FROM pgbench_accounts WHERE aid = :aid;\n")
        output = root / "results"
        with patch(
            "driftbench.benchmarking.pgbench._git_sha", return_value=SOURCE_SHA
        ), patch(
            "driftbench.benchmarking.pgbench._pgbench_version",
            return_value={"full": "pgbench (PostgreSQL) 16.9", "major": 16},
        ), patch(
            "driftbench.benchmarking.pgbench._postgresql_environment",
            return_value=(
                _postgresql_evidence()
                if postgresql_evidence is None
                else postgresql_evidence
            ),
        ), patch(
            "driftbench.benchmarking.pgbench._run_phase",
            side_effect=_successful_phase,
        ) as phase:
            outcome = run_paired_pgbench(
                policy=self.policy,
                candidate_script=candidate,
                output_dir=output,
                connection=PgBenchConnection(
                    database="driftbench_ci",
                    host="localhost",
                    port=5432,
                    username="driftbench",
                ),
            )
        return candidate, output, outcome, phase

    def test_runner_snapshots_and_executes_exact_copied_inputs(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source, output, outcome, phase = self._run(root)
            copied = output / "inputs" / "candidate.sql"
            policy_snapshot = output / "inputs" / "policy.json"
            environment_path = output / "environment.json"
            baseline = json.loads((output / "baseline.json").read_text(encoding="utf-8"))
            candidate = json.loads((output / "candidate.json").read_text(encoding="utf-8"))

            self.assertTrue(outcome.ok)
            self.assertEqual(copied.read_bytes(), source.read_bytes())
            self.assertEqual(
                json.loads(policy_snapshot.read_text(encoding="utf-8")),
                canonical_pgbench_policy_payload(self.policy),
            )
            self.assertEqual(phase.call_count, 12)
            self.assertTrue(
                all(call.kwargs["candidate_script"] == copied for call in phase.call_args_list)
            )

            self.assertEqual(baseline["inputs"], candidate["inputs"])
            self.assertEqual(baseline["environment"], candidate["environment"])
            self.assertEqual(
                candidate["workload"]["script_sha256"],
                candidate["inputs"]["candidate_script"]["sha256"],
            )
            self.assertEqual(
                candidate["inputs"]["policy"]["sha256"],
                pgbench_policy_sha256(self.policy),
            )
            for descriptor in [
                *candidate["inputs"].values(),
                candidate["environment"],
            ]:
                path = output / descriptor["path"]
                self.assertEqual(path.stat().st_size, descriptor["bytes"])
                self.assertEqual(_sha256(path), descriptor["sha256"])

            environment = json.loads(environment_path.read_text(encoding="utf-8"))
            self.assertEqual(environment["status"], "complete")
            self.assertEqual(environment["driftbench"]["source_sha"], SOURCE_SHA)
            self.assertEqual(environment["driftbench"]["source_state"], "clean")
            self.assertEqual(
                environment["driftbench"]["source_sha_source"], "git_head"
            )
            self.assertTrue(environment["driftbench"]["version"])
            self.assertEqual(environment["connection"]["database"], "driftbench_ci")
            self.assertNotIn("password", json.dumps(environment).lower())
            self.assertEqual(
                environment["postgresql"]["initialization"]["scale_factor_inferred"],
                1,
            )
            validate_benchmark_result(baseline)
            validate_benchmark_result(candidate)

            digest_mismatch = copy.deepcopy(candidate)
            digest_mismatch["workload"]["script_sha256"] = "f" * 64
            with self.assertRaisesRegex(BenchmarkResultError, "must match"):
                validate_benchmark_result(digest_mismatch)

    def test_runner_rejects_non_fresh_initialization_before_execution(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            evidence = copy.deepcopy(_postgresql_evidence())
            evidence[1]["initialization"]["pgbench_accounts"] = 99_999
            with self.assertRaisesRegex(
                PgBenchExecutionError,
                "pgbench initialization does not match the regression policy",
            ):
                self._run(root, postgresql_evidence=evidence)

            output = root / "results"
            environment = json.loads(
                (output / "environment.json").read_text(encoding="utf-8")
            )
            self.assertEqual(environment["status"], "failed")
            self.assertIn("initialization", environment["error"])
            decision = json.loads(
                (output / "decision.json").read_text(encoding="utf-8")
            )
            self.assertFalse(decision["ok"])

    def test_policy_and_environment_provenance_mismatches_fail_compatibility(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            _source, output, _outcome, _phase = self._run(Path(temporary))
            baseline = json.loads((output / "baseline.json").read_text(encoding="utf-8"))
            candidate = json.loads((output / "candidate.json").read_text(encoding="utf-8"))

            wrong_policy_baseline = copy.deepcopy(baseline)
            wrong_policy_candidate = copy.deepcopy(candidate)
            wrong_policy_baseline["inputs"]["policy"]["sha256"] = "f" * 64
            wrong_policy_candidate["inputs"]["policy"]["sha256"] = "f" * 64
            decision = evaluate_regression(
                wrong_policy_baseline, wrong_policy_candidate, self.policy
            )
            self.assertFalse(decision["ok"])
            self.assertTrue(
                any("policy snapshot" in reason for reason in decision["reasons"])
            )

            wrong_environment = copy.deepcopy(candidate)
            wrong_environment["environment"]["sha256"] = "f" * 64
            decision = evaluate_regression(baseline, wrong_environment, self.policy)
            self.assertFalse(decision["ok"])
            self.assertIn(
                "baseline/candidate environment descriptor mismatch",
                decision["compatibility"]["reasons"],
            )

    def test_environment_capture_failure_is_fail_closed_and_redacts_password(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            candidate = root / "candidate.sql"
            candidate.write_text("SELECT 1;\n", encoding="utf-8")
            output = root / "results"
            secret = "do-not-record-this-password"
            with patch.dict(os.environ, {"PGPASSWORD": secret}, clear=False), patch(
                "driftbench.benchmarking.pgbench._git_sha", return_value=SOURCE_SHA
            ), patch(
                "driftbench.benchmarking.pgbench._pgbench_version",
                return_value={"full": "pgbench (PostgreSQL) 16.9", "major": 16},
            ), patch(
                "driftbench.benchmarking.pgbench._postgresql_environment",
                side_effect=PgBenchExecutionError(f"database rejected {secret}"),
            ), patch(
                "driftbench.benchmarking.pgbench._run_phase"
            ) as phase:
                with self.assertRaises(PgBenchExecutionError):
                    run_paired_pgbench(
                        policy=self.policy,
                        candidate_script=candidate,
                        output_dir=output,
                        connection=PgBenchConnection(database="driftbench_ci"),
                    )

            phase.assert_not_called()
            self.assertTrue((output / "inputs" / "candidate.sql").is_file())
            self.assertTrue((output / "inputs" / "policy.json").is_file())
            environment = json.loads(
                (output / "environment.json").read_text(encoding="utf-8")
            )
            self.assertEqual(environment["status"], "failed")
            self.assertIn("<redacted>", environment["error"])
            for path in output.rglob("*"):
                if path.is_file():
                    self.assertNotIn(secret, path.read_text(encoding="utf-8"))
            self.assertTrue((output / "baseline.json").is_file())
            self.assertTrue((output / "candidate.json").is_file())
            self.assertTrue((output / "decision.json").is_file())

    def test_initial_dirty_source_writes_failure_evidence_without_any_phase(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            candidate = root / "candidate.sql"
            candidate.write_text("SELECT 1;\n", encoding="utf-8")
            output = root / "results"
            error = SourceProvenanceError(
                "DriftBench runtime source is dirty: M driftbench/runtime.py",
                source_sha=SOURCE_SHA,
                source_state="dirty",
                source_sha_source="git_head",
            )
            with patch(
                "driftbench.benchmarking.pgbench._git_sha", side_effect=error
            ), patch(
                "driftbench.benchmarking.pgbench._run_phase"
            ) as phase, patch(
                "driftbench.benchmarking.pgbench._postgresql_environment"
            ) as database:
                with self.assertRaisesRegex(PgBenchExecutionError, "source is dirty"):
                    run_paired_pgbench(
                        policy=self.policy,
                        candidate_script=candidate,
                        output_dir=output,
                        connection=PgBenchConnection(database="driftbench_ci"),
                    )

            phase.assert_not_called()
            database.assert_not_called()
            environment = json.loads(
                (output / "environment.json").read_text(encoding="utf-8")
            )
            self.assertEqual(environment["status"], "failed")
            self.assertEqual(environment["driftbench"]["source_state"], "dirty")
            order = json.loads(
                (output / "execution_order.json").read_text(encoding="utf-8")
            )
            self.assertEqual(order["completed"], [])
            self.assertFalse(
                json.loads((output / "baseline.json").read_text(encoding="utf-8"))[
                    "valid"
                ]
            )
            self.assertFalse(
                json.loads((output / "decision.json").read_text(encoding="utf-8"))[
                    "ok"
                ]
            )

    def test_final_source_recheck_rejects_head_or_dirty_change(self) -> None:
        cases = {
            "head": "b" * 40,
            "dirty": SourceProvenanceError(
                "DriftBench runtime source became dirty",
                source_sha=SOURCE_SHA,
                source_state="dirty",
                source_sha_source="git_head",
            ),
        }
        for name, final_state in cases.items():
            with self.subTest(name=name), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                candidate = root / "candidate.sql"
                candidate.write_text("SELECT 1;\n", encoding="utf-8")
                output = root / "results"
                with patch(
                    "driftbench.benchmarking.pgbench._git_sha",
                    side_effect=[SOURCE_SHA, final_state],
                ), patch(
                    "driftbench.benchmarking.pgbench._pgbench_version",
                    return_value={"full": "pgbench (PostgreSQL) 16.9", "major": 16},
                ), patch(
                    "driftbench.benchmarking.pgbench._postgresql_environment",
                    return_value=_postgresql_evidence(),
                ), patch(
                    "driftbench.benchmarking.pgbench._run_phase",
                    side_effect=_successful_phase,
                ) as phase:
                    with self.assertRaises(PgBenchExecutionError):
                        run_paired_pgbench(
                            policy=self.policy,
                            candidate_script=candidate,
                            output_dir=output,
                            connection=PgBenchConnection(database="driftbench_ci"),
                        )

                self.assertEqual(phase.call_count, 12)
                environment = json.loads(
                    (output / "environment.json").read_text(encoding="utf-8")
                )
                self.assertEqual(environment["status"], "failed")
                expected_state = "head_changed" if name == "head" else "dirty"
                self.assertEqual(
                    environment["driftbench"]["source_state"], expected_state
                )
                baseline = json.loads(
                    (output / "baseline.json").read_text(encoding="utf-8")
                )
                self.assertFalse(baseline["valid"])
                decision = json.loads(
                    (output / "decision.json").read_text(encoding="utf-8")
                )
                self.assertFalse(decision["ok"])


if __name__ == "__main__":
    unittest.main()
