from __future__ import annotations

import hashlib
import io
import json
import os
import shutil
import stat
import tempfile
import unicodedata
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from types import SimpleNamespace
from typing import Callable
from unittest.mock import patch

from driftbench.benchmarking.metrics import write_json_strict
from driftbench.benchmarking.pgbench import (
    _POSTGRESQL_SETTINGS,
    PgBenchConnection,
    PgBenchStdoutMetrics,
    run_paired_pgbench,
)
from driftbench.benchmarking.policy import load_pgbench_policy
from driftbench.benchmarking.verify import BenchmarkBundleError, verify_pgbench_bundle
from driftbench.cli import main
from ..helpers import REPO_ROOT


POLICY_PATH = (
    REPO_ROOT
    / "driftbench"
    / "benchmarking"
    / "policies"
    / "pgbench_ci_v1.json"
)


def _descriptor(path: Path, root: Path) -> dict:
    data = path.read_bytes()
    return {
        "path": path.relative_to(root).as_posix(),
        "sha256": hashlib.sha256(data).hexdigest(),
        "bytes": len(data),
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


def _phase_factory(candidate_tps: float):
    def phase(**kwargs) -> dict:
        output_root = Path(kwargs["output_root"])
        phase_dir = Path(kwargs["phase_dir"])
        phase_dir.mkdir(parents=True, exist_ok=True)
        role = kwargs["role"]
        tps = candidate_tps if role == "candidate" else 100.0
        successful = 20
        elapsed = successful / tps
        stdout = (
            "transaction type: <builtin: select only>\n"
            "scaling factor: 1\n"
            "query mode: simple\n"
            "number of clients: 2\n"
            "number of threads: 2\n"
            "duration: 5 s\n"
            f"number of transactions actually processed: {successful}\n"
            "number of failed transactions: 0 (0.000%)\n"
            "latency average = 1.000 ms\n"
            "initial connection time = 1.000 ms\n"
            f"tps = {tps:.12f} (without initial connection time)\n"
        )
        stdout_path = phase_dir / "pgbench.stdout.log"
        stderr_path = phase_dir / "pgbench.stderr.log"
        transaction_path = phase_dir / "transactions.123"
        stdout_path.write_bytes(stdout.encode("utf-8"))
        stderr_path.write_bytes(b"")
        transaction_path.write_bytes(
            "".join(
                f"0 {index} 1000 0 1700000000 {index}\n"
                for index in range(1, successful + 1)
            ).encode("utf-8")
        )
        return {
            "actual_seconds": elapsed,
            "parsed": PgBenchStdoutMetrics(
                transactions_successful=successful,
                transactions_failed=0,
                transactions_total=successful,
                reported_latency_mean_ms=1.0,
                reported_tps=tps,
                scale_factor=1,
                clients=2,
            ),
            "latencies_us": [1000] * successful,
            "failure_types": {},
            "artifacts": {
                "stdout": _descriptor(stdout_path, output_root),
                "stderr": _descriptor(stderr_path, output_root),
                "transaction_logs": [_descriptor(transaction_path, output_root)],
            },
            "returncode": 0,
        }

    return phase


def _make_bundle(root: Path, *, candidate_tps: float = 100.0) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    candidate = root / "candidate.sql"
    candidate.write_bytes(b"SELECT 1;\n")
    bundle = root / "results"
    policy = load_pgbench_policy(POLICY_PATH)
    with patch(
        "driftbench.benchmarking.pgbench._git_sha", return_value="a" * 40
    ), patch(
        "driftbench.benchmarking.pgbench._pgbench_version",
        return_value={"full": "pgbench (PostgreSQL) 16.9", "major": 16},
    ), patch(
        "driftbench.benchmarking.pgbench._postgresql_environment",
        return_value=_postgresql_evidence(),
    ), patch(
        "driftbench.benchmarking.pgbench._run_phase",
        side_effect=_phase_factory(candidate_tps),
    ):
        run_paired_pgbench(
            policy=policy,
            candidate_script=candidate,
            output_dir=bundle,
            connection=PgBenchConnection(
                database="driftbench_ci",
                host="localhost",
                port=5432,
                username="driftbench",
            ),
        )
    return bundle


def _rewrite_environment(bundle: Path, environment: dict) -> None:
    environment_path = bundle / "environment.json"
    write_json_strict(environment_path, environment)
    descriptor = _descriptor(environment_path, bundle)
    for core_name in (
        "baseline.json",
        "candidate.json",
        "decision.json",
        "execution_order.json",
    ):
        core_path = bundle / core_name
        core = json.loads(core_path.read_text(encoding="utf-8"))
        if "environment" in core:
            core["environment"] = dict(descriptor)
        write_json_strict(core_path, core)


def _delete_environment_path(environment: dict, path: tuple[str, ...]) -> None:
    parent = environment
    for component in path[:-1]:
        parent = parent[component]
    del parent[path[-1]]


def _set_environment_path(
    environment: dict, path: tuple[str, ...], value: object
) -> None:
    parent = environment
    for component in path[:-1]:
        parent = parent[component]
    parent[path[-1]] = value


class OfflineBundleVerifierTests(unittest.TestCase):
    def _cli(self, *arguments: str) -> tuple[int, dict, str]:
        stdout = io.StringIO()
        stderr = io.StringIO()
        with redirect_stdout(stdout), redirect_stderr(stderr):
            exit_code = main(list(arguments))
        return exit_code, json.loads(stdout.getvalue()), stderr.getvalue()

    def _assert_environment_mutations_rejected(
        self,
        cases: list[tuple[str, Callable[[dict], None]]],
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            template = _make_bundle(root / "template")
            cases_root = root / "cases"
            cases_root.mkdir()
            for index, (name, mutation) in enumerate(cases):
                with self.subTest(name=name):
                    bundle = cases_root / f"case-{index:03d}"
                    shutil.copytree(template, bundle)
                    environment_path = bundle / "environment.json"
                    environment = json.loads(
                        environment_path.read_text(encoding="utf-8")
                    )
                    mutation(environment)
                    _rewrite_environment(bundle, environment)
                    exit_code, payload, stderr = self._cli(
                        "benchmark", "verify", "--bundle", str(bundle), "--json"
                    )
                    self.assertEqual(exit_code, 4)
                    self.assertFalse(payload["verified"])
                    self.assertFalse(payload["ok"])
                    self.assertEqual(payload["outcome"], "verification_error")
                    self.assertEqual(stderr, "")

    def test_moved_complete_bundle_verifies_offline(self) -> None:
        self.assertEqual(len(_POSTGRESQL_SETTINGS), 11)
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            original = _make_bundle(root / "source")
            moved = root / "moved" / "bundle"
            moved.parent.mkdir()
            shutil.move(str(original), moved)
            verification = verify_pgbench_bundle(moved)
            exit_code, payload, stderr = self._cli(
                "benchmark", "verify", "--bundle", str(moved), "--json"
            )

        self.assertTrue(verification.ok)
        self.assertEqual(exit_code, 0)
        self.assertTrue(payload["verified"])
        self.assertEqual(payload["outcome"], "passed")
        self.assertEqual(stderr, "")

    def test_environment_missing_required_fields_fail_closed_after_rehash(self) -> None:
        first_setting = _POSTGRESQL_SETTINGS[0]
        paths = [
            ("schema_version",),
            ("status",),
            ("driftbench",),
            ("python",),
            ("platform",),
            ("connection",),
            ("postgresql",),
            ("pgbench",),
            ("driftbench", "version"),
            ("driftbench", "source_sha"),
            ("driftbench", "source_state"),
            ("driftbench", "source_sha_source"),
            ("python", "version"),
            ("python", "implementation"),
            ("platform", "os"),
            ("platform", "system"),
            ("platform", "release"),
            ("platform", "machine"),
            ("platform", "cpu"),
            ("platform", "logical_cpu_count"),
            ("connection", "database"),
            ("connection", "host"),
            ("connection", "port"),
            ("connection", "username"),
            ("postgresql", "full"),
            ("postgresql", "major"),
            ("postgresql", "current_database"),
            ("postgresql", "settings"),
            ("postgresql", "initialization"),
            *[
                ("postgresql", "settings", setting)
                for setting in _POSTGRESQL_SETTINGS
            ],
            ("postgresql", "settings", first_setting, "setting"),
            ("postgresql", "settings", first_setting, "unit"),
            ("postgresql", "settings", first_setting, "source"),
            ("postgresql", "initialization", "pgbench_branches"),
            ("postgresql", "initialization", "pgbench_tellers"),
            ("postgresql", "initialization", "pgbench_accounts"),
            ("postgresql", "initialization", "pgbench_history"),
            ("postgresql", "initialization", "scale_factor_inferred"),
            ("pgbench", "full"),
            ("pgbench", "major"),
        ]
        cases = [
            (
                ".".join(path),
                lambda environment, path=path: _delete_environment_path(
                    environment, path
                ),
            )
            for path in paths
        ]
        self._assert_environment_mutations_rejected(cases)

    def test_environment_wrong_types_and_values_fail_closed_after_rehash(self) -> None:
        first_setting = _POSTGRESQL_SETTINGS[0]
        mutations = [
            ("schema_version_type", ("schema_version",), 1.0),
            ("schema_version_value", ("schema_version",), "2.0"),
            ("status_type", ("status",), None),
            ("status_value", ("status",), "failed"),
            ("driftbench_version", ("driftbench", "version"), ""),
            ("source_sha", ("driftbench", "source_sha"), "A" * 40),
            ("source_state", ("driftbench", "source_state"), False),
            (
                "source_sha_source",
                ("driftbench", "source_sha_source"),
                None,
            ),
            (
                "source_sha_assertion",
                ("driftbench", "source_sha_assertion"),
                "GITHUB_SHA",
            ),
            ("python_version", ("python", "version"), ""),
            ("python_implementation", ("python", "implementation"), 1),
            *[
                (f"platform_{field}", ("platform", field), None)
                for field in ("os", "system", "release", "machine", "cpu")
            ],
            (
                "logical_cpu_count_bool",
                ("platform", "logical_cpu_count"),
                True,
            ),
            ("logical_cpu_count_zero", ("platform", "logical_cpu_count"), 0),
            ("connection_database", ("connection", "database"), ""),
            ("connection_host", ("connection", "host"), None),
            ("connection_port_bool", ("connection", "port"), True),
            ("connection_port_zero", ("connection", "port"), 0),
            ("connection_port_high", ("connection", "port"), 65536),
            ("connection_username", ("connection", "username"), None),
            ("postgresql_full", ("postgresql", "full"), ""),
            ("postgresql_major_type", ("postgresql", "major"), "16"),
            ("postgresql_major_policy", ("postgresql", "major"), 15),
            (
                "postgresql_current_database",
                ("postgresql", "current_database"),
                "",
            ),
            ("postgresql_settings", ("postgresql", "settings"), []),
            (
                "postgresql_setting_object",
                ("postgresql", "settings", first_setting),
                [],
            ),
            (
                "postgresql_setting_value",
                ("postgresql", "settings", first_setting, "setting"),
                None,
            ),
            (
                "postgresql_setting_unit",
                ("postgresql", "settings", first_setting, "unit"),
                1,
            ),
            (
                "postgresql_setting_source",
                ("postgresql", "settings", first_setting, "source"),
                None,
            ),
            (
                "initialization_bool",
                ("postgresql", "initialization", "pgbench_branches"),
                True,
            ),
            (
                "initialization_policy",
                ("postgresql", "initialization", "pgbench_accounts"),
                99999,
            ),
            ("pgbench_full", ("pgbench", "full"), ""),
            ("pgbench_major_type", ("pgbench", "major"), "16"),
            ("pgbench_major_policy", ("pgbench", "major"), 15),
        ]
        cases = [
            (
                name,
                lambda environment, path=path, value=value: _set_environment_path(
                    environment, path, value
                ),
            )
            for name, path, value in mutations
        ]
        self._assert_environment_mutations_rejected(cases)

    def test_environment_unknown_fields_fail_closed_after_rehash(self) -> None:
        first_setting = _POSTGRESQL_SETTINGS[0]
        object_paths = [
            (),
            ("driftbench",),
            ("python",),
            ("platform",),
            ("connection",),
            ("postgresql",),
            ("postgresql", "settings"),
            ("postgresql", "settings", first_setting),
            ("postgresql", "initialization"),
            ("pgbench",),
        ]
        cases = [
            (
                "root" if not path else ".".join(path),
                lambda environment, path=path: _set_environment_path(
                    environment, (*path, "unexpected"), "unverified"
                ),
            )
            for path in object_paths
        ]
        self._assert_environment_mutations_rejected(cases)

    def test_source_sha_assertion_is_the_only_optional_driftbench_field(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            bundle = _make_bundle(Path(temporary))
            environment_path = bundle / "environment.json"
            environment = json.loads(environment_path.read_text(encoding="utf-8"))
            environment["driftbench"][
                "source_sha_assertion"
            ] = "DRIFTBENCH_GIT_SHA"
            _rewrite_environment(bundle, environment)
            exit_code, payload, stderr = self._cli(
                "benchmark", "verify", "--bundle", str(bundle), "--json"
            )

        self.assertEqual(exit_code, 0)
        self.assertTrue(payload["verified"])
        self.assertEqual(payload["outcome"], "passed")
        self.assertEqual(stderr, "")

    def test_environment_accepts_nullable_cpu_count_and_string_platform_values(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            bundle = _make_bundle(Path(temporary))
            environment_path = bundle / "environment.json"
            environment = json.loads(environment_path.read_text(encoding="utf-8"))
            environment["platform"].update(
                {
                    "os": "",
                    "system": "",
                    "release": "",
                    "machine": "unavailable",
                    "cpu": "unavailable",
                    "logical_cpu_count": None,
                }
            )
            _rewrite_environment(bundle, environment)
            exit_code, payload, stderr = self._cli(
                "benchmark", "verify", "--bundle", str(bundle), "--json"
            )

        self.assertEqual(exit_code, 0)
        self.assertTrue(payload["verified"])
        self.assertEqual(payload["outcome"], "passed")
        self.assertEqual(stderr, "")

    def test_descriptor_target_tampering_and_missing_file_fail(self) -> None:
        targets = {
            "policy": lambda bundle: bundle / "inputs" / "policy.json",
            "candidate": lambda bundle: bundle / "inputs" / "candidate.sql",
            "environment": lambda bundle: bundle / "environment.json",
            "stdout": lambda bundle: bundle
            / json.loads((bundle / "baseline.json").read_text(encoding="utf-8"))[
                "repetitions"
            ][0]["artifacts"]["stdout"]["path"],
            "stderr": lambda bundle: bundle
            / json.loads((bundle / "baseline.json").read_text(encoding="utf-8"))[
                "repetitions"
            ][0]["artifacts"]["stderr"]["path"],
            "transaction_log": lambda bundle: bundle
            / json.loads((bundle / "baseline.json").read_text(encoding="utf-8"))[
                "repetitions"
            ][0]["artifacts"]["transaction_logs"][0]["path"],
        }
        for name, select in targets.items():
            with self.subTest(name=name), tempfile.TemporaryDirectory() as temporary:
                bundle = _make_bundle(Path(temporary))
                target = select(bundle)
                target.write_bytes(target.read_bytes() + b"tampered")
                with self.assertRaisesRegex(BenchmarkBundleError, "mismatch"):
                    verify_pgbench_bundle(bundle)

        with tempfile.TemporaryDirectory() as temporary:
            bundle = _make_bundle(Path(temporary))
            target = bundle / "inputs" / "candidate.sql"
            target.unlink()
            exit_code, payload, _stderr = self._cli(
                "benchmark", "verify", "--bundle", str(bundle), "--json"
            )
            self.assertEqual(exit_code, 4)
            self.assertFalse(payload["verified"])

    def test_rehashed_stdout_tampering_rebuilds_measurement_but_not_warmup_tps(self) -> None:
        def rewrite_stdout(bundle: Path, phase_key: str, replacement: str) -> None:
            baseline_path = bundle / "baseline.json"
            baseline = json.loads(baseline_path.read_text(encoding="utf-8"))
            phase = baseline[phase_key][0]
            descriptor = phase["artifacts"]["stdout"]
            stdout_path = bundle / descriptor["path"]
            text = stdout_path.read_text(encoding="utf-8")
            text = text.replace(
                "tps = 100.000000000000 (without initial connection time)\n",
                replacement,
            )
            stdout_path.write_bytes(text.encode("utf-8"))
            phase["artifacts"]["stdout"] = _descriptor(stdout_path, bundle)
            write_json_strict(baseline_path, baseline)

        with tempfile.TemporaryDirectory() as temporary:
            bundle = _make_bundle(Path(temporary))
            rewrite_stdout(
                bundle,
                "warmups",
                "tps = 1.000000000000 (without initial connection time)\n",
            )
            self.assertTrue(verify_pgbench_bundle(bundle).ok)

        for name, replacement in (
            (
                "mismatch",
                "tps = 90.000000000000 (without initial connection time)\n",
            ),
            ("missing", ""),
        ):
            with self.subTest(name=name), tempfile.TemporaryDirectory() as temporary:
                bundle = _make_bundle(Path(temporary))
                rewrite_stdout(bundle, "repetitions", replacement)
                with self.assertRaises(BenchmarkBundleError):
                    verify_pgbench_bundle(bundle)

    def test_metrics_decision_and_path_traversal_tampering_fail(self) -> None:
        mutations = {}

        def metrics(bundle: Path) -> None:
            path = bundle / "baseline.json"
            payload = json.loads(path.read_text(encoding="utf-8"))
            payload["metrics"]["tps"] += 1
            write_json_strict(path, payload)

        mutations["metrics"] = metrics

        def decision(bundle: Path) -> None:
            path = bundle / "decision.json"
            payload = json.loads(path.read_text(encoding="utf-8"))
            payload["ok"] = not payload["ok"]
            write_json_strict(path, payload)

        mutations["decision"] = decision

        def traversal(bundle: Path) -> None:
            path = bundle / "baseline.json"
            payload = json.loads(path.read_text(encoding="utf-8"))
            payload["inputs"]["candidate_script"]["path"] = "../candidate.sql"
            write_json_strict(path, payload)

        mutations["path_traversal"] = traversal

        for name, mutate in mutations.items():
            with self.subTest(name=name), tempfile.TemporaryDirectory() as temporary:
                bundle = _make_bundle(Path(temporary))
                mutate(bundle)
                exit_code, payload, _stderr = self._cli(
                    "benchmark", "verify", "--bundle", str(bundle), "--json"
                )
                self.assertEqual(exit_code, 4)
                self.assertFalse(payload["verified"])

    def test_role_phase_and_duplicate_log_layout_tampering_fail(self) -> None:
        def mutate_artifacts(bundle: Path, mutation) -> None:
            path = bundle / "baseline.json"
            payload = json.loads(path.read_text(encoding="utf-8"))
            mutation(payload)
            write_json_strict(path, payload)

        mutations = {
            "role": lambda payload: payload["repetitions"][0]["artifacts"][
                "stdout"
            ].update(
                {
                    "path": "raw/rep-01/candidate/measurement/pgbench.stdout.log"
                }
            ),
            "phase": lambda payload: payload["warmups"][0]["artifacts"][
                "stdout"
            ].update(
                {"path": "raw/rep-01/baseline/measurement/pgbench.stdout.log"}
            ),
            "duplicate_log": lambda payload: payload["repetitions"][0][
                "artifacts"
            ]["transaction_logs"].append(
                dict(
                    payload["repetitions"][0]["artifacts"]["transaction_logs"][
                        0
                    ]
                )
            ),
        }
        for name, mutation in mutations.items():
            with self.subTest(name=name), tempfile.TemporaryDirectory() as temporary:
                bundle = _make_bundle(Path(temporary))
                mutate_artifacts(bundle, mutation)
                with self.assertRaises(BenchmarkBundleError):
                    verify_pgbench_bundle(bundle)

    def test_noncanonical_absolute_drive_unc_backslash_and_nul_paths_fail(self) -> None:
        invalid_paths = (
            "/absolute/candidate.sql",
            "C:/candidate.sql",
            "//server/share/candidate.sql",
            "inputs\\candidate.sql",
            "inputs/candidate\x00.sql",
        )
        for invalid in invalid_paths:
            with self.subTest(path=repr(invalid)), tempfile.TemporaryDirectory() as temporary:
                bundle = _make_bundle(Path(temporary))
                baseline_path = bundle / "baseline.json"
                payload = json.loads(baseline_path.read_text(encoding="utf-8"))
                payload["inputs"]["candidate_script"]["path"] = invalid
                write_json_strict(baseline_path, payload)
                with self.assertRaises(BenchmarkBundleError):
                    verify_pgbench_bundle(bundle)

    def test_case_and_unicode_normalized_alias_collisions_fail(self) -> None:
        for name, alias_builder in (
            ("case", lambda value: value.upper()),
            ("nfc", lambda value: unicodedata.normalize("NFD", value)),
        ):
            with self.subTest(name=name), tempfile.TemporaryDirectory() as temporary:
                bundle = _make_bundle(Path(temporary))
                baseline_path = bundle / "baseline.json"
                payload = json.loads(baseline_path.read_text(encoding="utf-8"))
                logs = payload["repetitions"][0]["artifacts"]["transaction_logs"]
                original = logs[0]
                if name == "nfc":
                    old_path = bundle / original["path"]
                    renamed = old_path.with_name("transactions-\u00e9")
                    old_path.rename(renamed)
                    original["path"] = renamed.relative_to(bundle).as_posix()
                alias = dict(original)
                alias["path"] = alias_builder(original["path"])
                self.assertNotEqual(alias["path"], original["path"])
                logs.append(alias)
                write_json_strict(baseline_path, payload)
                with self.assertRaisesRegex(BenchmarkBundleError, "collision"):
                    verify_pgbench_bundle(bundle)

    def test_symlink_and_mocked_windows_reparse_targets_fail(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            bundle = _make_bundle(root)
            target = bundle / "inputs" / "candidate.sql"
            outside = root / "outside.sql"
            outside.write_bytes(target.read_bytes())
            target.unlink()
            try:
                target.symlink_to(outside)
            except OSError:
                pass
            else:
                with self.assertRaisesRegex(
                    BenchmarkBundleError, "symlink|reparse"
                ):
                    verify_pgbench_bundle(bundle)

        with tempfile.TemporaryDirectory() as temporary:
            bundle = _make_bundle(Path(temporary))
            target = (bundle / "inputs" / "candidate.sql").resolve()
            real_lstat = os.lstat

            def marked_lstat(path):
                metadata = real_lstat(path)
                if Path(path).absolute() == target:
                    return SimpleNamespace(
                        st_mode=metadata.st_mode,
                        st_file_attributes=0x400,
                    )
                return metadata

            with patch.object(
                stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400, create=True
            ), patch("driftbench.benchmarking.verify.os.lstat", side_effect=marked_lstat):
                with self.assertRaisesRegex(BenchmarkBundleError, "reparse"):
                    verify_pgbench_bundle(bundle)

    def test_verifier_does_not_call_git_database_network_or_subprocess(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            bundle = _make_bundle(Path(temporary))
            forbidden = AssertionError("offline verifier attempted external access")
            with patch("subprocess.run", side_effect=forbidden), patch(
                "socket.create_connection", side_effect=forbidden
            ), patch(
                "driftbench.benchmarking.pgbench._postgresql_environment",
                side_effect=forbidden,
            ), patch(
                "driftbench.benchmarking.pgbench._run_process",
                side_effect=forbidden,
            ):
                self.assertTrue(verify_pgbench_bundle(bundle).ok)

    def test_source_state_tampering_fails(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            bundle = _make_bundle(Path(temporary))
            environment_path = bundle / "environment.json"
            environment = json.loads(environment_path.read_text(encoding="utf-8"))
            environment["driftbench"]["source_state"] = "dirty"
            write_json_strict(environment_path, environment)
            descriptor = _descriptor(environment_path, bundle)
            for core_name in (
                "baseline.json",
                "candidate.json",
                "decision.json",
                "execution_order.json",
            ):
                core_path = bundle / core_name
                core = json.loads(core_path.read_text(encoding="utf-8"))
                if "environment" in core:
                    core["environment"] = dict(descriptor)
                write_json_strict(core_path, core)
            with self.assertRaisesRegex(BenchmarkBundleError, "source_state"):
                verify_pgbench_bundle(bundle)

    def test_verified_threshold_failure_is_exit_five(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            bundle = _make_bundle(Path(temporary), candidate_tps=60.0)
            verification = verify_pgbench_bundle(bundle)
            exit_code, payload, stderr = self._cli(
                "benchmark", "verify", "--bundle", str(bundle), "--json"
            )

        self.assertFalse(verification.ok)
        self.assertEqual(exit_code, 5)
        self.assertTrue(payload["verified"])
        self.assertEqual(payload["outcome"], "threshold_failed")
        self.assertEqual(stderr, "")

    def test_invalid_bundle_path_and_cli_usage_are_exit_three(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            missing = Path(temporary) / "missing"
            exit_code, payload, _stderr = self._cli(
                "benchmark", "verify", "--bundle", str(missing), "--json"
            )
            self.assertEqual(exit_code, 3)
            self.assertEqual(payload["outcome"], "configuration_error")

            stdout = io.StringIO()
            stderr = io.StringIO()
            with redirect_stdout(stdout), redirect_stderr(stderr):
                invalid_exit = main(["benchmark", "verify", "--unknown"])
            self.assertEqual(invalid_exit, 3)

            json_exit, json_payload, json_stderr = self._cli(
                "benchmark", "verify", "--unknown", "--json"
            )
            self.assertEqual(json_exit, 3)
            self.assertFalse(json_payload["verified"])
            self.assertEqual(json_payload["outcome"], "configuration_error")
            self.assertEqual(json_stderr, "")

    def test_unexpected_json_cli_failure_is_one_document_and_exit_four(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            bundle = _make_bundle(Path(temporary))
            with patch(
                "driftbench.cli.verify_pgbench_bundle",
                side_effect=RuntimeError("unexpected verifier failure"),
            ):
                exit_code, payload, stderr = self._cli(
                    "benchmark", "verify", "--bundle", str(bundle), "--json"
                )
        self.assertEqual(exit_code, 4)
        self.assertFalse(payload["verified"])
        self.assertEqual(payload["outcome"], "execution_error")
        self.assertIn("unexpected verifier failure", payload["error"])
        self.assertEqual(stderr, "")


if __name__ == "__main__":
    unittest.main()
