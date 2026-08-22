from __future__ import annotations

import json
import unittest
from pathlib import Path

import yaml

from ..helpers import (
    VALID_SPEC,
    ReliabilityTestMixin,
    read_json as _read_json,
    run_cli as _run_cli,
)


def _case_orchestrate_partial_failure_emits_manifest_and_exit_four(tmp_path: Path) -> None:
    success_dir = tmp_path / "success"
    setup_fail_dir = tmp_path / "setup_fail"
    run_fail_dir = tmp_path / "run_fail"
    for path in (success_dir, setup_fail_dir, run_fail_dir):
        path.mkdir()

    targets = tmp_path / "targets.yaml"
    manifest_path = tmp_path / "manifest.json"
    targets.write_text(
        yaml.safe_dump(
            {
                "targets": [
                    {
                        "name": "success",
                        "workdir": str(success_dir),
                        "run_command": "python -c \"from pathlib import Path; Path('ok.txt').write_text('ok')\"",
                        "output_globs": ["*.txt"],
                    },
                    {
                        "name": "setup-fail",
                        "workdir": str(setup_fail_dir),
                        "setup_command": "python -c \"import sys; sys.exit(7)\"",
                        "run_command": "python -c \"raise AssertionError('must not run')\"",
                    },
                    {
                        "name": "run-fail",
                        "workdir": str(run_fail_dir),
                        "run_command": "python -c \"import sys; sys.exit(9)\"",
                    },
                    {
                        "name": "invalid-workdir",
                        "workdir": str(tmp_path / "missing"),
                        "run_command": "python -c \"raise AssertionError('must not run')\"",
                    },
                ]
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )

    proc = _run_cli(
        "orchestrate",
        "--spec",
        str(VALID_SPEC),
        "--targets",
        str(targets),
        "--manifest-out",
        str(manifest_path),
        "--execute",
        "--json",
    )
    assert proc.returncode == 4, proc.stderr
    payload = json.loads(proc.stdout)
    manifest = _read_json(manifest_path)
    assert payload["ok"] is False
    assert payload["outcome"] == "partial_failure"
    assert manifest["ok"] is False
    assert manifest["outcome"] == "partial_failure"
    assert manifest["summary"]["completed"] == 1
    assert manifest["summary"]["failed"] == 3
    statuses = {item["target"]: item for item in manifest["targets"]}
    assert statuses["success"]["status"] == "completed"
    assert statuses["setup-fail"]["status"] == "setup_failed"
    assert statuses["setup-fail"]["run"] is None
    assert statuses["run-fail"]["status"] == "run_failed"
    assert statuses["invalid-workdir"]["status"] == "invalid_target_workdir"


def _case_orchestrate_all_failures_have_failed_outcome(tmp_path: Path) -> None:
    workdir = tmp_path / "workdir"
    workdir.mkdir()
    targets = tmp_path / "targets.yaml"
    manifest_path = tmp_path / "manifest.json"
    targets.write_text(
        yaml.safe_dump(
            {
                "targets": [
                    {
                        "name": "failure",
                        "workdir": str(workdir),
                        "run_command": "python -c \"import sys; sys.exit(2)\"",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    proc = _run_cli(
        "orchestrate",
        "--spec",
        str(VALID_SPEC),
        "--targets",
        str(targets),
        "--manifest-out",
        str(manifest_path),
        "--execute",
        "--json",
    )
    assert proc.returncode == 4, proc.stderr
    assert json.loads(proc.stdout)["outcome"] == "failed"
    manifest = _read_json(manifest_path)
    assert manifest["ok"] is False
    assert manifest["outcome"] == "failed"

class BenchmarkReliabilityTests(ReliabilityTestMixin, unittest.TestCase):
    def test_orchestrate_partial_failure_emits_manifest_and_exit_four(self) -> None:
        self._run_case(_case_orchestrate_partial_failure_emits_manifest_and_exit_four)

    def test_orchestrate_all_failures_have_failed_outcome(self) -> None:
        self._run_case(_case_orchestrate_all_failures_have_failed_outcome)
