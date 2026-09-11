from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[1]
RUNNER = ROOT / "ce_runner.py"


def _run(*args: str) -> subprocess.CompletedProcess[str]:
    environment = os.environ.copy()
    environment["PYTHONDONTWRITEBYTECODE"] = "1"
    return subprocess.run(
        [sys.executable, str(RUNNER), *args],
        check=False,
        capture_output=True,
        text=True,
        cwd=ROOT,
        env=environment,
    )


def _snapshot(root: Path) -> dict[str, str]:
    return {
        path.relative_to(root).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in root.rglob("*")
        if path.is_file()
    }


def test_plan_is_deterministic_and_review_only() -> None:
    first = _run("plan")
    second = _run("plan")
    assert first.returncode == second.returncode == 0
    assert first.stdout.encode("utf-8") == second.stdout.encode("utf-8")
    payload = json.loads(first.stdout)
    assert payload["experiment"]["experiment_id"] == "rel-ce-tpch"
    assert payload["experiment"]["review_only"] is True
    assert payload["experiment"]["dataset"]["scale_factor"] == 1.0


def test_registry_has_required_method_groups_and_claim_boundary() -> None:
    registry = yaml.safe_load((ROOT / "baselines.yaml").read_text(encoding="utf-8"))
    by_id = {entry["id"]: entry for entry in registry["estimators"]}
    assert set(registry["selection_policy"]["active_plan_anchors"]) == {
        "postgres-default",
        "postgres-extended-statistics",
    }
    assert {"mscn", "deepdb", "neurocard", "bayescard"}.issubset(by_id)
    assert {"factorjoin", "alece", "price"}.issubset(by_id)
    assert {"cardood", "distjoin", "zerocard"}.issubset(by_id)
    assert "not that DriftBench reproduced" in registry["claim_boundary"]
    assert registry["excluded_from_active_multi_table_set"][0]["id"] == "naru"
    assert set(registry["groups"]) == {entry["group"] for entry in by_id.values()}
    assert all(entry["adapter_status"] != "verified" for entry in by_id.values())
    assert all(
        entry["source"]["repository"] is None
        or len(entry["source"]["revision"]) == 40
        for entry in by_id.values()
    )


def test_unverified_estimator_is_blocked_without_side_effects(tmp_path: Path) -> None:
    for filename in ("ce_runner.py", "tpch_plan.yaml", "baselines.yaml"):
        shutil.copy2(ROOT / filename, tmp_path / filename)
    before = _snapshot(tmp_path)
    environment = os.environ.copy()
    environment["PYTHONDONTWRITEBYTECODE"] = "1"
    result = subprocess.run(
        [sys.executable, str(tmp_path / "ce_runner.py"), "run", "--estimators", "price"],
        check=False,
        capture_output=True,
        text=True,
        cwd=tmp_path,
        env=environment,
    )
    after = _snapshot(tmp_path)
    assert result.returncode == 78
    assert "ESTIMATOR_NOT_RUNNABLE" in result.stderr
    assert "No side effect occurred" in result.stderr
    assert before == after


def test_anchor_execution_is_still_review_blocked() -> None:
    result = _run("run")
    assert result.returncode == 78
    assert "REVIEW_ONLY" in result.stderr
    assert "No side effect occurred" in result.stderr


def test_probe_provenance_and_fairness_contract_are_explicit() -> None:
    plan = yaml.safe_load((ROOT / "tpch_plan.yaml").read_text(encoding="utf-8"))
    probes = plan["workload"]["probes"]
    assert len(probes) >= 4
    assert all(probe["template_id"] and probe["subquery_id"] and probe["provenance"] for probe in probes)
    assert plan["fairness"]["q_error"]["percentiles"] == ["p50", "p90", "p95", "p99", "max"]
    assert plan["fairness"]["policy_separation"] == ["static_frozen", "adaptive_updated"]
    registry = yaml.safe_load((ROOT / "baselines.yaml").read_text(encoding="utf-8"))
    assert plan["baseline_registry"] == {
        "path": "baselines.yaml",
        "group_ids": list(registry["groups"]),
    }


def test_legacy_census_shell_entrypoint_preserves_runner_arguments_and_exit_code() -> None:
    wrapper = ROOT / "run_ce_timeline_census.sh"
    wrapper_text = wrapper.read_text(encoding="utf-8")
    assert "legacy_census_runner.py" in wrapper_text
    assert '"$@"' in wrapper_text
    assert "/ce_runner.py" not in wrapper_text

    bash = shutil.which("bash")
    if bash is None or os.name == "nt":
        return
    wrapped = subprocess.run(
        [bash, str(wrapper), "not-a-command"],
        check=False,
        capture_output=True,
        text=True,
        cwd=ROOT,
    )
    direct = subprocess.run(
        [sys.executable, str(ROOT / "legacy_census_runner.py"), "not-a-command"],
        check=False,
        capture_output=True,
        text=True,
        cwd=ROOT,
    )
    assert wrapped.returncode == direct.returncode == 2
    assert "invalid choice" in wrapped.stderr
