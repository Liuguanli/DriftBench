"""Version-controlled pgbench regression policy and evaluator."""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from .metrics import MAX_INTEGER, BenchmarkResultError, validate_benchmark_result
from .provenance import pgbench_policy_sha256


DECISION_SCHEMA_VERSION = "1.0"
MAX_PGBENCH_CLIENTS = 1024
MAX_PGBENCH_JOBS = 256
MAX_PGBENCH_PHASE_SECONDS = 3600
MAX_PGBENCH_REPETITIONS = 20
MAX_PGBENCH_PLANNED_SECONDS = 24 * 60 * 60


class BenchmarkPolicyError(ValueError):
    """Raised when a threshold policy is malformed or unsupported."""


def _json_constant(value: str) -> None:
    raise BenchmarkPolicyError(f"policy contains invalid JSON number: {value}")


def _object(value: Any, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise BenchmarkPolicyError(f"{field} must be an object")
    return value


def _required(value: Mapping[str, Any], key: str, field: str) -> Any:
    if key not in value:
        raise BenchmarkPolicyError(f"missing required field: {field}.{key}")
    return value[key]


def _reject_unknown(
    value: Mapping[str, Any], allowed: set[str], field: str
) -> None:
    unknown = sorted(set(value) - allowed)
    if unknown:
        raise BenchmarkPolicyError(
            f"{field} contains unsupported field(s): {', '.join(unknown)}"
        )


def _positive_int(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise BenchmarkPolicyError(f"{field} must be a positive integer")
    if value > MAX_INTEGER:
        raise BenchmarkPolicyError(f"{field} must be <= {MAX_INTEGER}")
    return value


def _finite(value: Any, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise BenchmarkPolicyError(f"{field} must be a finite number")
    try:
        number = float(value)
    except (OverflowError, TypeError, ValueError) as exc:
        raise BenchmarkPolicyError(f"{field} must be a finite number") from exc
    if not math.isfinite(number):
        raise BenchmarkPolicyError(f"{field} must be finite")
    return number


@dataclass(frozen=True)
class PgBenchRegressionPolicy:
    schema_version: str
    policy_version: str
    postgresql_major: int
    pgbench_major: int
    workload: str
    scale_factor: int
    clients: int
    jobs: int
    warmup_seconds: int
    measurement_seconds: int
    repetitions: int
    min_tps_ratio: float
    max_p95_latency_ratio: float
    max_error_rate: float
    max_tps_relative_delta: float
    execution_order: str

    def run_config(self) -> dict[str, int]:
        return {
            "scale_factor": self.scale_factor,
            "clients": self.clients,
            "jobs": self.jobs,
            "warmup_seconds": self.warmup_seconds,
            "measurement_seconds": self.measurement_seconds,
            "repetitions": self.repetitions,
        }


def validate_pgbench_policy_operational_limits(
    policy: PgBenchRegressionPolicy,
) -> None:
    """Reject policies that can create unreasonable local work or processes."""

    limits = (
        ("clients", policy.clients, MAX_PGBENCH_CLIENTS),
        ("jobs", policy.jobs, MAX_PGBENCH_JOBS),
        ("warmup_seconds", policy.warmup_seconds, MAX_PGBENCH_PHASE_SECONDS),
        (
            "measurement_seconds",
            policy.measurement_seconds,
            MAX_PGBENCH_PHASE_SECONDS,
        ),
        ("repetitions", policy.repetitions, MAX_PGBENCH_REPETITIONS),
    )
    for name, value, maximum in limits:
        _positive_int(value, f"policy.config.{name}")
        if value > maximum:
            raise BenchmarkPolicyError(
                f"policy.config.{name} must be <= {maximum}"
            )
    if policy.jobs > policy.clients:
        raise BenchmarkPolicyError("policy.config.jobs must be <= policy.config.clients")

    planned_seconds = (
        2
        * policy.repetitions
        * (policy.warmup_seconds + policy.measurement_seconds)
    )
    if planned_seconds > MAX_PGBENCH_PLANNED_SECONDS:
        raise BenchmarkPolicyError(
            "policy planned phase time must be <= "
            f"{MAX_PGBENCH_PLANNED_SECONDS} seconds"
        )


def load_pgbench_policy(path: str | Path) -> PgBenchRegressionPolicy:
    """Load and strictly validate a version-controlled pgbench policy."""

    source = Path(path)
    try:
        raw = json.loads(source.read_text(encoding="utf-8"), parse_constant=_json_constant)
    except BenchmarkPolicyError:
        raise
    except Exception as exc:
        raise BenchmarkPolicyError(f"failed to read policy {source}: {exc}") from exc
    return parse_pgbench_policy_payload(raw)


def parse_pgbench_policy_payload(raw: Any) -> PgBenchRegressionPolicy:
    """Strictly validate an already-decoded policy payload."""

    root = _object(raw, "policy")
    _reject_unknown(
        root,
        {
            "schema_version",
            "policy_version",
            "benchmark",
            "workload",
            "execution_order",
            "versions",
            "config",
            "thresholds",
        },
        "policy",
    )
    schema_version = _required(root, "schema_version", "policy")
    if schema_version != "1.0":
        raise BenchmarkPolicyError("unsupported policy.schema_version")
    policy_version = _required(root, "policy_version", "policy")
    if not isinstance(policy_version, str) or not policy_version.strip():
        raise BenchmarkPolicyError("policy.policy_version must be non-empty")
    if _required(root, "benchmark", "policy") != "pgbench":
        raise BenchmarkPolicyError("policy.benchmark must be pgbench")
    workload = _required(root, "workload", "policy")
    if workload != "select_only":
        raise BenchmarkPolicyError("policy.workload must be select_only")
    execution_order = _required(root, "execution_order", "policy")
    if execution_order != "alternating_baseline_first":
        raise BenchmarkPolicyError(
            "policy.execution_order must be alternating_baseline_first"
        )
    versions = _object(_required(root, "versions", "policy"), "policy.versions")
    config = _object(_required(root, "config", "policy"), "policy.config")
    thresholds = _object(
        _required(root, "thresholds", "policy"), "policy.thresholds"
    )
    _reject_unknown(
        versions,
        {"postgresql_major", "pgbench_major"},
        "policy.versions",
    )
    _reject_unknown(
        config,
        {
            "scale_factor",
            "clients",
            "jobs",
            "warmup_seconds",
            "measurement_seconds",
            "repetitions",
        },
        "policy.config",
    )
    _reject_unknown(
        thresholds,
        {
            "min_tps_ratio",
            "max_p95_latency_ratio",
            "max_error_rate",
            "max_tps_relative_delta",
        },
        "policy.thresholds",
    )
    policy = PgBenchRegressionPolicy(
        schema_version=schema_version,
        policy_version=policy_version.strip(),
        postgresql_major=_positive_int(
            _required(versions, "postgresql_major", "policy.versions"),
            "policy.versions.postgresql_major",
        ),
        pgbench_major=_positive_int(
            _required(versions, "pgbench_major", "policy.versions"),
            "policy.versions.pgbench_major",
        ),
        workload=workload,
        scale_factor=_positive_int(
            _required(config, "scale_factor", "policy.config"),
            "policy.config.scale_factor",
        ),
        clients=_positive_int(
            _required(config, "clients", "policy.config"),
            "policy.config.clients",
        ),
        jobs=_positive_int(
            _required(config, "jobs", "policy.config"), "policy.config.jobs"
        ),
        warmup_seconds=_positive_int(
            _required(config, "warmup_seconds", "policy.config"),
            "policy.config.warmup_seconds",
        ),
        measurement_seconds=_positive_int(
            _required(config, "measurement_seconds", "policy.config"),
            "policy.config.measurement_seconds",
        ),
        repetitions=_positive_int(
            _required(config, "repetitions", "policy.config"),
            "policy.config.repetitions",
        ),
        min_tps_ratio=_finite(
            _required(thresholds, "min_tps_ratio", "policy.thresholds"),
            "policy.thresholds.min_tps_ratio",
        ),
        max_p95_latency_ratio=_finite(
            _required(thresholds, "max_p95_latency_ratio", "policy.thresholds"),
            "policy.thresholds.max_p95_latency_ratio",
        ),
        max_error_rate=_finite(
            _required(thresholds, "max_error_rate", "policy.thresholds"),
            "policy.thresholds.max_error_rate",
        ),
        max_tps_relative_delta=_finite(
            _required(thresholds, "max_tps_relative_delta", "policy.thresholds"),
            "policy.thresholds.max_tps_relative_delta",
        ),
        execution_order=execution_order,
    )
    if not 0 < policy.min_tps_ratio <= 1:
        raise BenchmarkPolicyError("min_tps_ratio must be in (0, 1]")
    if policy.max_p95_latency_ratio < 1:
        raise BenchmarkPolicyError("max_p95_latency_ratio must be >= 1")
    if not 0 <= policy.max_error_rate <= 1:
        raise BenchmarkPolicyError("max_error_rate must be in [0, 1]")
    if policy.max_tps_relative_delta != 0.05:
        raise BenchmarkPolicyError("max_tps_relative_delta must be fixed at 0.05")
    validate_pgbench_policy_operational_limits(policy)
    return policy


def _compatibility_reasons(
    baseline: Mapping[str, Any],
    candidate: Mapping[str, Any],
    policy: PgBenchRegressionPolicy,
) -> list[str]:
    reasons: list[str] = []
    if baseline.get("role") != "baseline":
        reasons.append("baseline result role must be baseline")
    if candidate.get("role") != "candidate":
        reasons.append("candidate result role must be candidate")
    baseline_workload = baseline.get("workload", {})
    candidate_workload = candidate.get("workload", {})
    if baseline_workload.get("name") != policy.workload:
        reasons.append("baseline workload does not match policy")
    if candidate_workload.get("name") != policy.workload:
        reasons.append("candidate workload does not match policy")
    if baseline_workload.get("source") != "builtin":
        reasons.append("baseline must use native pgbench builtin workload")
    if candidate_workload.get("source") != "driftbench_script":
        reasons.append("candidate must use a DriftBench-generated script")
    if baseline.get("git_sha") != candidate.get("git_sha"):
        reasons.append("baseline/candidate git_sha mismatch")
    if baseline.get("inputs") != candidate.get("inputs"):
        reasons.append("baseline/candidate input descriptors mismatch")
    if baseline.get("environment") != candidate.get("environment"):
        reasons.append("baseline/candidate environment descriptor mismatch")
    expected_policy_digest = pgbench_policy_sha256(policy)
    for role, result in (("baseline", baseline), ("candidate", candidate)):
        policy_digest = (
            result.get("inputs", {}).get("policy", {}).get("sha256")
        )
        if policy_digest != expected_policy_digest:
            reasons.append(f"{role} policy snapshot does not match policy")

    expected_config = policy.run_config()
    for name, expected in expected_config.items():
        baseline_value = baseline.get("config", {}).get(name)
        candidate_value = candidate.get("config", {}).get(name)
        if baseline_value != expected:
            reasons.append(f"baseline config.{name} does not match policy")
        if candidate_value != expected:
            reasons.append(f"candidate config.{name} does not match policy")
        if baseline_value != candidate_value:
            reasons.append(f"baseline/candidate config.{name} mismatch")

    for product, expected_major in (
        ("postgresql", policy.postgresql_major),
        ("pgbench", policy.pgbench_major),
    ):
        baseline_major = (
            baseline.get("versions", {}).get(product, {}).get("major")
        )
        candidate_major = (
            candidate.get("versions", {}).get(product, {}).get("major")
        )
        if baseline_major != expected_major:
            reasons.append(f"baseline {product} major does not match policy")
        if candidate_major != expected_major:
            reasons.append(f"candidate {product} major does not match policy")
        if baseline_major != candidate_major:
            reasons.append(f"baseline/candidate {product} major mismatch")
        baseline_full = (
            baseline.get("versions", {}).get(product, {}).get("full")
        )
        candidate_full = (
            candidate.get("versions", {}).get(product, {}).get("full")
        )
        if baseline_full != candidate_full:
            reasons.append(f"baseline/candidate {product} full version mismatch")
    return reasons


def evaluate_regression(
    baseline: Mapping[str, Any],
    candidate: Mapping[str, Any],
    policy: PgBenchRegressionPolicy,
    *,
    baseline_path: str | None = None,
    candidate_path: str | None = None,
) -> dict[str, Any]:
    """Evaluate paired results; invalid inputs return a fail-closed decision."""

    reasons: list[str] = []
    validity_checks: list[dict[str, Any]] = []
    for role, result in (("baseline", baseline), ("candidate", candidate)):
        try:
            validate_benchmark_result(result)
        except (BenchmarkResultError, TypeError, ArithmeticError) as exc:
            message = f"{role} metrics invalid: {exc}"
            reasons.append(message)
            validity_checks.append(
                {"name": f"{role}_metrics_valid", "passed": False, "reason": str(exc)}
            )
        else:
            validity_checks.append(
                {"name": f"{role}_metrics_valid", "passed": True}
            )

    compatibility_reasons: list[str] = []
    checks: list[dict[str, Any]] = list(validity_checks)
    if not reasons:
        compatibility_reasons = _compatibility_reasons(baseline, candidate, policy)
        reasons.extend(compatibility_reasons)
        checks.append(
            {
                "name": "baseline_candidate_compatible",
                "passed": not compatibility_reasons,
                "reasons": compatibility_reasons,
            }
        )

    if not reasons:
        baseline_metrics = baseline["metrics"]
        candidate_metrics = candidate["metrics"]
        baseline_tps = float(baseline_metrics["tps"])
        candidate_tps = float(candidate_metrics["tps"])
        baseline_p95 = float(baseline_metrics["latency_ms"]["p95"])
        candidate_p95 = float(candidate_metrics["latency_ms"]["p95"])
        if baseline_tps <= 0 or baseline_p95 <= 0:
            reasons.append("baseline TPS and p95 latency must be positive denominators")
        else:
            tps_threshold = baseline_tps * policy.min_tps_ratio
            latency_threshold = baseline_p95 * policy.max_p95_latency_ratio
            tps_passed = candidate_tps >= tps_threshold
            latency_passed = candidate_p95 <= latency_threshold
            checks.extend(
                [
                    {
                        "name": "candidate_median_tps",
                        "passed": tps_passed,
                        "actual": candidate_tps,
                        "operator": ">=",
                        "threshold": tps_threshold,
                        "ratio_to_baseline": candidate_tps / baseline_tps,
                    },
                    {
                        "name": "candidate_median_p95_latency_ms",
                        "passed": latency_passed,
                        "actual": candidate_p95,
                        "operator": "<=",
                        "threshold": latency_threshold,
                        "ratio_to_baseline": candidate_p95 / baseline_p95,
                    },
                ]
            )
            if not tps_passed:
                reasons.append("candidate median TPS is below the policy threshold")
            if not latency_passed:
                reasons.append("candidate median p95 latency exceeds the policy threshold")

        for role, result in (("baseline", baseline), ("candidate", candidate)):
            error_rate = float(result["metrics"]["errors"]["rate"])
            error_passed = error_rate <= policy.max_error_rate
            checks.append(
                {
                    "name": f"{role}_error_rate",
                    "passed": error_passed,
                    "actual": error_rate,
                    "operator": "<=",
                    "threshold": policy.max_error_rate,
                }
            )
            if not error_passed:
                reasons.append(f"{role} error rate exceeds the policy threshold")

    validity_ok = all(bool(check.get("passed")) for check in validity_checks)
    compatibility_status_reasons = list(compatibility_reasons)
    if not validity_ok:
        compatibility_status_reasons.append(
            "compatibility was not evaluated because benchmark metrics are invalid"
        )
    return {
        "schema_version": DECISION_SCHEMA_VERSION,
        "policy_version": policy.policy_version,
        "benchmark": "pgbench",
        "ok": not reasons and all(bool(check.get("passed")) for check in checks),
        "baseline_result": baseline_path,
        "candidate_result": candidate_path,
        "compatibility": {
            "ok": validity_ok and not compatibility_reasons,
            "reasons": compatibility_status_reasons,
        },
        "checks": checks,
        "reasons": reasons,
    }
