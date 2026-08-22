"""Versioned benchmark-result metrics and fail-closed validation.

Latency percentiles use the deterministic R-7 linear interpolation method:
for a sorted zero-indexed sample, the rank is ``(n - 1) * q`` and values on
either side of a fractional rank are linearly interpolated.  This is the
default percentile method used by NumPy, but the implementation here has no
runtime dependency on NumPy and also handles a one-element sample.
"""

from __future__ import annotations

import json
import math
import statistics
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


BENCHMARK_RESULT_SCHEMA_VERSION = "1.0"
PERCENTILE_METHOD = "linear-r7"
MAX_INTEGER = (1 << 63) - 1
MAX_TPS_RELATIVE_DELTA = 0.05


class BenchmarkResultError(ValueError):
    """Raised when benchmark metrics are missing, malformed, or inconsistent."""


def _finite_number(value: Any, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise BenchmarkResultError(f"{field} must be a finite number")
    try:
        number = float(value)
    except (OverflowError, TypeError, ValueError) as exc:
        raise BenchmarkResultError(f"{field} must be a finite number") from exc
    if not math.isfinite(number):
        raise BenchmarkResultError(f"{field} must be finite")
    return number


def _integer(value: Any, field: str, *, minimum: int | None = None) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise BenchmarkResultError(f"{field} must be an integer")
    if minimum is not None and value < minimum:
        raise BenchmarkResultError(f"{field} must be >= {minimum}")
    if value > MAX_INTEGER:
        raise BenchmarkResultError(f"{field} must be <= {MAX_INTEGER}")
    return value


def _mapping(value: Any, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise BenchmarkResultError(f"{field} must be an object")
    return value


def _sequence(value: Any, field: str) -> Sequence[Any]:
    if isinstance(value, (str, bytes)) or not isinstance(value, Sequence):
        raise BenchmarkResultError(f"{field} must be an array")
    return value


def _required(mapping: Mapping[str, Any], key: str, field: str) -> Any:
    if key not in mapping:
        raise BenchmarkResultError(f"missing required field: {field}.{key}")
    return mapping[key]


def _reject_unknown(
    mapping: Mapping[str, Any], allowed: set[str] | frozenset[str], field: str
) -> None:
    unknown = sorted(str(key) for key in mapping if key not in allowed)
    if unknown:
        raise BenchmarkResultError(
            f"{field} contains unsupported field(s): {', '.join(unknown)}"
        )


def _assert_no_non_finite(value: Any, field: str = "result") -> None:
    if isinstance(value, Mapping):
        for key, nested in value.items():
            _assert_no_non_finite(nested, f"{field}.{key}")
    elif isinstance(value, (list, tuple)):
        for index, nested in enumerate(value):
            _assert_no_non_finite(nested, f"{field}[{index}]")
    elif isinstance(value, float) and not math.isfinite(value):
        raise BenchmarkResultError(f"{field} must be finite")


def percentile_linear_r7(sorted_values: Sequence[float], quantile: float) -> float:
    """Return an R-7 linearly interpolated percentile from sorted values."""

    if not sorted_values:
        raise BenchmarkResultError("latency sample must not be empty")
    if not 0.0 <= quantile <= 1.0:
        raise BenchmarkResultError("quantile must be between 0 and 1")
    if len(sorted_values) == 1:
        return float(sorted_values[0])
    rank = (len(sorted_values) - 1) * quantile
    lower = math.floor(rank)
    upper = math.ceil(rank)
    if lower == upper:
        return float(sorted_values[lower])
    fraction = rank - lower
    return float(
        sorted_values[lower]
        + (sorted_values[upper] - sorted_values[lower]) * fraction
    )


def latency_metrics_from_microseconds(latencies_us: Iterable[int | float]) -> dict[str, float]:
    """Compute mean/p50/p95/p99 latency in milliseconds from transaction logs."""

    values_ms: list[float] = []
    for index, raw in enumerate(latencies_us):
        value = _finite_number(raw, f"latencies_us[{index}]")
        if value < 0:
            raise BenchmarkResultError("transaction latency must be >= 0")
        values_ms.append(value / 1000.0)
    if not values_ms:
        raise BenchmarkResultError("transaction latency log contains no samples")
    values_ms.sort()
    metrics = {
        "mean": float(statistics.fmean(values_ms)),
        "p50": percentile_linear_r7(values_ms, 0.50),
        "p95": percentile_linear_r7(values_ms, 0.95),
        "p99": percentile_linear_r7(values_ms, 0.99),
    }
    _validate_latency(metrics, "latency_ms")
    return metrics


def _validate_latency(value: Any, field: str) -> dict[str, float]:
    latency = _mapping(value, field)
    _reject_unknown(latency, {"mean", "p50", "p95", "p99"}, field)
    parsed: dict[str, float] = {}
    for name in ("mean", "p50", "p95", "p99"):
        number = _finite_number(_required(latency, name, field), f"{field}.{name}")
        if number < 0:
            raise BenchmarkResultError(f"{field}.{name} must be >= 0")
        parsed[name] = number
    if not parsed["p50"] <= parsed["p95"] <= parsed["p99"]:
        raise BenchmarkResultError(
            f"{field} percentiles must satisfy p50 <= p95 <= p99"
        )
    return parsed


def _validate_errors(value: Any, field: str) -> dict[str, Any]:
    errors = _mapping(value, field)
    _reject_unknown(errors, {"count", "total", "rate", "types"}, field)
    count = _integer(_required(errors, "count", field), f"{field}.count", minimum=0)
    total = _integer(_required(errors, "total", field), f"{field}.total", minimum=1)
    rate = _finite_number(_required(errors, "rate", field), f"{field}.rate")
    raw_types = _mapping(_required(errors, "types", field), f"{field}.types")
    types: dict[str, int] = {}
    for raw_name, raw_count in raw_types.items():
        if not isinstance(raw_name, str) or not raw_name:
            raise BenchmarkResultError(f"{field}.types keys must be non-empty strings")
        types[raw_name] = _integer(
            raw_count, f"{field}.types.{raw_name}", minimum=0
        )
    if count > total:
        raise BenchmarkResultError(f"{field}.count must not exceed total")
    if sum(types.values()) != count:
        raise BenchmarkResultError(f"{field}.types must sum to count")
    expected_rate = count / total
    if rate < 0 or rate > 1 or not math.isclose(rate, expected_rate, rel_tol=1e-9, abs_tol=1e-12):
        raise BenchmarkResultError(f"{field}.rate must equal count / total")
    return {"count": count, "total": total, "rate": rate, "types": types}


def _validate_artifact(value: Any, field: str) -> dict[str, Any]:
    artifact = _mapping(value, field)
    _reject_unknown(artifact, {"path", "sha256", "bytes"}, field)
    path = _required(artifact, "path", field)
    digest = _required(artifact, "sha256", field)
    if not isinstance(path, str) or not path:
        raise BenchmarkResultError(f"{field}.path must be non-empty")
    if (
        not isinstance(digest, str)
        or len(digest) != 64
        or any(ch not in "0123456789abcdef" for ch in digest)
    ):
        raise BenchmarkResultError(f"{field}.sha256 must be lowercase SHA-256 hex")
    byte_count = _integer(
        _required(artifact, "bytes", field), f"{field}.bytes", minimum=0
    )
    return {"path": path, "sha256": digest, "bytes": byte_count}


def _validate_input_artifact(
    value: Any, field: str, *, expected_path: str
) -> dict[str, Any]:
    artifact = _validate_artifact(value, field)
    if artifact["path"] != expected_path:
        raise BenchmarkResultError(f"{field}.path must be {expected_path}")
    if artifact["bytes"] <= 0:
        raise BenchmarkResultError(f"{field}.bytes must be > 0")
    return artifact


def _validate_artifacts(value: Any, field: str) -> None:
    artifacts = _mapping(value, field)
    _reject_unknown(artifacts, {"stdout", "stderr", "transaction_logs"}, field)
    _validate_artifact(_required(artifacts, "stdout", field), f"{field}.stdout")
    _validate_artifact(_required(artifacts, "stderr", field), f"{field}.stderr")
    logs = _sequence(
        _required(artifacts, "transaction_logs", field),
        f"{field}.transaction_logs",
    )
    if not logs:
        raise BenchmarkResultError(f"{field}.transaction_logs must not be empty")
    for index, artifact in enumerate(logs):
        _validate_artifact(artifact, f"{field}.transaction_logs[{index}]")


def build_repetition_metrics(
    *,
    index: int,
    latencies_us: Iterable[int | float],
    transactions_successful: int,
    transactions_failed: int,
    actual_measurement_seconds: float,
    artifacts: Mapping[str, Any],
    reported_tps: float,
    reported_latency_mean_ms: float,
    max_tps_relative_delta: float,
    failure_types: Mapping[str, int] | None = None,
) -> dict[str, Any]:
    """Build one measurement repetition from raw pgbench output and log samples."""

    index = _integer(index, "index", minimum=1)
    successful = _integer(
        transactions_successful, "transactions_successful", minimum=1
    )
    failed = _integer(transactions_failed, "transactions_failed", minimum=0)
    elapsed = _finite_number(actual_measurement_seconds, "actual_measurement_seconds")
    if elapsed <= 0:
        raise BenchmarkResultError("actual_measurement_seconds must be > 0")
    total = successful + failed
    normalized_failure_types = (
        dict(failure_types)
        if failure_types is not None
        else ({"unspecified": failed} if failed else {})
    )
    if any(
        not isinstance(name, str)
        or not name
        or isinstance(count, bool)
        or not isinstance(count, int)
        or count < 0
        for name, count in normalized_failure_types.items()
    ) or sum(normalized_failure_types.values()) != failed:
        raise BenchmarkResultError("failure_types must be non-negative counts summing to failures")
    latency_samples = list(latencies_us)
    if len(latency_samples) != successful:
        raise BenchmarkResultError(
            "successful transaction count must equal the latency sample count"
        )
    latency = latency_metrics_from_microseconds(latency_samples)
    tps = successful / elapsed
    reported_tps_value = _finite_number(reported_tps, "reported_tps")
    reported_latency = _finite_number(
        reported_latency_mean_ms, "reported_latency_mean_ms"
    )
    if reported_tps_value <= 0 or reported_latency < 0:
        raise BenchmarkResultError("pgbench reported invalid TPS or latency")
    tolerance = _finite_number(
        max_tps_relative_delta, "max_tps_relative_delta"
    )
    if tolerance != MAX_TPS_RELATIVE_DELTA:
        raise BenchmarkResultError(
            f"max_tps_relative_delta must be fixed at {MAX_TPS_RELATIVE_DELTA}"
        )
    relative_delta = abs(tps - reported_tps_value) / reported_tps_value
    tps_consistency = {
        "computed": tps,
        "reported": reported_tps_value,
        "relative_delta": relative_delta,
        "tolerance": tolerance,
        "passed": relative_delta <= tolerance,
    }
    repetition = {
        "index": index,
        "actual_measurement_seconds": elapsed,
        "transactions": {"successful": successful, "total": total},
        "tps": tps,
        "latency_ms": latency,
        "errors": {
            "count": failed,
            "total": total,
            "rate": failed / total,
            "types": dict(sorted(normalized_failure_types.items())),
        },
        "pgbench_reported": {
            "tps": reported_tps_value,
            "latency_mean_ms": reported_latency,
        },
        "tps_consistency": tps_consistency,
        "artifacts": dict(artifacts),
    }
    _validate_repetition(repetition, "repetition")
    return repetition


def _validate_repetition(value: Any, field: str) -> dict[str, Any]:
    repetition = _mapping(value, field)
    _reject_unknown(
        repetition,
        {
            "index",
            "actual_measurement_seconds",
            "transactions",
            "tps",
            "latency_ms",
            "errors",
            "pgbench_reported",
            "tps_consistency",
            "artifacts",
        },
        field,
    )
    index = _integer(_required(repetition, "index", field), f"{field}.index", minimum=1)
    elapsed = _finite_number(
        _required(repetition, "actual_measurement_seconds", field),
        f"{field}.actual_measurement_seconds",
    )
    if elapsed <= 0:
        raise BenchmarkResultError(f"{field}.actual_measurement_seconds must be > 0")
    transactions = _mapping(
        _required(repetition, "transactions", field), f"{field}.transactions"
    )
    _reject_unknown(transactions, {"successful", "total"}, f"{field}.transactions")
    successful = _integer(
        _required(transactions, "successful", f"{field}.transactions"),
        f"{field}.transactions.successful",
        minimum=1,
    )
    total = _integer(
        _required(transactions, "total", f"{field}.transactions"),
        f"{field}.transactions.total",
        minimum=1,
    )
    if successful > total:
        raise BenchmarkResultError(f"{field}.transactions.successful exceeds total")
    tps = _finite_number(_required(repetition, "tps", field), f"{field}.tps")
    if tps <= 0 or not math.isclose(
        tps, successful / elapsed, rel_tol=1e-9, abs_tol=1e-12
    ):
        raise BenchmarkResultError(f"{field}.tps must equal successful / actual seconds")
    latency = _validate_latency(
        _required(repetition, "latency_ms", field), f"{field}.latency_ms"
    )
    errors = _validate_errors(
        _required(repetition, "errors", field), f"{field}.errors"
    )
    if errors["total"] != total or errors["count"] != total - successful:
        raise BenchmarkResultError(f"{field}.errors is inconsistent with transactions")
    artifacts = _mapping(
        _required(repetition, "artifacts", field), f"{field}.artifacts"
    )
    _validate_artifacts(artifacts, f"{field}.artifacts")
    reported = _mapping(
        _required(repetition, "pgbench_reported", field),
        f"{field}.pgbench_reported",
    )
    _reject_unknown(
        reported, {"tps", "latency_mean_ms"}, f"{field}.pgbench_reported"
    )
    reported_tps = _finite_number(
        _required(reported, "tps", f"{field}.pgbench_reported"),
        f"{field}.pgbench_reported.tps",
    )
    reported_latency = _finite_number(
        _required(reported, "latency_mean_ms", f"{field}.pgbench_reported"),
        f"{field}.pgbench_reported.latency_mean_ms",
    )
    if reported_tps <= 0 or reported_latency < 0:
        raise BenchmarkResultError(f"{field}.pgbench_reported contains invalid values")
    consistency = _mapping(
        _required(repetition, "tps_consistency", field),
        f"{field}.tps_consistency",
    )
    _reject_unknown(
        consistency,
        {"computed", "reported", "relative_delta", "tolerance", "passed"},
        f"{field}.tps_consistency",
    )
    consistency_computed = _finite_number(
        _required(consistency, "computed", f"{field}.tps_consistency"),
        f"{field}.tps_consistency.computed",
    )
    consistency_reported = _finite_number(
        _required(consistency, "reported", f"{field}.tps_consistency"),
        f"{field}.tps_consistency.reported",
    )
    relative_delta = _finite_number(
        _required(consistency, "relative_delta", f"{field}.tps_consistency"),
        f"{field}.tps_consistency.relative_delta",
    )
    tolerance = _finite_number(
        _required(consistency, "tolerance", f"{field}.tps_consistency"),
        f"{field}.tps_consistency.tolerance",
    )
    passed = _required(consistency, "passed", f"{field}.tps_consistency")
    if not isinstance(passed, bool):
        raise BenchmarkResultError(f"{field}.tps_consistency.passed must be boolean")
    if tolerance != MAX_TPS_RELATIVE_DELTA:
        raise BenchmarkResultError(
            f"{field}.tps_consistency.tolerance must be {MAX_TPS_RELATIVE_DELTA}"
        )
    if consistency_computed <= 0 or not math.isclose(
        consistency_computed, tps, rel_tol=1e-9, abs_tol=1e-12
    ):
        raise BenchmarkResultError(
            f"{field}.tps_consistency.computed must match authoritative TPS"
        )
    if consistency_reported <= 0 or not math.isclose(
        consistency_reported, reported_tps, rel_tol=1e-9, abs_tol=1e-12
    ):
        raise BenchmarkResultError(
            f"{field}.tps_consistency.reported must match pgbench reported TPS"
        )
    expected_delta = abs(tps - reported_tps) / reported_tps
    if relative_delta < 0 or not math.isclose(
        relative_delta, expected_delta, rel_tol=1e-9, abs_tol=1e-12
    ):
        raise BenchmarkResultError(
            f"{field}.tps_consistency.relative_delta is inconsistent"
        )
    expected_passed = expected_delta <= tolerance
    if passed is not expected_passed:
        raise BenchmarkResultError(f"{field}.tps_consistency.passed is inconsistent")
    if not passed:
        raise BenchmarkResultError(
            f"{field} computed/reported TPS relative delta exceeds {tolerance}"
        )
    return {
        "index": index,
        "elapsed": elapsed,
        "successful": successful,
        "total": total,
        "tps": tps,
        "latency": latency,
        "errors": errors,
        "artifacts": dict(artifacts),
    }


def aggregate_repetitions(repetitions: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """Aggregate repetitions using medians and a total error rate."""

    if not repetitions:
        raise BenchmarkResultError("at least one repetition is required")
    parsed = [
        _validate_repetition(item, f"repetitions[{index}]")
        for index, item in enumerate(repetitions)
    ]
    indices = [item["index"] for item in parsed]
    if indices != list(range(1, len(parsed) + 1)):
        raise BenchmarkResultError("repetition indices must be contiguous starting at 1")
    error_count = _integer(
        sum(int(item["errors"]["count"]) for item in parsed),
        "aggregate errors.count",
        minimum=0,
    )
    error_total = _integer(
        sum(int(item["errors"]["total"]) for item in parsed),
        "aggregate errors.total",
        minimum=1,
    )
    error_types: dict[str, int] = {}
    for item in parsed:
        for name, count in item["errors"]["types"].items():
            error_types[name] = error_types.get(name, 0) + int(count)
    return {
        "aggregation": "median_of_repetitions",
        "tps": float(statistics.median(item["tps"] for item in parsed)),
        "latency_ms": {
            name: float(statistics.median(item["latency"][name] for item in parsed))
            for name in ("mean", "p50", "p95", "p99")
        },
        "errors": {
            "count": error_count,
            "total": error_total,
            "rate": error_count / error_total,
            "types": dict(sorted(error_types.items())),
        },
        "percentile_method": PERCENTILE_METHOD,
    }


def validate_benchmark_result(result: Mapping[str, Any]) -> None:
    """Validate a BenchmarkRunResult v1 and fail closed on any inconsistency."""

    root = _mapping(result, "result")
    _assert_no_non_finite(root)
    _reject_unknown(
        root,
        {
            "schema_version",
            "benchmark",
            "role",
            "workload",
            "config",
            "git_sha",
            "versions",
            "inputs",
            "environment",
            "warmups",
            "repetitions",
            "metrics",
            "valid",
        },
        "result",
    )
    if _required(root, "schema_version", "result") != BENCHMARK_RESULT_SCHEMA_VERSION:
        raise BenchmarkResultError("unsupported BenchmarkRunResult schema_version")
    if _required(root, "benchmark", "result") != "pgbench":
        raise BenchmarkResultError("result.benchmark must be pgbench")
    role = _required(root, "role", "result")
    if role not in {"baseline", "candidate"}:
        raise BenchmarkResultError("result.role must be baseline or candidate")
    if _required(root, "valid", "result") is not True:
        raise BenchmarkResultError("result.valid must be true")

    workload = _mapping(_required(root, "workload", "result"), "result.workload")
    _reject_unknown(
        workload, {"name", "source", "script_sha256"}, "result.workload"
    )
    name = _required(workload, "name", "result.workload")
    source = _required(workload, "source", "result.workload")
    if not isinstance(name, str) or not name:
        raise BenchmarkResultError("result.workload.name must be non-empty")
    expected_source = "builtin" if role == "baseline" else "driftbench_script"
    if source != expected_source:
        raise BenchmarkResultError(
            f"result.workload.source must be {expected_source} for role {role}"
        )
    script_digest: str | None = None
    if role == "candidate":
        digest = _required(workload, "script_sha256", "result.workload")
        if (
            not isinstance(digest, str)
            or len(digest) != 64
            or any(ch not in "0123456789abcdef" for ch in digest)
        ):
            raise BenchmarkResultError("candidate script_sha256 must be SHA-256 hex")
        script_digest = digest
    elif "script_sha256" in workload:
        raise BenchmarkResultError("baseline workload must not contain script_sha256")

    inputs = _mapping(_required(root, "inputs", "result"), "result.inputs")
    _reject_unknown(inputs, {"policy", "candidate_script"}, "result.inputs")
    _validate_input_artifact(
        _required(inputs, "policy", "result.inputs"),
        "result.inputs.policy",
        expected_path="inputs/policy.json",
    )
    candidate_input = _validate_input_artifact(
        _required(inputs, "candidate_script", "result.inputs"),
        "result.inputs.candidate_script",
        expected_path="inputs/candidate.sql",
    )
    if script_digest is not None and script_digest != candidate_input["sha256"]:
        raise BenchmarkResultError(
            "candidate script_sha256 must match inputs.candidate_script.sha256"
        )
    _validate_input_artifact(
        _required(root, "environment", "result"),
        "result.environment",
        expected_path="environment.json",
    )

    config = _mapping(_required(root, "config", "result"), "result.config")
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
        "result.config",
    )
    _integer(_required(config, "scale_factor", "result.config"), "result.config.scale_factor", minimum=1)
    _integer(_required(config, "clients", "result.config"), "result.config.clients", minimum=1)
    _integer(_required(config, "jobs", "result.config"), "result.config.jobs", minimum=1)
    warmup_seconds = _finite_number(
        _required(config, "warmup_seconds", "result.config"),
        "result.config.warmup_seconds",
    )
    measurement_seconds = _finite_number(
        _required(config, "measurement_seconds", "result.config"),
        "result.config.measurement_seconds",
    )
    repetition_count = _integer(
        _required(config, "repetitions", "result.config"),
        "result.config.repetitions",
        minimum=1,
    )
    if warmup_seconds <= 0 or measurement_seconds <= 0:
        raise BenchmarkResultError("warmup and measurement durations must be > 0")

    git_sha = _required(root, "git_sha", "result")
    if (
        not isinstance(git_sha, str)
        or len(git_sha) != 40
        or any(ch not in "0123456789abcdef" for ch in git_sha)
    ):
        raise BenchmarkResultError(
            "result.git_sha must be a full lowercase 40-character Git SHA"
        )
    versions = _mapping(_required(root, "versions", "result"), "result.versions")
    _reject_unknown(versions, {"postgresql", "pgbench"}, "result.versions")
    for product in ("postgresql", "pgbench"):
        version = _mapping(
            _required(versions, product, "result.versions"),
            f"result.versions.{product}",
        )
        _reject_unknown(version, {"full", "major"}, f"result.versions.{product}")
        full = _required(version, "full", f"result.versions.{product}")
        if not isinstance(full, str) or not full:
            raise BenchmarkResultError(f"result.versions.{product}.full must be non-empty")
        _integer(
            _required(version, "major", f"result.versions.{product}"),
            f"result.versions.{product}.major",
            minimum=1,
        )

    warmups = _sequence(_required(root, "warmups", "result"), "result.warmups")
    if len(warmups) != repetition_count:
        raise BenchmarkResultError("warmup count must equal config.repetitions")
    for index, raw in enumerate(warmups, start=1):
        warmup = _mapping(raw, f"result.warmups[{index - 1}]")
        _reject_unknown(
            warmup,
            {
                "index",
                "actual_seconds",
                "transactions_successful",
                "transactions_failed",
                "artifacts",
            },
            f"result.warmups[{index - 1}]",
        )
        if _integer(
            _required(warmup, "index", f"result.warmups[{index - 1}]"),
            f"result.warmups[{index - 1}].index",
            minimum=1,
        ) != index:
            raise BenchmarkResultError("warmup indices must be contiguous starting at 1")
        actual = _finite_number(
            _required(warmup, "actual_seconds", f"result.warmups[{index - 1}]"),
            f"result.warmups[{index - 1}].actual_seconds",
        )
        if actual <= 0:
            raise BenchmarkResultError("warmup actual_seconds must be > 0")
        successful = _integer(
            _required(warmup, "transactions_successful", f"result.warmups[{index - 1}]"),
            f"result.warmups[{index - 1}].transactions_successful",
            minimum=1,
        )
        failed = _integer(
            _required(warmup, "transactions_failed", f"result.warmups[{index - 1}]"),
            f"result.warmups[{index - 1}].transactions_failed",
            minimum=0,
        )
        if failed != 0 or successful <= 0:
            raise BenchmarkResultError("warmup must complete transactions without errors")
        _validate_artifacts(
            _required(warmup, "artifacts", f"result.warmups[{index - 1}]"),
            f"result.warmups[{index - 1}].artifacts",
        )

    repetitions_raw = _sequence(
        _required(root, "repetitions", "result"), "result.repetitions"
    )
    if len(repetitions_raw) != repetition_count:
        raise BenchmarkResultError("measurement repetition count does not match config")
    repetitions = [
        _validate_repetition(raw, f"result.repetitions[{index}]")
        for index, raw in enumerate(repetitions_raw)
    ]
    expected_aggregate = aggregate_repetitions(list(repetitions_raw))
    metrics = _mapping(_required(root, "metrics", "result"), "result.metrics")
    _reject_unknown(
        metrics,
        {"aggregation", "tps", "latency_ms", "errors", "percentile_method"},
        "result.metrics",
    )
    if _required(metrics, "aggregation", "result.metrics") != "median_of_repetitions":
        raise BenchmarkResultError("result.metrics.aggregation is invalid")
    if _required(metrics, "percentile_method", "result.metrics") != PERCENTILE_METHOD:
        raise BenchmarkResultError("result.metrics.percentile_method is invalid")
    actual_tps = _finite_number(_required(metrics, "tps", "result.metrics"), "result.metrics.tps")
    if not math.isclose(actual_tps, expected_aggregate["tps"], rel_tol=1e-9, abs_tol=1e-12):
        raise BenchmarkResultError("aggregate TPS does not match repetition median")
    actual_latency = _validate_latency(
        _required(metrics, "latency_ms", "result.metrics"),
        "result.metrics.latency_ms",
    )
    for name, expected in expected_aggregate["latency_ms"].items():
        if not math.isclose(actual_latency[name], expected, rel_tol=1e-9, abs_tol=1e-12):
            raise BenchmarkResultError(f"aggregate latency {name} is inconsistent")
    actual_errors = _validate_errors(
        _required(metrics, "errors", "result.metrics"), "result.metrics.errors"
    )
    expected_errors = expected_aggregate["errors"]
    if (
        actual_errors["count"] != expected_errors["count"]
        or actual_errors["total"] != expected_errors["total"]
        or actual_errors["types"] != expected_errors["types"]
        or not math.isclose(
            float(actual_errors["rate"]),
            float(expected_errors["rate"]),
            rel_tol=1e-9,
            abs_tol=1e-12,
        )
    ):
        raise BenchmarkResultError("aggregate errors are inconsistent")


def write_json_strict(path: str | Path, payload: Mapping[str, Any]) -> Path:
    """Write UTF-8 JSON while rejecting NaN and Infinity."""

    _assert_no_non_finite(payload)
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_bytes(
        (
            json.dumps(
                payload,
                indent=2,
                sort_keys=True,
                ensure_ascii=False,
                allow_nan=False,
            )
            + "\n"
        ).encode("utf-8")
    )
    return destination
