"""Runner-neutral benchmark metrics and regression-gate primitives."""

from .metrics import (
    BENCHMARK_RESULT_SCHEMA_VERSION,
    BenchmarkResultError,
    aggregate_repetitions,
    build_repetition_metrics,
    latency_metrics_from_microseconds,
    validate_benchmark_result,
    write_json_strict,
)
from .policy import (
    BenchmarkPolicyError,
    PgBenchRegressionPolicy,
    evaluate_regression,
    load_pgbench_policy,
)

__all__ = [
    "BENCHMARK_RESULT_SCHEMA_VERSION",
    "BenchmarkPolicyError",
    "BenchmarkResultError",
    "PgBenchRegressionPolicy",
    "aggregate_repetitions",
    "build_repetition_metrics",
    "evaluate_regression",
    "latency_metrics_from_microseconds",
    "load_pgbench_policy",
    "validate_benchmark_result",
    "write_json_strict",
]
