import copy
import json
import math
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path

from jsonschema import Draft202012Validator, ValidationError

from driftbench.benchmarking.metrics import (
    BenchmarkResultError,
    MAX_INTEGER,
    MAX_TPS_RELATIVE_DELTA,
    aggregate_repetitions,
    build_repetition_metrics,
    latency_metrics_from_microseconds,
    validate_benchmark_result,
    write_json_strict,
)
from driftbench.benchmarking.pgbench import (
    build_paired_execution_plan,
    parse_pgbench_stdout,
    parse_pgbench_transaction_log_summary,
    parse_pgbench_transaction_logs,
)
from driftbench.benchmarking.policy import (
    BenchmarkPolicyError,
    MAX_PGBENCH_CLIENTS,
    MAX_PGBENCH_JOBS,
    MAX_PGBENCH_PHASE_SECONDS,
    MAX_PGBENCH_PLANNED_SECONDS,
    MAX_PGBENCH_REPETITIONS,
    evaluate_regression,
    load_pgbench_policy,
)
from driftbench.benchmarking.provenance import pgbench_policy_sha256
from ..helpers import REPO_ROOT


POLICY_PATH = (
    REPO_ROOT
    / "driftbench"
    / "benchmarking"
    / "policies"
    / "pgbench_ci_v1.json"
)
SCHEMA_PATH = (
    REPO_ROOT
    / "driftbench"
    / "benchmarking"
    / "schemas"
    / "benchmark_run_result_v1.schema.json"
)
POLICY_DIGEST = pgbench_policy_sha256(load_pgbench_policy(POLICY_PATH))


def _artifact_set() -> dict:
    artifact = {"path": "raw/file.log", "sha256": "0" * 64, "bytes": 1}
    return {
        "stdout": dict(artifact),
        "stderr": dict(artifact),
        "transaction_logs": [dict(artifact)],
    }


def _result(
    role: str,
    *,
    tps: float = 100.0,
    latency_ms: float = 10.0,
    failed: int = 0,
) -> dict:
    repetitions = []
    for index in range(1, 4):
        successful = 100 - failed
        repetitions.append(
            build_repetition_metrics(
                index=index,
                latencies_us=[latency_ms * 1000] * successful,
                transactions_successful=successful,
                transactions_failed=failed,
                actual_measurement_seconds=successful / tps,
                artifacts=_artifact_set(),
                reported_tps=tps,
                reported_latency_mean_ms=latency_ms,
                max_tps_relative_delta=MAX_TPS_RELATIVE_DELTA,
            )
        )
    workload = {
        "name": "select_only",
        "source": "builtin" if role == "baseline" else "driftbench_script",
    }
    if role == "candidate":
        workload["script_sha256"] = "a" * 64
    result = {
        "schema_version": "1.0",
        "benchmark": "pgbench",
        "role": role,
        "workload": workload,
        "config": {
            "scale_factor": 1,
            "clients": 2,
            "jobs": 2,
            "warmup_seconds": 3,
            "measurement_seconds": 5,
            "repetitions": 3,
        },
        "git_sha": "d" * 40,
        "versions": {
            "postgresql": {"full": "PostgreSQL 16.9", "major": 16},
            "pgbench": {"full": "pgbench (PostgreSQL) 16.9", "major": 16},
        },
        "inputs": {
            "policy": {
                "path": "inputs/policy.json",
                "sha256": POLICY_DIGEST,
                "bytes": 1,
            },
            "candidate_script": {
                "path": "inputs/candidate.sql",
                "sha256": "a" * 64,
                "bytes": 1,
            },
        },
        "environment": {
            "path": "environment.json",
            "sha256": "e" * 64,
            "bytes": 1,
        },
        "warmups": [
            {
                "index": index,
                "actual_seconds": 3.01,
                "transactions_successful": 10,
                "transactions_failed": 0,
                "artifacts": _artifact_set(),
            }
            for index in range(1, 4)
        ],
        "repetitions": repetitions,
        "metrics": aggregate_repetitions(repetitions),
        "valid": True,
    }
    validate_benchmark_result(result)
    return result


class PgBenchParserTests(unittest.TestCase):
    def test_stdout_parser_requires_all_metrics(self) -> None:
        parsed = parse_pgbench_stdout(
            """transaction type: <builtin: select only>
scaling factor: 1
query mode: simple
number of clients: 2
number of threads: 2
duration: 5 s
number of transactions actually processed: 100
number of failed transactions: 0 (0.000%)
latency average = 0.500 ms
initial connection time = 1.000 ms
tps = 2000.000000 (without initial connection time)
"""
        )
        self.assertEqual(parsed.transactions_successful, 100)
        self.assertEqual(parsed.transactions_failed, 0)
        self.assertEqual(parsed.transactions_total, 100)
        self.assertEqual(parsed.scale_factor, 1)
        self.assertEqual(parsed.clients, 2)
        self.assertEqual(parsed.reported_tps, 2000.0)

    def test_stdout_parser_fails_closed_without_error_count(self) -> None:
        with self.assertRaisesRegex(BenchmarkResultError, "failed transaction count"):
            parse_pgbench_stdout(
                """scaling factor: 1
number of clients: 2
number of transactions actually processed: 10
latency average = 1 ms
tps = 10
"""
            )

    def test_stdout_parser_rejects_missing_nonfinite_and_nonpositive_tps(self) -> None:
        template = """scaling factor: 1
number of clients: 2
number of transactions actually processed: 10
number of failed transactions: 0
latency average = 1 ms
{tps_line}
"""
        for name, tps_line in (
            ("missing", ""),
            ("nan", "tps = NaN"),
            ("infinity", "tps = inf"),
            ("zero", "tps = 0"),
            ("negative", "tps = -1"),
        ):
            with self.subTest(name=name), self.assertRaises(BenchmarkResultError):
                parse_pgbench_stdout(template.format(tps_line=tps_line))

    def test_stdout_parser_supports_processed_attempted_ratio(self) -> None:
        parsed = parse_pgbench_stdout(
            """scaling factor: 1
number of clients: 2
number of transactions actually processed: 9/10
latency average = 1 ms
tps = 9
"""
        )
        self.assertEqual(parsed.transactions_failed, 1)
        self.assertEqual(parsed.transactions_total, 10)

    def test_transaction_logs_merge_and_reject_malformed_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            one = root / "transactions.1"
            two = root / "transactions.2"
            one.write_text("0 1 1000 0 1700000000 1\n", encoding="utf-8")
            two.write_text("1 1 2000 0 1700000000 2\n", encoding="utf-8")
            self.assertEqual(parse_pgbench_transaction_logs([two, one]), [1000, 2000])
            two.write_text("partial row\n", encoding="utf-8")
            with self.assertRaisesRegex(BenchmarkResultError, "partial"):
                parse_pgbench_transaction_logs([one, two])

    def test_transaction_logs_reject_empty_negative_and_non_numeric(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            log = Path(tmp) / "transactions"
            log.write_text("", encoding="utf-8")
            with self.assertRaisesRegex(BenchmarkResultError, "empty"):
                parse_pgbench_transaction_logs([log])
            log.write_text("0 1 -1 0 1 1\n", encoding="utf-8")
            with self.assertRaisesRegex(BenchmarkResultError, "negative"):
                parse_pgbench_transaction_logs([log])
            log.write_text("0 1 NaN 0 1 1\n", encoding="utf-8")
            with self.assertRaisesRegex(BenchmarkResultError, "invalid"):
                parse_pgbench_transaction_logs([log])

    def test_transaction_logs_count_pg16_typed_failures(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            log = Path(tmp) / "transactions"
            log.write_text(
                "0 1 1000 0 1700000000 1\n"
                "0 2 failed 0 1700000000 2\n"
                "0 3 serialization 0 1700000000 3\n"
                "0 4 deadlock 0 1700000000 4\n",
                encoding="utf-8",
            )
            summary = parse_pgbench_transaction_log_summary([log])
            self.assertEqual(summary.latencies_us, (1000,))
            self.assertEqual(summary.transactions_successful, 1)
            self.assertEqual(summary.transactions_failed, 3)
            self.assertEqual(summary.transactions_total, 4)
            self.assertEqual(
                summary.failure_types,
                {"deadlock": 1, "failed": 1, "serialization": 1},
            )


class BenchmarkMetricsContractTests(unittest.TestCase):
    def test_latency_uses_documented_linear_r7_percentiles(self) -> None:
        metrics = latency_metrics_from_microseconds(
            value * 1000 for value in range(1, 21)
        )
        self.assertEqual(metrics["mean"], 10.5)
        self.assertEqual(metrics["p50"], 10.5)
        self.assertAlmostEqual(metrics["p95"], 19.05)
        self.assertAlmostEqual(metrics["p99"], 19.81)

    def test_warmup_is_not_part_of_measurement_latency(self) -> None:
        measurement = build_repetition_metrics(
            index=1,
            latencies_us=[1000, 2000, 3000],
            transactions_successful=3,
            transactions_failed=0,
            actual_measurement_seconds=1.5,
            artifacts=_artifact_set(),
            reported_tps=2.0,
            reported_latency_mean_ms=9999.0,
            max_tps_relative_delta=MAX_TPS_RELATIVE_DELTA,
        )
        self.assertEqual(measurement["latency_ms"]["p95"], 2.9)
        self.assertEqual(measurement["tps"], 2.0)
        self.assertEqual(measurement["tps_consistency"]["computed"], 2.0)

    def test_tps_dual_source_boundary_and_large_mismatch(self) -> None:
        at_boundary = build_repetition_metrics(
            index=1,
            latencies_us=[1000] * 19,
            transactions_successful=19,
            transactions_failed=0,
            actual_measurement_seconds=19 / 95,
            artifacts=_artifact_set(),
            reported_tps=100.0,
            reported_latency_mean_ms=1.0,
            max_tps_relative_delta=MAX_TPS_RELATIVE_DELTA,
        )
        self.assertAlmostEqual(at_boundary["tps_consistency"]["relative_delta"], 0.05)
        self.assertTrue(at_boundary["tps_consistency"]["passed"])

        upper_boundary = build_repetition_metrics(
            index=1,
            latencies_us=[1000] * 21,
            transactions_successful=21,
            transactions_failed=0,
            actual_measurement_seconds=21 / 105.0,
            artifacts=_artifact_set(),
            reported_tps=100.0,
            reported_latency_mean_ms=1.0,
            max_tps_relative_delta=MAX_TPS_RELATIVE_DELTA,
        )
        self.assertEqual(upper_boundary["tps_consistency"]["relative_delta"], 0.05)
        self.assertTrue(upper_boundary["tps_consistency"]["passed"])

        for computed, reported in (
            (94.999, 100.0),
            (105.001, 100.0),
            (2.0, 999.0),
        ):
            with self.subTest(computed=computed, reported=reported), self.assertRaisesRegex(
                BenchmarkResultError, "computed/reported TPS relative delta"
            ):
                build_repetition_metrics(
                    index=1,
                    latencies_us=[1000] * 19,
                    transactions_successful=19,
                    transactions_failed=0,
                    actual_measurement_seconds=19 / computed,
                    artifacts=_artifact_set(),
                    reported_tps=reported,
                    reported_latency_mean_ms=1.0,
                    max_tps_relative_delta=MAX_TPS_RELATIVE_DELTA,
                )

    def test_aggregation_uses_repetition_medians_and_total_error_rate(self) -> None:
        repetitions = [
            build_repetition_metrics(
                index=index,
                latencies_us=[latency * 1000] * 10,
                transactions_successful=10,
                transactions_failed=0,
                actual_measurement_seconds=10 / tps,
                artifacts=_artifact_set(),
                reported_tps=tps,
                reported_latency_mean_ms=latency,
                max_tps_relative_delta=MAX_TPS_RELATIVE_DELTA,
            )
            for index, tps, latency in ((1, 10, 1), (2, 100, 2), (3, 20, 3))
        ]
        metrics = aggregate_repetitions(repetitions)
        self.assertEqual(metrics["tps"], 20.0)
        self.assertEqual(metrics["latency_ms"]["p95"], 2.0)
        self.assertEqual(metrics["errors"]["rate"], 0.0)

    def test_validator_rejects_missing_nan_zero_and_percentile_inversion(self) -> None:
        valid = _result("baseline")
        missing = copy.deepcopy(valid)
        del missing["metrics"]["latency_ms"]["p99"]
        with self.assertRaises(BenchmarkResultError):
            validate_benchmark_result(missing)

        non_finite = copy.deepcopy(valid)
        non_finite["metrics"]["tps"] = math.nan
        with self.assertRaisesRegex(BenchmarkResultError, "finite"):
            validate_benchmark_result(non_finite)

        zero = copy.deepcopy(valid)
        zero["repetitions"][0]["transactions"]["successful"] = 0
        with self.assertRaises(BenchmarkResultError):
            validate_benchmark_result(zero)

        inverted = copy.deepcopy(valid)
        inverted["repetitions"][0]["latency_ms"]["p50"] = 99
        with self.assertRaisesRegex(BenchmarkResultError, "p50 <= p95 <= p99"):
            validate_benchmark_result(inverted)

    def test_validator_rejects_oversized_integers_without_arithmetic_leaks(self) -> None:
        huge = 10**10000

        oversized_config = _result("baseline")
        oversized_config["config"]["clients"] = huge
        with self.assertRaisesRegex(BenchmarkResultError, "must be <="):
            validate_benchmark_result(oversized_config)

        oversized_transactions = _result("candidate")
        repetition = oversized_transactions["repetitions"][0]
        repetition["transactions"]["successful"] = huge
        repetition["transactions"]["total"] = huge
        repetition["errors"]["total"] = huge
        with self.assertRaisesRegex(BenchmarkResultError, "must be <="):
            validate_benchmark_result(oversized_transactions)

    def test_aggregate_rejects_signed_64_bit_overflow(self) -> None:
        repetitions = []
        for index in (1, 2):
            repetition = copy.deepcopy(_result("baseline")["repetitions"][0])
            repetition["index"] = index
            repetition["actual_measurement_seconds"] = 1.0
            repetition["transactions"] = {
                "successful": 1,
                "total": MAX_INTEGER,
            }
            repetition["tps"] = 1.0
            repetition["pgbench_reported"]["tps"] = 1.0
            repetition["tps_consistency"] = {
                "computed": 1.0,
                "reported": 1.0,
                "relative_delta": 0.0,
                "tolerance": MAX_TPS_RELATIVE_DELTA,
                "passed": True,
            }
            repetition["errors"] = {
                "count": MAX_INTEGER - 1,
                "total": MAX_INTEGER,
                "rate": (MAX_INTEGER - 1) / MAX_INTEGER,
                "types": {"failed": MAX_INTEGER - 1},
            }
            repetitions.append(repetition)

        with self.assertRaisesRegex(
            BenchmarkResultError, "aggregate errors.count must be <="
        ):
            aggregate_repetitions(repetitions)

    def test_strict_json_writer_rejects_nan(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(BenchmarkResultError):
                write_json_strict(Path(tmp) / "bad.json", {"value": math.nan})

    def test_versioned_json_schema_validates_instances_strictly(self) -> None:
        schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
        self.assertEqual(schema["title"], "DriftBench BenchmarkRunResult v1")
        self.assertIn("metrics", schema["required"])
        Draft202012Validator.check_schema(schema)
        validator = Draft202012Validator(schema)
        validator.validate(_result("baseline"))
        validator.validate(_result("candidate"))

        integer_schemas = []

        def collect_integer_schemas(value) -> None:
            if isinstance(value, dict):
                if value.get("type") == "integer":
                    integer_schemas.append(value)
                for nested in value.values():
                    collect_integer_schemas(nested)
            elif isinstance(value, list):
                for nested in value:
                    collect_integer_schemas(nested)

        collect_integer_schemas(schema)
        self.assertTrue(integer_schemas)
        self.assertTrue(
            all(item.get("maximum") == MAX_INTEGER for item in integer_schemas)
        )

        missing_digest = _result("candidate")
        del missing_digest["workload"]["script_sha256"]
        with self.assertRaises(ValidationError):
            validator.validate(missing_digest)
        with self.assertRaises(BenchmarkResultError):
            validate_benchmark_result(missing_digest)

        wrong_role_source = _result("baseline")
        wrong_role_source["workload"]["source"] = "driftbench_script"
        wrong_role_source["workload"]["script_sha256"] = "a" * 64
        with self.assertRaises(ValidationError):
            validator.validate(wrong_role_source)
        with self.assertRaises(BenchmarkResultError):
            validate_benchmark_result(wrong_role_source)

        unknown_field = _result("baseline")
        unknown_field["unexpected"] = True
        with self.assertRaises(ValidationError):
            validator.validate(unknown_field)
        with self.assertRaisesRegex(BenchmarkResultError, "unsupported"):
            validate_benchmark_result(unknown_field)

        oversized_integer = _result("baseline")
        oversized_integer["config"]["clients"] = MAX_INTEGER + 1
        with self.assertRaises(ValidationError):
            validator.validate(oversized_integer)
        with self.assertRaises(BenchmarkResultError):
            validate_benchmark_result(oversized_integer)


class RegressionPolicyTests(unittest.TestCase):
    def setUp(self) -> None:
        self.policy = load_pgbench_policy(POLICY_PATH)

    def test_shipped_policy_is_the_approved_ci_policy(self) -> None:
        self.assertEqual(self.policy.postgresql_major, 16)
        self.assertEqual(self.policy.pgbench_major, 16)
        self.assertEqual(self.policy.scale_factor, 1)
        self.assertEqual(self.policy.clients, 2)
        self.assertGreaterEqual(self.policy.warmup_seconds, 3)
        self.assertGreaterEqual(self.policy.measurement_seconds, 5)
        self.assertEqual(self.policy.repetitions, 3)
        self.assertEqual(self.policy.min_tps_ratio, 0.7)
        self.assertEqual(self.policy.max_p95_latency_ratio, 1.5)
        self.assertEqual(self.policy.max_error_rate, 0.0)
        self.assertEqual(self.policy.max_tps_relative_delta, 0.05)

    def test_policy_rejects_missing_and_non_finite_thresholds(self) -> None:
        raw = json.loads(POLICY_PATH.read_text(encoding="utf-8"))
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "policy.json"
            missing = copy.deepcopy(raw)
            del missing["thresholds"]["min_tps_ratio"]
            path.write_text(json.dumps(missing), encoding="utf-8")
            with self.assertRaises(BenchmarkPolicyError):
                load_pgbench_policy(path)
            invalid = copy.deepcopy(raw)
            invalid["thresholds"]["min_tps_ratio"] = math.nan
            path.write_text(json.dumps(invalid), encoding="utf-8")
            with self.assertRaises(BenchmarkPolicyError):
                load_pgbench_policy(path)

    def test_policy_rejects_integer_above_signed_64_bit(self) -> None:
        raw = json.loads(POLICY_PATH.read_text(encoding="utf-8"))
        raw["config"]["clients"] = MAX_INTEGER + 1
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "policy.json"
            path.write_text(json.dumps(raw), encoding="utf-8")
            with self.assertRaisesRegex(BenchmarkPolicyError, "must be <="):
                load_pgbench_policy(path)

    def test_policy_rejects_values_above_operational_limits(self) -> None:
        cases = {
            "clients": MAX_PGBENCH_CLIENTS + 1,
            "jobs": MAX_PGBENCH_JOBS + 1,
            "warmup_seconds": MAX_PGBENCH_PHASE_SECONDS + 1,
            "measurement_seconds": MAX_PGBENCH_PHASE_SECONDS + 1,
            "repetitions": MAX_PGBENCH_REPETITIONS + 1,
        }
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "policy.json"
            for field, value in cases.items():
                with self.subTest(field=field):
                    raw = json.loads(POLICY_PATH.read_text(encoding="utf-8"))
                    raw["config"][field] = value
                    path.write_text(json.dumps(raw), encoding="utf-8")
                    with self.assertRaisesRegex(
                        BenchmarkPolicyError, f"policy.config.{field} must be <="
                    ):
                        load_pgbench_policy(path)

    def test_policy_rejects_jobs_above_clients_and_total_time_budget(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "policy.json"
            raw = json.loads(POLICY_PATH.read_text(encoding="utf-8"))
            raw["config"].update({"clients": 2, "jobs": 3})
            path.write_text(json.dumps(raw), encoding="utf-8")
            with self.assertRaisesRegex(BenchmarkPolicyError, "jobs must be <="):
                load_pgbench_policy(path)

            raw = json.loads(POLICY_PATH.read_text(encoding="utf-8"))
            raw["config"].update(
                {
                    "warmup_seconds": MAX_PGBENCH_PHASE_SECONDS,
                    "measurement_seconds": MAX_PGBENCH_PHASE_SECONDS,
                    "repetitions": MAX_PGBENCH_REPETITIONS,
                }
            )
            path.write_text(json.dumps(raw), encoding="utf-8")
            with self.assertRaisesRegex(
                BenchmarkPolicyError,
                f"planned phase time must be <= {MAX_PGBENCH_PLANNED_SECONDS}",
            ):
                load_pgbench_policy(path)

    def test_execution_plan_revalidates_directly_constructed_policy(self) -> None:
        unsafe = replace(
            self.policy, repetitions=MAX_PGBENCH_REPETITIONS + 1
        )
        with self.assertRaisesRegex(BenchmarkPolicyError, "repetitions must be <="):
            build_paired_execution_plan(unsafe)

    def test_policy_rejects_unknown_keys_at_every_object_level(self) -> None:
        raw = json.loads(POLICY_PATH.read_text(encoding="utf-8"))
        cases = (
            (raw, "policy"),
            (raw["versions"], "policy.versions"),
            (raw["config"], "policy.config"),
            (raw["thresholds"], "policy.thresholds"),
        )
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "policy.json"
            for target, field in cases:
                with self.subTest(field=field):
                    target["zz_typo"] = True
                    target["aa_typo"] = True
                    path.write_text(json.dumps(raw), encoding="utf-8")
                    with self.assertRaises(BenchmarkPolicyError) as raised:
                        load_pgbench_policy(path)
                    self.assertEqual(
                        str(raised.exception),
                        f"{field} contains unsupported field(s): aa_typo, zz_typo",
                    )
                    del target["aa_typo"]
                    del target["zz_typo"]

    def test_thresholds_are_inclusive_at_boundary(self) -> None:
        baseline = _result("baseline", tps=100.0, latency_ms=10.0)
        candidate = _result("candidate", tps=70.0, latency_ms=15.0)
        decision = evaluate_regression(baseline, candidate, self.policy)
        self.assertTrue(decision["ok"], decision["reasons"])

    def test_tps_and_latency_just_outside_boundary_fail(self) -> None:
        baseline = _result("baseline", tps=100.0, latency_ms=10.0)
        low_tps = _result("candidate", tps=69.999, latency_ms=15.0)
        decision = evaluate_regression(baseline, low_tps, self.policy)
        self.assertFalse(decision["ok"])
        self.assertTrue(any("TPS" in reason for reason in decision["reasons"]))

        high_latency = _result("candidate", tps=70.0, latency_ms=15.001)
        decision = evaluate_regression(baseline, high_latency, self.policy)
        self.assertFalse(decision["ok"])
        self.assertTrue(any("p95" in reason for reason in decision["reasons"]))

    def test_nonzero_errors_and_incompatible_provenance_fail(self) -> None:
        baseline = _result("baseline")
        candidate = _result("candidate", failed=1)
        decision = evaluate_regression(baseline, candidate, self.policy)
        self.assertFalse(decision["ok"])
        self.assertTrue(any("error rate" in reason for reason in decision["reasons"]))

        candidate = _result("candidate")
        candidate["versions"]["postgresql"]["major"] = 15
        decision = evaluate_regression(baseline, candidate, self.policy)
        self.assertFalse(decision["ok"])
        self.assertTrue(any("PostgreSQL".lower() in reason.lower() for reason in decision["reasons"]))

    def test_full_version_mismatch_fails_compatibility(self) -> None:
        baseline = _result("baseline")
        for product, different_version in (
            ("postgresql", "PostgreSQL 16.10"),
            ("pgbench", "pgbench (PostgreSQL) 16.10"),
        ):
            with self.subTest(product=product):
                candidate = _result("candidate")
                candidate["versions"][product]["full"] = different_version
                decision = evaluate_regression(baseline, candidate, self.policy)
                self.assertFalse(decision["ok"])
                self.assertFalse(decision["compatibility"]["ok"])
                self.assertIn(
                    f"baseline/candidate {product} full version mismatch",
                    decision["compatibility"]["reasons"],
                )

    def test_git_sha_mismatch_fails_compatibility(self) -> None:
        baseline = _result("baseline")
        candidate = _result("candidate")
        candidate["git_sha"] = "f" * 40
        decision = evaluate_regression(baseline, candidate, self.policy)
        self.assertFalse(decision["ok"])
        self.assertFalse(decision["compatibility"]["ok"])
        self.assertIn(
            "baseline/candidate git_sha mismatch",
            decision["compatibility"]["reasons"],
        )

    def test_invalid_metrics_return_fail_closed_decision(self) -> None:
        baseline = _result("baseline")
        candidate = _result("candidate")
        candidate["metrics"]["tps"] = math.inf
        decision = evaluate_regression(baseline, candidate, self.policy)
        self.assertFalse(decision["ok"])
        self.assertTrue(any("invalid" in reason for reason in decision["reasons"]))
        self.assertFalse(decision["compatibility"]["ok"])
        self.assertTrue(decision["compatibility"]["reasons"])

        candidate = _result("candidate")
        candidate["metrics"]["tps"] = 10**10000
        decision = evaluate_regression(baseline, candidate, self.policy)
        self.assertFalse(decision["ok"])
        self.assertTrue(any("finite" in reason for reason in decision["reasons"]))

        candidate = _result("candidate")
        huge = 10**10000
        repetition = candidate["repetitions"][0]
        repetition["transactions"]["successful"] = huge
        repetition["transactions"]["total"] = huge
        repetition["errors"]["total"] = huge
        decision = evaluate_regression(baseline, candidate, self.policy)
        self.assertFalse(decision["ok"])
        self.assertTrue(any("invalid" in reason for reason in decision["reasons"]))

    def test_aggregate_error_types_must_match_repetitions(self) -> None:
        result = _result("candidate", failed=1)
        self.assertEqual(result["metrics"]["errors"]["types"], {"unspecified": 3})
        result["metrics"]["errors"]["types"] = {"fabricated": 3}
        with self.assertRaisesRegex(BenchmarkResultError, "aggregate errors"):
            validate_benchmark_result(result)

        decision = evaluate_regression(_result("baseline"), result, self.policy)
        self.assertFalse(decision["ok"])
        self.assertTrue(any("invalid" in reason for reason in decision["reasons"]))

    def test_execution_plan_is_deterministic_ab_ba_ab(self) -> None:
        plan = build_paired_execution_plan(self.policy)
        self.assertEqual(
            [pair["order"] for pair in plan],
            [
                ["baseline", "candidate"],
                ["candidate", "baseline"],
                ["baseline", "candidate"],
            ],
        )
        self.assertTrue(
            all(pair["phases_per_role"] == ["warmup", "measurement"] for pair in plan)
        )


if __name__ == "__main__":
    unittest.main()
