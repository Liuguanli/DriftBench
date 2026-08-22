import hashlib
import json
import os
import re
import shutil
import subprocess
import unittest
from pathlib import Path

from driftbench.benchmarking.metrics import (
    aggregate_repetitions,
    latency_metrics_from_microseconds,
    validate_benchmark_result,
)
from driftbench.benchmarking.pgbench import (
    parse_pgbench_stdout,
    parse_pgbench_transaction_log_summary,
)
from driftbench.benchmarking.policy import evaluate_regression, load_pgbench_policy


POSTGRESQL_SETTINGS = {
    "effective_cache_size",
    "fsync",
    "full_page_writes",
    "jit",
    "max_connections",
    "max_parallel_workers",
    "max_parallel_workers_per_gather",
    "random_page_cost",
    "shared_buffers",
    "synchronous_commit",
    "work_mem",
}


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


@unittest.skipUnless(
    os.environ.get("DRIFTBENCH_REQUIRE_PG_INTEGRATION") == "1",
    "set DRIFTBENCH_REQUIRE_PG_INTEGRATION=1 in the real PostgreSQL gate",
)
class RealPgBenchIntegrationTests(unittest.TestCase):
    """No-mock assertions run only by benchmark-regression-pgbench."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.pgbench = shutil.which(os.environ.get("PGBENCH_BINARY", "pgbench"))
        if not cls.pgbench:
            raise AssertionError("pgbench is required by the integration gate")
        artifact_value = os.environ.get("DRIFTBENCH_PGBENCH_ARTIFACT_DIR")
        if not artifact_value:
            raise AssertionError("DRIFTBENCH_PGBENCH_ARTIFACT_DIR is required")
        cls.artifact_root = Path(artifact_value).expanduser().resolve()
        if not cls.artifact_root.is_dir():
            raise AssertionError(f"artifact directory does not exist: {cls.artifact_root}")

    def _load(self, name: str) -> dict:
        path = self.artifact_root / name
        self.assertTrue(path.is_file(), path)
        return json.loads(path.read_text(encoding="utf-8"))

    def _assert_descriptor(self, descriptor: dict) -> Path:
        path = self.artifact_root / descriptor["path"]
        self.assertTrue(path.is_file(), path)
        self.assertEqual(path.stat().st_size, descriptor["bytes"])
        self.assertEqual(_sha256(path), descriptor["sha256"])
        return path

    def test_client_and_server_are_postgresql_16(self) -> None:
        client = subprocess.run(
            [self.pgbench, "--version"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout
        match = re.search(r"\b(\d+)\.", client)
        self.assertIsNotNone(match, client)
        self.assertEqual(int(match.group(1)), 16)

        import psycopg2

        with psycopg2.connect(
            dbname=os.environ["PGDATABASE"],
            host=os.environ.get("PGHOST", "localhost"),
            port=int(os.environ.get("PGPORT", "5432")),
            user=os.environ.get("PGUSER", "postgres"),
            connect_timeout=10,
        ) as database:
            with database.cursor() as cursor:
                cursor.execute("SELECT current_setting('server_version_num')")
                server_version_num = int(cursor.fetchone()[0])
        self.assertEqual(server_version_num // 10000, 16)

    def test_pgbench_initialization_has_scale_one_rows(self) -> None:
        import psycopg2

        expected = {
            "pgbench_branches": 1,
            "pgbench_tellers": 10,
            "pgbench_accounts": 100000,
            "pgbench_history": 0,
        }
        with psycopg2.connect(
            dbname=os.environ["PGDATABASE"],
            host=os.environ.get("PGHOST", "localhost"),
            port=int(os.environ.get("PGPORT", "5432")),
            user=os.environ.get("PGUSER", "postgres"),
            connect_timeout=10,
        ) as database:
            with database.cursor() as cursor:
                for table, count in expected.items():
                    cursor.execute(f"SELECT count(*) FROM {table}")
                    self.assertEqual(cursor.fetchone()[0], count, table)

    def test_results_are_recomputable_from_raw_logs(self) -> None:
        for role in ("baseline", "candidate"):
            result = self._load(f"{role}.json")
            validate_benchmark_result(result)
            self.assertEqual(len(result["warmups"]), 3)
            self.assertEqual(len(result["repetitions"]), 3)
            for phase in [*result["warmups"], *result["repetitions"]]:
                artifacts = phase["artifacts"]
                for descriptor in [
                    artifacts["stdout"],
                    artifacts["stderr"],
                    *artifacts["transaction_logs"],
                ]:
                    self._assert_descriptor(descriptor)
                logs = [
                    self.artifact_root / descriptor["path"]
                    for descriptor in artifacts["transaction_logs"]
                ]
                summary = parse_pgbench_transaction_log_summary(logs)
                if "transactions" in phase:
                    self.assertEqual(
                        summary.transactions_successful,
                        phase["transactions"]["successful"],
                    )
                    self.assertEqual(
                        summary.transactions_total,
                        phase["transactions"]["total"],
                    )
                    self.assertEqual(
                        summary.transactions_failed,
                        phase["errors"]["count"],
                    )
                    self.assertEqual(summary.failure_types, phase["errors"]["types"])
                    self.assertEqual(
                        latency_metrics_from_microseconds(summary.latencies_us),
                        phase["latency_ms"],
                    )
                    self.assertAlmostEqual(
                        phase["tps"],
                        summary.transactions_successful
                        / phase["actual_measurement_seconds"],
                    )
                    stdout_path = self.artifact_root / artifacts["stdout"]["path"]
                    parsed_stdout = parse_pgbench_stdout(
                        stdout_path.read_text(encoding="utf-8")
                    )
                    consistency = phase["tps_consistency"]
                    self.assertEqual(consistency["computed"], phase["tps"])
                    self.assertEqual(
                        consistency["reported"], parsed_stdout.reported_tps
                    )
                    self.assertEqual(consistency["tolerance"], 0.05)
                    self.assertTrue(consistency["passed"])
                    self.assertLessEqual(consistency["relative_delta"], 0.05)
                else:
                    self.assertEqual(
                        summary.transactions_successful,
                        phase["transactions_successful"],
                    )
                    self.assertEqual(
                        summary.transactions_failed,
                        phase["transactions_failed"],
                    )
            self.assertEqual(
                aggregate_repetitions(result["repetitions"]), result["metrics"]
            )

    def test_bundle_provenance_matches_real_environment(self) -> None:
        baseline = self._load("baseline.json")
        candidate = self._load("candidate.json")
        self.assertEqual(baseline["inputs"], candidate["inputs"])
        self.assertEqual(baseline["environment"], candidate["environment"])

        for result in (baseline, candidate):
            for descriptor in [*result["inputs"].values(), result["environment"]]:
                self._assert_descriptor(descriptor)

        policy_path = self.artifact_root / baseline["inputs"]["policy"]["path"]
        candidate_path = self.artifact_root / baseline["inputs"]["candidate_script"][
            "path"
        ]
        environment_path = self.artifact_root / baseline["environment"]["path"]
        self.assertEqual(policy_path.relative_to(self.artifact_root).as_posix(), "inputs/policy.json")
        self.assertEqual(
            candidate_path.relative_to(self.artifact_root).as_posix(),
            "inputs/candidate.sql",
        )
        self.assertEqual(
            environment_path.relative_to(self.artifact_root).as_posix(),
            "environment.json",
        )
        self.assertEqual(
            candidate["workload"]["script_sha256"],
            baseline["inputs"]["candidate_script"]["sha256"],
        )

        environment = json.loads(environment_path.read_text(encoding="utf-8"))
        self.assertEqual(environment["status"], "complete")
        self.assertNotIn("password", json.dumps(environment).lower())
        self.assertEqual(baseline["versions"], candidate["versions"])
        self.assertEqual(baseline["git_sha"], candidate["git_sha"])
        self.assertEqual(environment["driftbench"]["source_sha"], baseline["git_sha"])
        self.assertRegex(baseline["git_sha"], r"^[0-9a-f]{40}$")
        self.assertEqual(environment["driftbench"]["source_state"], "clean")
        self.assertEqual(
            environment["driftbench"]["source_sha_source"], "git_head"
        )
        source_override = os.environ.get("DRIFTBENCH_GIT_SHA")
        if source_override:
            self.assertEqual(environment["driftbench"]["source_sha"], source_override)

        postgresql = environment["postgresql"]
        self.assertEqual(
            {"full": postgresql["full"], "major": postgresql["major"]},
            baseline["versions"]["postgresql"],
        )
        self.assertEqual(environment["pgbench"], baseline["versions"]["pgbench"])
        self.assertEqual(postgresql["major"], 16)
        self.assertEqual(environment["pgbench"]["major"], 16)
        self.assertEqual(set(postgresql["settings"]), POSTGRESQL_SETTINGS)

        import psycopg2

        with psycopg2.connect(
            dbname=os.environ["PGDATABASE"],
            host=os.environ.get("PGHOST", "localhost"),
            port=int(os.environ.get("PGPORT", "5432")),
            user=os.environ.get("PGUSER", "postgres"),
            connect_timeout=10,
        ) as database:
            with database.cursor() as cursor:
                cursor.execute(
                    "SELECT version(), current_setting('server_version_num'), current_database()"
                )
                full, version_number, current_database = cursor.fetchone()
                cursor.execute(
                    "SELECT name, setting, unit, source "
                    "FROM pg_catalog.pg_settings "
                    "WHERE name = ANY(%s) ORDER BY name",
                    (sorted(POSTGRESQL_SETTINGS),),
                )
                settings = {
                    str(name): {
                        "setting": str(setting),
                        "unit": None if unit is None else str(unit),
                        "source": str(source),
                    }
                    for name, setting, unit, source in cursor.fetchall()
                }
                cursor.execute(
                    "SELECT "
                    "(SELECT count(*) FROM public.pgbench_branches), "
                    "(SELECT count(*) FROM public.pgbench_tellers), "
                    "(SELECT count(*) FROM public.pgbench_accounts), "
                    "(SELECT count(*) FROM public.pgbench_history)"
                )
                branches, tellers, accounts, history = cursor.fetchone()

        self.assertEqual(postgresql["full"], full)
        self.assertEqual(postgresql["major"], int(version_number) // 10000)
        self.assertEqual(postgresql["current_database"], current_database)
        self.assertEqual(postgresql["settings"], settings)
        self.assertEqual(
            postgresql["initialization"],
            {
                "pgbench_branches": branches,
                "pgbench_tellers": tellers,
                "pgbench_accounts": accounts,
                "pgbench_history": history,
                "scale_factor_inferred": branches,
            },
        )
        self.assertEqual(postgresql["initialization"]["scale_factor_inferred"], 1)

    def test_decision_and_execution_order_pass(self) -> None:
        baseline = self._load("baseline.json")
        candidate = self._load("candidate.json")
        decision = self._load("decision.json")
        self.assertTrue(decision["ok"], decision.get("reasons"))
        policy_path = self.artifact_root / baseline["inputs"]["policy"]["path"]
        self.assertTrue(policy_path.is_file(), policy_path)
        recomputed = evaluate_regression(
            baseline,
            candidate,
            load_pgbench_policy(policy_path),
            baseline_path="baseline.json",
            candidate_path="candidate.json",
        )
        self.assertEqual(recomputed, decision)

        order = self._load("execution_order.json")
        self.assertEqual(
            [pair["order"] for pair in order["pairs"]],
            [
                ["baseline", "candidate"],
                ["candidate", "baseline"],
                ["baseline", "candidate"],
            ],
        )
        self.assertEqual(len(order["completed"]), 12)


if __name__ == "__main__":
    unittest.main()
