import tempfile
import unittest
from pathlib import Path

from driftbench.data.pgbench import PgBenchData, PgBenchQueries
from ..helpers import BenchmarkAdapterTestMixin


class PgBenchAdapterTests(BenchmarkAdapterTestMixin, unittest.TestCase):
    def test_pgbench_data_synth_filesystem_contract(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "out"
            result = PgBenchData(scale_factor=1).generate(output_dir=out)
            self._assert_result_is_filesystem_contract(result, out)
            self.assertEqual(result.benchmark, "pgbench")

    def test_pgbench_data_synth_four_tables(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "out"
            result = PgBenchData(scale_factor=1).generate(output_dir=out)
            csv_names = {p.stem for p in result.files if p.suffix == ".csv"}
            self.assertIn("pgbench_branches", csv_names)
            self.assertIn("pgbench_tellers", csv_names)
            self.assertIn("pgbench_accounts", csv_names)
            self.assertIn("pgbench_history", csv_names)

    def test_pgbench_data_synth_row_counts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "out"
            result = PgBenchData(scale_factor=1).generate(output_dir=out)

            def row_count(name):
                f = next(p for p in result.files if p.stem == name)
                return len(f.read_text(encoding="utf-8").splitlines()) - 1

            self.assertEqual(row_count("pgbench_branches"), 1)
            self.assertEqual(row_count("pgbench_tellers"), 10)
            self.assertEqual(row_count("pgbench_accounts"), 100_000)
            self.assertEqual(row_count("pgbench_history"), 0)

    def test_pgbench_queries_all_workloads(self) -> None:
        for workload in ("tpcb", "simple_update", "select_only"):
            with tempfile.TemporaryDirectory() as tmp:
                out = Path(tmp) / "out"
                result = PgBenchQueries(workload=workload).generate(output_dir=out)
                self._assert_result_is_filesystem_contract(result, out)
                sql_file = out / "pgbench" / "queries" / f"pgbench_{workload}.sql"
                self.assertTrue(sql_file.exists(), f"Missing SQL for workload={workload}")
