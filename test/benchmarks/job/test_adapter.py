import json
import tempfile
import unittest
from pathlib import Path

from driftbench.data.job import JOBData, JOBQueries
from ..helpers import BenchmarkAdapterTestMixin


class JOBAdapterTests(BenchmarkAdapterTestMixin, unittest.TestCase):
    def test_job_data_synth_filesystem_contract(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "out"
            result = JOBData(scale_factor=1).generate(output_dir=out)
            self._assert_result_is_filesystem_contract(result, out)
            self.assertEqual(result.benchmark, "job")
            self.assertEqual(result.artifact_type, "data")

    def test_job_data_synth_produces_expected_tables(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "out"
            result = JOBData(scale_factor=1).generate(output_dir=out)
            expected_tables = {
                "title", "name", "cast_info", "movie_info",
                "keyword", "movie_keyword", "company_name", "movie_companies",
            }
            csv_names = {p.stem for p in result.files if p.suffix == ".csv"}
            self.assertTrue(expected_tables.issubset(csv_names),
                            f"Missing tables: {expected_tables - csv_names}")

    def test_job_data_synth_row_counts_scale_with_sf(self) -> None:
        with tempfile.TemporaryDirectory() as tmp1, tempfile.TemporaryDirectory() as tmp2:
            out1 = Path(tmp1) / "out"
            out2 = Path(tmp2) / "out"
            r1 = JOBData(scale_factor=1).generate(output_dir=out1)
            r2 = JOBData(scale_factor=2).generate(output_dir=out2)

            def row_count(result, name):
                f = next(p for p in result.files if p.stem == name)
                return len(f.read_text(encoding="utf-8").splitlines()) - 1

            self.assertEqual(row_count(r1, "title"), 500)
            self.assertEqual(row_count(r2, "title"), 1000)
            self.assertEqual(row_count(r1, "cast_info"), 5000)
            self.assertEqual(row_count(r2, "cast_info"), 10000)

    def test_job_queries_generate(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "out"
            result = JOBQueries().generate(output_dir=out)
            self._assert_result_is_filesystem_contract(result, out)
            bundle = out / "job" / "queries" / "job_all_queries.sql"
            self.assertTrue(bundle.exists())
            # 20 individual query files + 1 bundle = 21 files
            sql_files = [p for p in result.files if p.suffix == ".sql"]
            self.assertEqual(len(sql_files), 21)

    def test_job_queries_manifest_has_join_complexity(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "out"
            JOBQueries().generate(output_dir=out)
            manifest = out / "job" / "queries" / "job_queries_manifest.json"
            payload = json.loads(manifest.read_text(encoding="utf-8"))
            self.assertIn("join_complexity", payload)
            self.assertIn("8_table", payload["join_complexity"])
