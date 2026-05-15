import os
import shutil
import tempfile
import unittest
from pathlib import Path
import json

from driftbench.data import GenerationResult, OutputDirRequiredError
from driftbench.data.dsb import DSBData, DSBQueries
from driftbench.data.tpch import TPCHData, TPCHQueries
from driftbench.data.tpcds import TPCDSData, TPCDSQueries
from driftbench.data.ycsb import YCSBData, YCSBQueries
from driftbench.data.tpcc import TPCCData, TPCCQueries
from driftbench.data.tpcc_skew import TPCCSkewData, TPCCSkewQueries
from driftbench.data.job import JOBData, JOBQueries
from driftbench.data.pgbench import PgBenchData, PgBenchQueries
from driftbench.data.benchbase import BenchBaseData, BenchBaseQueries


class BenchmarkAdapterTests(unittest.TestCase):
    def setUp(self) -> None:
        # Redirect the default data dir to a temp dir so tests never write to
        # ~/.driftbench/data/ and each test starts with a clean slate.
        self._default_data_tmpdir = tempfile.mkdtemp(prefix="driftbench_test_")
        os.environ["DRIFTBENCH_DATA_DIR"] = self._default_data_tmpdir

    def tearDown(self) -> None:
        del os.environ["DRIFTBENCH_DATA_DIR"]
        shutil.rmtree(self._default_data_tmpdir, ignore_errors=True)

    def _assert_result_is_filesystem_contract(
        self,
        result: GenerationResult,
        output_root: Path,
    ) -> None:
        self.assertIsInstance(result, GenerationResult)
        self.assertIsInstance(result.output_dir, Path)
        self.assertTrue(result.output_dir.exists())
        self.assertEqual(result.output_dir.resolve(), output_root.resolve())
        self.assertTrue(result.metadata.exists())
        self.assertGreater(result.metadata.stat().st_size, 0)
        payload = json.loads(result.metadata.read_text(encoding="utf-8"))
        if "files" in payload:
            for rel in payload["files"]:
                self.assertFalse(str(rel).startswith("/"))
        self.assertGreaterEqual(len(result.files), 1)
        for path in result.files:
            self.assertIsInstance(path, Path)
            self.assertTrue(path.exists())
            self.assertTrue(path.is_file())
            self.assertGreater(path.stat().st_size, 0)
            self.assertTrue(str(path).startswith(str(output_root.resolve())))

    def test_output_dir_defaults_when_none(self) -> None:
        # setUp redirects DRIFTBENCH_DATA_DIR to a temp dir, so generate(output_dir=None)
        # writes there instead of ~/.driftbench/data/.
        result = YCSBData(scale_factor=1).generate(output_dir=None)
        self.assertIsInstance(result, GenerationResult)
        self.assertTrue(result.output_dir.exists())
        self.assertTrue(result.output_dir.resolve().is_relative_to(Path(self._default_data_tmpdir).resolve()))

    def test_generate_reuses_existing_data_without_force(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "out"
            adapter = YCSBData(scale_factor=1)
            r1 = adapter.generate(output_dir=out)
            # Mutate a generated file so we can detect if it gets regenerated
            sentinel = r1.files[0]
            sentinel.write_text("SENTINEL", encoding="utf-8")
            # Second call without force= should reuse and NOT overwrite
            r2 = adapter.generate(output_dir=out)
            self.assertEqual(sentinel.read_text(encoding="utf-8"), "SENTINEL")
            self.assertEqual(r1.files, r2.files)

    def test_generate_force_regenerates_existing_data(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "out"
            adapter = YCSBData(scale_factor=1)
            r1 = adapter.generate(output_dir=out)
            sentinel = r1.files[0]
            original_content = sentinel.read_text(encoding="utf-8")
            sentinel.write_text("SENTINEL", encoding="utf-8")
            # force=True should overwrite
            adapter.generate(output_dir=out, force=True)
            self.assertEqual(sentinel.read_text(encoding="utf-8"), original_content)

    def test_tpch_data_generate_with_explicit_source_dir(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            source = tmp_path / "src"
            source.mkdir(parents=True, exist_ok=True)
            (source / "customer.tbl").write_text("1|customer|\n", encoding="utf-8")

            out = tmp_path / "out"
            result = TPCHData(scale_factor=1, source_dir=source).generate(output_dir=out)

            self._assert_result_is_filesystem_contract(result, out)
            self.assertTrue(any(path.name == "customer.tbl" for path in result.files))

    def test_tpch_queries_generate_qgen_mode(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            out = tmp_path / "out"
            result = TPCHQueries(query_ids=[1, 3], queries_per_template=2, mode="qgen").generate(
                output_dir=out
            )
            self._assert_result_is_filesystem_contract(result, out)
            self.assertEqual(len(result.files), 2)
            self.assertTrue((out / "tpch" / "queries" / "tpch_queries.sql").exists())
            self.assertTrue((out / "tpch" / "queries" / "tpch_queries.csv").exists())

    def test_tpch_data_plan_mode_for_remote_server(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            out = tmp_path / "out"
            result = TPCHData(scale_factor=1000, mode="plan").generate(output_dir=out)
            self._assert_result_is_filesystem_contract(result, out)
            script = out / "tpch" / "data" / "sf_1000" / "generate_tpch_data.sh"
            self.assertTrue(script.exists())
            self.assertEqual(len(result.files), 1)
            script_text = script.read_text(encoding="utf-8")
            self.assertIn("SCALE_FACTOR=1000", script_text)
            self.assertIn("dbgen", script_text)
            # Plan mode should not materialize table data on local machine.
            self.assertFalse((out / "tpch" / "data" / "sf_1000" / "tables").exists())

    def test_other_benchmarks_generate(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            out = tmp_path / "artifacts"

            ycsb_data = YCSBData(scale_factor=2).generate(output_dir=out)
            ycsb_queries = YCSBQueries(workload="B").generate(output_dir=out)
            tpcds_data = TPCDSData(scale_factor=3).generate(output_dir=out)
            tpcds_queries = TPCDSQueries().generate(output_dir=out)
            dsb_data = DSBData(scale_factor=2).generate(output_dir=out)
            dsb_queries = DSBQueries().generate(output_dir=out)

            for result in [
                ycsb_data,
                ycsb_queries,
                tpcds_data,
                tpcds_queries,
                dsb_data,
                dsb_queries,
            ]:
                self._assert_result_is_filesystem_contract(result, out)

    # ------------------------------------------------------------------
    # TPC-C tests
    # ------------------------------------------------------------------

    def test_tpcc_data_synth_filesystem_contract(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "out"
            result = TPCCData(scale_factor=1, mode="synth").generate(output_dir=out)
            self._assert_result_is_filesystem_contract(result, out)
            self.assertEqual(result.benchmark, "tpcc")
            self.assertEqual(result.artifact_type, "data")

    def test_tpcc_data_synth_produces_nine_csv_tables(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "out"
            result = TPCCData(scale_factor=1, mode="synth").generate(output_dir=out)
            expected_tables = {
                "warehouse", "district", "customer", "item", "stock",
                "orders", "new_order", "order_line", "history",
            }
            csv_names = {p.stem for p in result.files if p.suffix == ".csv"}
            self.assertTrue(expected_tables.issubset(csv_names),
                            f"Missing tables: {expected_tables - csv_names}")

    def test_tpcc_data_synth_row_counts_scale_with_sf(self) -> None:
        with tempfile.TemporaryDirectory() as tmp1, tempfile.TemporaryDirectory() as tmp2:
            out1 = Path(tmp1) / "out"
            out2 = Path(tmp2) / "out"
            r1 = TPCCData(scale_factor=1, mode="synth").generate(output_dir=out1)
            r2 = TPCCData(scale_factor=2, mode="synth").generate(output_dir=out2)

            def row_count(result, name):
                f = next(p for p in result.files if p.stem == name)
                lines = f.read_text(encoding="utf-8").splitlines()
                return len(lines) - 1  # subtract header

            # warehouse grows linearly with W
            self.assertEqual(row_count(r1, "warehouse"), 1)
            self.assertEqual(row_count(r2, "warehouse"), 2)
            # district: 10 per warehouse
            self.assertEqual(row_count(r1, "district"), 10)
            self.assertEqual(row_count(r2, "district"), 20)
            # customer: 3000 per warehouse
            self.assertEqual(row_count(r1, "customer"), 3000)
            self.assertEqual(row_count(r2, "customer"), 6000)

    def test_tpcc_data_plan_mode(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "out"
            result = TPCCData(scale_factor=5, mode="plan").generate(output_dir=out)
            self._assert_result_is_filesystem_contract(result, out)
            script = out / "tpcc" / "data" / "generate_tpcc_data.sh"
            self.assertTrue(script.exists())
            self.assertIn("WAREHOUSES=5", script.read_text(encoding="utf-8"))

    def test_tpcc_queries_generate(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "out"
            result = TPCCQueries().generate(output_dir=out)
            self._assert_result_is_filesystem_contract(result, out)
            expected_txns = {"new_order", "payment", "order_status", "delivery", "stock_level"}
            sql_names = {p.stem for p in result.files if p.suffix == ".sql"}
            self.assertTrue(expected_txns.issubset(sql_names))
            bundle = out / "tpcc" / "queries" / "tpcc_all_transactions.sql"
            self.assertTrue(bundle.exists())

    # ------------------------------------------------------------------
    # TPC-C Skew tests
    # ------------------------------------------------------------------

    def test_tpcc_skew_data_synth_filesystem_contract(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "out"
            result = TPCCSkewData(
                scale_factor=1, hot_warehouse_fraction=0.2, skew_factor=0.99
            ).generate(output_dir=out)
            self._assert_result_is_filesystem_contract(result, out)
            self.assertEqual(result.benchmark, "tpcc_skew")

    def test_tpcc_skew_data_weight_manifest_exists(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "out"
            result = TPCCSkewData(scale_factor=4, hot_warehouse_fraction=0.25).generate(output_dir=out)
            wts = out / "tpcc_skew" / "data" / "warehouse_access_weights.csv"
            self.assertTrue(wts.exists())
            lines = wts.read_text(encoding="utf-8").splitlines()
            # header + 4 warehouse rows
            self.assertEqual(len(lines), 5)
            # check is_hot column: 1 hot warehouse out of 4 (ceil(4 * 0.25) = 1)
            hot_rows = [l for l in lines[1:] if l.endswith(",1")]
            self.assertEqual(len(hot_rows), 1)

    def test_tpcc_skew_weight_sum_is_one(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "out"
            TPCCSkewData(scale_factor=5, skew_factor=1.0).generate(output_dir=out)
            wts = out / "tpcc_skew" / "data" / "warehouse_access_weights.csv"
            import csv as csv_mod
            with wts.open(encoding="utf-8") as f:
                reader = csv_mod.DictReader(f)
                total = sum(float(row["access_probability"]) for row in reader)
            self.assertAlmostEqual(total, 1.0, places=5)

    def test_tpcc_skew_queries_generate(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "out"
            result = TPCCSkewQueries(scale_factor=4, hot_warehouse_fraction=0.25).generate(output_dir=out)
            self._assert_result_is_filesystem_contract(result, out)
            bundle = out / "tpcc_skew" / "queries" / "tpcc_skew_all_transactions.sql"
            self.assertTrue(bundle.exists())
            content = bundle.read_text(encoding="utf-8")
            self.assertIn("Zipf alpha=", content)
            self.assertIn("warehouse_access_weights.csv", content)

    # ------------------------------------------------------------------
    # JOB tests
    # ------------------------------------------------------------------

    def test_job_data_synth_filesystem_contract(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "out"
            result = JOBData(scale_factor=1, mode="synth").generate(output_dir=out)
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

    def test_job_data_plan_mode(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "out"
            result = JOBData(scale_factor=1, mode="plan").generate(output_dir=out)
            self._assert_result_is_filesystem_contract(result, out)
            script = out / "job" / "data" / "download_imdb.sh"
            self.assertTrue(script.exists())
            self.assertIn("imdb.tgz", script.read_text(encoding="utf-8"))

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

    # ------------------------------------------------------------------
    # pgbench tests
    # ------------------------------------------------------------------

    def test_pgbench_data_synth_filesystem_contract(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "out"
            result = PgBenchData(scale_factor=1, mode="synth").generate(output_dir=out)
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

    def test_pgbench_data_plan_mode(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "out"
            result = PgBenchData(scale_factor=2, mode="plan").generate(output_dir=out)
            self._assert_result_is_filesystem_contract(result, out)
            script = out / "pgbench" / "data" / "init_pgbench.sh"
            self.assertTrue(script.exists())
            self.assertIn("SCALE_FACTOR=2", script.read_text(encoding="utf-8"))

    def test_pgbench_queries_all_workloads(self) -> None:
        for workload in ("tpcb", "simple_update", "select_only"):
            with tempfile.TemporaryDirectory() as tmp:
                out = Path(tmp) / "out"
                result = PgBenchQueries(workload=workload).generate(output_dir=out)
                self._assert_result_is_filesystem_contract(result, out)
                sql_file = out / "pgbench" / "queries" / f"pgbench_{workload}.sql"
                self.assertTrue(sql_file.exists(), f"Missing SQL for workload={workload}")

    # ------------------------------------------------------------------
    # BenchBase tests
    # ------------------------------------------------------------------

    def test_benchbase_data_tpcc_filesystem_contract(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "out"
            result = BenchBaseData(benchmark_name="tpcc", scale_factor=1).generate(output_dir=out)
            self._assert_result_is_filesystem_contract(result, out)
            self.assertEqual(result.benchmark, "benchbase")

    def test_benchbase_data_generates_xml_and_script(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "out"
            result = BenchBaseData(benchmark_name="tpcc").generate(output_dir=out)
            xml = out / "benchbase" / "tpcc" / "data" / "tpcc_load_config.xml"
            script = out / "benchbase" / "tpcc" / "data" / "load.sh"
            self.assertTrue(xml.exists())
            self.assertTrue(script.exists())
            content = xml.read_text(encoding="utf-8")
            self.assertIn("<scalefactor>", content)
            self.assertIn("NewOrder", content)
            self.assertIn("load=true", content)
            self.assertIn("execute=false", content)

    def test_benchbase_queries_generates_execute_config(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "out"
            result = BenchBaseQueries(
                benchmark_name="smallbank", terminals=8, duration=120
            ).generate(output_dir=out)
            self._assert_result_is_filesystem_contract(result, out)
            xml = out / "benchbase" / "smallbank" / "queries" / "smallbank_execute_config.xml"
            self.assertTrue(xml.exists())
            content = xml.read_text(encoding="utf-8")
            self.assertIn("<terminals>8</terminals>", content)
            self.assertIn("<time>120</time>", content)
            self.assertIn("execute=true", content)
            self.assertIn("load=false", content)

    def test_benchbase_supported_benchmarks_all_generate(self) -> None:
        benchmarks = ["tpcc", "ycsb", "seats", "auctionmark", "smallbank",
                      "epinions", "wikipedia", "twitter", "voter"]
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "out"
            for bm in benchmarks:
                result = BenchBaseData(benchmark_name=bm).generate(output_dir=out)
                self.assertIsInstance(result, GenerationResult, f"Failed for benchmark={bm}")

    def test_benchbase_invalid_benchmark_raises(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "out"
            with self.assertRaises(ValueError):
                BenchBaseData(benchmark_name="nonexistent").generate(output_dir=out)


if __name__ == "__main__":
    unittest.main()
