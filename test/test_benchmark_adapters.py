import tempfile
import unittest
from pathlib import Path
import json

from driftbench.data import GenerationResult, OutputDirRequiredError
from driftbench.data.dsb import DSBData, DSBQueries
from driftbench.data.tpch import TPCHData, TPCHQueries
from driftbench.data.tpcds import TPCDSData, TPCDSQueries
from driftbench.data.ycsb import YCSBData, YCSBQueries


class BenchmarkAdapterTests(unittest.TestCase):
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

    def test_output_dir_is_required(self) -> None:
        with self.assertRaises(OutputDirRequiredError):
            YCSBData().generate(output_dir=None)
        with self.assertRaises(OutputDirRequiredError):
            YCSBQueries().generate(output_dir=None)

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


if __name__ == "__main__":
    unittest.main()
