import tempfile
import unittest
from pathlib import Path
import json
from unittest.mock import patch

from driftbench.data import GenerationResult, OutputDirRequiredError
from driftbench.data.dsb import DSBData, DSBQueries
from driftbench.data.tpch import TPCHData, TPCHQueries, data as tpch_data
from driftbench.data.tpcds import TPCDSData, TPCDSQueries
from driftbench.data.ycsb import YCSBData, YCSBQueries


class _MockHTTPResponse:
    def __init__(self, payload: bytes) -> None:
        self._payload = payload

    def __enter__(self) -> "_MockHTTPResponse":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def read(self) -> bytes:
        return self._payload


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
            result = TPCHData(scale_factor=1, source_dir=source, mode="copy").generate(output_dir=out)

            self._assert_result_is_filesystem_contract(result, out)
            self.assertTrue(any(path.name == "customer.tbl" for path in result.files))

    def test_tpch_data_auto_mode_fallback_generates_synthetic_tbls(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            out = tmp_path / "out"

            result = TPCHData(scale_factor=1, mode="auto").generate(output_dir=out)
            self._assert_result_is_filesystem_contract(result, out)
            self.assertEqual(len(result.files), 8)
            names = {path.name for path in result.files}
            self.assertEqual(
                names,
                {
                    "region.tbl",
                    "nation.tbl",
                    "supplier.tbl",
                    "customer.tbl",
                    "part.tbl",
                    "partsupp.tbl",
                    "orders.tbl",
                    "lineitem.tbl",
                },
            )
            payload = json.loads(result.metadata.read_text(encoding="utf-8"))
            self.assertEqual(payload.get("source"), "synthetic_auto_fallback")

    def test_tpch_data_copy_mode_without_source_raises(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            out = tmp_path / "out"
            with self.assertRaises(FileNotFoundError):
                TPCHData(scale_factor=1, mode="copy").generate(output_dir=out)

    def test_tpch_data_download_mode_uses_python_download_path(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            out = tmp_path / "out"

            def fake_urlopen(url: str, timeout: int = 60):
                del timeout
                payload = f"downloaded-from|{url}|".encode("utf-8")
                return _MockHTTPResponse(payload)

            with patch("driftbench.data.tpch.urlopen", side_effect=fake_urlopen):
                result = TPCHData(scale_factor=1, mode="download").generate(output_dir=out)

            self._assert_result_is_filesystem_contract(result, out)
            self.assertEqual(len(result.files), 4)
            payload = json.loads(result.metadata.read_text(encoding="utf-8"))
            self.assertEqual(payload.get("source"), "downloaded_sample_tbl")
            for path in result.files:
                text = path.read_text(encoding="utf-8")
                self.assertTrue(text.startswith("downloaded-from|https://raw.githubusercontent.com/"))

    def test_tpch_data_helper_defaults_to_auto_mode_with_download_fallback(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            out = tmp_path / "out"

            def fake_urlopen(url: str, timeout: int = 60):
                del timeout
                payload = f"default-download|{url}|".encode("utf-8")
                return _MockHTTPResponse(payload)

            with patch("driftbench.data.tpch.urlopen", side_effect=fake_urlopen):
                result = tpch_data(scale_factor=1).generate(output_dir=out)

            payload = json.loads(result.metadata.read_text(encoding="utf-8"))
            self.assertEqual(payload.get("mode"), "auto")
            self.assertEqual(payload.get("source"), "downloaded_sample_tbl_auto_fallback")

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

    def test_tpcds_queries_support_all_or_selected_ids(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            out = tmp_path / "out"

            all_result = TPCDSQueries().generate(output_dir=out)
            all_payload = json.loads(all_result.metadata.read_text(encoding="utf-8"))
            self.assertEqual(all_payload.get("query_count"), 99)
            self.assertEqual(all_payload.get("query_ids")[0], 1)
            self.assertEqual(all_payload.get("query_ids")[-1], 99)

            selected_result = TPCDSQueries(query_ids=[3, 7, "12", 7]).generate(output_dir=out)
            selected_payload = json.loads(selected_result.metadata.read_text(encoding="utf-8"))
            self.assertEqual(selected_payload.get("query_count"), 3)
            self.assertEqual(selected_payload.get("query_ids"), [3, 7, 12])

    def test_tpcds_data_supports_scale_in_synth_mode(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            out = tmp_path / "out"

            result = TPCDSData(scale_factor=2, mode="synth").generate(output_dir=out)
            self._assert_result_is_filesystem_contract(result, out)
            payload = json.loads(result.metadata.read_text(encoding="utf-8"))
            self.assertEqual(payload.get("mode"), "synth")
            self.assertEqual(len(result.files), 4)

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
