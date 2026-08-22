import tempfile
import unittest
from pathlib import Path

from driftbench.data import GenerationResult
from driftbench.data.benchbase import BenchBaseData, BenchBaseQueries
from ..helpers import BenchmarkAdapterTestMixin


class BenchBaseAdapterTests(BenchmarkAdapterTestMixin, unittest.TestCase):
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
