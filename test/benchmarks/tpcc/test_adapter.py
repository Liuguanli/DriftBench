import tempfile
import unittest
from pathlib import Path

from driftbench.data.tpcc import TPCCData, TPCCQueries
from ..helpers import BenchmarkAdapterTestMixin


class TPCCAdapterTests(BenchmarkAdapterTestMixin, unittest.TestCase):
    def test_tpcc_data_synth_filesystem_contract(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "out"
            result = TPCCData(scale_factor=1).generate(output_dir=out)
            self._assert_result_is_filesystem_contract(result, out)
            self.assertEqual(result.benchmark, "tpcc")
            self.assertEqual(result.artifact_type, "data")

    def test_tpcc_data_synth_produces_nine_csv_tables(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "out"
            result = TPCCData(scale_factor=1).generate(output_dir=out)
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
            r1 = TPCCData(scale_factor=1).generate(output_dir=out1)
            r2 = TPCCData(scale_factor=2).generate(output_dir=out2)

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
