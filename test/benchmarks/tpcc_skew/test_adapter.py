import tempfile
import unittest
import json
from pathlib import Path

from driftbench.data.tpcc import TPCCData
from driftbench.data.tpcc_skew import TPCCSkewData, TPCCSkewQueries
from ..helpers import BenchmarkAdapterTestMixin


class TPCCSkewAdapterTests(BenchmarkAdapterTestMixin, unittest.TestCase):
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

            manifest = json.loads(
                (out / "tpcc_skew" / "data" / "tpcc_skew_data_manifest.json")
                .read_text(encoding="utf-8")
            )
            self.assertAlmostEqual(
                sum(manifest["warehouse_access_weights"]), 1.0, places=12
            )
            self.assertAlmostEqual(
                manifest["warehouse_access_weight_sum"], 1.0, places=12
            )
            self.assertEqual(manifest["hot_warehouse_count"], 1)
            for table, expected in manifest["tables"].items():
                path = out / "tpcc_skew" / "data" / f"{table}.csv"
                self.assertEqual(
                    len(path.read_text(encoding="utf-8").splitlines()) - 1,
                    expected,
                )

    def test_tpcc_skew_preserves_base_tpcc_table_cardinalities(self) -> None:
        with tempfile.TemporaryDirectory() as base_tmp, tempfile.TemporaryDirectory() as skew_tmp:
            base = TPCCData(scale_factor=2).generate(output_dir=Path(base_tmp) / "out")
            skew = TPCCSkewData(
                scale_factor=2,
                hot_warehouse_fraction=0.5,
                skew_factor=0.99,
            ).generate(output_dir=Path(skew_tmp) / "out")

            base_manifest = json.loads(base.metadata.read_text(encoding="utf-8"))
            skew_manifest = json.loads(skew.metadata.read_text(encoding="utf-8"))
            self.assertEqual(skew_manifest["tables"], base_manifest["tables"])

            def table_rows(result):
                return {
                    path.stem: len(path.read_text(encoding="utf-8").splitlines()) - 1
                    for path in result.files
                    if path.suffix == ".csv"
                    and path.name != "warehouse_access_weights.csv"
                }

            self.assertEqual(table_rows(skew), table_rows(base))
            self.assertEqual(table_rows(skew), skew_manifest["tables"])

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
