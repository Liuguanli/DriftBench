import csv
import tempfile
import unittest
import json
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

            for result in (r1, r2):
                manifest = json.loads(result.metadata.read_text(encoding="utf-8"))
                observed = {
                    path.stem: row_count(result, path.stem)
                    for path in result.files
                    if path.suffix == ".csv"
                }
                self.assertEqual(manifest["tables"], observed)

            self.assertEqual(row_count(r1, "orders"), 3000)
            self.assertEqual(row_count(r2, "orders"), 6000)
            self.assertEqual(row_count(r1, "new_order"), 900)
            self.assertEqual(row_count(r2, "new_order"), 1800)

    def test_tpcc_orders_lines_and_new_order_relationships_are_consistent(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            result = TPCCData(scale_factor=1).generate(output_dir=Path(tmp) / "out")
            paths = {path.stem: path for path in result.files if path.suffix == ".csv"}

            with paths["orders"].open(encoding="utf-8", newline="") as stream:
                orders = {
                    (row["o_w_id"], row["o_d_id"], row["o_id"]): row
                    for row in csv.DictReader(stream)
                }
            with paths["new_order"].open(encoding="utf-8", newline="") as stream:
                new_orders = {
                    (row["no_w_id"], row["no_d_id"], row["no_o_id"])
                    for row in csv.DictReader(stream)
                }

            line_counts: dict[tuple[str, str, str], int] = {}
            delivery_values: dict[tuple[str, str, str], set[str]] = {}
            with paths["order_line"].open(encoding="utf-8", newline="") as stream:
                for row in csv.DictReader(stream):
                    key = (row["ol_w_id"], row["ol_d_id"], row["ol_o_id"])
                    line_counts[key] = line_counts.get(key, 0) + 1
                    delivery_values.setdefault(key, set()).add(row["ol_delivery_d"])

            expected_new_orders = {
                key for key, row in orders.items() if row["o_carrier_id"] == ""
            }
            self.assertEqual(new_orders, expected_new_orders)
            self.assertEqual(len(new_orders), len(orders) * 3 // 10)
            self.assertEqual(
                sum(int(row["o_ol_cnt"]) for row in orders.values()),
                sum(line_counts.values()),
            )
            for key, row in orders.items():
                self.assertEqual(line_counts[key], int(row["o_ol_cnt"]))
                if key in new_orders:
                    self.assertEqual(delivery_values[key], {""})
                else:
                    self.assertNotIn("", delivery_values[key])

            manifest = json.loads(result.metadata.read_text(encoding="utf-8"))
            self.assertEqual(manifest["tables"]["order_line"], sum(line_counts.values()))
            self.assertEqual(manifest["tables"]["new_order"], len(new_orders))

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
