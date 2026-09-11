import tempfile
import unittest
from pathlib import Path

from driftbench.data.tpch import TPCHData, TPCHQueries
from ..helpers import BenchmarkAdapterTestMixin


class TPCHAdapterTests(BenchmarkAdapterTestMixin, unittest.TestCase):
    _TABLES = (
        "region", "nation", "supplier", "customer", "part", "partsupp",
        "orders", "lineitem",
    )
    _LINEITEM_TBL = (
        "{ok}|155190|7706|1|{qty}|21168.23|0.04|0.02|N|O|"
        "1996-03-13|1996-02-12|1996-03-22|DELIVER IN PERSON|TRUCK|to beans|\n"
    )

    def setUp(self) -> None:
        super().setUp()
        self._tmpdir = self._default_data_tmpdir

    def _tpch_lineitem_source(self, rows: int = 10) -> Path:
        source = Path(self._tmpdir) / "src"
        source.mkdir(parents=True, exist_ok=True)
        for table in self._TABLES:
            if table != "lineitem":
                (source / f"{table}.tbl").write_text("1|value|\n", encoding="utf-8")
        body = "".join(
            self._LINEITEM_TBL.format(ok=i, qty=10 + i) for i in range(1, rows + 1)
        )
        (source / "lineitem.tbl").write_text(body, encoding="utf-8")
        return source

    def test_tpch_data_generate_with_explicit_source_dir(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            source = tmp_path / "src"
            source.mkdir(parents=True, exist_ok=True)
            for table in self._TABLES:
                (source / f"{table}.tbl").write_text("1|value|\n", encoding="utf-8")

            out = tmp_path / "out"
            result = TPCHData(scale_factor=1, source_dir=source).generate(output_dir=out)

            self._assert_result_is_filesystem_contract(result, out)
            self.assertEqual(
                [path.name for path in result.files],
                [f"{table}.tbl" for table in self._TABLES],
            )

    def test_tpch_copy_reports_all_invalid_tables_before_output_mutation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            source = tmp_path / "src"
            source.mkdir()
            (source / "region.tbl").write_text("", encoding="utf-8")
            (source / "nation.tbl").mkdir()
            output = tmp_path / "out"

            with self.assertRaises(FileNotFoundError) as caught:
                TPCHData(source_dir=source).generate(output_dir=output, force=True)

            message = str(caught.exception)
            for table in self._TABLES:
                self.assertIn(f"{table}.tbl", message)
            self.assertFalse(output.exists())

    def test_tpch_copy_rejects_each_invalid_file_kind_before_mutation(self) -> None:
        for invalid_kind in ("missing", "empty", "directory"):
            with self.subTest(invalid_kind=invalid_kind), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                source = root / "src"
                source.mkdir()
                for table in self._TABLES:
                    (source / f"{table}.tbl").write_text("1|value|\n", encoding="utf-8")
                invalid = source / "lineitem.tbl"
                if invalid_kind == "missing":
                    invalid.unlink()
                elif invalid_kind == "empty":
                    invalid.write_text("", encoding="utf-8")
                else:
                    invalid.unlink()
                    invalid.mkdir()

                output = root / "out"
                output.mkdir()
                sentinel = output / "keep.txt"
                sentinel.write_text("unchanged", encoding="utf-8")
                with self.assertRaises(FileNotFoundError):
                    TPCHData(source_dir=source).generate(
                        output_dir=output, force=True
                    )
                self.assertEqual(sentinel.read_text(encoding="utf-8"), "unchanged")
                self.assertFalse((output / "tpch").exists())

    def test_tpch_copy_rejects_symlink_table_before_mutation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "src"
            source.mkdir()
            for table in self._TABLES:
                (source / f"{table}.tbl").write_text("1|value|\n", encoding="utf-8")
            target = source / "lineitem-target.tbl"
            target.write_text("1|value|\n", encoding="utf-8")
            invalid = source / "lineitem.tbl"
            invalid.unlink()
            try:
                invalid.symlink_to(target)
            except OSError as exc:
                self.skipTest(f"symlinks unavailable: {exc}")

            output = root / "out"
            with self.assertRaises(FileNotFoundError):
                TPCHData(source_dir=source).generate(output_dir=output, force=True)
            self.assertFalse(output.exists())

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

    def test_as_csv_writes_tpch_header(self) -> None:
        import pandas as pd

        source = self._tpch_lineitem_source(rows=10)
        out = Path(self._tmpdir) / "out"
        result = TPCHData(scale_factor=1, source_dir=source).generate(output_dir=out)

        csv_result = result.as_csv()
        csv_file = next(f for f in csv_result.files if f.name == "lineitem.csv")
        df = pd.read_csv(csv_file)

        self.assertEqual(
            list(df.columns),
            [
                "l_orderkey", "l_partkey", "l_suppkey", "l_linenumber",
                "l_quantity", "l_extendedprice", "l_discount", "l_tax",
                "l_returnflag", "l_linestatus", "l_shipdate", "l_commitdate",
                "l_receiptdate", "l_shipinstruct", "l_shipmode", "l_comment",
            ],
        )
        # Header must not be counted as a data row.
        self.assertEqual(len(df), 10)
        self.assertEqual(df["l_quantity"].iloc[0], 11)
