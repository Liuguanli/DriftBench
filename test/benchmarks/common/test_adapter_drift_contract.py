import json
import os
import shutil
import tempfile
import unittest
from pathlib import Path

from driftbench.data import GenerationResult
from driftbench.data.tpch import TPCHData
from driftbench.data.ycsb import YCSBData

class DriftAPITests(unittest.TestCase):
    """Tests for GenerationResult.drift() and GenerationResult.drift_multi()."""

    def setUp(self) -> None:
        self._tmpdir = tempfile.mkdtemp(prefix="driftbench_drift_test_")
        os.environ["DRIFTBENCH_DATA_DIR"] = self._tmpdir

    def tearDown(self) -> None:
        del os.environ["DRIFTBENCH_DATA_DIR"]
        shutil.rmtree(self._tmpdir, ignore_errors=True)

    def _make_result(
        self,
        benchmark: str,
        csvs: dict,  # {stem: (header_list, rows_list)}
    ) -> GenerationResult:
        """Write CSVs to a temp subdir and return a GenerationResult."""
        out = Path(self._tmpdir) / benchmark
        out.mkdir(parents=True, exist_ok=True)
        files = []
        for stem, (header, rows) in csvs.items():
            p = out / f"{stem}.csv"
            lines = [",".join(header)]
            for row in rows:
                lines.append(",".join(str(v) for v in row))
            p.write_text("\n".join(lines) + "\n", encoding="utf-8")
            files.append(p)
        meta = out / "manifest.json"
        meta.write_text(
            json.dumps({"benchmark": benchmark, "artifact_type": "data"}),
            encoding="utf-8",
        )
        return GenerationResult(
            benchmark=benchmark,
            artifact_type="data",
            output_dir=out,
            files=files,
            metadata=meta,
        )

    # ------------------------------------------------------------------
    # P3-1: drift() single-table
    # ------------------------------------------------------------------

    def test_drift_tpch_outlier_injection(self) -> None:
        header = [
            "l_orderkey", "l_partkey", "l_suppkey", "l_linenumber",
            "l_quantity", "l_extendedprice", "l_discount", "l_tax",
            "l_returnflag", "l_linestatus",
        ]
        rows = [
            [i, i, i, 1, 10 + i, 1000.0 + i * 10, 0.05, 0.02, "N", "O"]
            for i in range(1, 21)
        ]
        result = self._make_result("tpch", {"lineitem": (header, rows)})
        drifted = result.drift("lineitem", "outlier_injection", column="l_quantity", n=5)

        self.assertIsInstance(drifted, GenerationResult)
        self.assertEqual(len(drifted.files), 1)
        self.assertTrue(drifted.files[0].exists())
        import pandas as pd
        df = pd.read_csv(drifted.files[0])
        # outlier_injection appends n rows to the original
        self.assertEqual(len(df), 25)

    def test_drift_ycsb_vary_cardinality(self) -> None:
        import pandas as pd
        with tempfile.TemporaryDirectory() as tmp:
            result = YCSBData(scale_factor=1).generate(output_dir=Path(tmp) / "out")
            drifted = result.drift("usertable", "vary_cardinality", scale=0.5)
            self.assertIsInstance(drifted, GenerationResult)
            self.assertEqual(len(drifted.files), 1)
            self.assertTrue(drifted.files[0].exists())
            df = pd.read_csv(drifted.files[0])
            self.assertEqual(len(df), 500)  # 1000 * 0.5

    def test_drift_output_is_independent_of_original(self) -> None:
        header = ["l_orderkey", "l_quantity"]
        rows = [[i, float(i * 10)] for i in range(1, 11)]
        result = self._make_result("tpch", {"lineitem": (header, rows)})
        original_files = list(result.files)

        result.drift("lineitem", "outlier_injection", column="l_quantity", n=3)

        # original GenerationResult is a frozen dataclass — files unchanged
        self.assertEqual(result.files, original_files)
        self.assertTrue(result.files[0].exists())

    def test_drift_writes_fresh_manifest(self) -> None:
        header = ["l_orderkey", "l_quantity"]
        rows = [[i, float(i * 10)] for i in range(1, 11)]
        result = self._make_result("tpch", {"lineitem": (header, rows)})

        drifted = result.drift("lineitem", "outlier_injection", column="l_quantity", n=3)

        self.assertNotEqual(drifted.metadata, result.metadata)
        self.assertTrue(drifted.metadata.exists())
        payload = json.loads(drifted.metadata.read_text(encoding="utf-8"))
        self.assertEqual(payload["benchmark"], "tpch")
        self.assertEqual(payload["drift_kind"], "single_table")
        self.assertEqual(payload["table"], "lineitem")
        self.assertEqual(payload["drift_type"], "outlier_injection")
        self.assertEqual(payload["files"], [drifted.files[0].name])

    def test_drift_unknown_table_raises(self) -> None:
        header = ["l_orderkey", "l_quantity"]
        rows = [[1, 10.0]]
        result = self._make_result("tpch", {"lineitem": (header, rows)})
        with self.assertRaises(ValueError):
            result.drift("no_such_table", "outlier_injection", column="l_quantity", n=1)

    # ------------------------------------------------------------------
    # P3-2: drift_multi() multi-table
    # ------------------------------------------------------------------

    def test_drift_multi_tpch_skew_column(self) -> None:
        header_li = ["l_orderkey", "l_quantity", "l_extendedprice"]
        rows_li = [[i, 10 + i, 1000.0 + i * 10] for i in range(1, 21)]
        header_ord = ["o_orderkey", "o_custkey", "o_totalprice"]
        rows_ord = [[i, i, 5000.0 + i] for i in range(1, 21)]
        result = self._make_result(
            "tpch",
            {
                "lineitem": (header_li, rows_li),
                "orders": (header_ord, rows_ord),
            },
        )
        steps = [
            {
                "op": "skew_column",
                "target": "lineitem",
                "column": "l_quantity",
                "fraction": 0.2,
                "skewness": 2,
            }
        ]
        drifted = result.drift_multi(steps, relationships=[])

        self.assertIsInstance(drifted, GenerationResult)
        self.assertEqual(len(drifted.files), 2)
        drift_stems = {f.stem for f in drifted.files}
        self.assertIn("lineitem_drifted", drift_stems)
        self.assertIn("orders_drifted", drift_stems)
        for f in drifted.files:
            self.assertTrue(f.exists())
            self.assertGreater(f.stat().st_size, 0)

    def test_drift_multi_writes_fresh_manifest(self) -> None:
        header_li = ["l_orderkey", "l_quantity"]
        rows_li = [[i, 10 + i] for i in range(1, 6)]
        header_ord = ["o_orderkey", "o_totalprice"]
        rows_ord = [[i, 5000.0 + i] for i in range(1, 6)]
        result = self._make_result(
            "tpch",
            {
                "lineitem": (header_li, rows_li),
                "orders": (header_ord, rows_ord),
            },
        )

        drifted = result.drift_multi(
            [{"op": "skew_column", "target": "lineitem", "column": "l_quantity", "fraction": 0.5}],
            relationships=[],
        )

        self.assertNotEqual(drifted.metadata, result.metadata)
        payload = json.loads(drifted.metadata.read_text(encoding="utf-8"))
        self.assertEqual(payload["benchmark"], "tpch")
        self.assertEqual(payload["drift_kind"], "multi_table")
        self.assertEqual(sorted(payload["files"]), sorted(path.name for path in drifted.files))

    def test_drift_multi_tpch_uses_builtin_relationships(self) -> None:
        import pandas as pd

        result = self._make_result(
            "tpch",
            {
                "orders": (
                    ["o_orderkey", "o_custkey"],
                    [[1, 101], [2, 102], [3, 103]],
                ),
                "lineitem": (
                    ["l_orderkey", "l_partkey", "l_suppkey"],
                    [[1, 10, 20], [1, 11, 21], [2, 12, 22], [3, 13, 23]],
                ),
            },
        )
        steps = [
            {
                "op": "delete_keys",
                "target": "orders",
                "key_column": "o_orderkey",
                "count": 1,
                "propagate": [{"relationship": "lineitem_orders", "policy": "drop"}],
            }
        ]

        drifted = result.drift_multi(steps, seed=42)

        orders_drifted = next(f for f in drifted.files if f.stem == "orders_drifted")
        lineitem_drifted = next(f for f in drifted.files if f.stem == "lineitem_drifted")
        orders_df = pd.read_csv(orders_drifted)
        lineitem_df = pd.read_csv(lineitem_drifted)

        self.assertEqual(len(orders_df), 2)
        self.assertEqual(set(lineitem_df["l_orderkey"]), set(orders_df["o_orderkey"]))

    def test_drift_multi_job_uses_builtin_relationships(self) -> None:
        import pandas as pd

        result = self._make_result(
            "job",
            {
                "title": (["id", "title"], [[1, "a"], [2, "b"], [3, "c"]]),
                "movie_info": (
                    ["id", "movie_id", "info_type_id", "info"],
                    [[1, 1, 10, "x"], [2, 2, 10, "y"], [3, 3, 10, "z"]],
                ),
            },
        )
        steps = [
            {
                "op": "delete_keys",
                "target": "title",
                "key_column": "id",
                "count": 1,
                "propagate": [{"relationship": "movie_info_title", "policy": "drop"}],
            }
        ]

        drifted = result.drift_multi(steps, seed=42)

        title_drifted = next(f for f in drifted.files if f.stem == "title_drifted")
        movie_info_drifted = next(f for f in drifted.files if f.stem == "movie_info_drifted")
        title_df = pd.read_csv(title_drifted)
        movie_info_df = pd.read_csv(movie_info_drifted)

        self.assertEqual(len(title_df), 2)
        self.assertEqual(set(movie_info_df["movie_id"]), set(title_df["id"]))

    def test_drift_multi_untouched_tables_preserve_row_count(self) -> None:
        import pandas as pd
        header_li = ["l_orderkey", "l_quantity"]
        rows_li = [[i, 10 + i] for i in range(1, 21)]
        header_ord = ["o_orderkey", "o_totalprice"]
        rows_ord = [[i, 5000.0 + i] for i in range(1, 6)]
        result = self._make_result(
            "tpch",
            {
                "lineitem": (header_li, rows_li),
                "orders": (header_ord, rows_ord),
            },
        )
        steps = [
            {
                "op": "skew_column",
                "target": "lineitem",
                "column": "l_quantity",
                "fraction": 0.5,
            }
        ]
        drifted = result.drift_multi(steps, relationships=[])
        orders_drifted = next(f for f in drifted.files if f.stem == "orders_drifted")
        self.assertEqual(len(pd.read_csv(orders_drifted)), 5)


class CsvHeaderAndDriftFixTests(unittest.TestCase):
    """as_csv() header injection and the .tbl→.drift() guard rail."""

    # One schema-correct TPC-H lineitem row (16 cols + trailing pipe),
    # matching real dbgen output. l_quantity is the 5th column.
    _LINEITEM_TBL = (
        "{ok}|155190|7706|1|{qty}|21168.23|0.04|0.02|N|O|"
        "1996-03-13|1996-02-12|1996-03-22|DELIVER IN PERSON|TRUCK|to beans|\n"
    )

    def setUp(self) -> None:
        self._tmpdir = tempfile.mkdtemp(prefix="driftbench_csvhdr_test_")
        os.environ["DRIFTBENCH_DATA_DIR"] = self._tmpdir

    def tearDown(self) -> None:
        del os.environ["DRIFTBENCH_DATA_DIR"]
        shutil.rmtree(self._tmpdir, ignore_errors=True)

    def _tpch_lineitem_source(self, rows: int = 10) -> Path:
        source = Path(self._tmpdir) / "src"
        source.mkdir(parents=True, exist_ok=True)
        body = "".join(
            self._LINEITEM_TBL.format(ok=i, qty=10 + i) for i in range(1, rows + 1)
        )
        (source / "lineitem.tbl").write_text(body, encoding="utf-8")
        return source

    def test_end_to_end_generate_as_csv_drift(self) -> None:
        import pandas as pd

        source = self._tpch_lineitem_source(rows=10)
        out = Path(self._tmpdir) / "out"
        result = TPCHData(scale_factor=1, source_dir=source).generate(output_dir=out)

        drifted = result.as_csv().drift(
            "lineitem", "outlier_injection", column="l_quantity", n=5
        )
        self.assertIsInstance(drifted, GenerationResult)
        self.assertEqual(len(drifted.files), 1)
        self.assertTrue(drifted.files[0].exists())
        df = pd.read_csv(drifted.files[0])
        # outlier_injection appends n rows to the 10 originals.
        self.assertEqual(len(df), 15)

    def test_drift_on_unconverted_tbl_raises_helpful_error(self) -> None:
        source = self._tpch_lineitem_source(rows=3)
        out = Path(self._tmpdir) / "out"
        result = TPCHData(scale_factor=1, source_dir=source).generate(output_dir=out)

        with self.assertRaises(ValueError) as ctx:
            result.drift("lineitem", "outlier_injection", column="l_quantity", n=1)
        msg = str(ctx.exception)
        self.assertIn("as_csv()", msg)
        self.assertIn(".tbl", msg)


class SpecPythonParityTests(unittest.TestCase):
    """The DriftSpec YAML path and the .drift() Python path must agree.

    Builds a headered TPC-H lineitem.csv via the adapter + as_csv(), then
    proves: (a) the spec engine runs against that file, (b) spec and Python
    produce byte-identical drift for the same seed/params (incl. a non-default
    seed, which exercises the spec single-table seed-threading fix), and
    (c) .drift()'s emitted hidden DriftSpec YAML reproduces its own output.
    """

    _LINEITEM_TBL = (
        "{ok}|155190|7706|1|{qty}|21168.23|0.04|0.02|N|O|"
        "1996-03-13|1996-02-12|1996-03-22|DELIVER IN PERSON|TRUCK|to beans|\n"
    )

    def setUp(self) -> None:
        self._tmpdir = tempfile.mkdtemp(prefix="driftbench_parity_test_")
        os.environ["DRIFTBENCH_DATA_DIR"] = self._tmpdir

    def tearDown(self) -> None:
        del os.environ["DRIFTBENCH_DATA_DIR"]
        shutil.rmtree(self._tmpdir, ignore_errors=True)

    def _headered_lineitem_csv(self, rows: int = 10) -> Path:
        """Return path to a header'd lineitem.csv via TPCHData().as_csv()."""
        source = Path(self._tmpdir) / "src"
        source.mkdir(parents=True, exist_ok=True)
        body = "".join(
            self._LINEITEM_TBL.format(ok=i, qty=10 + i) for i in range(1, rows + 1)
        )
        (source / "lineitem.tbl").write_text(body, encoding="utf-8")
        out = Path(self._tmpdir) / "out"
        result = TPCHData(scale_factor=1, source_dir=source).generate(output_dir=out)
        csv_result = result.as_csv()
        return next(f for f in csv_result.files if f.name == "lineitem.csv")

    def _run_spec(
        self, source_csv: Path, out_csv: Path, seed: int, n: int = 5
    ) -> None:
        """Write a minimal single-table DriftSpec and execute it via run_all."""
        import yaml
        from driftbench.spec.core import run_all

        spec = {
            "pattern_id": "parity-lineitem",
            "seed": seed,
            "type": {
                "family": "data",
                "category": "drift",
                "subtype": "single_table",
            },
            "data_source": {
                "kind": "csv",
                "path": str(source_csv),
                "schema_extractor": {
                    "source_type": "csv",
                    "sample_size": 0,
                    "schema_output_path": str(
                        out_csv.parent / "spec_schema.json"
                    ),
                },
            },
            "variables": {
                "base_table": "lineitem",
                "drifts": [
                    {
                        "name": "outlier_injection",
                        "drift_type": "outlier_injection",
                        "output_path": str(out_csv),
                        "column": "l_quantity",
                        "n": n,
                    }
                ],
            },
        }
        spec_path = out_csv.parent / "parity_spec.yaml"
        with spec_path.open("w", encoding="utf-8") as f:
            yaml.safe_dump(spec, f, sort_keys=False)
        run_all(str(spec_path))

    def test_spec_path_runs_against_as_csv_output(self) -> None:
        import pandas as pd

        source_csv = self._headered_lineitem_csv(rows=10)
        spec_out = Path(self._tmpdir) / "specrun" / "drifted.csv"
        spec_out.parent.mkdir(parents=True, exist_ok=True)

        self._run_spec(source_csv, spec_out, seed=42, n=5)

        self.assertTrue(spec_out.exists())
        df = pd.read_csv(spec_out)
        self.assertIn("l_quantity", df.columns)
        self.assertEqual(len(df), 15)  # 10 base + 5 injected

    def test_spec_and_python_drift_byte_identical_default_seed(self) -> None:
        source_csv = self._headered_lineitem_csv(rows=10)
        result = GenerationResult(
            benchmark="tpch",
            artifact_type="data",
            output_dir=source_csv.parent,
            files=[source_csv],
            metadata=source_csv.parent / "m.json",
        )
        py = result.drift(
            "lineitem", "outlier_injection", column="l_quantity", n=5, seed=42
        )
        py_bytes = py.files[0].read_bytes()

        spec_out = Path(self._tmpdir) / "specrun" / "drifted.csv"
        spec_out.parent.mkdir(parents=True, exist_ok=True)
        self._run_spec(source_csv, spec_out, seed=42, n=5)

        self.assertEqual(py_bytes, spec_out.read_bytes())

    def test_spec_and_python_drift_byte_identical_custom_seed(self) -> None:
        # seed=7 exercises the spec single-table seed-threading fix; before it,
        # the spec path silently used seed=42 and this would diverge.
        source_csv = self._headered_lineitem_csv(rows=10)
        result = GenerationResult(
            benchmark="tpch",
            artifact_type="data",
            output_dir=source_csv.parent,
            files=[source_csv],
            metadata=source_csv.parent / "m.json",
        )
        py = result.drift(
            "lineitem", "outlier_injection", column="l_quantity", n=5, seed=7
        )
        py_bytes = py.files[0].read_bytes()

        spec_out = Path(self._tmpdir) / "specrun7" / "drifted.csv"
        spec_out.parent.mkdir(parents=True, exist_ok=True)
        self._run_spec(source_csv, spec_out, seed=7, n=5)

        self.assertEqual(py_bytes, spec_out.read_bytes())

    def test_drift_emits_reproducible_driftspec_yaml(self) -> None:
        from driftbench.spec.core import run_all

        source_csv = self._headered_lineitem_csv(rows=10)
        result = GenerationResult(
            benchmark="tpch",
            artifact_type="data",
            output_dir=source_csv.parent,
            files=[source_csv],
            metadata=source_csv.parent / "m.json",
        )
        py = result.drift(
            "lineitem", "outlier_injection", column="l_quantity", n=5, seed=13
        )
        out_csv = py.files[0]
        original_bytes = out_csv.read_bytes()

        # The hidden YAML is not in result.files but is recorded in the manifest.
        manifest = json.loads(py.metadata.read_text(encoding="utf-8"))
        self.assertIn("driftspec", manifest)
        spec_path = out_csv.parent / manifest["driftspec"]
        self.assertTrue(spec_path.exists())
        self.assertNotIn(spec_path, py.files)

        # Re-running the emitted spec must regenerate byte-identical output.
        out_csv.unlink()
        run_all(str(spec_path))
        self.assertTrue(out_csv.exists())
        self.assertEqual(out_csv.read_bytes(), original_bytes)


if __name__ == "__main__":
    unittest.main()
