from __future__ import annotations

import csv
import hashlib
import json
import os
import tempfile
import unittest
from contextlib import ExitStack
from pathlib import Path
from unittest.mock import patch

from driftbench.data.ssb import SSBData, SSBQueries, data, queries
from ..helpers import BenchmarkAdapterTestMixin


HEADERS = {
    "customer": "c_custkey c_name c_address c_city c_nation c_region c_phone c_mktsegment".split(),
    "part": "p_partkey p_name p_mfgr p_category p_brand1 p_color p_type p_size p_container".split(),
    "supplier": "s_suppkey s_name s_address s_city s_nation s_region s_phone".split(),
    "date": "d_datekey d_date d_dayofweek d_month d_year d_yearmonthnum d_yearmonth d_daynuminweek d_daynuminmonth d_daynuminyear d_monthnuminyear d_weeknuminyear d_sellingseason d_lastdayinweekfl d_lastdayinmonthfl d_holidayfl d_weekdayfl".split(),
    "lineorder": "lo_orderkey lo_linenumber lo_custkey lo_partkey lo_suppkey lo_orderdate lo_orderpriority lo_shippriority lo_quantity lo_extendedprice lo_ordtotalprice lo_discount lo_revenue lo_supplycost lo_tax lo_commitdate lo_shipmode".split(),
}
ROWS = {
    "customer": ['1', 'Customer#1', 'Road, "West"', 'UNITED KI1', 'UNITED STATES', 'AMERICA', '10-100-100-1000', 'BUILDING'],
    "part": ['1', 'green part', 'MFGR#1', 'MFGR#12', 'MFGR#121', 'green', 'STANDARD', '2', 'SM CASE'],
    "supplier": ['1', 'Supplier#1', 'Supplier road', 'UNITED KI1', 'UNITED STATES', 'AMERICA', '10-200-200-2000'],
    "date": ['19930101', 'January 1, 1993', 'Friday', 'January', '1993', '199301', 'Jan1993', '6', '1', '1', '1', '1', 'Winter', '0', '0', '1', '1'],
    "lineorder": ['1', '1', '1', '1', '1', '19930101', '1-URGENT', '0', '10', '10000', '10000', '2', '9800', '6000', '0', '19930101', 'AIR'],
}
QUERY_IDS = ['1.1', '1.2', '1.3', '2.1', '2.2', '2.3', '3.1', '3.2', '3.3', '3.4', '4.1', '4.2', '4.3']


def write_source(root: Path) -> Path:
    root.mkdir(parents=True)
    for table, row in ROWS.items():
        (root / f"{table}.tbl").write_text("|".join(row) + "|\n", encoding="utf-8")
    return root


def snapshot(root: Path) -> dict[str, bytes]:
    return {path.relative_to(root).as_posix(): path.read_bytes() for path in root.rglob("*") if path.is_file()}


def manifest(result) -> dict:
    return json.loads(result.metadata.read_text(encoding="utf-8"))


def directory_link(source: Path, target: Path) -> None:
    try:
        target.symlink_to(source, target_is_directory=True)
    except OSError:
        if os.name != "nt":
            raise
        import _winapi
        _winapi.CreateJunction(str(source), str(target))


class SSBAdapterTests(BenchmarkAdapterTestMixin, unittest.TestCase):
    def test_import_preserves_values_and_emits_canonical_headers_schema_and_hashes(self):
        with tempfile.TemporaryDirectory() as tmp:
            source = write_source(Path(tmp) / "source")
            original = snapshot(source)
            root = Path(tmp) / "output"
            result = data(source_dir=source).generate(root)
            self._assert_result_is_filesystem_contract(result, root)
            self.assertEqual({path.name for path in result.files}, {*(f"{name}.csv" for name in ROWS), "schema.sql"})
            for table, expected_row in ROWS.items():
                with (root / "ssb" / "data" / f"{table}.csv").open(encoding="utf-8", newline="") as stream:
                    rows = list(csv.reader(stream))
                self.assertEqual(rows, [HEADERS[table], expected_row])
            schema = (root / "ssb/data/schema.sql").read_text()
            self.assertEqual(schema.count("CREATE TABLE"), 5)
            self.assertIn('CREATE TABLE "date"', schema)
            self.assertIn("lo_extendedprice BIGINT", schema)
            payload = manifest(result)
            self.assertEqual(payload["tables"], {name: 1 for name in ROWS})
            self.assertEqual(payload["mode"], "import")
            self.assertEqual(payload["dialect"], "postgresql")
            for name, content in original.items():
                self.assertEqual(payload["source_files"][name], {"bytes": len(content), "sha256": hashlib.sha256(content).hexdigest()})
            self.assertIn("219403ad7d1dd32ae1f97b5553abf92129fccd7f", payload["source_contract"])
            self.assertEqual(snapshot(source), original)

    def test_all_thirteen_queries_contain_their_standard_predicates_and_joins(self):
        predicates = {
            "1.1": "d_year = 1993", "1.2": "d_yearmonthnum = 199401", "1.3": "d_weeknuminyear = 6",
            "2.1": "p_category = 'MFGR#12'", "2.2": "p_brand1 BETWEEN 'MFGR#2221' AND 'MFGR#2228'",
            # Keep the original Rev3 paper SQL variant consistently.
            "2.3": "p_brand1 = 'MFGR#2221'", "3.1": "c_region = 'ASIA'",
            "3.2": "c_nation = 'UNITED STATES'", "3.3": "c_city IN ('UNITED KI1', 'UNITED KI5')",
            "3.4": "d_yearmonth = 'Dec1997'", "4.1": "p_mfgr IN ('MFGR#1', 'MFGR#2')",
            "4.2": "d_year IN (1997, 1998)", "4.3": "p_category = 'MFGR#14'",
        }
        with tempfile.TemporaryDirectory() as tmp:
            result = queries().generate(tmp)
            self._assert_result_is_filesystem_contract(result, Path(tmp))
            self.assertEqual(manifest(result)["query_ids"], QUERY_IDS)
            self.assertEqual(manifest(result)["source_contract"], "https://www.cs.umb.edu/~poneil/StarSchemaB.PDF")
            sql_files = [path for path in result.files if path.name != "schema.sql"]
            self.assertEqual(len(sql_files), 13)
            for qid, path in zip(QUERY_IDS, sql_files):
                sql = path.read_text()
                self.assertEqual(path.name, f"q{qid.replace('.', '_')}.sql")
                self.assertIn(predicates[qid], sql)
                self.assertIn('JOIN "date" ON lo_orderdate = d_datekey', sql)
                self.assertNotIn("SELECT *", sql)
                if qid.startswith("4."):
                    self.assertIn("SUM(lo_revenue - lo_supplycost)", sql)
                    self.assertIn("JOIN customer ON lo_custkey = c_custkey", sql)

    def test_selected_query_order_and_cache_identity(self):
        with tempfile.TemporaryDirectory() as tmp:
            first = queries(query_ids=["3.2", "1.1"]).generate(tmp)
            self.assertEqual([path.name for path in first.files], ["q3_2.sql", "q1_1.sql", "schema.sql"])
            self.assertEqual(manifest(first)["query_ids"], ["3.2", "1.1"])
            first_key = manifest(first)["cache"]["fingerprint"]
            self.assertTrue(queries(query_ids=("3.2", "1.1")).generate(tmp).reused_local)
            second = queries(query_ids=["1.1"]).generate(tmp)
            self.assertFalse(second.reused_local)
            self.assertNotEqual(manifest(second)["cache"]["fingerprint"], first_key)
            self.assertEqual([path.name for path in second.files], ["q1_1.sql", "schema.sql"])

    def test_identical_rerun_force_and_tamper_recovery(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "output"
            source = write_source(Path(tmp) / "source")
            for adapter in (data(source), queries()):
                first = adapter.generate(root)
                expected = {path.name: path.read_bytes() for path in [*first.files, first.metadata]}
                self.assertTrue(adapter.generate(root).reused_local)
                forced = adapter.generate(root, force=True)
                self.assertFalse(forced.reused_local)
                self.assertEqual({path.name: path.read_bytes() for path in [*forced.files, forced.metadata]}, expected)
                for path in (first.files[0], first.metadata):
                    path.write_text("tampered", encoding="utf-8")
                    recovered = adapter.generate(root)
                    self.assertFalse(recovered.reused_local)
                    self.assertEqual({path.name: path.read_bytes() for path in [*recovered.files, recovered.metadata]}, expected)

    def test_changed_source_invalidates_cache_and_keeps_unrelated_output(self):
        with tempfile.TemporaryDirectory() as tmp:
            source = write_source(Path(tmp) / "source")
            root = Path(tmp) / "output"
            first = data(source).generate(root)
            key = manifest(first)["cache"]["fingerprint"]
            note = root / "ssb/data/my-notes.txt"
            note.write_bytes(b"keep")
            customer = source / "customer.tbl"
            customer.write_text(customer.read_text().replace("Customer#1", "Customer#2"))
            changed = snapshot(source)
            second = data(source).generate(root)
            self.assertFalse(second.reused_local)
            self.assertNotEqual(manifest(second)["cache"]["fingerprint"], key)
            self.assertIn("Customer#2", (root / "ssb/data/customer.csv").read_text())
            self.assertEqual(snapshot(source), changed)
            self.assertEqual(note.read_bytes(), b"keep")

    def test_invalid_query_ids_and_force_fail_without_writing(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "output"
            source = write_source(Path(tmp) / "source")
            for value in ([], "1.1", ["1.1", "1.1"], ["0.1"], [True], [["1.1"]], ["1.1;DROP TABLE lineorder"]):
                with self.subTest(value=value), self.assertRaisesRegex(ValueError, "query_ids"):
                    queries(query_ids=value).generate(root)
                self.assertFalse(root.exists())
            for adapter in (SSBData(source), SSBQueries()):
                with self.assertRaisesRegex(ValueError, "force"):
                    adapter.generate(root, force="yes")
                self.assertFalse(root.exists())

    def test_missing_source_and_incomplete_tables_fail_before_artifacts(self):
        with tempfile.TemporaryDirectory() as tmp:
            parent = Path(tmp)
            for value in (None, "", False, parent / "missing"):
                with self.subTest(value=value), self.assertRaises(ValueError):
                    data(source_dir=value).generate(parent / "output")
                self.assertFalse((parent / "output").exists())
            source = write_source(parent / "source")
            for name in ROWS:
                path = source / f"{name}.tbl"
                original = path.read_bytes()
                path.unlink()
                with self.subTest(table=name), self.assertRaisesRegex(ValueError, "missing or empty"):
                    data(source).generate(parent / "output")
                self.assertFalse((parent / "output").exists())
                path.write_bytes(original)

    def test_malformed_late_table_does_not_replace_existing_artifacts_or_source(self):
        with tempfile.TemporaryDirectory() as tmp:
            source = write_source(Path(tmp) / "source")
            root = Path(tmp) / "output"
            data(source).generate(root)
            expected = snapshot(root)
            for content in ("1|2|\n", "bad|" + "|".join(ROWS["lineorder"][1:]) + "|\n", ""):
                (source / "lineorder.tbl").write_text(content)
                source_before = snapshot(source)
                with self.subTest(content=content), self.assertRaises(ValueError):
                    data(source).generate(root)
                self.assertEqual(snapshot(root), expected)
                self.assertEqual(snapshot(source), source_before)

    def test_input_output_overlap_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            source = write_source(Path(tmp) / "source")
            expected = snapshot(source)
            for output in (source, source / "nested", Path(tmp)):
                with self.subTest(output=output), self.assertRaisesRegex(ValueError, "overlap"):
                    data(source).generate(output)
                self.assertEqual(snapshot(source), expected)

    def test_managed_directory_collision_does_not_partially_write(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            collision = root / "ssb/queries/schema.sql"
            collision.mkdir(parents=True)
            note = collision / "notes.txt"
            note.write_bytes(b"keep")
            before = snapshot(root)
            with self.assertRaisesRegex(ValueError, "regular file"):
                queries().generate(root)
            self.assertEqual(snapshot(root), before)

    def test_output_hardlink_and_directory_link_do_not_touch_external_target(self):
        with tempfile.TemporaryDirectory() as tmp:
            parent = Path(tmp)
            external = parent / "external.sql"
            external.write_bytes(b"keep")
            target = parent / "hardlink-output/ssb/queries/schema.sql"
            target.parent.mkdir(parents=True)
            os.link(external, target)
            with self.assertRaisesRegex(ValueError, "hardlink"):
                queries().generate(parent / "hardlink-output")
            self.assertEqual(external.read_bytes(), b"keep")
            self.assertEqual(list(target.parent.iterdir()), [target])
            outside = parent / "outside"
            outside.mkdir()
            root = parent / "link-output"
            root.mkdir()
            try:
                directory_link(outside, root / "ssb")
            except OSError as exc:
                self.skipTest(f"directory links unavailable: {exc}")
            with self.assertRaisesRegex(ValueError, "escapes|symlinks|junctions"):
                queries().generate(root)
            self.assertEqual(list(outside.iterdir()), [])

    def test_artifact_generation_never_invokes_native_database_or_network(self):
        with tempfile.TemporaryDirectory() as tmp:
            source = write_source(Path(tmp) / "source")
            with ExitStack() as stack:
                guards = [stack.enter_context(patch(target, side_effect=AssertionError(target))) for target in (
                    "subprocess.run", "subprocess.Popen", "os.system", "socket.create_connection",
                    "urllib.request.urlopen", "psycopg2.connect",
                )]
                data(source).generate(Path(tmp) / "output")
                queries().generate(Path(tmp) / "output")
                for guard in guards:
                    guard.assert_not_called()
