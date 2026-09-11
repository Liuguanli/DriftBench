"""Import SSB dbgen tables and generate the thirteen SSB analytical queries.

No dbgen download, native generation, database execution or conformance audit
occurs here. Imported tables are converted to headered CSV for explicit loading
and optional single-table Drift; relational integrity is not automatically checked.
"""
from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import ClassVar

from .base import BenchmarkArtifact, GenerationResult
from ._input_artifacts import checked_output, signature, source_directory, source_file

SOURCE_URL = "https://github.com/electrum/ssb-dbgen/tree/219403ad7d1dd32ae1f97b5553abf92129fccd7f"
SPEC_URL = "https://www.cs.umb.edu/~poneil/StarSchemaB.PDF"

# Column order follows the SSB dbgen output, not its legacy TPC-H dss.ddl.
SCHEMAS = {
    "customer": "c_custkey c_name c_address c_city c_nation c_region c_phone c_mktsegment".split(),
    "part": "p_partkey p_name p_mfgr p_category p_brand1 p_color p_type p_size p_container".split(),
    "supplier": "s_suppkey s_name s_address s_city s_nation s_region s_phone".split(),
    "date": "d_datekey d_date d_dayofweek d_month d_year d_yearmonthnum d_yearmonth d_daynuminweek d_daynuminmonth d_daynuminyear d_monthnuminyear d_weeknuminyear d_sellingseason d_lastdayinweekfl d_lastdayinmonthfl d_holidayfl d_weekdayfl".split(),
    "lineorder": "lo_orderkey lo_linenumber lo_custkey lo_partkey lo_suppkey lo_orderdate lo_orderpriority lo_shippriority lo_quantity lo_extendedprice lo_ordtotalprice lo_discount lo_revenue lo_supplycost lo_tax lo_commitdate lo_shipmode".split(),
}
_TEXT = set("c_name c_address c_city c_nation c_region c_phone c_mktsegment p_name p_mfgr p_category p_brand1 p_color p_type p_container s_name s_address s_city s_nation s_region s_phone d_date d_dayofweek d_month d_yearmonth d_sellingseason d_lastdayinweekfl d_lastdayinmonthfl d_holidayfl d_weekdayfl lo_orderpriority lo_shippriority lo_shipmode".split())


def _schema_sql() -> str:
    statements = ["-- SSB schema for PostgreSQL; prices remain in dbgen's integer units."]
    for table, columns in SCHEMAS.items():
        definitions = [f"    {column} {'VARCHAR(100)' if column in _TEXT else 'BIGINT'} NOT NULL" for column in columns]
        statements.append(f'CREATE TABLE "{table}" (\n' + ",\n".join(definitions) + "\n);")
    return "\n\n".join(statements) + "\n"


def _query_sql() -> dict[str, str]:
    queries: dict[str, str] = {}
    for qid, predicate in {
        "1.1": "d_year = 1993 AND lo_discount BETWEEN 1 AND 3 AND lo_quantity < 25",
        "1.2": "d_yearmonthnum = 199401 AND lo_discount BETWEEN 4 AND 6 AND lo_quantity BETWEEN 26 AND 35",
        "1.3": "d_weeknuminyear = 6 AND d_year = 1994 AND lo_discount BETWEEN 5 AND 7 AND lo_quantity BETWEEN 26 AND 35",
    }.items():
        queries[qid] = 'SELECT SUM(lo_extendedprice * lo_discount) AS revenue\nFROM lineorder JOIN "date" ON lo_orderdate = d_datekey\nWHERE ' + predicate + ";\n"
    for qid, predicate in {
        "2.1": "p_category = 'MFGR#12' AND s_region = 'AMERICA'",
        "2.2": "p_brand1 BETWEEN 'MFGR#2221' AND 'MFGR#2228' AND s_region = 'ASIA'",
        # Follow Rev.3's SQL (its prose and downstream variants differ).
        "2.3": "p_brand1 = 'MFGR#2221' AND s_region = 'EUROPE'",
    }.items():
        queries[qid] = 'SELECT SUM(lo_revenue) AS revenue, d_year, p_brand1\nFROM lineorder JOIN "date" ON lo_orderdate = d_datekey\nJOIN part ON lo_partkey = p_partkey\nJOIN supplier ON lo_suppkey = s_suppkey\nWHERE ' + predicate + "\nGROUP BY d_year, p_brand1\nORDER BY d_year, p_brand1;\n"
    for qid, columns, predicate in [
        ("3.1", "c_nation, s_nation", "c_region = 'ASIA' AND s_region = 'ASIA' AND d_year BETWEEN 1992 AND 1997"),
        ("3.2", "c_city, s_city", "c_nation = 'UNITED STATES' AND s_nation = 'UNITED STATES' AND d_year BETWEEN 1992 AND 1997"),
        ("3.3", "c_city, s_city", "c_city IN ('UNITED KI1', 'UNITED KI5') AND s_city IN ('UNITED KI1', 'UNITED KI5') AND d_year BETWEEN 1992 AND 1997"),
        ("3.4", "c_city, s_city", "c_city IN ('UNITED KI1', 'UNITED KI5') AND s_city IN ('UNITED KI1', 'UNITED KI5') AND d_yearmonth = 'Dec1997'"),
    ]:
        queries[qid] = f'SELECT {columns}, d_year, SUM(lo_revenue) AS revenue\nFROM lineorder JOIN "date" ON lo_orderdate = d_datekey\nJOIN customer ON lo_custkey = c_custkey\nJOIN supplier ON lo_suppkey = s_suppkey\nWHERE {predicate}\nGROUP BY {columns}, d_year\nORDER BY d_year, revenue DESC;\n'
    for qid, columns, predicate in [
        ("4.1", "d_year, c_nation", "c_region = 'AMERICA' AND s_region = 'AMERICA' AND p_mfgr IN ('MFGR#1', 'MFGR#2')"),
        ("4.2", "d_year, s_nation, p_category", "c_region = 'AMERICA' AND s_region = 'AMERICA' AND d_year IN (1997, 1998) AND p_mfgr IN ('MFGR#1', 'MFGR#2')"),
        ("4.3", "d_year, s_city, p_brand1", "c_region = 'AMERICA' AND s_nation = 'UNITED STATES' AND d_year IN (1997, 1998) AND p_category = 'MFGR#14'"),
    ]:
        queries[qid] = f'SELECT {columns}, SUM(lo_revenue - lo_supplycost) AS profit\nFROM lineorder JOIN "date" ON lo_orderdate = d_datekey\nJOIN customer ON lo_custkey = c_custkey\nJOIN supplier ON lo_suppkey = s_suppkey\nJOIN part ON lo_partkey = p_partkey\nWHERE {predicate}\nGROUP BY {columns}\nORDER BY {columns};\n'
    return queries


_QUERIES = _query_sql()


@dataclass
class SSBData(BenchmarkArtifact):
    source_dir: str | Path | None = None
    benchmark: ClassVar[str] = "ssb"
    artifact_type: ClassVar[str] = "data"

    def generate(self, output_dir: str | Path | None = None, force: bool = False) -> GenerationResult:
        if type(force) is not bool:
            raise ValueError("force must be a boolean")
        source = source_directory(self.source_dir, "source_dir (five local ssb-dbgen .tbl files)")
        inputs = {table: source_file(source, f"{table}.tbl") for table in SCHEMAS}
        provenance = {f"{table}.tbl": signature(path) for table, path in inputs.items()}
        parameters = {"mode": "import", "source_dir": str(source), "source_files": provenance, "schema_version": 1}
        relatives = [f"ssb/data/{table}.csv" for table in SCHEMAS] + ["ssb/data/schema.sql"]
        manifest_name = "ssb/data/ssb_data_manifest.json"
        root = checked_output(self, output_dir, relatives + [manifest_name], (source,))
        if not force:
            cached = self._load_existing(root / manifest_name, root, parameters)
            if cached is not None:
                return cached
        counts = {}
        # Finish validation/conversion before replacing any existing managed file.
        with TemporaryDirectory(prefix="driftbench-ssb-") as temporary:
            staged = Path(temporary)
            for table, columns in SCHEMAS.items():
                count = 0
                with inputs[table].open(encoding="utf-8", newline="") as src, (staged / f"{table}.csv").open("w", encoding="utf-8", newline="") as dst:
                    writer = csv.writer(dst)
                    writer.writerow(columns)
                    for line_number, line in enumerate(src, 1):
                        row = line.rstrip("\r\n")
                        if row.endswith("|"):
                            row = row[:-1]
                        values = row.split("|")
                        if len(values) != len(columns):
                            raise ValueError(f"{table}.tbl line {line_number}: expected {len(columns)} columns, got {len(values)}")
                        for column, value in zip(columns, values):
                            if column not in _TEXT:
                                try:
                                    int(value)
                                except ValueError as exc:
                                    raise ValueError(f"{table}.tbl line {line_number}: {column} must be an integer") from exc
                        writer.writerow(values)
                        count += 1
                if not count:
                    raise ValueError(f"{table}.tbl must contain at least one data row")
                counts[table] = count
            if provenance != {f"{table}.tbl": signature(path) for table, path in inputs.items()}:
                raise ValueError("SSB source files changed during import; retry with stable inputs")
            (staged / "schema.sql").write_text(_schema_sql(), encoding="utf-8")
            import shutil
            for relative in relatives:
                destination = root / relative
                destination.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(staged / destination.name, destination)
        files = [root / relative for relative in relatives]
        metadata = self._write_manifest(root / manifest_name, {
            "benchmark": "ssb", "artifact_type": "data", "mode": "import", "tables": counts,
            "dialect": "postgresql", "source_files": provenance, "source_contract": SOURCE_URL,
            "note": "Imports supplied dbgen tables. No generation, database loading or integrity/conformance audit is performed.",
            "files": relatives,
        }, parameters, root=root)
        return self._result(root, files, metadata)


@dataclass
class SSBQueries(BenchmarkArtifact):
    query_ids: list[str] | tuple[str, ...] | None = None
    benchmark: ClassVar[str] = "ssb"
    artifact_type: ClassVar[str] = "queries"

    def generate(self, output_dir: str | Path | None = None, force: bool = False) -> GenerationResult:
        if type(force) is not bool:
            raise ValueError("force must be a boolean")
        ids = list(_QUERIES) if self.query_ids is None else self.query_ids
        if not isinstance(ids, (list, tuple)) or not ids or any(not isinstance(qid, str) or qid not in _QUERIES for qid in ids) or len(set(ids)) != len(ids):
            raise ValueError(f"query_ids must be a nonempty list of unique SSB IDs: {', '.join(_QUERIES)}")
        ids = list(ids)
        parameters = {"query_ids": ids, "dialect": "postgresql", "template_version": 1}
        relatives = [f"ssb/queries/q{qid.replace('.', '_')}.sql" for qid in ids] + ["ssb/queries/schema.sql"]
        manifest_name = "ssb/queries/ssb_queries_manifest.json"
        root = checked_output(self, output_dir, relatives + [manifest_name])
        if not force:
            cached = self._load_existing(root / manifest_name, root, parameters)
            if cached is not None:
                return cached
        files = [self._write_text(root / relative, f"-- SSB query {qid}; PostgreSQL-compatible SQL.\n" + _QUERIES[qid]) for qid, relative in zip(ids, relatives)]
        files.append(self._write_text(root / relatives[-1], _schema_sql()))
        metadata = self._write_manifest(root / manifest_name, {
            "benchmark": "ssb", "artifact_type": "queries", "query_ids": ids,
            "dialect": "postgresql", "source_contract": SPEC_URL,
            "note": "DriftBench SQL renderings of the 13 SSB query definitions; no query execution or conformance audit.",
            "files": relatives,
        }, parameters, root=root)
        return self._result(root, files, metadata)


def data(source_dir: str | Path | None = None) -> SSBData:
    """Import five local ssb-dbgen .tbl files as headered CSV plus SQL schema."""
    return SSBData(source_dir=source_dir)


def queries(query_ids: list[str] | tuple[str, ...] | None = None) -> SSBQueries:
    """Export all 13 SSB SQL queries, or a selection such as ['1.1', '3.2']."""
    return SSBQueries(query_ids=query_ids)
