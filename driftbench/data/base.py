from __future__ import annotations

import csv
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional


class OutputDirRequiredError(ValueError):
    """Kept for backwards compatibility. No longer raised by the library."""


def get_default_data_dir() -> Path:
    """Return the default directory for generated benchmark artifacts.

    Checks DRIFTBENCH_DATA_DIR env var first; falls back to ~/.driftbench/data/.
    """
    env = os.environ.get("DRIFTBENCH_DATA_DIR", "").strip()
    if env:
        return Path(env).expanduser().resolve()
    return Path.home() / ".driftbench" / "data"


# Column headers for the headerless pipe-delimited tables produced by the
# benchmark adapters. Keyed by (benchmark, table_stem). TPC-H follows the
# canonical TPC-H spec column order (verified against dbgen output); TPC-DS
# follows the synthetic field order emitted by TPCDSData._generate_synth.
# When a (benchmark, stem) is absent, as_csv() falls back to a headerless
# conversion (unchanged behaviour).
_TBL_SCHEMAS: Dict[tuple[str, str], List[str]] = {
    ("tpch", "region"): ["r_regionkey", "r_name", "r_comment"],
    ("tpch", "nation"): ["n_nationkey", "n_name", "n_regionkey", "n_comment"],
    ("tpch", "part"): [
        "p_partkey", "p_name", "p_mfgr", "p_brand", "p_type",
        "p_size", "p_container", "p_retailprice", "p_comment",
    ],
    ("tpch", "supplier"): [
        "s_suppkey", "s_name", "s_address", "s_nationkey",
        "s_phone", "s_acctbal", "s_comment",
    ],
    ("tpch", "partsupp"): [
        "ps_partkey", "ps_suppkey", "ps_availqty", "ps_supplycost", "ps_comment",
    ],
    ("tpch", "customer"): [
        "c_custkey", "c_name", "c_address", "c_nationkey", "c_phone",
        "c_acctbal", "c_mktsegment", "c_comment",
    ],
    ("tpch", "orders"): [
        "o_orderkey", "o_custkey", "o_orderstatus", "o_totalprice",
        "o_orderdate", "o_orderpriority", "o_clerk", "o_shippriority",
        "o_comment",
    ],
    ("tpch", "lineitem"): [
        "l_orderkey", "l_partkey", "l_suppkey", "l_linenumber",
        "l_quantity", "l_extendedprice", "l_discount", "l_tax",
        "l_returnflag", "l_linestatus", "l_shipdate", "l_commitdate",
        "l_receiptdate", "l_shipinstruct", "l_shipmode", "l_comment",
    ],
    ("tpcds", "date_dim"): [
        "d_date_sk", "d_date_id", "d_date", "d_year", "d_moy", "d_dom",
    ],
    ("tpcds", "store"): ["s_store_sk", "s_store_id", "s_store_name", "s_status"],
    ("tpcds", "item"): [
        "i_item_sk", "i_item_id", "i_item_desc", "i_category", "i_current_price",
    ],
    ("tpcds", "customer"): [
        "c_customer_sk", "c_customer_id", "c_first_name",
        "c_last_name", "c_email_address",
    ],
    ("tpcds", "store_sales"): [
        "ss_sold_date_sk", "ss_item_sk", "ss_customer_sk", "ss_store_sk",
        "ss_quantity", "ss_net_paid", "ss_net_profit",
    ],
}

_DRIFT_RESERVED_PARAMS = ("table", "drift_type", "seed", "output_path")


@dataclass(frozen=True)
class GenerationResult:
    benchmark: str
    artifact_type: str
    output_dir: Path
    files: list[Path]
    metadata: Path

    def summary(self) -> dict[str, Any]:
        """Return a lightweight, JSON-serializable summary of this result.

        Keys: ``benchmark``, ``artifact_type``, ``output_dir`` (str),
        ``file_count`` (int), ``tables`` (sorted unique stems). Designed
        for logging, UI surfaces, and quick assertions without reaching
        into dataclass internals or filesystem paths.
        """
        return {
            "benchmark": self.benchmark,
            "artifact_type": self.artifact_type,
            "output_dir": str(self.output_dir),
            "file_count": len(self.files),
            "tables": sorted({f.stem for f in self.files}),
        }

    def as_csv(self) -> "GenerationResult":
        """Convert any pipe-delimited .tbl/.dat files to .csv and return a new result.

        Non-.tbl/.dat files are carried over unchanged. The original files are
        kept alongside the new .csv files. When a column schema is known for
        (benchmark, table_stem), a header row is written so the CSV is
        self-describing and usable directly by .drift(); otherwise the CSV is
        written headerless (unchanged legacy behaviour).
        """
        converted: list[Path] = []
        for f in self.files:
            if f.suffix not in (".tbl", ".dat"):
                converted.append(f)
                continue
            csv_path = f.with_suffix(".csv")
            header = _TBL_SCHEMAS.get((self.benchmark, f.stem))
            with f.open(encoding="utf-8") as src, csv_path.open("w", encoding="utf-8", newline="") as dst:
                writer = csv.writer(dst)
                if header is not None:
                    writer.writerow(header)
                for line in src:
                    row = line.rstrip("\n").rstrip("|").split("|")
                    writer.writerow(row)
            converted.append(csv_path)
        return GenerationResult(
            benchmark=self.benchmark,
            artifact_type=self.artifact_type,
            output_dir=self.output_dir,
            files=converted,
            metadata=self.metadata,
        )

    def drift(
        self,
        table: str,
        drift_type: str,
        output_path: Optional[str | Path] = None,
        seed: int = 42,
        **params: Any,
    ) -> "GenerationResult":
        """Apply single-table drift to one benchmark table.

        Args:
            table: stem name of the CSV (e.g. "lineitem").
            drift_type: one of the drift types supported by SingleTableDriftGenerator.
            output_path: destination CSV; defaults to <output_dir>/<table>_<drift_type>.csv.
            seed: random seed forwarded to the drift generator.
            **params: keyword arguments forwarded to apply_drift().

        Returns:
            A new GenerationResult pointing at the drifted CSV.
        """
        reserved = [k for k in _DRIFT_RESERVED_PARAMS if k in params]
        if reserved:
            raise TypeError(
                f"drift() received reserved keyword(s) {reserved} in **params; "
                f"pass them as dedicated arguments instead "
                f"({', '.join(_DRIFT_RESERVED_PARAMS)})."
            )

        from driftbench.core.schema.csv_extractor import CSVSchemaExtractor
        from driftbench.core.data.single_table import SingleTableDriftGenerator

        csv_file: Optional[Path] = None
        for f in self.files:
            if f.suffix == ".csv" and f.stem == table:
                csv_file = f
                break
        if csv_file is None:
            unconverted = next(
                (
                    f
                    for f in self.files
                    if f.suffix in (".tbl", ".dat") and f.stem == table
                ),
                None,
            )
            if unconverted is not None:
                raise ValueError(
                    f"Table '{table}' is in {unconverted.suffix} format; "
                    f"call .as_csv() before .drift()."
                )
            raise ValueError(
                f"No CSV file for table '{table}' in result. "
                f"Available files: {[f.name for f in self.files]}"
            )

        schema = CSVSchemaExtractor(str(csv_file)).extract_schema()
        gen = SingleTableDriftGenerator(str(csv_file), schema, table, seed=seed)
        drifted_df = gen.apply_drift(drift_type, **params)

        if output_path is None:
            out = self.output_dir / f"{table}_{drift_type}.csv"
        else:
            out = Path(output_path).expanduser().resolve()

        out.parent.mkdir(parents=True, exist_ok=True)
        drifted_df.to_csv(out, index=False)
        # Emit the equivalent DriftSpec YAML as a hidden side artifact so the
        # exact same drift is reproducible/shareable via the spec engine
        # (driftbench.spec.core.run_all). Kept out of result.files; discoverable
        # through the manifest's "driftspec" key.
        spec_path = self._emit_drift_spec(
            source_csv=csv_file,
            table=table,
            drift_type=drift_type,
            out=out,
            seed=seed,
            params=params,
        )
        metadata = self._write_drift_manifest(
            output_dir=out.parent,
            manifest_name=f"{out.stem}_manifest.json",
            files=[out],
            extra={
                "drift_kind": "single_table",
                "source_files": [str(f) for f in self.files],
                "table": table,
                "drift_type": drift_type,
                "seed": seed,
                "params": params,
                "driftspec": spec_path.name,
            },
        )

        return GenerationResult(
            benchmark=self.benchmark,
            artifact_type=self.artifact_type,
            output_dir=out.parent,
            files=[out],
            metadata=metadata,
        )

    def drift_multi(
        self,
        steps: List[Dict[str, Any]],
        output_dir: Optional[str | Path] = None,
        seed: int = 42,
        relationships: Optional[List[Dict[str, Any]]] = None,
    ) -> "GenerationResult":
        """Apply multi-table drift across all benchmark tables.

        Args:
            steps: list of drift_step dicts forwarded to MultiTableDriftGenerator.apply_steps().
            output_dir: directory for drifted CSVs; defaults to self.output_dir.
            seed: random seed forwarded to the drift generator.
            relationships: FK relationship list; if None, uses _known_relationships(self.benchmark).

        Returns:
            A new GenerationResult pointing at all drifted CSV files.
        """
        import pandas as pd
        from driftbench.core.data.multi_table import MultiTableDriftGenerator

        tables: Dict[str, Any] = {}
        for f in self.files:
            if f.suffix == ".csv":
                tables[f.stem] = pd.read_csv(f)

        if not tables:
            raise ValueError("No CSV files found in result for multi-table drift.")

        rels = relationships if relationships is not None else _known_relationships(self.benchmark)
        gen = MultiTableDriftGenerator(tables, rels, seed=seed)
        gen.apply_steps(steps)

        if output_dir is None:
            out_dir = self.output_dir
        else:
            out_dir = Path(output_dir).expanduser().resolve()
        out_dir.mkdir(parents=True, exist_ok=True)

        out_files: List[Path] = []
        for name, df in gen.tables.items():
            out_path = out_dir / f"{name}_drifted.csv"
            df.to_csv(out_path, index=False)
            out_files.append(out_path)
        metadata = self._write_drift_manifest(
            output_dir=out_dir,
            manifest_name=f"{self.benchmark}_multi_drift_manifest.json",
            files=out_files,
            extra={
                "drift_kind": "multi_table",
                "source_files": [str(f) for f in self.files],
                "steps": steps,
                "seed": seed,
                "relationships": rels,
            },
        )

        return GenerationResult(
            benchmark=self.benchmark,
            artifact_type=self.artifact_type,
            output_dir=out_dir,
            files=out_files,
            metadata=metadata,
        )

    def _write_drift_manifest(
        self,
        output_dir: Path,
        manifest_name: str,
        files: list[Path],
        extra: Optional[Dict[str, Any]] = None,
    ) -> Path:
        payload: Dict[str, Any] = {
            "benchmark": self.benchmark,
            "artifact_type": self.artifact_type,
            "files": [str(path.resolve().relative_to(output_dir.resolve())) for path in files],
        }
        if extra:
            payload.update(extra)
        metadata = output_dir / manifest_name
        metadata.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
        return metadata

    def _emit_drift_spec(
        self,
        source_csv: Path,
        table: str,
        drift_type: str,
        out: Path,
        seed: int,
        params: Dict[str, Any],
    ) -> Path:
        """Serialize the equivalent DriftSpec YAML next to the drifted CSV.

        Running the emitted file through driftbench.spec.core.run_all reproduces
        a byte-identical drifted CSV: same source, seed, drift_type and params,
        and ``sample_size: 0`` so the spec engine extracts schema from the full
        CSV exactly as the Python path does.
        """
        import yaml

        spec_path = out.parent / f"{out.stem}.driftspec.yaml"
        spec: Dict[str, Any] = {
            "pattern_id": f"{self.benchmark}-{table}-{drift_type}",
            "seed": int(seed),
            "type": {"family": "data", "category": "drift", "subtype": "single_table"},
            "data_source": {
                "kind": "csv",
                "path": str(source_csv.resolve()),
                "schema_extractor": {
                    "source_type": "csv",
                    "sample_size": 0,
                    "schema_output_path": str(
                        out.parent / f"{out.stem}_schema.json"
                    ),
                },
            },
            "variables": {
                "base_table": table,
                "drifts": [
                    {
                        "name": drift_type,
                        "drift_type": drift_type,
                        "output_path": str(out.resolve()),
                        **_yaml_safe(params),
                    }
                ],
            },
        }
        with spec_path.open("w", encoding="utf-8") as f:
            yaml.safe_dump(spec, f, default_flow_style=False, sort_keys=False)
        return spec_path


def _yaml_safe(value: Any) -> Any:
    """Recursively coerce values to YAML-native types.

    Primitives and (possibly nested) lists/dicts of primitives pass through
    unchanged; anything else (Path, numpy scalar, custom object) is stringified
    so the emitted DriftSpec never fails to serialize and stays readable.
    """
    if isinstance(value, dict):
        return {str(k): _yaml_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_yaml_safe(v) for v in value]
    if value is None or isinstance(value, (str, bool, int, float)):
        return value
    return str(value)


def _known_relationships(benchmark: str) -> List[Dict[str, Any]]:
    """Return hard-coded FK relationships for supported benchmarks."""
    if benchmark == "tpch":
        return [
            {"name": "lineitem_orders",   "fact": "lineitem", "fk": "l_orderkey",  "dim": "orders",   "pk": "o_orderkey"},
            {"name": "lineitem_part",      "fact": "lineitem", "fk": "l_partkey",   "dim": "part",     "pk": "p_partkey"},
            {"name": "lineitem_supplier",  "fact": "lineitem", "fk": "l_suppkey",   "dim": "supplier", "pk": "s_suppkey"},
            {"name": "orders_customer",    "fact": "orders",   "fk": "o_custkey",   "dim": "customer", "pk": "c_custkey"},
            {"name": "customer_nation",    "fact": "customer", "fk": "c_nationkey", "dim": "nation",   "pk": "n_nationkey"},
            {"name": "supplier_nation",    "fact": "supplier", "fk": "s_nationkey", "dim": "nation",   "pk": "n_nationkey"},
            {"name": "nation_region",      "fact": "nation",   "fk": "n_regionkey", "dim": "region",   "pk": "r_regionkey"},
        ]
    if benchmark == "job":
        return [
            {"name": "movie_info_title",      "fact": "movie_info",      "fk": "movie_id",   "dim": "title",        "pk": "id"},
            {"name": "cast_info_title",       "fact": "cast_info",       "fk": "movie_id",   "dim": "title",        "pk": "id"},
            {"name": "cast_info_name",        "fact": "cast_info",       "fk": "person_id",  "dim": "name",         "pk": "id"},
            {"name": "movie_companies_title", "fact": "movie_companies", "fk": "movie_id",   "dim": "title",        "pk": "id"},
            {"name": "movie_companies_co",    "fact": "movie_companies", "fk": "company_id", "dim": "company_name", "pk": "id"},
            {"name": "movie_keyword_title",   "fact": "movie_keyword",   "fk": "movie_id",   "dim": "title",        "pk": "id"},
            {"name": "movie_keyword_kw",      "fact": "movie_keyword",   "fk": "keyword_id", "dim": "keyword",      "pk": "id"},
        ]
    return []


class BenchmarkArtifact:
    """Common utilities for benchmark data/query generators."""

    benchmark: str
    artifact_type: str

    def _require_output_dir(self, output_dir: str | Path | None) -> Path:
        """Resolve output_dir, defaulting to get_default_data_dir() if None."""
        if output_dir is None:
            root = get_default_data_dir()
            print(
                f"[driftbench] No output_dir specified. "
                f"Writing {self.benchmark}/{self.artifact_type} to: {root}"
            )
        else:
            root = Path(output_dir).expanduser().resolve()
        root.mkdir(parents=True, exist_ok=True)
        return root

    def _load_existing(self, manifest_path: Path, root: Path) -> GenerationResult | None:
        """Return a GenerationResult if all previously generated files still exist.

        Returns None when the manifest is missing, unreadable, or any listed
        file has been deleted — signalling that generation must proceed.
        """
        if not manifest_path.exists():
            return None
        try:
            payload = json.loads(manifest_path.read_text(encoding="utf-8"))
            rel_paths: list[str] = payload.get("files", [])
            if not rel_paths:
                return None
            files = [root / p for p in rel_paths]
            if all(f.exists() and f.is_file() for f in files):
                return GenerationResult(
                    benchmark=self.benchmark,
                    artifact_type=self.artifact_type,
                    output_dir=root,
                    files=files,
                    metadata=manifest_path,
                )
        except Exception:
            pass
        return None

    def _write_text(self, path: Path, content: str) -> Path:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
        return path

    def _write_json(self, path: Path, payload: dict[str, Any]) -> Path:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
        return path

    def _paths_relative_to(self, base: Path, paths: list[Path]) -> list[str]:
        base_resolved = base.resolve()
        return [str(path.resolve().relative_to(base_resolved)) for path in paths]

    def _result(
        self,
        output_dir: Path,
        files: list[Path],
        metadata: Path,
    ) -> GenerationResult:
        return GenerationResult(
            benchmark=self.benchmark,
            artifact_type=self.artifact_type,
            output_dir=output_dir,
            files=files,
            metadata=metadata,
        )
