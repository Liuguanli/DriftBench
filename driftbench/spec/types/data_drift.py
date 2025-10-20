# driftbench/spec/types/data_drift.py
import os, json
from typing import Any, Dict
from ..registry import register
from ...core.schema.factory import get_schema_extractor
from ...core.data.single_table import SingleTableDriftGenerator

def _ensure_dir(p: str):
    d = os.path.dirname(p)
    if d and not os.path.exists(d):
        os.makedirs(d, exist_ok=True)

def _load_or_extract_schema_for_table(global_ds: Dict[str, Any],
                                      table_cfg: Dict[str, Any],
                                      pattern_id: str) -> Dict[str, Any]:
    se = (table_cfg.get("schema_extractor") or {}).copy()
    schema_path = table_cfg.get("schema_path") or se.get("schema_output_path")
    if schema_path and os.path.exists(schema_path):
        with open(schema_path, "r", encoding="utf-8") as f:
            return json.load(f)

    sample_size = int(se.get("sample_size", global_ds.get("schema_extractor", {}).get("sample_size", 1000)))
    source_type = se.get("source_type") or (table_cfg.get("kind") or global_ds.get("kind") or "csv")

    out_dir = global_ds.get("schema_extractor", {}).get("schema_output_dir") or \
              "./output/intermediate_yaml/schemas"
    _ensure_dir(out_dir)
    out_default = os.path.join(out_dir, f"{pattern_id}_{table_cfg.get('name','table')}_schema.json")
    schema_output_path = schema_path or se.get("schema_output_path") or out_default
    _ensure_dir(schema_output_path)

    if source_type == "csv":
        path = table_cfg.get("path"); 
        if not path: raise ValueError(f"CSV table '{table_cfg.get('name')}' requires 'path'.")
        extractor = get_schema_extractor(source_type="csv", csv_path=path, sample_size=sample_size)
    elif source_type == "parquet":
        path = table_cfg.get("path"); 
        if not path: raise ValueError(f"Parquet table '{table_cfg.get('name')}' requires 'path'.")
        extractor = get_schema_extractor(source_type="parquet", parquet_path=path, sample_size=sample_size)
    elif source_type == "postgres":
        if table_cfg.get("uri") and table_cfg.get("table"):
            extractor = get_schema_extractor(source_type="postgres",
                                             uri=table_cfg["uri"],
                                             table=table_cfg["table"],
                                             sample_size=sample_size)
        else:
            schema_name = table_cfg.get("schema_name") or global_ds.get("schema_name")
            if not schema_name: raise ValueError("Postgres schema extraction needs 'schema_name'.")
            if "db_config" in table_cfg and isinstance(table_cfg["db_config"], dict):
                db_config = table_cfg["db_config"]
            elif "db_config_path" in table_cfg:
                with open(table_cfg["db_config_path"], "r", encoding="utf-8") as f:
                    db_config = json.load(f)
            elif "db_config_path" in global_ds:
                with open(global_ds["db_config_path"], "r", encoding="utf-8") as f:
                    db_config = json.load(f)
            else:
                raise ValueError("Postgres schema extraction needs db_config or db_config_path.")
            extractor = get_schema_extractor(source_type="postgres",
                                             db_config=db_config, schema_name=schema_name, sample_size=sample_size)
    else:
        raise ValueError(f"Unsupported source_type: {source_type}")

    schema = extractor.extract_schema()
    with open(schema_output_path, "w", encoding="utf-8") as f:
        json.dump(schema, f, indent=2, default=str)
    return schema

def _run_single_table(local_path: str, schema: Dict[str, Any], base_table: str, drifts: list[Dict[str, Any]]) -> None:
    gen = SingleTableDriftGenerator(local_path, schema, base_table=base_table)
    for drift in drifts:
        drift_type = drift.get("drift_type")
        out_path = drift.get("output_path")
        if not drift_type or not out_path:
            raise ValueError("Each drift needs 'drift_type' and 'output_path'.")
        _ensure_dir(out_path)
        kwargs = {k: v for k, v in drift.items() if k not in ("name", "drift_type", "output_path")}
        df = gen.apply_drift(drift_type=drift_type, **kwargs)
        df.to_csv(out_path, index=False)
        print(f"[DATA DRIFT OK] {drift.get('name', drift_type)} -> {out_path}")

@register(family="data", category="drift", subtype="single_table")
def handle_data_single_table(spec: Dict[str, Any]) -> None:
    ds = spec.get("data_source", {}) or {}
    variables = spec.get("variables", {}) or {}
    base_table = variables.get("base_table")
    if not base_table: raise ValueError("variables.base_table is required.")
    path = ds.get("path")
    if not path: raise ValueError("data_source.path is required for single_table.")
    table_cfg = {
        "name": base_table,
        "kind": ds.get("kind", "csv"),
        "path": path,
        "base_table": base_table,
        "schema_extractor": ds.get("schema_extractor") or {},
    }
    schema = _load_or_extract_schema_for_table(ds, table_cfg, spec.get("pattern_id", "data-drift"))
    _run_single_table(path, schema, base_table, variables.get("drifts", []))

@register(family="data", category="drift", subtype="multi_table")
def handle_data_multi_table(spec: Dict[str, Any]) -> None:
    ds = spec.get("data_source", {}) or {}
    variables = spec.get("variables", {}) or {}
    tables = variables.get("tables")
    if not tables or not isinstance(tables, list):
        raise ValueError("variables.tables must be a non-empty list for multi_table.")
    pattern_id = spec.get("pattern_id", "data-drift")
    for tcfg in tables:
        name = tcfg.get("name") or tcfg.get("base_table") or "table"
        path = tcfg.get("path")
        base_table = tcfg.get("base_table")
        if not base_table: raise ValueError(f"Table '{name}' requires 'base_table'.")
        if not path: raise ValueError(f"Table '{name}' requires local 'path' to run drifts.")
        schema = _load_or_extract_schema_for_table(ds, tcfg, pattern_id)
        _run_single_table(path, schema, base_table, tcfg.get("drifts", []))
