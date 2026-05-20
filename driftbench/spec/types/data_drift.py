# driftbench/spec/types/data_drift.py
import os, json, importlib
from typing import Any, Dict
import re
from ..registry import register
from ...core.schema.factory import get_schema_extractor
from ...core.data.single_table import SingleTableDriftGenerator
from ...core.data.multi_table import MultiTableDriftGenerator
import pandas as pd

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

def _load_filter_modules(modules: Any) -> None:
    if not modules:
        return
    if isinstance(modules, str):
        modules = [modules]
    for mod in modules:
        importlib.import_module(mod)


def _run_single_table(local_path: str, schema: Dict[str, Any], base_table: str,
                      drifts: list[Dict[str, Any]], filter_registry_modules: Any = None,
                      seed: int = 42) -> None:
    gen = SingleTableDriftGenerator(local_path, schema, base_table=base_table, seed=seed)
    _load_filter_modules(filter_registry_modules)
    for drift in drifts:
        _load_filter_modules(drift.get("filter_registry_modules") or drift.get("filter_func_module"))
        drift_type = drift.get("drift_type")
        out_path = drift.get("output_path")
        if not drift_type or not out_path:
            raise ValueError("Each drift needs 'drift_type' and 'output_path'.")
        _ensure_dir(out_path)
        kwargs = {k: v for k, v in drift.items() if k not in ("name", "drift_type", "output_path")}
        df = gen.apply_drift(drift_type=drift_type, **kwargs)
        df.to_csv(out_path, index=False)
        print(f"[DATA DRIFT OK] {drift.get('name', drift_type)} -> {out_path}")

_DDL_CACHE: Dict[str, Dict[str, list[str]]] = {}


def _parse_ddl_columns(ddl_path: str) -> Dict[str, list[str]]:
    if ddl_path in _DDL_CACHE:
        return _DDL_CACHE[ddl_path]
    text = ""
    with open(ddl_path, "r", encoding="utf-8", errors="ignore") as f:
        text = f.read()
    tables: Dict[str, list[str]] = {}
    for match in re.finditer(r"CREATE\s+TABLE\s+(\w+)\s*\((.*?)\);", text, re.S | re.I):
        name = match.group(1).strip().lower()
        body = match.group(2)
        cols: list[str] = []
        parts: list[str] = []
        buf: list[str] = []
        depth = 0
        for ch in body:
            if ch == "(":
                depth += 1
            elif ch == ")" and depth > 0:
                depth -= 1
            if ch == "," and depth == 0:
                parts.append("".join(buf))
                buf = []
                continue
            buf.append(ch)
        if buf:
            parts.append("".join(buf))
        for raw in parts:
            line = raw.strip()
            if not line:
                continue
            if line.lower().startswith(("primary", "foreign", "unique", "constraint")):
                continue
            cols.append(line.split()[0].lower())
        if cols:
            tables[name] = cols
    _DDL_CACHE[ddl_path] = tables
    return tables


def _columns_from_ddl(ddl_path: str, table_name: str) -> list[str]:
    tables = _parse_ddl_columns(ddl_path)
    key = table_name.lower()
    if key not in tables:
        raise ValueError(f"Table '{table_name}' not found in DDL: {ddl_path}")
    return tables[key]


def _load_table_frame(table_cfg: Dict[str, Any]) -> pd.DataFrame:
    path = table_cfg.get("path")
    if not path:
        raise ValueError("Table config requires 'path'.")
    fmt = (table_cfg.get("format") or "").lower()
    delimiter = table_cfg.get("delimiter")
    columns = table_cfg.get("columns")
    ddl_path = table_cfg.get("ddl_path")
    use_ddl = bool(table_cfg.get("use_ddl_columns", False))
    if ddl_path and (columns is None or use_ddl):
        table_name = table_cfg.get("ddl_table") or table_cfg.get("name")
        if not table_name:
            raise ValueError("DDL-based load requires table name (use 'name' or 'ddl_table').")
        columns = _columns_from_ddl(ddl_path, table_name)
        table_cfg["columns"] = columns
    drop_last_empty = bool(table_cfg.get("drop_last_empty", False))

    if fmt == "tbl" or delimiter or columns:
        sep = delimiter or "|"
        if columns:
            names = list(columns)
            if drop_last_empty:
                names.append("_extra")
            df = pd.read_csv(path, sep=sep, header=None, names=names)
            if drop_last_empty and "_extra" in df.columns:
                df = df.drop(columns=["_extra"])
            return df
        df = pd.read_csv(path, sep=sep, header=None)
        if drop_last_empty and df.shape[1] > 0:
            df = df.iloc[:, :-1]
        return df

    return pd.read_csv(path)

@register(family="data", category="drift", subtype="single_table")
def handle_data_single_table(spec: Dict[str, Any]) -> None:
    ds = spec.get("data_source", {}) or {}
    variables = spec.get("variables", {}) or {}
    filter_registry_modules = spec.get("filter_registry_modules") or variables.get("filter_registry_modules")
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
    _run_single_table(path, schema, base_table, variables.get("drifts", []),
                      filter_registry_modules=filter_registry_modules,
                      seed=int(spec.get("seed", 42)))

@register(family="data", category="drift", subtype="multi_table")
def handle_data_multi_table(spec: Dict[str, Any]) -> None:
    ds = spec.get("data_source", {}) or {}
    variables = spec.get("variables", {}) or {}
    filter_registry_modules = spec.get("filter_registry_modules") or variables.get("filter_registry_modules")
    tables = variables.get("tables")
    if not tables or not isinstance(tables, list):
        raise ValueError("variables.tables must be a non-empty list for multi_table.")

    drift_steps = variables.get("drift_steps")
    relationships = variables.get("relationships")
    if drift_steps:
        tables_data: Dict[str, pd.DataFrame] = {}
        output_paths: Dict[str, str] = {}
        table_keys: Dict[str, str] = {}
        ddl_path_default = variables.get("ddl_path")
        use_ddl_default = variables.get("use_ddl_columns")
        for tcfg in tables:
            name = tcfg.get("name") or tcfg.get("base_table") or "table"
            if ddl_path_default and "ddl_path" not in tcfg:
                tcfg["ddl_path"] = ddl_path_default
            if use_ddl_default is not None and "use_ddl_columns" not in tcfg:
                tcfg["use_ddl_columns"] = use_ddl_default
            tables_data[name] = _load_table_frame(tcfg)
            if tcfg.get("output_path"):
                output_paths[name] = tcfg["output_path"]
            if tcfg.get("key_column"):
                table_keys[name] = tcfg["key_column"]

        gen = MultiTableDriftGenerator(
            tables=tables_data,
            relationships=relationships or [],
            table_keys=table_keys,
            seed=int(spec.get("seed", 42)),
        )
        gen.apply_steps(drift_steps)
        if variables.get("validate_integrity", True):
            gen.validate_integrity()
        for name, df in gen.tables.items():
            out_path = output_paths.get(name)
            if not out_path:
                continue
            _ensure_dir(out_path)
            df.to_csv(out_path, index=False)
            print(f"[DATA DRIFT OK] {name} -> {out_path}")
        return

    pattern_id = spec.get("pattern_id", "data-drift")
    for tcfg in tables:
        name = tcfg.get("name") or tcfg.get("base_table") or "table"
        path = tcfg.get("path")
        base_table = tcfg.get("base_table")
        if not base_table: raise ValueError(f"Table '{name}' requires 'base_table'.")
        if not path: raise ValueError(f"Table '{name}' requires local 'path' to run drifts.")
        schema = _load_or_extract_schema_for_table(ds, tcfg, pattern_id)
        _run_single_table(path, schema, base_table, tcfg.get("drifts", []),
                          filter_registry_modules=filter_registry_modules,
                          seed=int(spec.get("seed", 42)))
