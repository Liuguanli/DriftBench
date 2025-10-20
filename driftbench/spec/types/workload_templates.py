# driftbench/spec/types/workload_templates.py
import os, json
from typing import Dict, Any, List, Tuple

from ...core.schema.factory import get_schema_extractor
from ...core.workload.template_generator import TemplateGenerator, TemplateGeneratorMulti
from ...core.workload.sql_generator import generate_sql_queries
from ...core.temporal.time_stamp_generator import generate_timestamps
from ...core.utils import save_templates, save_sqls, save_sqls_with_timestamps

from ..registry import register

from datetime import date, datetime
from decimal import Decimal
import numpy as np

def _build_single_table_schema_from_db(
    variables: Dict[str, Any],
    ds: Dict[str, Any],
    raw_schema: Dict[str, Any],
    base_table: str,
) -> Dict[str, Any]:
    """Pick one table from a DB-level schema and wrap it as single-table schema."""
    physical_hint = ds.get("physical_table") or base_table

    # locate container
    containers = None
    for key in ("tables", "relations", "models"):
        if isinstance(raw_schema.get(key), dict):
            containers = raw_schema[key]
            break

    if containers:
        target = physical_hint or next(iter(containers.keys()))
        entry = containers.get(target)
        if entry is None:
            bare = (target or "").split(".")[-1].strip('"')
            for k in containers:
                if k.endswith(f".{bare}") or k.endswith(f'"{bare}"'):
                    entry = containers[k]; target = k; break
        if entry is None:
            k0 = next(iter(containers.keys())); entry = containers[k0]; target = k0
        cols = entry.get("columns") or entry
        phys = target.split(".")[-1].strip('"') if "." in target else target
    else:
        # table-level fallback
        cols = raw_schema["columns"]
        phys = raw_schema.get("table") or physical_hint or base_table

    nrows = _resolve_num_rows(variables, ds, phys, base_table)
    return {
        "source": {"table": phys, "columns": cols},
        "tables": {base_table: {"table": phys, "columns": cols, "num_rows": nrows}},
    }

# ---------------- JSON safety ----------------
def _json_safe(x):
    if x is None or isinstance(x, (bool, int, float, str)):
        return x
    if isinstance(x, (date, datetime)):
        return x.isoformat()
    if isinstance(x, Decimal):
        return float(x)
    if isinstance(x, (np.integer,)):
        return int(x)
    if isinstance(x, (np.floating,)):
        return float(x)
    if isinstance(x, (np.ndarray,)):
        return x.tolist()
    if isinstance(x, (list, tuple)):
        return [_json_safe(v) for v in x]
    if isinstance(x, dict):
        return {k: _json_safe(v) for k, v in x.items()}
    return str(x)

def _ensure_dir(p: str):
    d = os.path.dirname(p)
    if d and not os.path.exists(d):
        os.makedirs(d, exist_ok=True)

def _fix_intermediate_yaml(p: str) -> str:
    if not isinstance(p, str):
        return p
    return (p
            .replace("output/intermediate/", "output/intermediate_yaml/")
            .replace("output\\intermediate\\", "output\\intermediate_yaml\\"))

# ---------------- helpers: num_rows & multi schema ----------------
def _resolve_num_rows(variables: Dict[str, Any], ds: Dict[str, Any],
                      physical_or_logical: str, base_table: str | None) -> int:
    """Find row count hint for a table."""
    ts = ds.get("table_stats") or {}
    if isinstance(ts, dict):
        e = ts.get(physical_or_logical) or ts.get(physical_or_logical.lower())
        if isinstance(e, dict) and "num_rows" in e:
            return int(e["num_rows"])
    vts = variables.get("table_stats") or {}
    if isinstance(vts, dict):
        e = vts.get(physical_or_logical) or (base_table and vts.get(base_table))
        if isinstance(e, dict) and "num_rows" in e:
            return int(e["num_rows"])
    if "num_rows" in ds:
        return int(ds["num_rows"])
    if "num_rows" in variables:
        return int(variables["num_rows"])
    return 100000

def _build_multi_table_schema_from_db(
    variables: Dict[str, Any],
    ds: Dict[str, Any],
    raw_schema: Dict[str, Any],
    phys_tables: List[str],
) -> Dict[str, Any]:
    """
    Build a TemplateGeneratorMulti-friendly schema:
      {
        "source": { "table": <first_phys>, "columns": [...] },
        "tables": {
          "<phys>": { "table": <phys>, "columns": [...], "num_rows": <int> },
          ...
        }
      }
    """
    # locate container of tables in DB-level schema
    containers = None
    for key in ("tables", "relations", "models"):
        if isinstance(raw_schema.get(key), dict):
            containers = raw_schema[key]
            break

    # fallback if single table returned by extractor
    if containers is None and "table" in raw_schema and "columns" in raw_schema:
        phys = raw_schema.get("table")
        nrows = _resolve_num_rows(variables, ds, phys, variables.get("base_table"))
        return {
            "source": {"table": phys, "columns": raw_schema["columns"]},
            "tables": {phys: {"table": phys, "columns": raw_schema["columns"], "num_rows": nrows}},
        }
    if containers is None:
        raise ValueError("Cannot locate table collection in schema (expected keys: tables/relations/models).")

    def _find_entry(name: str):
        if name in containers:
            return name, containers[name]
        bare = name.split(".")[-1].strip('"')
        for k in containers:
            if k.endswith(f".{bare}") or k.endswith(f'"{bare}"'):
                return k, containers[k]
        return None, None

    tables: Dict[str, Any] = {}
    for phys in phys_tables:
        k, entry = _find_entry(phys)
        if entry is None:
            raise ValueError(f"Table '{phys}' not found in extracted schema.")
        cols = entry.get("columns") or entry
        nrows = _resolve_num_rows(variables, ds, phys, variables.get("base_table"))
        tables[phys] = {"table": phys, "columns": cols, "num_rows": nrows}

    first = phys_tables[0]
    return {"source": {"table": first, "columns": tables[first]["columns"]}, "tables": tables}

# ---------------- schema extraction / loading ----------------
def _extract_or_load_schema(spec: Dict[str, Any]) -> Dict[str, Any]:
    """
    Return either:
      (A) Single-table schema:
          {
            "source": {"table": "<phys>", "columns": [...]},
            "tables": { "<base>": {"table": "<phys>", "columns": [...], "num_rows": <int>} }
          }
      or
      (B) DB-level container (for multi runs to build on):
          { "__db_level_schema__": <raw_schema> }
    """
    variables = spec["variables"]
    base_table = variables.get("base_table")
    ds = spec.get("data_source") or {}
    se = ds.get("schema_extractor", {})
    source_type = se.get("source_type") or ds.get("kind")

    # If any run requests multi (candidate_tables), we defer to run-time:
    need_multi = any(("candidate_tables" in r) or ("candidate_tables_ref" in r)
                     for r in variables.get("runs", []))

    def _save_and_return_single(phys: str, cols: List[Dict[str, Any]]) -> Dict[str, Any]:
        nrows = _resolve_num_rows(variables, ds, phys, base_table)
        schema_obj = {
            "source": {"table": phys, "columns": cols},
            "tables": {base_table: {"table": phys, "columns": cols, "num_rows": nrows}},
        }
        out_path = _fix_intermediate_yaml(
            se.get("schema_output_path", f"./output/intermediate_yaml/{spec.get('pattern_id','spec')}_schema.json")
        )
        _ensure_dir(out_path)
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(schema_obj, f, indent=2, default=str)
        return schema_obj

    # ---- If a schema file is provided, normalize and inject num_rows ----
    schema_path = variables.get("schema_path")
    if schema_path:
        with open(schema_path, "r", encoding="utf-8") as f:
            raw = json.load(f)

        # If already multi-table, ensure num_rows and source exist; return as-is for runs
        if "tables" in raw and isinstance(raw["tables"], dict) and len(raw["tables"]) >= 1:
            for k, v in raw["tables"].items():
                if "num_rows" not in v:
                    v["num_rows"] = _resolve_num_rows(variables, ds, k, base_table)
            if "source" not in raw:
                first = next(iter(raw["tables"].keys()))
                raw["source"] = {"table": first, "columns": raw["tables"][first]["columns"]}
            return raw

        # Otherwise adapt to single-table
        if "source" in raw and "columns" in raw["source"]:
            phys = raw["source"].get("table") or base_table
            return _save_and_return_single(phys, raw["source"]["columns"])
        if "table" in raw and "columns" in raw:
            phys = raw.get("table") or base_table
            return _save_and_return_single(phys, raw["columns"])
        raise ValueError("Provided schema file is missing required fields.")

    # ---- Otherwise extract from data_source (CSV/Parquet/Postgres) ----
    if source_type == "csv":
        csv_path = ds.get("path")
        if not csv_path:
            raise ValueError("For CSV, set data_source.path")
        extractor = get_schema_extractor("csv", csv_path=csv_path, sample_size=se.get("sample_size", 1000))
        sch = extractor.extract_schema()  # {"table": "...", "columns": [...]}
        phys = sch.get("table") or os.path.basename(csv_path)
        return _save_and_return_single(phys, sch["columns"])

    if source_type == "parquet":
        pq_path = ds.get("path")
        if not pq_path:
            raise ValueError("For Parquet, set data_source.path")
        extractor = get_schema_extractor("parquet", parquet_path=pq_path, sample_size=se.get("sample_size", 1000))
        sch = extractor.extract_schema()
        phys = sch.get("table") or os.path.basename(pq_path)
        return _save_and_return_single(phys, sch["columns"])

    if source_type == "postgres":
        kw = {"source_type": "postgres", "sample_size": se.get("sample_size", 1000)}
        if ds.get("uri") and ds.get("table"):
            kw.update(uri=ds["uri"], table=ds["table"])
        else:
            schema_name = ds.get("schema_name") or se.get("schema_name")
            if not schema_name:
                raise ValueError("Postgres requires 'schema_name' when using db_config/db_config_path.")
            if "db_config" in ds and isinstance(ds["db_config"], dict):
                db_config = ds["db_config"]
            elif "db_config_path" in ds:
                with open(ds["db_config_path"], "r", encoding="utf-8") as f:
                    db_config = json.load(f)
            else:
                raise ValueError("Provide either uri+table or db_config/db_config_path for Postgres.")
            kw.update(db_config=db_config, schema_name=schema_name)

        extractor = get_schema_extractor(**kw)
        raw_schema = extractor.extract_schema()

        # If any run needs multi, return DB-level and let runs choose tables:
        if need_multi:
            return {"__db_level_schema__": raw_schema}

        # Otherwise: single-table pick
        physical_hint = ds.get("physical_table") or base_table
        containers = None
        for key in ("tables", "relations", "models"):
            if isinstance(raw_schema.get(key), dict):
                containers = raw_schema[key]; break
        if containers:
            target = physical_hint or next(iter(containers.keys()))
            entry = containers.get(target)
            if entry is None:
                bare = (target or "").split(".")[-1].strip('"')
                for k in containers:
                    if k.endswith(f".{bare}") or k.endswith(f'"{bare}"'):
                        entry = containers[k]; target = k; break
            if entry is None:
                k0 = next(iter(containers.keys())); entry = containers[k0]; target = k0
            cols = entry.get("columns") or entry
            phys = target.split(".")[-1].strip('"') if "." in target else target
        else:
            cols = raw_schema["columns"]
            phys = raw_schema.get("table") or physical_hint or base_table

        return _save_and_return_single(phys, cols)

    raise ValueError(f"Unsupported source_type: {source_type}")

# ---------------- main handler ----------------
@register(family="workload", category="templates", subtype="selection_payload")
def handle_workload_templates(spec: Dict[str, Any]) -> None:
    variables = spec["variables"]
    ds = spec.get("data_source") or {}

    # Could be single-table schema or a DB-level container for multi runs
    schema_or_db = _extract_or_load_schema(spec)

    # 1) Generate templates per run
    defaults = variables.get("defaults", {})
    run_index: Dict[str, str] = {}

    for run in variables.get("runs", []):
        cfg = dict(defaults); cfg.update(run)
        outp = _fix_intermediate_yaml(cfg["output_path"])
        _ensure_dir(outp)

        # Detect multi-table request
        candidate_tables = cfg.get("candidate_tables")
        if not candidate_tables and "candidate_tables_ref" in cfg:
            cand_sets = variables.get("candidate_sets", {})
            candidate_tables = cand_sets.get(cfg["candidate_tables_ref"])

        if candidate_tables:
            # MULTI-TABLE branch
            # If we have DB-level schema, build a multi schema for this run
            if "__db_level_schema__" in schema_or_db:
                multi_schema = _build_multi_table_schema_from_db(
                    variables, ds, schema_or_db["__db_level_schema__"], candidate_tables
                )
            else:
                # If schema_path already provided multi-table schema, reuse it
                multi_schema = schema_or_db
                # Ensure num_rows exists for all selected tables
                for phys in candidate_tables:
                    if phys not in multi_schema.get("tables", {}):
                        raise ValueError(f"Table '{phys}' is not present in provided schema_path.")
                    if "num_rows" not in multi_schema["tables"][phys]:
                        multi_schema["tables"][phys]["num_rows"] = _resolve_num_rows(
                            variables, ds, phys, variables.get("base_table")
                        )

            gen = TemplateGeneratorMulti(multi_schema, candidate_tables=candidate_tables, seed=spec.get("seed", 42))
            templates = gen.generate_templates(
                num_templates=cfg.get("num_templates", 5),
                max_predicates=cfg.get("max_predicates", 3),
                max_payload_columns=cfg.get("max_payload_columns", 2),
                selectivity=cfg.get("selectivity") or {},
                value_range=cfg.get("value_range") or variables.get("defaults", {}).get("value_range"),
                join_count=int(cfg.get("join_count", variables.get("defaults", {}).get("join_count", 1))),
            )
        else:
            # SINGLE-TABLE branch
            base_table = variables["base_table"]
            # NEW: if we only have DB-level schema (because some run is multi),
            # build a single-table schema for this run.
            if "__db_level_schema__" in schema_or_db:
                schema = _build_single_table_schema_from_db(
                    variables, ds, schema_or_db["__db_level_schema__"], base_table
                )
            else:
                schema = schema_or_db
            # Defensive: ensure num_rows exists
            if "num_rows" not in schema["tables"][base_table]:
                schema["tables"][base_table]["num_rows"] = _resolve_num_rows(
                    variables, ds, schema["tables"][base_table]["table"], base_table
                )
            gen = TemplateGenerator(schema, base_table=base_table)
            kwargs = {k: cfg[k] for k in ["num_templates","max_predicates","max_payload_columns","selectivity","value_range"] if k in cfg}
            templates = gen.generate_templates(**kwargs)

        templates = _json_safe(templates)
        save_templates(templates, outp)
        if "name" in run:
            run_index[run["name"]] = outp
        print(f"[TEMPLATES OK] {run.get('name','')} -> {outp}")

    # 2) Generate SQL queries (optional)
    for qrun in variables.get("query_runs", []):
        ref = qrun["template"]
        if ref in run_index:
            template_file = run_index[ref]
        else:
            template_file = _fix_intermediate_yaml(ref)

        qpt = int(qrun.get("queries_per_template", 300))
        dist_config = qrun.get("dist_config", {})

        sqls = generate_sql_queries(template_file=template_file, dist_config=dist_config, queries_per_template=qpt)
        print(f"[SQL OK] {qrun.get('name','')} using {template_file} -> {len(sqls)} queries")

        for out in qrun.get("outputs", []):
            out_path = out["path"]; _ensure_dir(out_path)
            ts = out.get("timestamp")
            if ts is None:
                save_sqls(_json_safe(sqls), out_path)
                print(f"[WRITE] workload -> {out_path}")
            else:
                timestamps = generate_timestamps(
                    count=len(sqls),
                    start_time=ts.get("start_time", "2025-07-01T00:00:00"),
                    pattern=ts.get("pattern", "uniform"),
                    queries_per_minute=int(ts.get("queries_per_minute", 300)),
                )
                save_sqls_with_timestamps(_json_safe(sqls), _json_safe(timestamps), out_path)
                print(f"[WRITE] temporal ({ts.get('pattern')}) -> {out_path}")
