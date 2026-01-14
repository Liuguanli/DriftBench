from __future__ import annotations

import csv
import json
import os
from typing import Any, Dict, List

_META_FIELDS = {
    "trace_type",
    "pattern_id",
    "seed",
    "base_table",
    "type_family",
    "type_category",
    "type_subtype",
    "data_source_kind",
    "data_source_path",
    "schema_source_type",
    "schema_sample_size",
    "schema_output_path",
    "data_source_json",
}

_ALIAS_FIELDS = {
    "drift_name": "name",
    "run_name": "name",
    "template_name": "template",
}


def trace_to_spec(
    trace_path: str,
    output_path: str,
    trace_type: str | None = None,
    mapping_path: str | None = None,
) -> Dict[str, Any]:
    mapping = _load_mapping(mapping_path) if mapping_path else None
    if mapping:
        spec = build_spec_from_mapping(trace_path, mapping, trace_type=trace_type)
    else:
        summary = load_trace_summary(trace_path)
        spec = build_spec(summary, trace_type=trace_type)
    write_spec_yaml(spec, output_path)
    return spec


def load_trace_summary(trace_path: str) -> Dict[str, Any]:
    if trace_path.lower().endswith(".json"):
        with open(trace_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            return _normalize_trace_summary(_trace_records_to_summary(data))
        if isinstance(data, dict):
            return _normalize_trace_summary(data)
        raise ValueError("Unsupported JSON structure; expected object or list of records.")

    if trace_path.lower().endswith(".csv"):
        rows = _load_csv_rows(trace_path)
        if _is_redbench_stats(rows):
            return _normalize_trace_summary(_redbench_stats_to_summary(rows, trace_path))
        return _normalize_trace_summary(_trace_records_to_summary(rows))

    raise ValueError("Unsupported trace file type; use .csv or .json")


def build_spec(summary: Dict[str, Any], trace_type: str | None = None) -> Dict[str, Any]:
    trace_type = trace_type or _infer_trace_type(summary)

    type_defaults = (
        {"family": "data", "category": "drift", "subtype": "single_table"}
        if trace_type == "data"
        else {"family": "workload", "category": "templates", "subtype": "selection_payload"}
    )
    type_info = {**type_defaults, **(summary.get("type") or {})}

    spec: Dict[str, Any] = {
        "pattern_id": summary.get("pattern_id") or f"trace-{trace_type}",
        "type": type_info,
    }
    if summary.get("seed") is not None:
        spec["seed"] = summary.get("seed")
    if summary.get("data_source"):
        spec["data_source"] = summary.get("data_source")

    variables = summary.get("variables")
    if variables is None:
        if trace_type == "data":
            variables = {
                "base_table": summary.get("base_table"),
                "drifts": summary.get("drifts", []),
            }
        else:
            variables = {"base_table": summary.get("base_table")}
            if summary.get("template_defaults"):
                variables["defaults"] = summary.get("template_defaults")
            if summary.get("template_runs"):
                variables["runs"] = summary.get("template_runs")
            if summary.get("query_runs"):
                variables["query_runs"] = summary.get("query_runs")

    spec["variables"] = variables
    return _compact(spec)


def build_spec_from_mapping(
    trace_path: str,
    mapping: Dict[str, Any],
    trace_type: str | None = None,
) -> Dict[str, Any]:
    if trace_path.lower().endswith(".csv"):
        rows = _load_csv_rows(trace_path)
        if _is_redbench_stats(rows) or mapping.get("mode") == "redbench_stats":
            return _redbench_stats_to_spec(rows, trace_path, mapping, trace_type=trace_type)
    summary = load_trace_summary(trace_path)
    spec = build_spec(summary, trace_type=trace_type)
    return _apply_spec_overrides(spec, mapping)


def write_spec_yaml(spec: Dict[str, Any], output_path: str) -> None:
    os.makedirs(os.path.dirname(output_path), exist_ok=True) if os.path.dirname(output_path) else None
    import yaml

    with open(output_path, "w", encoding="utf-8") as f:
        yaml.safe_dump(spec, f, sort_keys=False)


def _infer_trace_type(summary: Dict[str, Any]) -> str:
    if summary.get("trace_type"):
        return str(summary.get("trace_type"))
    type_info = summary.get("type") or {}
    if type_info.get("family"):
        return str(type_info.get("family"))
    if summary.get("drifts"):
        return "data"
    if summary.get("template_runs") or summary.get("query_runs"):
        return "workload"
    raise ValueError("Unable to infer trace_type; provide trace_type or type.family.")


def _load_mapping(mapping_path: str) -> Dict[str, Any]:
    with open(mapping_path, "r", encoding="utf-8") as f:
        mapping = json.load(f)
    if not isinstance(mapping, dict):
        raise ValueError("Mapping JSON must be an object.")
    return mapping


def _normalize_trace_summary(summary: Dict[str, Any]) -> Dict[str, Any]:
    normalized: Dict[str, Any] = {}
    normalized["pattern_id"] = summary.get("pattern_id")
    normalized["seed"] = summary.get("seed")
    normalized["trace_type"] = summary.get("trace_type") or summary.get("family")

    type_info = {}
    if isinstance(summary.get("type"), dict):
        type_info.update(summary.get("type") or {})
    if summary.get("type_family"):
        type_info["family"] = summary.get("type_family")
    if summary.get("type_category"):
        type_info["category"] = summary.get("type_category")
    if summary.get("type_subtype"):
        type_info["subtype"] = summary.get("type_subtype")
    normalized["type"] = type_info or None

    data_source = summary.get("data_source") or summary.get("source")
    if not data_source and summary.get("data_source_json"):
        data_source = summary.get("data_source_json")
    if not data_source:
        data_source = _build_data_source_from_meta(summary)
    normalized["data_source"] = data_source

    normalized["base_table"] = summary.get("base_table")
    normalized["drifts"] = summary.get("drifts") or summary.get("data_drifts")
    normalized["template_defaults"] = summary.get("template_defaults") or summary.get("defaults")
    normalized["template_runs"] = summary.get("template_runs") or summary.get("runs")
    normalized["query_runs"] = summary.get("query_runs")
    normalized["variables"] = summary.get("variables")

    return _compact(normalized)


def _load_csv_rows(trace_path: str) -> List[Dict[str, Any]]:
    with open(trace_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = []
        for row in reader:
            rows.append({k: _coerce(v) for k, v in row.items()})
    return rows


def _trace_records_to_summary(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    meta: Dict[str, Any] = {}
    data_source: Dict[str, Any] | None = None
    defaults: Dict[str, Any] | None = None
    drifts: List[Dict[str, Any]] = []
    template_runs: List[Dict[str, Any]] = []
    query_runs: List[Dict[str, Any]] = []
    untyped_rows: List[Dict[str, Any]] = []

    for row in rows:
        row = {k: v for k, v in row.items() if v is not None}
        record_type = _record_type(row)

        if record_type in {"meta", "metadata"}:
            meta.update(_pick_meta(row))
            data_source = data_source or _extract_data_source(row)
            continue
        if record_type in {"data_source", "source"}:
            data_source = _extract_data_source(row) or data_source
            continue
        if record_type in {"defaults", "template_defaults"}:
            defaults = _extract_defaults(row)
            continue
        if record_type in {"drift", "data_drift"}:
            drifts.append(_extract_record(row))
            continue
        if record_type in {"template_run", "run"}:
            template_runs.append(_extract_record(row))
            continue
        if record_type in {"query_run", "query"}:
            query_runs.append(_extract_record(row))
            continue

        untyped_rows.append(row)

    if not meta and rows:
        meta.update(_pick_meta(rows[0]))

    if data_source is None:
        data_source = _build_data_source_from_meta(meta)

    trace_type = meta.get("trace_type")
    for row in untyped_rows:
        row_kind = _infer_row_kind(row, trace_type)
        if row_kind == "query_run":
            query_runs.append(_extract_record(row))
        elif row_kind == "template_run":
            template_runs.append(_extract_record(row))
        else:
            drifts.append(_extract_record(row))

    return {
        "pattern_id": meta.get("pattern_id"),
        "seed": meta.get("seed"),
        "trace_type": trace_type,
        "type_family": meta.get("type_family"),
        "type_category": meta.get("type_category"),
        "type_subtype": meta.get("type_subtype"),
        "data_source": data_source,
        "base_table": meta.get("base_table"),
        "template_defaults": defaults,
        "template_runs": template_runs,
        "query_runs": query_runs,
        "drifts": drifts,
    }


def _is_redbench_stats(rows: List[Dict[str, Any]]) -> bool:
    if not rows:
        return False
    required = {
        "workload_type",
        "user_id",
        "instance_id",
        "number_of_queries",
        "query_repetition_rate",
    }
    keys = set(rows[0].keys())
    return required.issubset(keys)


def _redbench_stats_to_summary(rows: List[Dict[str, Any]], trace_path: str) -> Dict[str, Any]:
    stats = []
    for row in rows:
        cleaned = {k: v for k, v in row.items() if v is not None}
        if cleaned:
            stats.append(_compact(cleaned))

    basename = os.path.splitext(os.path.basename(trace_path))[0]
    return {
        "pattern_id": basename or "redbench-stats",
        "trace_type": "workload",
        "variables": {
            "trace_stats": stats,
            "notes": [
                "Derived from RedBench stats.csv; add data_source, base_table, and template/query settings to run.",
            ],
        },
    }


def _redbench_stats_to_spec(
    rows: List[Dict[str, Any]],
    trace_path: str,
    mapping: Dict[str, Any],
    trace_type: str | None = None,
) -> Dict[str, Any]:
    base = os.path.splitext(os.path.basename(trace_path))[0]
    prefix = mapping.get("pattern_id_prefix")
    pattern_id = mapping.get("pattern_id") or (f"{prefix}_{base}" if prefix else base)

    type_info = mapping.get("type") or {
        "family": "workload",
        "category": "templates",
        "subtype": "selection_payload",
    }
    if trace_type:
        type_info = dict(type_info)
        type_info["family"] = trace_type

    spec: Dict[str, Any] = {"pattern_id": pattern_id, "type": type_info}
    if mapping.get("seed") is not None:
        spec["seed"] = mapping.get("seed")
    if mapping.get("data_source"):
        spec["data_source"] = mapping.get("data_source")

    variables: Dict[str, Any] = {}
    if mapping.get("base_table"):
        variables["base_table"] = mapping.get("base_table")
    if mapping.get("defaults"):
        variables["defaults"] = mapping.get("defaults")

    runs = _build_redbench_runs(rows, mapping, pattern_id)
    if runs:
        variables["runs"] = runs
    query_runs = _build_redbench_query_runs(rows, mapping, pattern_id)
    if query_runs:
        variables["query_runs"] = query_runs

    spec["variables"] = _compact(variables)
    return _compact(spec)


def _build_redbench_runs(
    rows: List[Dict[str, Any]],
    mapping: Dict[str, Any],
    pattern_id: str,
) -> List[Dict[str, Any]]:
    cfg = mapping.get("run_from") or {}
    if not cfg:
        return []
    name_field = cfg.get("name_from")
    if not name_field:
        return []

    runs = []
    for row in rows:
        raw_name = row.get(name_field)
        if raw_name is None:
            continue
        name = _normalize_name(raw_name, cfg.get("name_prefix"), cfg.get("name_suffix"))
        run: Dict[str, Any] = {"name": name}

        run["output_path"] = _format_path(
            cfg.get("output_path_template") or "./output/intermediate_yaml/{pattern_id}_{name}_templates.json",
            pattern_id,
            name,
        )

        num_templates = _to_int(
            row.get(cfg.get("num_templates_from")) if cfg.get("num_templates_from") else None,
            cfg.get("num_templates"),
            cfg.get("num_templates_min", 1),
            cfg.get("num_templates_max"),
        )
        if num_templates is not None:
            run["num_templates"] = num_templates

        max_predicates = _to_int(
            row.get(cfg.get("max_predicates_from")) if cfg.get("max_predicates_from") else None,
            cfg.get("max_predicates"),
            cfg.get("max_predicates_min", 1),
            cfg.get("max_predicates_max"),
        )
        if max_predicates is not None:
            run["max_predicates"] = max_predicates

        if cfg.get("max_payload_columns") is not None:
            run["max_payload_columns"] = cfg.get("max_payload_columns")
        if cfg.get("selectivity") is not None:
            run["selectivity"] = cfg.get("selectivity")
        if cfg.get("value_range") is not None:
            run["value_range"] = cfg.get("value_range")
        if cfg.get("join_count") is not None:
            run["join_count"] = cfg.get("join_count")

        runs.append(_compact(run))
    return runs


def _build_redbench_query_runs(
    rows: List[Dict[str, Any]],
    mapping: Dict[str, Any],
    pattern_id: str,
) -> List[Dict[str, Any]]:
    cfg = mapping.get("query_from") or {}
    if not cfg:
        return []
    name_field = cfg.get("name_from") or cfg.get("template_ref_from")
    if not name_field:
        return []

    query_runs = []
    for row in rows:
        raw_name = row.get(name_field)
        if raw_name is None:
            continue
        name = _normalize_name(raw_name, cfg.get("name_prefix"), cfg.get("name_suffix"))
        template_ref = row.get(cfg.get("template_ref_from")) if cfg.get("template_ref_from") else name
        template_ref = _normalize_name(template_ref, None, None)

        qrun: Dict[str, Any] = {"name": name, "template": template_ref}
        qpt = _to_int(
            row.get(cfg.get("queries_per_template_from")) if cfg.get("queries_per_template_from") else None,
            cfg.get("queries_per_template", 300),
            cfg.get("queries_per_template_min", 1),
            cfg.get("queries_per_template_max"),
        )
        if qpt is not None:
            qrun["queries_per_template"] = qpt

        if cfg.get("dist_config") is not None:
            qrun["dist_config"] = cfg.get("dist_config")

        output_path = _format_path(
            cfg.get("output_path_template") or "./output/workload/{pattern_id}_{name}_sqls.csv",
            pattern_id,
            name,
        )
        qrun["outputs"] = [{"type": "workload", "path": output_path}]

        query_runs.append(_compact(qrun))
    return query_runs


def _record_type(row: Dict[str, Any]) -> str:
    record_type = row.get("record_type") or row.get("section") or ""
    return str(record_type).strip().lower()


def _pick_meta(row: Dict[str, Any]) -> Dict[str, Any]:
    meta = {}
    for key in _META_FIELDS:
        if key in row and row[key] is not None:
            meta[key] = row[key]
    return meta


def _extract_data_source(row: Dict[str, Any]) -> Dict[str, Any] | None:
    data_source = None
    if isinstance(row.get("data_source"), dict):
        data_source = row.get("data_source")
    if isinstance(row.get("data_source_json"), dict):
        data_source = row.get("data_source_json")
    if data_source:
        return data_source

    data_source = _build_data_source_from_meta(row)
    return data_source


def _build_data_source_from_meta(meta: Dict[str, Any]) -> Dict[str, Any] | None:
    if meta.get("data_source_json"):
        return meta.get("data_source_json")

    data_source: Dict[str, Any] = {}
    if meta.get("data_source_kind"):
        data_source["kind"] = meta.get("data_source_kind")
    if meta.get("data_source_path"):
        data_source["path"] = meta.get("data_source_path")

    schema_extractor: Dict[str, Any] = {}
    if meta.get("schema_source_type"):
        schema_extractor["source_type"] = meta.get("schema_source_type")
    if meta.get("schema_sample_size"):
        schema_extractor["sample_size"] = meta.get("schema_sample_size")
    if meta.get("schema_output_path"):
        schema_extractor["schema_output_path"] = meta.get("schema_output_path")
    if schema_extractor:
        data_source["schema_extractor"] = schema_extractor

    return data_source or None


def _extract_defaults(row: Dict[str, Any]) -> Dict[str, Any] | None:
    if isinstance(row.get("defaults"), dict):
        return row.get("defaults")
    if isinstance(row.get("defaults_json"), dict):
        return row.get("defaults_json")
    cleaned = _extract_record(row)
    return cleaned or None


def _extract_record(row: Dict[str, Any]) -> Dict[str, Any]:
    record: Dict[str, Any] = {}
    for key, value in row.items():
        if key in _META_FIELDS or key in {"record_type", "section"}:
            continue
        record[_ALIAS_FIELDS.get(key, key)] = value
    return _compact(record)


def _infer_row_kind(row: Dict[str, Any], trace_type: str | None) -> str:
    if row.get("drift_type") or row.get("drift_name"):
        return "drift"
    if row.get("queries_per_template") or row.get("dist_config") or row.get("outputs"):
        return "query_run"
    if row.get("selectivity") or row.get("max_predicates") or row.get("max_payload_columns"):
        return "template_run"
    if trace_type == "workload":
        return "query_run"
    return "drift"


def _coerce(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (dict, list, int, float, bool)):
        return value
    if isinstance(value, str):
        stripped = value.strip()
        if stripped == "":
            return None
        try:
            return json.loads(stripped)
        except json.JSONDecodeError:
            return stripped
    return value


def _compact(obj: Any) -> Any:
    if isinstance(obj, dict):
        compacted: Dict[str, Any] = {}
        for key, value in obj.items():
            value = _compact(value)
            if value is None:
                continue
            if isinstance(value, (dict, list)) and not value:
                continue
            compacted[key] = value
        return compacted
    if isinstance(obj, list):
        compacted_list = []
        for item in obj:
            item = _compact(item)
            if item is None:
                continue
            if isinstance(item, (dict, list)) and not item:
                continue
            compacted_list.append(item)
        return compacted_list
    return obj


def _apply_spec_overrides(spec: Dict[str, Any], mapping: Dict[str, Any]) -> Dict[str, Any]:
    if mapping.get("pattern_id"):
        spec["pattern_id"] = mapping.get("pattern_id")
    if mapping.get("seed") is not None:
        spec["seed"] = mapping.get("seed")
    if mapping.get("type"):
        spec["type"] = mapping.get("type")
    if mapping.get("data_source"):
        spec["data_source"] = mapping.get("data_source")

    if mapping.get("variables"):
        spec["variables"] = mapping.get("variables")
        return _compact(spec)

    variables = spec.get("variables") or {}
    if mapping.get("base_table"):
        variables["base_table"] = mapping.get("base_table")
    if mapping.get("defaults"):
        variables["defaults"] = mapping.get("defaults")
    if mapping.get("runs"):
        variables["runs"] = mapping.get("runs")
    if mapping.get("query_runs"):
        variables["query_runs"] = mapping.get("query_runs")
    spec["variables"] = variables
    return _compact(spec)


def _normalize_name(value: Any, prefix: str | None, suffix: str | None) -> str:
    text = str(value).strip()
    text = text.replace(" ", "_").replace("/", "_")
    if prefix:
        text = f"{prefix}{text}"
    if suffix:
        text = f"{text}{suffix}"
    return text


def _format_path(template: str, pattern_id: str, name: str) -> str:
    return template.format(pattern_id=pattern_id, name=name)


def _to_int(value: Any, default: Any, minimum: int | None, maximum: int | None) -> int | None:
    if value is None:
        value = default
    if value is None:
        return None
    try:
        num = int(round(float(value)))
    except (TypeError, ValueError):
        return None
    if minimum is not None and num < minimum:
        num = minimum
    if maximum is not None and num > maximum:
        num = maximum
    return num
