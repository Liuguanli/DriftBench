# driftbench/spec/types/workload_sql_templates.py
import os
from typing import Dict, Any, List

from ...core.temporal.time_stamp_generator import generate_timestamps
from ...core.utils import save_sqls, save_sqls_with_timestamps
from ...core.workload.tpch_sql_generator import (
    generate_tpch_queries,
    generate_tpch_queries_indexed,
    generate_tpch_queries_indexed_qgen,
    list_tpch_query_ids,
)
from ..registry import register


def _ensure_dir(path: str) -> None:
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)


def _merge_params(base: Dict[str, Any], overrides: Dict[str, Any]) -> Dict[str, Any]:
    merged = dict(base or {})
    for k, v in (overrides or {}).items():
        merged[str(k)] = v
    return merged


@register(family="workload", category="sql_templates", subtype="tpch")
def handle_tpch_sql_templates(spec: Dict[str, Any]) -> None:
    variables = spec["variables"]
    template_dir = variables.get("template_dir")
    if not template_dir:
        raise ValueError("TPCH workload requires variables.template_dir")

    defaults = variables.get("defaults", {})
    default_query_ids = variables.get("query_ids")
    base_params = variables.get("params", {})
    default_param_mode = defaults.get("param_mode", "custom")
    qgen_dist_file = variables.get("qgen_dist_file") or defaults.get("qgen_dist_file")
    seed = int(spec.get("seed", 42))

    runs = variables.get("query_runs") or variables.get("runs") or []
    if not runs:
        raise ValueError("TPCH workload requires variables.query_runs or variables.runs")

    for run in runs:
        run_query_ids = run.get("query_ids") or default_query_ids
        if not run_query_ids:
            run_query_ids = list_tpch_query_ids(template_dir)
        run_query_ids = [str(qid) for qid in run_query_ids]

        qpt = int(run.get("queries_per_template", defaults.get("queries_per_template", 1)))
        shuffle = bool(run.get("shuffle", defaults.get("shuffle", True)))

        param_mode = run.get("param_mode", default_param_mode)
        if param_mode == "qgen":
            entries = generate_tpch_queries_indexed_qgen(
                template_dir=template_dir,
                query_ids=run_query_ids,
                queries_per_template=qpt,
                seed=seed,
                shuffle=shuffle,
                dist_file=run.get("qgen_dist_file") or qgen_dist_file,
            )
        elif param_mode == "custom":
            params = _merge_params(base_params, run.get("params"))
            entries = generate_tpch_queries_indexed(
                template_dir=template_dir,
                query_ids=run_query_ids,
                param_specs=params,
                queries_per_template=qpt,
                seed=seed,
                shuffle=shuffle,
            )
        else:
            raise ValueError(f"Unsupported param_mode: {param_mode}")
        sqls = [entry["sql"] for entry in entries]

        outputs = run.get("outputs")
        if not outputs:
            out_path = run.get("output_path")
            if not out_path:
                raise ValueError("Each TPCH run needs outputs or output_path")
            outputs = [{"type": "workload", "path": out_path}]

        for out in outputs:
            out_path = out["path"]
            out_type = out.get("type", "workload")
            if out_type == "split":
                os.makedirs(out_path, exist_ok=True)
                name_tmpl = out.get("filename_template", "{query_id}_{index}.sql")
                for entry in entries:
                    filename = name_tmpl.format(query_id=entry["query_id"], index=entry["index"])
                    save_sqls([entry["sql"]], os.path.join(out_path, filename))
                print(f"[WRITE] split -> {out_path}")
                continue

            _ensure_dir(out_path)
            if out_type == "temporal":
                ts = out.get("timestamp") or {}
                timestamps = generate_timestamps(
                    count=len(sqls),
                    start_time=ts.get("start_time", "2025-07-01T00:00:00"),
                    pattern=ts.get("pattern", "uniform"),
                    queries_per_minute=int(ts.get("queries_per_minute", 300)),
                )
                save_sqls_with_timestamps(sqls, timestamps, out_path)
                print(f"[WRITE] temporal ({ts.get('pattern','uniform')}) -> {out_path}")
            else:
                save_sqls(sqls, out_path)
                print(f"[WRITE] workload -> {out_path}")
