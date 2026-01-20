import os
import re
import random
import datetime
from typing import Dict, List, Any, Iterable, Tuple


def _strip_template_lines(lines: Iterable[str]) -> str:
    cleaned: List[str] = []
    for line in lines:
        stripped = line.strip()
        if not stripped:
            continue
        if stripped.startswith("--"):
            continue
        if stripped.startswith(":") and re.match(r"^:[a-zA-Z]", stripped):
            continue
        cleaned.append(line.rstrip())
    return "\n".join(cleaned).strip()


def load_tpch_templates(template_dir: str, query_ids: Iterable[int | str]) -> Dict[str, str]:
    templates: Dict[str, str] = {}
    for qid in query_ids:
        qid_str = str(qid)
        path = os.path.join(template_dir, f"{qid_str}.sql")
        if not os.path.exists(path):
            raise FileNotFoundError(f"TPC-H template not found: {path}")
        with open(path, "r", encoding="utf-8") as f:
            templates[qid_str] = _strip_template_lines(f.readlines())
    return templates


def list_tpch_query_ids(template_dir: str) -> List[str]:
    ids: List[Tuple[int, str]] = []
    for name in os.listdir(template_dir):
        if not name.endswith(".sql"):
            continue
        stem = name[:-4]
        if stem.isdigit():
            ids.append((int(stem), stem))
    return [sid for _, sid in sorted(ids)]


def extract_param_indices(template_sql: str) -> List[int]:
    return sorted({int(m.group(1)) for m in re.finditer(r":(\d+)", template_sql)})


def _sample_param_value(defn: Any, rng: random.Random) -> Any:
    if isinstance(defn, list):
        return rng.choice(defn)
    if not isinstance(defn, dict):
        return defn

    dtype = defn.get("type")
    if not dtype:
        if "values" in defn or "choices" in defn:
            dtype = "choice"
        else:
            dtype = "fixed"

    if dtype == "choice":
        values = defn.get("values") or defn.get("choices") or []
        if not values:
            raise ValueError("choice parameter requires values or choices")
        return rng.choice(values)
    if dtype == "fixed":
        if "value" in defn:
            return defn["value"]
        values = defn.get("values") or defn.get("choices")
        if values:
            return values[0]
        raise ValueError("fixed parameter requires value or values")
    if dtype == "int_range":
        return rng.randint(int(defn["min"]), int(defn["max"]))
    if dtype == "float_range":
        val = rng.uniform(float(defn["min"]), float(defn["max"]))
        precision = defn.get("precision")
        return round(val, int(precision)) if precision is not None else val
    if dtype == "date_range":
        start = datetime.date.fromisoformat(defn["start"])
        end = datetime.date.fromisoformat(defn["end"])
        if end < start:
            raise ValueError("date_range end must be >= start")
        days = (end - start).days
        offset = rng.randint(0, days) if days else 0
        fmt = defn.get("format")
        value = start + datetime.timedelta(days=offset)
        return value.strftime(fmt) if fmt else value.isoformat()

    raise ValueError(f"Unsupported param type: {dtype}")


def _normalize_params_map(param_specs: Dict[Any, Any]) -> Dict[str, Any]:
    return {str(k): v for k, v in (param_specs or {}).items()}


def _stringify_value(value: Any) -> str:
    if isinstance(value, (datetime.date, datetime.datetime)):
        return value.isoformat()
    return str(value)


def render_tpch_template(template_sql: str, params: List[Any]) -> str:
    rendered = template_sql
    for idx, val in enumerate(params, 1):
        rendered = re.sub(rf":{idx}(?!\d)", _stringify_value(val), rendered)
    return rendered


def generate_tpch_queries(
    template_dir: str,
    query_ids: Iterable[int | str],
    param_specs: Dict[Any, Any],
    queries_per_template: int = 1,
    seed: int = 42,
    shuffle: bool = True,
) -> List[str]:
    entries = generate_tpch_queries_indexed(
        template_dir=template_dir,
        query_ids=query_ids,
        param_specs=param_specs,
        queries_per_template=queries_per_template,
        seed=seed,
        shuffle=shuffle,
    )
    return [entry["sql"] for entry in entries]


def generate_tpch_queries_indexed(
    template_dir: str,
    query_ids: Iterable[int | str],
    param_specs: Dict[Any, Any],
    queries_per_template: int = 1,
    seed: int = 42,
    shuffle: bool = True,
) -> List[Dict[str, Any]]:
    rng = random.Random(seed)
    query_ids = [str(qid) for qid in query_ids]
    templates = load_tpch_templates(template_dir, query_ids)
    params_map = _normalize_params_map(param_specs)

    entries: List[Dict[str, Any]] = []
    for qid in query_ids:
        template_sql = templates[qid]
        param_defs = params_map.get(qid)
        needed = extract_param_indices(template_sql)
        if param_defs is None:
            if needed:
                raise ValueError(f"Missing params for TPCH query {qid} (requires {needed})")
            param_defs = []
        if needed and len(param_defs) < max(needed):
            raise ValueError(
                f"Insufficient params for TPCH query {qid} (need {max(needed)}, got {len(param_defs)})"
            )
        for idx in range(1, int(queries_per_template) + 1):
            params = [_sample_param_value(defn, rng) for defn in param_defs]
            entries.append(
                {
                    "query_id": qid,
                    "index": idx,
                    "sql": render_tpch_template(template_sql, params),
                }
            )

    if shuffle:
        rng.shuffle(entries)
    return entries
