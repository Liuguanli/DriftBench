import os
import re
import random
import datetime
from typing import Dict, List, Any, Iterable, Tuple

_DSS_CACHE: Dict[str, Dict[str, List[Tuple[str, int]]]] = {}
_SIZES = list(range(1, 51))
_CCODE = list(range(25))


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
    if dtype == "dss_dist":
        dist_file = defn["dist_file"]
        dist_name = defn["dist_name"]
        return _sample_dss_dist(dist_file, dist_name, rng)

    raise ValueError(f"Unsupported param type: {dtype}")


def _load_dss_file(dist_file: str) -> Dict[str, List[Tuple[str, int]]]:
    if dist_file in _DSS_CACHE:
        return _DSS_CACHE[dist_file]

    dists: Dict[str, List[Tuple[str, int]]] = {}
    current: str | None = None
    cumulative = 0
    expected_count: int | None = None
    with open(dist_file, "r", encoding="utf-8", errors="ignore") as f:
        for raw in f:
            line = raw.strip()
            if not line:
                continue
            if line.startswith("#"):
                continue
            low = line.lower()
            if low.startswith("begin "):
                current = line.split(None, 1)[1].strip()
                dists[current] = []
                cumulative = 0
                expected_count = None
                continue
            if low.startswith("end "):
                if expected_count is not None and current is not None:
                    if len(dists[current]) != expected_count:
                        pass
                current = None
                expected_count = None
                continue
            if current is None:
                continue
            if "|" not in line:
                continue
            token, weight_str = [p.strip() for p in line.split("|", 1)]
            if token.lower() == "count":
                try:
                    expected_count = int(weight_str)
                except ValueError:
                    expected_count = None
                continue
            try:
                weight = int(weight_str)
            except ValueError:
                continue
            cumulative += weight
            dists[current].append((token, cumulative))

    _DSS_CACHE[dist_file] = dists
    return dists


def _sample_dss_dist(dist_file: str, dist_name: str, rng: random.Random) -> str:
    _, token = _sample_dss_dist_index(dist_file, dist_name, rng)
    return token


def _dss_dist_entries(dist_file: str, dist_name: str) -> List[Tuple[str, int]]:
    dists = _load_dss_file(dist_file)
    if dist_name not in dists:
        raise ValueError(f"Distribution '{dist_name}' not found in {dist_file}")
    return dists[dist_name]


def _dss_token_at(dist_file: str, dist_name: str, index: int) -> str:
    entries = _dss_dist_entries(dist_file, dist_name)
    if index < 0 or index >= len(entries):
        raise ValueError(f"Index {index} out of range for distribution '{dist_name}'")
    return entries[index][0]


def _dss_cumulative_at(dist_file: str, dist_name: str, index: int) -> int:
    entries = _dss_dist_entries(dist_file, dist_name)
    if index < 0 or index >= len(entries):
        raise ValueError(f"Index {index} out of range for distribution '{dist_name}'")
    return entries[index][1]


def _sample_dss_dist_index(
    dist_file: str, dist_name: str, rng: random.Random
) -> Tuple[int, str]:
    entries = _dss_dist_entries(dist_file, dist_name)
    if not entries:
        raise ValueError(f"Distribution '{dist_name}' is empty in {dist_file}")
    prev = entries[0][1]
    for _, cum in entries[1:]:
        if cum < prev:
            raise ValueError(f"Distribution '{dist_name}' has non-monotonic weights in {dist_file}")
        prev = cum
    total = entries[-1][1]
    if total <= 0:
        raise ValueError(f"Distribution '{dist_name}' has no positive weight total in {dist_file}")
    roll = rng.randint(1, total)
    for idx, (_, cum) in enumerate(entries):
        if cum >= roll:
            return idx, entries[idx][0]
    return len(entries) - 1, entries[-1][0]


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


def generate_tpch_queries_indexed_qgen(
    template_dir: str,
    query_ids: Iterable[int | str],
    queries_per_template: int = 1,
    seed: int = 42,
    shuffle: bool = True,
    dist_file: str | None = None,
    scale: float = 1.0,
) -> List[Dict[str, Any]]:
    rng = random.Random(seed)
    query_ids = [str(qid) for qid in query_ids]
    templates = load_tpch_templates(template_dir, query_ids)
    dist_file = _resolve_dss_path(template_dir, dist_file)

    entries: List[Dict[str, Any]] = []
    for qid in query_ids:
        template_sql = templates[qid]
        needed = extract_param_indices(template_sql)
        for idx in range(1, int(queries_per_template) + 1):
            params = _qgen_params_for_query(qid, rng, dist_file, scale)
            if needed and len(params) < max(needed):
                raise ValueError(
                    f"qgen params insufficient for TPCH query {qid} (need {max(needed)}, got {len(params)})"
                )
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


def _resolve_dss_path(template_dir: str, dist_file: str | None) -> str:
    if dist_file:
        return dist_file
    candidate = os.path.join(os.path.dirname(template_dir), "dists.dss")
    if os.path.exists(candidate):
        return candidate
    raise ValueError("dists.dss not found; set qgen_dist_file or use dss_dist with explicit dist_file")


def _qgen_month_start_from_offset(start_year: int, offset_months: int) -> str:
    year = start_year + offset_months // 12
    month = offset_months % 12 + 1
    return f"19{year:02d}-{month:02d}-01"


def _qgen_year_start(year: int) -> str:
    return f"19{year:02d}-01-01"


def _qgen_brand(rng: random.Random) -> str:
    return f"Brand#{rng.randint(1, 5)}{rng.randint(1, 5)}"


def _qgen_params_for_query(
    qid: str, rng: random.Random, dist_file: str, scale: float = 1.0
) -> List[Any]:
    if qid == "1":
        return [rng.randint(60, 120)]
    if qid == "2":
        size = rng.randint(1, 50)
        p_type = _sample_dss_dist(dist_file, "p_types", rng)
        suffix = p_type.split()[-1]
        region = _sample_dss_dist(dist_file, "regions", rng)
        return [size, suffix, region]
    if qid == "3":
        segment = _sample_dss_dist(dist_file, "msegmnt", rng)
        start = datetime.date(1995, 3, 1)
        date = start + datetime.timedelta(days=rng.randint(0, 30))
        return [segment, date.isoformat()]
    if qid == "4":
        offset = rng.randint(0, 57)
        return [_qgen_month_start_from_offset(93, offset)]
    if qid == "5":
        region = _sample_dss_dist(dist_file, "regions", rng)
        year = rng.randint(93, 97)
        return [region, _qgen_year_start(year)]
    if qid == "6":
        year = rng.randint(93, 97)
        date = _qgen_year_start(year)
        discount = f"0.{rng.randint(2, 9):02d}"
        quantity = rng.randint(24, 25)
        return [date, discount, quantity]
    if qid == "7":
        idx1, nation1 = _sample_dss_dist_index(dist_file, "nations2", rng)
        idx2, nation2 = _sample_dss_dist_index(dist_file, "nations2", rng)
        while idx2 == idx1:
            idx2, nation2 = _sample_dss_dist_index(dist_file, "nations2", rng)
        return [nation1, nation2]
    if qid == "8":
        idx, nation = _sample_dss_dist_index(dist_file, "nations2", rng)
        region_idx = _dss_cumulative_at(dist_file, "nations", idx)
        region = _dss_token_at(dist_file, "regions", int(region_idx))
        p_type = _sample_dss_dist(dist_file, "p_types", rng)
        return [nation, region, p_type]
    if qid == "9":
        color = _sample_dss_dist(dist_file, "colors", rng)
        return [color]
    if qid == "10":
        offset = rng.randint(1, 24)
        return [_qgen_month_start_from_offset(93, offset)]
    if qid == "11":
        nation = _sample_dss_dist(dist_file, "nations2", rng)
        fraction = 0.0001 / float(scale)
        return [nation, f"{fraction:.10f}"]
    if qid == "12":
        idx1, mode1 = _sample_dss_dist_index(dist_file, "smode", rng)
        idx2, mode2 = _sample_dss_dist_index(dist_file, "smode", rng)
        while idx2 == idx1:
            idx2, mode2 = _sample_dss_dist_index(dist_file, "smode", rng)
        year = rng.randint(93, 97)
        return [mode1, mode2, _qgen_year_start(year)]
    if qid == "13":
        word1 = _sample_dss_dist(dist_file, "Q13a", rng)
        word2 = _sample_dss_dist(dist_file, "Q13b", rng)
        return [word1, word2]
    if qid == "14":
        offset = rng.randint(0, 59)
        return [_qgen_month_start_from_offset(93, offset)]
    if qid == "15":
        offset = rng.randint(0, 57)
        return [_qgen_month_start_from_offset(93, offset)]
    if qid == "16":
        brand = _qgen_brand(rng)
        p_type = _sample_dss_dist(dist_file, "p_types", rng)
        prefix = p_type.rsplit(" ", 1)[0]
        sizes = list(_SIZES)
        rng.shuffle(sizes)
        size_params = sizes[:8]
        return [brand, prefix] + size_params
    if qid == "17":
        brand = _qgen_brand(rng)
        container = _sample_dss_dist(dist_file, "p_cntr", rng)
        return [brand, container]
    if qid == "18":
        return [rng.randint(312, 315)]
    if qid == "19":
        brands = [_qgen_brand(rng) for _ in range(3)]
        q4 = rng.randint(1, 10)
        q5 = rng.randint(10, 20)
        q6 = rng.randint(20, 30)
        return brands + [q4, q5, q6]
    if qid == "20":
        color = _sample_dss_dist(dist_file, "colors", rng)
        year = rng.randint(93, 97)
        nation = _sample_dss_dist(dist_file, "nations2", rng)
        return [color, _qgen_year_start(year), nation]
    if qid == "21":
        nation = _sample_dss_dist(dist_file, "nations2", rng)
        return [nation]
    if qid == "22":
        codes = list(_CCODE)
        rng.shuffle(codes)
        return [10 + code for code in codes[:8]]

    raise ValueError(f"qgen param_mode not implemented for TPCH query {qid}")
