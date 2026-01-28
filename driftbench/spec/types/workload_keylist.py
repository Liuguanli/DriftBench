# driftbench/spec/types/workload_keylist.py
from __future__ import annotations

import os
import struct
import random
import bisect
from typing import Any, Dict, List

import pandas as pd

from ..registry import register


def _ensure_dir(path: str) -> None:
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)


def _build_zipf_cdf(n: int, alpha: float) -> tuple[list[float], float]:
    weights = [1.0 / ((i + 1) ** alpha) for i in range(n)]
    total = 0.0
    cdf: list[float] = []
    for w in weights:
        total += w
        cdf.append(total)
    return cdf, total


def _sample_indices_uniform(rng: random.Random, n: int, count: int) -> list[int]:
    return [rng.randrange(n) for _ in range(count)]


def _sample_indices_hotspot(rng: random.Random, n: int, count: int, hotspot_frac: float) -> list[int]:
    hotspot_size = max(1, int(n * hotspot_frac))
    return [rng.randrange(hotspot_size) for _ in range(count)]


def _sample_indices_zipf(rng: random.Random, n: int, count: int, alpha: float) -> list[int]:
    cdf, total = _build_zipf_cdf(n, alpha)
    out: list[int] = []
    for _ in range(count):
        r = rng.random() * total
        idx = bisect.bisect_left(cdf, r)
        out.append(min(idx, n - 1))
    return out


def _write_bin(values: List[int], out_path: str, type_format: str, output_has_size: bool) -> None:
    _ensure_dir(out_path)
    fmt = "<Q" if type_format == "u64" else "<q"
    with open(out_path, "wb") as handle:
        if output_has_size:
            handle.write(struct.pack("<Q", len(values)))
        for v in values:
            handle.write(struct.pack(fmt, int(v)))


@register(family="workload", category="keylist", subtype="single_table")
def handle_workload_keylist(spec: Dict[str, Any]) -> None:
    variables = spec.get("variables") or {}
    ds = spec.get("data_source") or {}
    path = ds.get("path")
    if not path:
        raise ValueError("data_source.path is required for keylist workload.")

    key_column = variables.get("key_column", "key")
    type_format = variables.get("type_format", "u64")
    output_has_size = bool(variables.get("output_has_size", False))

    df = pd.read_csv(path, usecols=[key_column])
    keys = df[key_column].dropna().tolist()
    if not keys:
        raise ValueError("No keys loaded for workload keylist.")

    seed = int(spec.get("seed", 42))
    rng = random.Random(seed)

    for run in variables.get("query_runs", []):
        name = run.get("name", "run")
        query_type = run.get("query_type", "point")
        distribution = run.get("distribution", "uniform")
        count = int(run.get("count", 10000))
        zipf_alpha = float(run.get("zipf_alpha", 1.2))
        hotspot_frac = float(run.get("hotspot_frac", 0.1))
        r_size = int(run.get("r_size", 0))

        max_index = len(keys) - 1
        if query_type == "range" and r_size > 0 and len(keys) > r_size:
            max_index = max(0, len(keys) - r_size - 1)

        if distribution == "uniform":
            indices = _sample_indices_uniform(rng, max_index + 1, count)
        elif distribution == "hotspot":
            indices = _sample_indices_hotspot(rng, max_index + 1, count, hotspot_frac)
        elif distribution == "zipf":
            if zipf_alpha <= 1.0:
                raise ValueError("zipf_alpha must be > 1.0.")
            indices = _sample_indices_zipf(rng, max_index + 1, count, zipf_alpha)
        else:
            raise ValueError(f"Unsupported distribution: {distribution}")

        values = [keys[i] for i in indices]

        out_path = run.get("output_path")
        if not out_path:
            raise ValueError("Each query_run needs output_path.")
        _write_bin(values, out_path, type_format, output_has_size)
        print(f"[KEYLIST OK] {name} -> {out_path}")
