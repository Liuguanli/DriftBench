# driftbench/spec/core.py
# driftbench/spec/core.py
from .types import workload_templates  # ensure loaded
from .types import workload_sql_templates  # ensure loaded
from .types import data_drift          # ensure loaded

import yaml, random
import numpy as np
from typing import Dict, Any, Tuple
from .registry import get_handler

def load_spec(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def seed_everything(seed: int | None) -> None:
    if seed is not None:
        random.seed(seed); np.random.seed(seed)

def get_type_triple(spec: Dict[str, Any]) -> Tuple[str, str, str]:
    t = spec.get("type", {})
    return (t.get("family", ""), t.get("category", ""), t.get("subtype", ""))

def migrate_spec(spec: Dict[str, Any]) -> Dict[str, Any]:
    # reserved for future migrations; keep idempotent
    spec.setdefault("spec_version", 1)
    return spec

def validate_spec(spec: Dict[str, Any]) -> None:
    if "type" not in spec:
        raise ValueError("Missing 'type' in spec.")
    if "variables" not in spec:
        raise ValueError("Missing 'variables' in spec.")

def run_all(spec_path: str) -> None:
    spec = load_spec(spec_path)
    spec = migrate_spec(spec)
    validate_spec(spec)
    seed_everything(spec.get("seed"))
    triple = get_type_triple(spec)
    handler = get_handler(triple)
    handler(spec)
