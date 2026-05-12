# driftbench/spec/core.py
from __future__ import annotations

import importlib
import random
from typing import Any, Dict, Tuple

import numpy as np
import yaml

from .registry import get_handler

_TYPES_LOADED = False


def ensure_handlers_loaded() -> None:
    global _TYPES_LOADED
    if _TYPES_LOADED:
        return
    importlib.import_module("driftbench.spec.types.workload_templates")
    importlib.import_module("driftbench.spec.types.workload_sql_templates")
    importlib.import_module("driftbench.spec.types.workload_keylist")
    importlib.import_module("driftbench.spec.types.data_drift")
    _TYPES_LOADED = True


def load_spec(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        loaded = yaml.safe_load(f)
    if not isinstance(loaded, dict):
        raise ValueError(
            "Spec root must be a YAML mapping/object (for example: `type: ...` and `variables: ...`)."
        )
    return loaded


def seed_everything(seed: int | None) -> None:
    if seed is not None:
        random.seed(seed)
        np.random.seed(seed)


def get_type_triple(spec: Dict[str, Any]) -> Tuple[str, str, str]:
    t = spec.get("type", {})
    if not isinstance(t, dict):
        return ("", "", "")
    return (t.get("family", ""), t.get("category", ""), t.get("subtype", ""))


def migrate_spec(spec: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(spec, dict):
        raise ValueError("Spec must be a YAML mapping/object.")
    # reserved for future migrations; keep idempotent
    spec.setdefault("spec_version", 1)
    return spec


def validate_spec(spec: Dict[str, Any]) -> None:
    if "type" not in spec:
        raise ValueError("Missing 'type' in spec.")
    if "variables" not in spec:
        raise ValueError("Missing 'variables' in spec.")

    type_obj = spec.get("type")
    if not isinstance(type_obj, dict):
        raise ValueError(
            "Invalid 'type': expected mapping with keys 'family', 'category', and 'subtype'."
        )

    variables_obj = spec.get("variables")
    if not isinstance(variables_obj, dict):
        raise ValueError("Invalid 'variables': expected mapping/object.")


def run_all(spec_path: str) -> None:
    spec = load_spec(spec_path)
    spec = migrate_spec(spec)
    validate_spec(spec)
    seed_everything(spec.get("seed"))
    ensure_handlers_loaded()
    triple = get_type_triple(spec)
    handler = get_handler(triple)
    handler(spec)
