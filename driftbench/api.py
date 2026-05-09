"""Stable public Python API for DriftBench integrations.

This module is the supported import surface for downstream projects in P0.
Avoid depending on internal module paths when possible.
"""

from __future__ import annotations

from typing import Any, Dict, Tuple

from driftbench.core.data.filter_registry import get_filter, register_filter
from driftbench.core.schema.factory import get_schema_extractor
from driftbench.spec.core import (
    load_spec,
    run_all as run_spec,
    validate_spec,
)
from driftbench.spec.trace_spec import trace_to_spec


def run_spec_and_return_summary(spec_path: str) -> Dict[str, Any]:
    """Run a DriftSpec and return a minimal execution summary.

    The underlying handlers are currently side-effect based (write outputs).
    This helper standardizes a simple summary contract for integrations.
    """
    run_spec(spec_path)
    return {
        "ok": True,
        "spec_path": spec_path,
    }


def load_and_validate_spec(spec_path: str) -> Tuple[Dict[str, Any], Dict[str, str]]:
    """Load and validate a spec, returning normalized type metadata.

    Returns:
        (spec_dict, type_info)
    """
    spec = load_spec(spec_path)
    validate_spec(spec)
    type_info = spec.get("type", {}) or {}
    return spec, {
        "family": str(type_info.get("family", "")),
        "category": str(type_info.get("category", "")),
        "subtype": str(type_info.get("subtype", "")),
    }


__all__ = [
    "get_filter",
    "get_schema_extractor",
    "load_and_validate_spec",
    "load_spec",
    "register_filter",
    "run_spec",
    "run_spec_and_return_summary",
    "trace_to_spec",
    "validate_spec",
]

