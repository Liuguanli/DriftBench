from __future__ import annotations

import math
from typing import Any


TPCH_PARAM_SPECS_IDENTITY_SCHEMA = "driftbench.tpch-param-specs-identity/v1"
_TPCH_PARAM_SPECS_MAX_DEPTH = 64


class TPCHParamSpecsIdentityError(ValueError):
    """Raised when custom TPC-H parameters cannot form a stable identity."""


def canonicalize_tpch_param_specs(
    value: Any,
    *,
    require_remote_safe: bool,
) -> dict[str, Any]:
    """Return a JSON-native identity that preserves generator-visible semantics.

    The TPC-H custom generator distinguishes container and scalar types and can
    stringify nested mappings in insertion order.  A tagged tree therefore
    records exact scalar/container types, mapping key types, and mapping order
    instead of applying ordinary JSON normalization.
    """

    try:
        root = _canonicalize_node(
            value,
            require_remote_safe=require_remote_safe,
            active_containers=set(),
            depth=0,
        )
    except RecursionError as exc:
        raise TPCHParamSpecsIdentityError(
            "TPC-H custom param_specs nesting is too deep"
        ) from exc
    return {"schema": TPCH_PARAM_SPECS_IDENTITY_SCHEMA, "root": root}


def _canonicalize_node(
    value: Any,
    *,
    require_remote_safe: bool,
    active_containers: set[int],
    depth: int,
) -> dict[str, Any]:
    if depth > _TPCH_PARAM_SPECS_MAX_DEPTH:
        raise TPCHParamSpecsIdentityError(
            "TPC-H custom param_specs nesting is too deep"
        )
    value_type = type(value)
    if value is None:
        return {"type": "none"}
    if value_type is str:
        return {"type": "str", "value": value}
    if value_type is bool:
        return {"type": "bool", "value": value}
    if value_type is int:
        # Store generator-visible decimal text under an explicit type tag so it
        # cannot be confused with a JSON floating number.
        try:
            rendered = str(value)
        except ValueError as exc:
            raise TPCHParamSpecsIdentityError(
                "TPC-H custom param_specs integers must be renderable"
            ) from exc
        return {"type": "int", "value": rendered}
    if value_type is float:
        if not math.isfinite(value):
            raise TPCHParamSpecsIdentityError(
                "TPC-H custom param_specs numbers must be finite"
            )
        # hex() is an exact, deterministic representation and distinguishes
        # integral floats (1.0) from integers (1), including signed zero.
        return {"type": "float", "value": value.hex()}

    if value_type is not dict and value_type is not list and value_type is not tuple:
        raise TPCHParamSpecsIdentityError(
            "TPC-H custom param_specs must use plain mappings and ordered sequences"
        )

    marker = id(value)
    if marker in active_containers:
        raise TPCHParamSpecsIdentityError(
            "TPC-H custom param_specs must not be cyclic"
        )
    active_containers.add(marker)
    try:
        if value_type is dict:
            # Validate exact key types before any lookup. A hostile key with a
            # colliding hash can otherwise run user-defined equality from
            # dict.get()/membership checks during eligibility validation.
            for key in value:
                key_type = type(key)
                if key_type is not str and key_type is not int:
                    raise TPCHParamSpecsIdentityError(
                        "TPC-H custom param_specs keys must be strings or integers"
                    )
            parameter_type = value.get("type")
            if require_remote_safe and (
                "dist_file" in value
                or (
                    type(parameter_type) is str
                    and parameter_type == "dss_dist"
                )
            ):
                raise TPCHParamSpecsIdentityError(
                    "TPC-H custom parameters that read distribution files are not remote-cache eligible"
                )
            items: list[dict[str, Any]] = []
            for key, item in value.items():
                items.append(
                    {
                        "key": _canonicalize_node(
                            key,
                            require_remote_safe=require_remote_safe,
                            active_containers=active_containers,
                            depth=depth + 1,
                        ),
                        "value": _canonicalize_node(
                            item,
                            require_remote_safe=require_remote_safe,
                            active_containers=active_containers,
                            depth=depth + 1,
                        ),
                    }
                )
            return {"type": "dict", "items": items}

        return {
            "type": "list" if value_type is list else "tuple",
            "items": [
                _canonicalize_node(
                    item,
                    require_remote_safe=require_remote_safe,
                    active_containers=active_containers,
                    depth=depth + 1,
                )
                for item in value
            ],
        }
    finally:
        active_containers.remove(marker)
