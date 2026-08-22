"""Load and execute DriftSpecs through the registered handler surface."""

from __future__ import annotations

import inspect
import random
import re
from collections.abc import Mapping
from os import PathLike
from typing import Any, Dict, Tuple

import numpy as np
import yaml

from .registry import Handler, get_handler
from .types import data_drift  # noqa: F401 - ensure handlers are registered
from .types import workload_keylist  # noqa: F401 - ensure handlers are registered
from .types import workload_query_mix  # noqa: F401 - ensure handlers are registered
from .types import workload_sql_templates  # noqa: F401 - ensure handlers are registered
from .types import workload_templates  # noqa: F401 - ensure handlers are registered


_BINDING_RE = re.compile(r"^\$\{([A-Za-z_][A-Za-z0-9_]*)\}$")


def load_spec(path: str | PathLike[str]) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as stream:
        payload = yaml.safe_load(stream)
    if not isinstance(payload, dict):
        raise ValueError("DriftSpec root must be a mapping.")
    return payload


def seed_everything(seed: int | None) -> None:
    if seed is not None:
        random.seed(seed)
        np.random.seed(seed)


def get_type_triple(spec: Dict[str, Any]) -> Tuple[str, str, str]:
    spec_type = spec.get("type", {})
    return (
        spec_type.get("family", ""),
        spec_type.get("category", ""),
        spec_type.get("subtype", ""),
    )


def migrate_spec(spec: Dict[str, Any]) -> Dict[str, Any]:
    # Reserved for future migrations; keep idempotent.
    spec.setdefault("spec_version", 1)
    return spec


def validate_spec(spec: Dict[str, Any]) -> None:
    if "type" not in spec:
        raise ValueError("Missing 'type' in spec.")
    if "variables" not in spec:
        raise ValueError("Missing 'variables' in spec.")


def run_spec(
    spec_path: str | PathLike[str],
    *,
    bindings: Mapping[str, Any] | None = None,
    runtime_inputs: Mapping[str, Any] | None = None,
) -> Any:
    """Execute one DriftSpec and return its registered handler result.

    Bindings replace only scalar values that are exactly ``${NAME}``; partial
    string interpolation is intentionally unsupported.  Every placeholder
    must have a binding and every supplied binding must be used.

    Process-global Python and NumPy RNG states are restored even when a
    handler raises.  ``seed`` and other semantic controls remain owned by the
    spec rather than by executor keyword overrides.
    """

    spec = _bind_spec(load_spec(spec_path), bindings)
    spec = migrate_spec(spec)
    validate_spec(spec)
    normalized_runtime_inputs = _runtime_input_map(runtime_inputs)

    python_state = random.getstate()
    numpy_state = np.random.get_state()
    try:
        seed_everything(spec.get("seed"))
        handler = get_handler(get_type_triple(spec))
        return _invoke_handler(handler, spec, normalized_runtime_inputs)
    finally:
        random.setstate(python_state)
        np.random.set_state(numpy_state)


def run_all(
    spec_path: str | PathLike[str],
    *,
    bindings: Mapping[str, Any] | None = None,
    runtime_inputs: Mapping[str, Any] | None = None,
) -> Any:
    """Backward-compatible alias for :func:`run_spec`."""

    return run_spec(spec_path, bindings=bindings, runtime_inputs=runtime_inputs)


def _bind_spec(
    spec: Dict[str, Any], bindings: Mapping[str, Any] | None
) -> Dict[str, Any]:
    if bindings is None:
        binding_map: dict[str, Any] = {}
    elif not isinstance(bindings, Mapping):
        raise TypeError("bindings must be a mapping or None")
    else:
        binding_map = dict(bindings)

    invalid_names = sorted(
        repr(name)
        for name in binding_map
        if not isinstance(name, str) or not _BINDING_RE.fullmatch(f"${{{name}}}")
    )
    if invalid_names:
        raise ValueError(f"invalid binding names: {invalid_names}")

    used: set[str] = set()
    missing: set[str] = set()

    def resolve(value: Any) -> Any:
        if isinstance(value, str):
            match = _BINDING_RE.fullmatch(value)
            if match is None:
                return value
            name = match.group(1)
            if name not in binding_map:
                missing.add(name)
                return value
            used.add(name)
            return binding_map[name]
        if isinstance(value, list):
            return [resolve(item) for item in value]
        if isinstance(value, tuple):
            return tuple(resolve(item) for item in value)
        if isinstance(value, Mapping):
            resolved: dict[Any, Any] = {}
            for key, item in value.items():
                resolved_key = resolve(key)
                try:
                    duplicate = resolved_key in resolved
                except TypeError as exc:
                    raise TypeError("a binding used as a mapping key must be hashable") from exc
                if duplicate:
                    raise ValueError(
                        f"binding resolution produced duplicate key: {resolved_key!r}"
                    )
                resolved[resolved_key] = resolve(item)
            return resolved
        return value

    resolved_spec = resolve(spec)
    if missing:
        raise ValueError(f"missing bindings: {sorted(missing)}")
    unused = set(binding_map) - used
    if unused:
        raise ValueError(f"unused bindings: {sorted(unused)}")
    return resolved_spec


def _runtime_input_map(
    runtime_inputs: Mapping[str, Any] | None,
) -> dict[str, Any]:
    if runtime_inputs is None:
        return {}
    if not isinstance(runtime_inputs, Mapping):
        raise TypeError("runtime_inputs must be a mapping or None")
    invalid = sorted(
        repr(name)
        for name in runtime_inputs
        if not isinstance(name, str) or not name
    )
    if invalid:
        raise ValueError(f"runtime input names must be non-empty strings: {invalid}")
    return dict(runtime_inputs)


def _invoke_handler(
    handler: Handler,
    spec: Dict[str, Any],
    runtime_inputs: dict[str, Any],
) -> Any:
    if not runtime_inputs:
        return handler(spec)

    parameters = inspect.signature(handler).parameters
    accepts_runtime_inputs = "runtime_inputs" in parameters or any(
        parameter.kind is inspect.Parameter.VAR_KEYWORD
        for parameter in parameters.values()
    )
    if not accepts_runtime_inputs:
        triple = get_type_triple(spec)
        raise ValueError(
            f"DriftSpec handler {triple!r} does not accept runtime_inputs"
        )
    return handler(spec, runtime_inputs=runtime_inputs)


__all__ = [
    "get_type_triple",
    "load_spec",
    "migrate_spec",
    "run_all",
    "run_spec",
    "seed_everything",
    "validate_spec",
]
