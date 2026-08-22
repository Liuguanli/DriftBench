"""Deterministic query-workload mix drift primitives.

This module deliberately models only a change in query-template frequency.  It
does not claim to estimate predicate selectivity, database cost, or arrival
rates.  The implementation is dependency-free and safe to import from the
stable :mod:`driftbench.api` surface.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import random
import tempfile
from dataclasses import dataclass, field
from numbers import Real
from pathlib import Path
from typing import Any, Iterable, Mapping


QUERY_MIX_ALGORITHM = "driftbench.query-workload-mix/v1"
QUERY_MIX_OUTPUT_SCHEMA = "driftbench.query-template-mix-result/v1"
_MISSING = object()


@dataclass(frozen=True)
class QueryTemplate:
    """A query template or non-SQL workload operation.

    ``sql`` is optional because adapters such as TPC-DS and YCSB expose only
    query identifiers or operation names in their public artifacts.
    """

    template_id: str
    sql: str | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not isinstance(self.template_id, str) or not self.template_id.strip():
            raise ValueError("template_id must be a non-empty string")
        if self.sql is not None and not isinstance(self.sql, str):
            raise TypeError("sql must be a string or None")
        if not isinstance(self.metadata, Mapping):
            raise TypeError("metadata must be a mapping")
        normalized = _canonical_json_value(dict(self.metadata), "metadata")
        object.__setattr__(self, "template_id", self.template_id.strip())
        object.__setattr__(self, "metadata", normalized)


@dataclass(frozen=True)
class QueryWorkloadMixResult:
    """Baseline and drifted deterministic workload samples."""

    baseline: tuple[QueryTemplate, ...]
    drifted: tuple[QueryTemplate, ...]
    baseline_weights: Mapping[str, float]
    target_weights: Mapping[str, float]
    sample_size: int
    seed: int
    semantic_hash: str
    algorithm: str = QUERY_MIX_ALGORITHM


@dataclass(frozen=True)
class QueryTemplateMixSpec:
    """Validated executable inputs parsed from a ``template_mix`` DriftSpec."""

    templates: tuple[QueryTemplate, ...]
    baseline_weights: Mapping[str, float]
    target_weights: Mapping[str, float]
    sample_size: int
    seed: int
    output_path: Path


def apply_query_workload_mix_drift(
    templates: Iterable[QueryTemplate],
    *,
    baseline_weights: Mapping[str, Real] | None = None,
    target_weights: Mapping[str, Real],
    sample_size: int = 1000,
    seed: int = 42,
) -> QueryWorkloadMixResult:
    """Sample baseline and target query mixes with isolated deterministic RNGs.

    Weight maps must be complete: every template ID appears exactly once and
    no unknown IDs are accepted.  Values are normalized, so callers may use
    either probabilities or integer weights.
    """

    template_tuple = tuple(templates)
    if not template_tuple:
        raise ValueError("templates must not be empty")
    if not all(isinstance(template, QueryTemplate) for template in template_tuple):
        raise TypeError("templates must contain QueryTemplate values")

    template_ids = tuple(template.template_id for template in template_tuple)
    if len(set(template_ids)) != len(template_ids):
        raise ValueError("template IDs must be unique")
    if isinstance(sample_size, bool) or not isinstance(sample_size, int) or sample_size <= 0:
        raise ValueError("sample_size must be a positive integer")
    if isinstance(seed, bool) or not isinstance(seed, int):
        raise TypeError("seed must be an integer")

    if baseline_weights is None:
        baseline_weights = {template_id: 1.0 for template_id in template_ids}
    normalized_baseline = _normalize_weights(
        baseline_weights, template_ids, "baseline_weights"
    )
    normalized_target = _normalize_weights(
        target_weights, template_ids, "target_weights"
    )

    baseline_rng = random.Random(_derived_seed(seed, "baseline"))
    drifted_rng = random.Random(_derived_seed(seed, "drifted"))
    baseline = _sample(template_tuple, normalized_baseline, sample_size, baseline_rng)
    drifted = _sample(template_tuple, normalized_target, sample_size, drifted_rng)

    semantic_payload = {
        "algorithm": QUERY_MIX_ALGORITHM,
        "templates": [
            {
                "template_id": template.template_id,
                "sql": template.sql,
                "metadata": template.metadata,
            }
            for template in template_tuple
        ],
        "baseline_weights": normalized_baseline,
        "target_weights": normalized_target,
        "sample_size": sample_size,
        "seed": seed,
    }
    semantic_hash = hashlib.sha256(_canonical_json_bytes(semantic_payload)).hexdigest()
    return QueryWorkloadMixResult(
        baseline=baseline,
        drifted=drifted,
        baseline_weights=normalized_baseline,
        target_weights=normalized_target,
        sample_size=sample_size,
        seed=seed,
        semantic_hash=semantic_hash,
    )


def parse_query_template_mix_spec(
    spec: Mapping[str, Any],
    *,
    runtime_inputs: Mapping[str, Any] | None = None,
) -> QueryTemplateMixSpec:
    """Validate and normalize an executable query-template mix DriftSpec.

    The spec owns ``seed`` and ``variables.sample_size``. Runtime inputs may
    only provide richer ``QueryTemplate`` objects under the
    ``query_templates`` key; their IDs must exactly match the ordered
    ``variables.template_ids`` list.
    """

    if not isinstance(spec, Mapping):
        raise TypeError("spec must be a mapping")
    spec_type = spec.get("type")
    expected_type = {
        "family": "workload",
        "category": "drift",
        "subtype": "template_mix",
    }
    if not isinstance(spec_type, Mapping) or any(
        spec_type.get(key) != value for key, value in expected_type.items()
    ):
        raise ValueError("query mix spec type must be workload/drift/template_mix")

    seed = spec.get("seed", _MISSING)
    if seed is _MISSING:
        raise ValueError("query mix spec requires top-level seed")
    if isinstance(seed, bool) or not isinstance(seed, int):
        raise TypeError("query mix spec seed must be an integer")

    variables = spec.get("variables")
    if not isinstance(variables, Mapping):
        raise TypeError("query mix spec variables must be a mapping")

    sample_size = variables.get("sample_size", _MISSING)
    if sample_size is _MISSING:
        raise ValueError("query mix spec requires variables.sample_size")
    if (
        isinstance(sample_size, bool)
        or not isinstance(sample_size, int)
        or sample_size <= 0
    ):
        raise ValueError("variables.sample_size must be a positive integer")

    template_ids = _parse_template_ids(variables.get("template_ids"))
    runtime_map = _parse_runtime_inputs(runtime_inputs)
    template_override = runtime_map.get("query_templates", _MISSING)
    templates = _templates_for_ids(template_ids, template_override)
    baseline_weights = _expand_weight_config(
        template_ids, variables.get("baseline"), "variables.baseline"
    )
    target_weights = _expand_weight_config(
        template_ids, variables.get("target"), "variables.target"
    )

    output_value = variables.get("output_path", _MISSING)
    if output_value is _MISSING:
        raise ValueError("query mix spec requires variables.output_path")
    if not isinstance(output_value, (str, os.PathLike)):
        raise TypeError("variables.output_path must be a path string")
    output_text = os.fspath(output_value)
    if not isinstance(output_text, str):
        raise TypeError("variables.output_path must resolve to a string path")
    if not output_text.strip():
        raise ValueError("variables.output_path must not be empty")

    return QueryTemplateMixSpec(
        templates=templates,
        baseline_weights=baseline_weights,
        target_weights=target_weights,
        sample_size=sample_size,
        seed=seed,
        output_path=Path(output_text),
    )


def execute_query_template_mix_spec(
    spec: Mapping[str, Any],
    *,
    runtime_inputs: Mapping[str, Any] | None = None,
) -> QueryWorkloadMixResult:
    """Execute a template-mix spec and atomically write its JSON output."""

    parsed = parse_query_template_mix_spec(spec, runtime_inputs=runtime_inputs)
    result = apply_query_workload_mix_drift(
        parsed.templates,
        baseline_weights=parsed.baseline_weights,
        target_weights=parsed.target_weights,
        sample_size=parsed.sample_size,
        seed=parsed.seed,
    )
    _write_result_json(parsed.output_path, parsed.templates, result)
    return result


def _parse_template_ids(value: Any) -> tuple[str, ...]:
    if not isinstance(value, (list, tuple)) or not value:
        raise ValueError("variables.template_ids must be a non-empty list")
    if not all(isinstance(item, str) and item.strip() for item in value):
        raise ValueError("variables.template_ids must contain non-empty strings")
    template_ids = tuple(item.strip() for item in value)
    if len(set(template_ids)) != len(template_ids):
        raise ValueError("variables.template_ids must be unique")
    return template_ids


def _parse_runtime_inputs(
    runtime_inputs: Mapping[str, Any] | None,
) -> dict[str, Any]:
    if runtime_inputs is None:
        return {}
    if not isinstance(runtime_inputs, Mapping):
        raise TypeError("runtime_inputs must be a mapping or None")
    invalid_names = [
        name for name in runtime_inputs if not isinstance(name, str) or not name
    ]
    if invalid_names:
        raise ValueError(
            "runtime input names must be non-empty strings: "
            f"{sorted(map(repr, invalid_names))}"
        )
    unknown = set(runtime_inputs) - {"query_templates"}
    if unknown:
        raise ValueError(f"unused runtime inputs: {sorted(unknown)}")
    return dict(runtime_inputs)


def _templates_for_ids(
    template_ids: tuple[str, ...], override: Any
) -> tuple[QueryTemplate, ...]:
    if override is _MISSING:
        return tuple(QueryTemplate(template_id) for template_id in template_ids)
    if isinstance(override, (str, bytes)):
        raise TypeError(
            "runtime_inputs.query_templates must contain QueryTemplate values"
        )
    try:
        supplied = tuple(override)
    except TypeError as exc:
        raise TypeError(
            "runtime_inputs.query_templates must be an iterable of QueryTemplate values"
        ) from exc
    if not supplied or not all(isinstance(item, QueryTemplate) for item in supplied):
        raise TypeError(
            "runtime_inputs.query_templates must contain QueryTemplate values"
        )
    by_id: dict[str, QueryTemplate] = {}
    for template in supplied:
        if template.template_id in by_id:
            raise ValueError(
                "runtime_inputs.query_templates contains duplicate template IDs"
            )
        by_id[template.template_id] = template
    expected = set(template_ids)
    actual = set(by_id)
    missing = sorted(expected - actual)
    extra = sorted(actual - expected)
    if missing or extra:
        details: list[str] = []
        if missing:
            details.append(f"missing IDs: {missing}")
        if extra:
            details.append(f"unknown IDs: {extra}")
        raise ValueError(
            "runtime_inputs.query_templates must exactly match variables.template_ids "
            f"({'; '.join(details)})"
        )
    return tuple(by_id[template_id] for template_id in template_ids)


def _expand_weight_config(
    template_ids: tuple[str, ...], value: Any, field: str
) -> dict[str, float]:
    if not isinstance(value, Mapping):
        raise TypeError(f"{field} must be a mapping")

    forms = ["mode" in value, "weights" in value, "focus" in value]
    if sum(forms) != 1:
        raise ValueError(
            f"{field} must use exactly one of mode=uniform, weights, or focus"
        )

    if "mode" in value:
        if set(value) != {"mode"} or value.get("mode") != "uniform":
            raise ValueError(f"{field}.mode must be uniform with no extra fields")
        weights: Mapping[str, Real] = {
            template_id: 1.0 for template_id in template_ids
        }
    elif "weights" in value:
        if set(value) != {"weights"} or not isinstance(value["weights"], Mapping):
            raise ValueError(f"{field}.weights must be a mapping with no extra fields")
        weights = value["weights"]
    else:
        if set(value) != {"focus", "remaining_total"}:
            raise ValueError(
                f"{field}.focus requires exactly focus and remaining_total"
            )
        focus = value["focus"]
        if not isinstance(focus, Mapping):
            raise TypeError(f"{field}.focus must be a mapping")
        invalid_focus_ids = [key for key in focus if not isinstance(key, str)]
        if invalid_focus_ids:
            raise TypeError(
                f"{field}.focus IDs must be strings: "
                f"{sorted(map(repr, invalid_focus_ids))}"
            )
        unknown = set(focus) - set(template_ids)
        if unknown:
            raise ValueError(f"{field}.focus contains unknown IDs: {sorted(unknown)}")
        remaining_ids = [item for item in template_ids if item not in focus]
        remaining_total = _nonnegative_real(
            value["remaining_total"], f"{field}.remaining_total"
        )
        if not remaining_ids and remaining_total != 0:
            raise ValueError(
                f"{field}.remaining_total must be zero when every template is focused"
            )
        each = remaining_total / len(remaining_ids) if remaining_ids else 0.0
        weights = {
            template_id: focus.get(template_id, each)
            for template_id in template_ids
        }

    return _normalize_weights(weights, template_ids, field)


def _nonnegative_real(value: Any, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, Real):
        raise TypeError(f"{field} must be a real number")
    converted = float(value)
    if not math.isfinite(converted) or converted < 0:
        raise ValueError(f"{field} must be finite and non-negative")
    return converted


def _write_result_json(
    output_path: Path,
    templates: tuple[QueryTemplate, ...],
    result: QueryWorkloadMixResult,
) -> None:
    payload = {
        "schema": QUERY_MIX_OUTPUT_SCHEMA,
        "algorithm": result.algorithm,
        "seed": result.seed,
        "sample_size": result.sample_size,
        "semantic_hash": result.semantic_hash,
        "templates": [
            {
                "template_id": template.template_id,
                "sql": template.sql,
                "metadata": template.metadata,
            }
            for template in templates
        ],
        "baseline_weights": result.baseline_weights,
        "target_weights": result.target_weights,
        "baseline": [template.template_id for template in result.baseline],
        "drifted": [template.template_id for template in result.drifted],
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temp_name = tempfile.mkstemp(
        prefix=f".{output_path.name}.", suffix=".tmp", dir=output_path.parent
    )
    temp_path = Path(temp_name)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(_canonical_json_bytes(payload) + b"\n")
        os.replace(temp_path, output_path)
    finally:
        if temp_path.exists():
            temp_path.unlink()


def _normalize_weights(
    weights: Mapping[str, Real], template_ids: tuple[str, ...], name: str
) -> dict[str, float]:
    if not isinstance(weights, Mapping):
        raise TypeError(f"{name} must be a mapping")
    invalid_ids = [template_id for template_id in weights if not isinstance(template_id, str)]
    if invalid_ids:
        raise TypeError(
            f"{name} IDs must be strings: {sorted(map(repr, invalid_ids))}"
        )
    expected = set(template_ids)
    actual = set(weights)
    missing = sorted(expected - actual)
    extra = sorted(actual - expected)
    if missing or extra:
        details: list[str] = []
        if missing:
            details.append(f"missing IDs: {missing}")
        if extra:
            details.append(f"unknown IDs: {extra}")
        raise ValueError(f"{name} must exactly match template IDs ({'; '.join(details)})")

    numeric: dict[str, float] = {}
    for template_id in template_ids:
        value = weights[template_id]
        if isinstance(value, bool) or not isinstance(value, Real):
            raise TypeError(f"{name}[{template_id!r}] must be a real number")
        converted = float(value)
        if not math.isfinite(converted) or converted < 0:
            raise ValueError(f"{name}[{template_id!r}] must be finite and non-negative")
        numeric[template_id] = converted
    total = math.fsum(numeric.values())
    if total <= 0:
        raise ValueError(f"{name} must have a positive total weight")
    return {template_id: numeric[template_id] / total for template_id in template_ids}


def _sample(
    templates: tuple[QueryTemplate, ...],
    weights: Mapping[str, float],
    sample_size: int,
    rng: random.Random,
) -> tuple[QueryTemplate, ...]:
    cumulative: list[float] = []
    total = 0.0
    for template in templates:
        total += weights[template.template_id]
        cumulative.append(total)
    cumulative[-1] = 1.0

    sampled: list[QueryTemplate] = []
    for _ in range(sample_size):
        draw = rng.random()
        index = 0
        while index < len(cumulative) - 1 and draw >= cumulative[index]:
            index += 1
        sampled.append(templates[index])
    return tuple(sampled)


def _derived_seed(seed: int, stream: str) -> int:
    digest = hashlib.sha256(f"{QUERY_MIX_ALGORITHM}|{seed}|{stream}".encode("utf-8"))
    return int.from_bytes(digest.digest()[:16], "big")


def _canonical_json_bytes(payload: Any) -> bytes:
    normalized = _canonical_json_value(payload, "payload")
    return json.dumps(
        normalized,
        allow_nan=False,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _canonical_json_value(value: Any, path: str) -> Any:
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError(f"{path} contains a non-finite float")
        return value
    if isinstance(value, Mapping):
        normalized: dict[str, Any] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise TypeError(f"{path} mapping keys must be strings")
            normalized[key] = _canonical_json_value(item, f"{path}.{key}")
        return {key: normalized[key] for key in sorted(normalized)}
    if isinstance(value, (list, tuple)):
        return [
            _canonical_json_value(item, f"{path}[{index}]")
            for index, item in enumerate(value)
        ]
    raise TypeError(f"{path} contains unsupported type {type(value).__name__}")


__all__ = [
    "QUERY_MIX_ALGORITHM",
    "QUERY_MIX_OUTPUT_SCHEMA",
    "QueryTemplate",
    "QueryTemplateMixSpec",
    "QueryWorkloadMixResult",
    "apply_query_workload_mix_drift",
    "execute_query_template_mix_spec",
    "parse_query_template_mix_spec",
]
