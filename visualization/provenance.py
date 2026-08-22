"""One canonical provenance and cache contract for generation and validation."""

from __future__ import annotations

from typing import Any, Mapping, Sequence

from . import VISUALIZATION_SCHEMA_VERSION
from .artifacts import semantic_hash
from .benchmarks import (
    BenchmarkDefinition,
    get_scenario_entry,
    scenario_config,
)
from .drift_scenarios import SPEC_EXECUTOR, SPEC_EXECUTOR_VERSION
from .specs import CanonicalSpec


CACHE_SCHEMA = "driftbench.visualization-cache/v4"


def configuration_projection(
    *,
    definition: BenchmarkDefinition,
    kind: str,
    scenario: str,
    spec_descriptor: Mapping[str, Any],
    spec_semantic_sha256: str,
    effect_policy: Mapping[str, Any],
    analysis: Mapping[str, Any],
    render: Mapping[str, Any],
) -> dict[str, Any]:
    """Return every registry/spec/render input that can affect an artifact."""

    entry = get_scenario_entry(kind, definition.name, scenario)
    return {
        "schema": VISUALIZATION_SCHEMA_VERSION,
        "registry_schema": scenario_config()["schema_version"],
        "benchmark": definition.name,
        "kind": kind,
        "scenario": scenario,
        "definition": {
            "name": definition.name,
            "title": definition.title,
            "description": definition.description,
            "adapter_module": definition.adapter_module,
            "adapter": definition.adapter,
            "scale": dict(definition.scale),
            "default_data_target": {
                "table": definition.table,
                "column": definition.column,
            },
            "capabilities": dict(definition.query_capabilities),
            "limitations": definition.limitations,
        },
        "scenario_entry": dict(entry),
        "spec": {
            "descriptor": dict(spec_descriptor),
            "semantic_sha256": spec_semantic_sha256,
            "effect_policy": dict(effect_policy),
        },
        "executor": {
            "engine": SPEC_EXECUTOR,
            "engine_version": SPEC_EXECUTOR_VERSION,
        },
        "analysis": dict(analysis),
        "render": dict(render),
    }


def configuration_hash(
    *,
    definition: BenchmarkDefinition,
    kind: str,
    scenario: str,
    spec_descriptor: Mapping[str, Any],
    spec_semantic_sha256: str,
    effect_policy: Mapping[str, Any],
    analysis: Mapping[str, Any],
    render: Mapping[str, Any],
) -> str:
    return semantic_hash(
        configuration_projection(
            definition=definition,
            kind=kind,
            scenario=scenario,
            spec_descriptor=spec_descriptor,
            spec_semantic_sha256=spec_semantic_sha256,
            effect_policy=effect_policy,
            analysis=analysis,
            render=render,
        )
    )


def configuration_hash_for_spec(
    spec: CanonicalSpec,
    *,
    definition: BenchmarkDefinition,
    analysis: Mapping[str, Any],
    render: Mapping[str, Any],
) -> str:
    return configuration_hash(
        definition=definition,
        kind=spec.kind,
        scenario=spec.scenario,
        spec_descriptor=spec.descriptor,
        spec_semantic_sha256=spec.semantic_sha256,
        effect_policy=spec.payload["effect_policy"],
        analysis=analysis,
        render=render,
    )


def resolved_spec_hash(
    *,
    spec_semantic_sha256: str,
    seed: int,
    sample_size: int,
    inputs: Sequence[Mapping[str, Any]],
    resolved_parameters: Mapping[str, Any],
) -> str:
    return semantic_hash(
        {
            "spec_semantic_sha256": spec_semantic_sha256,
            "seed": seed,
            "sample_size": sample_size,
            "inputs": list(inputs),
            "resolved_parameters": dict(resolved_parameters),
        }
    )


def cache_fingerprint(
    *,
    driftbench_version: str,
    benchmark: str,
    kind: str,
    scenario: str,
    seed: int,
    sample_size: int,
    config_sha256: str,
    spec_descriptor: Mapping[str, Any],
    analysis: Mapping[str, Any],
    render: Mapping[str, Any],
    inputs: Sequence[Mapping[str, Any]],
) -> str:
    return semantic_hash(
        {
            "schema": CACHE_SCHEMA,
            "driftbench_version": driftbench_version,
            "benchmark": benchmark,
            "kind": kind,
            "scenario": scenario,
            "seed": seed,
            "sample_size": sample_size,
            "config_sha256": config_sha256,
            "spec": dict(spec_descriptor),
            "analysis": dict(analysis),
            "render": dict(render),
            "executor": {
                "engine": SPEC_EXECUTOR,
                "engine_version": SPEC_EXECUTOR_VERSION,
            },
            "inputs": list(inputs),
        }
    )


def manifest_semantic_hash(payload: Mapping[str, Any]) -> str:
    """Hash a complete manifest except its timestamp and self hash."""

    projected = dict(payload)
    projected.pop("generated_at", None)
    projected.pop("semantic_sha256", None)
    return semantic_hash(projected)


__all__ = [
    "CACHE_SCHEMA",
    "cache_fingerprint",
    "configuration_hash",
    "configuration_hash_for_spec",
    "configuration_projection",
    "manifest_semantic_hash",
    "resolved_spec_hash",
]
