"""Canonical, portable DriftSpec loading and trace descriptors."""

from __future__ import annotations

import math
import re
from dataclasses import dataclass
from importlib import resources
from pathlib import Path
from typing import Any, Mapping

import yaml

from driftbench.api import parse_query_template_mix_spec, validate_spec

from .artifacts import (
    atomic_write_text,
    ensure_managed_path,
    file_descriptor,
    reject_machine_paths,
    semantic_hash,
)
from .benchmarks import BENCHMARK_ORDER, get_scenario_entry, scenario_entries


DATA_TYPES = {
    ("data", "drift", "single_table"),
    ("data", "drift", "multi_table"),
}
QUERY_TYPE = ("workload", "drift", "template_mix")

_TOP_LEVEL_KEYS = {
    "spec_version",
    "pattern_id",
    "seed",
    "type",
    "metadata",
    "effect_policy",
    "data_source",
    "variables",
}
_SINGLE_TABLE_OPERATIONS = frozenset(
    {"vary_cardinality", "value_skew", "outlier_injection"}
)
_EFFECT_METRICS = {
    "vary_cardinality": frozenset(
        {"absolute_row_rate", "row_reduction_rate", "row_growth_rate"}
    ),
    "value_skew": frozenset(
        {
            "ks_distance",
            "normalized_wasserstein_p95_p05",
            "jensen_shannon_divergence_bits",
            "total_variation_distance",
        }
    ),
    "outlier_injection": frozenset(
        {
            "tail_gain_over_baseline_p99",
            "normalized_wasserstein_p95_p05",
            "row_growth_rate",
        }
    ),
    "delete_keys": frozenset(
        {
            "row_reduction_rate",
            "target_stratum_share_shift_pp",
            "target_stratum_share_reduction_pp",
            "orphan_count",
        }
    ),
    "query": frozenset(
        {
            "jensen_shannon_divergence_bits",
            "total_variation_distance",
            "max_mover_absolute_pp",
        }
    ),
}
_RUNTIME_PLACEHOLDER_RE = re.compile(
    r"^\$\{DRIFTBENCH_(?:(?:INPUT|OUTPUT)(?:_[A-Z][A-Z0-9_]*)?|SCHEMA)\}$"
)
_TEMPLATE_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]*$")
_JOB_SCENARIO_RE = re.compile(r"^(pre|post)_([0-9]+)_title_deletion$")
_JOB_TABLES = frozenset(
    {
        "cast_info",
        "company_name",
        "company_type",
        "info_type",
        "keyword",
        "kind_type",
        "movie_companies",
        "movie_info",
        "movie_keyword",
        "name",
        "title",
    }
)
_JOB_RELATIONSHIPS = {
    "cast_info_title": {
        "name": "cast_info_title",
        "fact": "cast_info",
        "fk": "movie_id",
        "dim": "title",
        "pk": "id",
    },
    "movie_info_title": {
        "name": "movie_info_title",
        "fact": "movie_info",
        "fk": "movie_id",
        "dim": "title",
        "pk": "id",
    },
    "movie_companies_title": {
        "name": "movie_companies_title",
        "fact": "movie_companies",
        "fk": "movie_id",
        "dim": "title",
        "pk": "id",
    },
    "movie_keyword_title": {
        "name": "movie_keyword_title",
        "fact": "movie_keyword",
        "fk": "movie_id",
        "dim": "title",
        "pk": "id",
    },
    "cast_info_name": {
        "name": "cast_info_name",
        "fact": "cast_info",
        "fk": "person_id",
        "dim": "name",
        "pk": "id",
    },
    "movie_companies_co": {
        "name": "movie_companies_co",
        "fact": "movie_companies",
        "fk": "company_id",
        "dim": "company_name",
        "pk": "id",
    },
    "movie_keyword_kw": {
        "name": "movie_keyword_kw",
        "fact": "movie_keyword",
        "fk": "keyword_id",
        "dim": "keyword",
        "pk": "id",
    },
}
_JOB_TITLE_RELATIONSHIPS = frozenset(
    {
        "cast_info_title",
        "movie_info_title",
        "movie_companies_title",
        "movie_keyword_title",
    }
)


@dataclass(frozen=True)
class CanonicalSpec:
    kind: str
    benchmark: str
    scenario: str
    path: Path
    payload: Mapping[str, Any]
    descriptor: Mapping[str, Any]
    semantic_sha256: str
    type_triple: tuple[str, str, str]
    rationale: str


def expected_artifact_keys() -> tuple[tuple[str, str, str], ...]:
    return tuple(
        (kind, benchmark, scenario)
        for benchmark in BENCHMARK_ORDER
        for kind in ("data", "query")
        for scenario, _ in scenario_entries(kind, benchmark)
    )


def load_canonical_spec(
    output_root: Path,
    *,
    kind: str,
    benchmark: str,
    scenario: str,
) -> CanonicalSpec:
    entry = get_scenario_entry(kind, benchmark, scenario)
    relative = Path(str(entry["spec"]))
    destination = ensure_managed_path(output_root, *relative.parts)
    if not destination.is_file():
        source = resources.files("visualization").joinpath(*relative.parts)
        try:
            text = source.read_text(encoding="utf-8")
        except (FileNotFoundError, OSError) as exc:
            raise ValueError(
                f"canonical DriftSpec is missing: {relative.as_posix()}"
            ) from exc
        atomic_write_text(destination, text)

    payload = yaml.safe_load(destination.read_text(encoding="utf-8"))
    if not isinstance(payload, Mapping):
        raise ValueError(f"DriftSpec must be a mapping: {relative.as_posix()}")
    normalized = dict(payload)
    type_triple = validate_canonical_spec(
        normalized,
        kind=kind,
        benchmark=benchmark,
        scenario=scenario,
    )
    descriptor = file_descriptor(destination, output_root)
    return CanonicalSpec(
        kind=kind,
        benchmark=benchmark,
        scenario=scenario,
        path=destination,
        payload=normalized,
        descriptor=descriptor,
        semantic_sha256=semantic_hash(normalized),
        type_triple=type_triple,
        rationale=str(entry["rationale"]),
    )


def validate_canonical_spec(
    payload: Mapping[str, Any],
    *,
    kind: str,
    benchmark: str,
    scenario: str,
) -> tuple[str, str, str]:
    if not isinstance(payload, Mapping):
        raise ValueError("canonical DriftSpec must be a mapping")
    _reject_unsafe_runtime_features(payload)
    reject_machine_paths(payload, "driftspec")
    _require_exact_keys(payload, _TOP_LEVEL_KEYS, "DriftSpec")
    validate_spec(dict(payload))
    if (
        isinstance(payload.get("spec_version"), bool)
        or not isinstance(payload.get("spec_version"), int)
        or payload.get("spec_version") != 1
    ):
        raise ValueError("canonical DriftSpec must use spec_version 1")
    if (
        isinstance(payload.get("seed"), bool)
        or not isinstance(payload.get("seed"), int)
        or payload.get("seed") != 42
    ):
        raise ValueError("canonical DriftSpec seed must be 42")

    entry = get_scenario_entry(kind, benchmark, scenario)
    expected_pattern = (
        f"visualization-{benchmark}-query-{scenario}"
        if kind == "query"
        else f"visualization-{benchmark}-{scenario}"
    ).replace("_", "-")
    if payload.get("pattern_id") != expected_pattern:
        raise ValueError(
            f"canonical DriftSpec pattern_id must be {expected_pattern!r}"
        )

    if kind == "data":
        operation = str(entry["operation"])
        expected_type = (
            ("data", "drift", "multi_table")
            if operation == "delete_keys"
            else ("data", "drift", "single_table")
        )
    elif kind == "query":
        operation = "query"
        expected_type = QUERY_TYPE
    else:  # get_scenario_entry currently owns this error; keep the boundary explicit.
        raise ValueError(f"unsupported visualization kind: {kind}")

    type_payload = _require_mapping(payload.get("type"), "DriftSpec type")
    _require_exact_keys(
        type_payload, {"family", "category", "subtype"}, "DriftSpec type"
    )
    type_triple = tuple(
        type_payload[key] for key in ("family", "category", "subtype")
    )
    if type_triple != expected_type:
        raise ValueError(
            "canonical DriftSpec type mismatch: "
            f"expected {expected_type}, got {type_triple}"
        )

    metadata = payload.get("metadata")
    comparison = _validate_metadata(
        metadata,
        kind=kind,
        benchmark=benchmark,
        scenario=scenario,
        multi_table=expected_type[-1] == "multi_table",
        entry=entry,
    )
    if kind == "query":
        _validate_query_spec(payload, benchmark=benchmark)
    elif operation == "delete_keys":
        _validate_job_spec(
            payload,
            benchmark=benchmark,
            scenario=scenario,
            comparison=comparison,
        )
    else:
        if operation not in _SINGLE_TABLE_OPERATIONS:
            raise ValueError(
                f"unsupported canonical single-table operation: {operation}"
            )
        _validate_single_table_spec(
            payload,
            scenario=scenario,
            operation=operation,
            comparison=comparison,
        )
    _validate_effect_policy(
        payload.get("effect_policy"), allowed_metrics=_EFFECT_METRICS[operation]
    )
    return type_triple  # type: ignore[return-value]


def _validate_metadata(
    value: Any,
    *,
    kind: str,
    benchmark: str,
    scenario: str,
    multi_table: bool,
    entry: Mapping[str, Any],
) -> Mapping[str, Any] | None:
    metadata = _require_mapping(value, "DriftSpec metadata")
    expected_keys = (
        {"benchmark", "kind", "scenario", "comparison"}
        if kind == "data"
        else {"benchmark", "kind", "scenario"}
    )
    _require_exact_keys(metadata, expected_keys, "DriftSpec metadata")
    expected_identity = {
        "benchmark": benchmark,
        "kind": kind,
        "scenario": scenario,
    }
    if any(
        metadata.get(key) != expected for key, expected in expected_identity.items()
    ):
        raise ValueError(
            f"DriftSpec identity mismatch for {benchmark}/{kind}/{scenario}"
        )
    if kind == "query":
        return None

    comparison = _require_mapping(
        metadata.get("comparison"), "DriftSpec metadata.comparison"
    )
    comparison_keys = (
        {"table", "column", "stratum"} if multi_table else {"table", "column"}
    )
    _require_exact_keys(
        comparison, comparison_keys, "DriftSpec metadata.comparison"
    )
    target = _require_mapping(entry.get("target"), "scenario registry target")
    _require_exact_keys(target, {"table", "column"}, "scenario registry target")
    if comparison.get("table") != target.get("table") or comparison.get(
        "column"
    ) != target.get("column"):
        raise ValueError("DriftSpec comparison target does not match scenario registry")
    return comparison


def _validate_single_table_spec(
    payload: Mapping[str, Any],
    *,
    scenario: str,
    operation: str,
    comparison: Mapping[str, Any] | None,
) -> None:
    assert comparison is not None
    source = _require_mapping(payload.get("data_source"), "DriftSpec data_source")
    _require_exact_keys(
        source, {"kind", "path", "schema_extractor"}, "DriftSpec data_source"
    )
    if source.get("kind") != "csv" or source.get("path") != "${DRIFTBENCH_INPUT}":
        raise ValueError(
            "single-table DriftSpec requires its exact CSV input placeholder"
        )
    extractor = _require_mapping(
        source.get("schema_extractor"), "DriftSpec data_source.schema_extractor"
    )
    _require_exact_keys(
        extractor,
        {"source_type", "sample_size", "schema_output_path"},
        "DriftSpec data_source.schema_extractor",
    )
    if (
        extractor.get("source_type") != "csv"
        or isinstance(extractor.get("sample_size"), bool)
        or not isinstance(extractor.get("sample_size"), int)
        or extractor.get("sample_size") != 1000
        or extractor.get("schema_output_path") != "${DRIFTBENCH_SCHEMA}"
    ):
        raise ValueError(
            "single-table DriftSpec requires the canonical schema extractor"
        )

    variables = _require_mapping(payload.get("variables"), "DriftSpec variables")
    _require_exact_keys(variables, {"base_table", "drifts"}, "DriftSpec variables")
    if variables.get("base_table") != comparison["table"]:
        raise ValueError("variables.base_table must match the comparison table")
    drifts = variables.get("drifts")
    if not isinstance(drifts, list) or len(drifts) != 1:
        raise ValueError("canonical single-table DriftSpec requires exactly one drift")
    drift = _require_mapping(drifts[0], "DriftSpec variables.drifts[0]")
    drift_type = drift.get("drift_type")
    if drift_type == "selective_deletion":
        raise ValueError("selective_deletion is not an approved canonical operation")
    if (
        not isinstance(drift_type, str)
        or drift_type not in _SINGLE_TABLE_OPERATIONS
        or drift_type != operation
    ):
        raise ValueError("DriftSpec operation does not match the scenario registry")

    common = {"name", "drift_type", "output_path"}
    operation_keys = {
        "vary_cardinality": {"scale"},
        "value_skew": {"columns", "portion", "skewness"},
        "outlier_injection": {
            "column",
            "n_ratio",
            "extreme_direction",
            "extreme_scale",
        },
    }
    _require_exact_keys(
        drift, common | operation_keys[operation], "DriftSpec variables.drifts[0]"
    )
    if drift.get("name") != scenario:
        raise ValueError("canonical drift name must match its scenario")
    if drift.get("output_path") != "${DRIFTBENCH_OUTPUT}":
        raise ValueError("single-table drift requires the exact output placeholder")

    if operation == "vary_cardinality":
        scale = _finite_number(drift.get("scale"), "vary_cardinality scale")
        if scale <= 0.0 or math.isclose(scale, 1.0, rel_tol=0.0, abs_tol=1e-12):
            raise ValueError(
                "vary_cardinality scale must be positive and change row count"
            )
        return
    if operation == "value_skew":
        if drift.get("columns") != [comparison["column"]]:
            raise ValueError("value_skew columns must match the comparison column")
        portion = _finite_number(drift.get("portion"), "value_skew portion")
        skewness = _finite_number(drift.get("skewness"), "value_skew skewness")
        if not 0.0 < portion <= 1.0 or skewness <= 0.0:
            raise ValueError("value_skew parameters are outside the canonical domain")
        return

    if drift.get("column") != comparison["column"]:
        raise ValueError("outlier_injection column must match the comparison column")
    ratio = _finite_number(drift.get("n_ratio"), "outlier_injection n_ratio")
    scale = _finite_number(
        drift.get("extreme_scale"), "outlier_injection extreme_scale"
    )
    direction = drift.get("extreme_direction")
    if (
        not 0.0 < ratio <= 1.0
        or scale <= 1.0
        or not isinstance(direction, str)
        or direction not in {"high", "low"}
    ):
        raise ValueError(
            "outlier_injection parameters are outside the canonical domain"
        )


def _validate_query_spec(payload: Mapping[str, Any], *, benchmark: str) -> None:
    source = _require_mapping(payload.get("data_source"), "DriftSpec data_source")
    _require_exact_keys(source, {"kind", "benchmark"}, "DriftSpec data_source")
    if source != {"kind": "benchmark_adapter", "benchmark": benchmark}:
        raise ValueError("query DriftSpec must use its registered benchmark adapter")

    variables = _require_mapping(payload.get("variables"), "DriftSpec variables")
    _require_exact_keys(
        variables,
        {"template_ids", "baseline", "target", "sample_size", "output_path"},
        "DriftSpec variables",
    )
    template_ids = variables.get("template_ids")
    if (
        not isinstance(template_ids, list)
        or len(template_ids) < 2
        or any(
            not isinstance(item, str) or not _TEMPLATE_ID_RE.fullmatch(item)
            for item in template_ids
        )
        or len(set(template_ids)) != len(template_ids)
    ):
        raise ValueError("query template_ids must be unique, safe identifiers")
    if (
        isinstance(variables.get("sample_size"), bool)
        or not isinstance(variables.get("sample_size"), int)
        or variables.get("sample_size") != 1000
    ):
        raise ValueError("canonical query DriftSpec sample_size must be 1000")
    if variables.get("output_path") != "${DRIFTBENCH_OUTPUT}":
        raise ValueError("query DriftSpec requires the exact output placeholder")

    ids = tuple(template_ids)
    baseline = _validate_weight_form(variables.get("baseline"), ids, "baseline")
    target = _validate_weight_form(variables.get("target"), ids, "target")
    if all(
        math.isclose(baseline[item], target[item], rel_tol=0.0, abs_tol=1e-12)
        for item in ids
    ):
        raise ValueError("query baseline and target distributions must differ")


def _validate_weight_form(
    value: Any, template_ids: tuple[str, ...], field: str
) -> dict[str, float]:
    config = _require_mapping(value, f"DriftSpec variables.{field}")
    keys = set(config)
    if keys == {"mode"}:
        if config.get("mode") != "uniform":
            raise ValueError(f"{field} mode must be uniform")
        weight = 1.0 / len(template_ids)
        return {template_id: weight for template_id in template_ids}

    if keys == {"weights"}:
        weights = _require_mapping(
            config.get("weights"), f"DriftSpec variables.{field}.weights"
        )
        if set(weights) != set(template_ids):
            raise ValueError(f"{field} weights must exactly cover template_ids")
        normalized = {
            template_id: _nonnegative_weight(
                weights[template_id], f"{field} weight for {template_id}"
            )
            for template_id in template_ids
        }
        _require_unit_mass(normalized.values(), field)
        return normalized

    if keys == {"focus", "remaining_total"}:
        focus = _require_mapping(
            config.get("focus"), f"DriftSpec variables.{field}.focus"
        )
        focus_ids = set(focus)
        if not focus_ids or not focus_ids < set(template_ids):
            raise ValueError(f"{field} focus must be a non-empty proper subset")
        focus_weights = {
            template_id: _nonnegative_weight(
                focus[template_id], f"{field} focus weight for {template_id}"
            )
            for template_id in focus_ids
        }
        remaining_total = _nonnegative_weight(
            config.get("remaining_total"), f"{field} remaining_total"
        )
        _require_unit_mass((*focus_weights.values(), remaining_total), field)
        remaining = tuple(item for item in template_ids if item not in focus_ids)
        each = remaining_total / len(remaining)
        return {
            template_id: focus_weights.get(template_id, each)
            for template_id in template_ids
        }

    raise ValueError(
        f"{field} must use exactly one approved weight form: uniform, weights, or focus"
    )


def _validate_job_spec(
    payload: Mapping[str, Any],
    *,
    benchmark: str,
    scenario: str,
    comparison: Mapping[str, Any] | None,
) -> None:
    if benchmark != "job" or comparison is None:
        raise ValueError("multi-table canonical drift is restricted to JOB")
    source = _require_mapping(payload.get("data_source"), "DriftSpec data_source")
    _require_exact_keys(source, {"kind"}, "DriftSpec data_source")
    if source.get("kind") != "multi_table":
        raise ValueError("JOB drift requires the canonical multi-table data source")

    variables = _require_mapping(payload.get("variables"), "DriftSpec variables")
    _require_exact_keys(
        variables,
        {"validate_integrity", "tables", "relationships", "drift_steps"},
        "DriftSpec variables",
    )
    if variables.get("validate_integrity") is not True:
        raise ValueError("JOB canonical drift must validate referential integrity")
    _validate_job_tables(variables.get("tables"))
    _validate_job_relationships(variables.get("relationships"))

    match = _JOB_SCENARIO_RE.fullmatch(scenario)
    if match is None:
        raise ValueError("unsupported JOB deletion scenario")
    direction, year_text = match.groups()
    year = int(year_text)
    range_key = "max" if direction == "pre" else "min"
    boundary = year if direction == "pre" else year + 1
    expected_filter = {
        "column": comparison["column"],
        range_key: boundary,
    }
    stratum = _require_mapping(
        comparison.get("stratum"), "DriftSpec metadata.comparison.stratum"
    )
    _require_exact_keys(
        stratum, {"column", range_key}, "DriftSpec metadata.comparison.stratum"
    )
    if dict(stratum) != expected_filter:
        raise ValueError("JOB comparison stratum does not match its scenario boundary")

    steps = variables.get("drift_steps")
    if not isinstance(steps, list) or len(steps) != 1:
        raise ValueError("JOB canonical drift requires exactly one drift step")
    step = _require_mapping(steps[0], "DriftSpec variables.drift_steps[0]")
    _require_exact_keys(
        step,
        {"op", "target", "key_column", "filter", "fraction", "propagate"},
        "DriftSpec variables.drift_steps[0]",
    )
    if (
        step.get("op") != "delete_keys"
        or step.get("target") != "title"
        or step.get("key_column") != "id"
    ):
        raise ValueError("JOB drift step must delete title.id keys")
    deletion_filter = _require_mapping(
        step.get("filter"), "DriftSpec variables.drift_steps[0].filter"
    )
    _require_exact_keys(
        deletion_filter,
        {"column", range_key},
        "DriftSpec variables.drift_steps[0].filter",
    )
    if dict(deletion_filter) != dict(stratum):
        raise ValueError("JOB deletion filter must exactly match comparison.stratum")
    if _finite_number(step.get("fraction"), "JOB deletion fraction") != 0.40:
        raise ValueError("JOB canonical deletion fraction must be 0.40")
    _validate_job_propagation(step.get("propagate"))


def _validate_job_tables(value: Any) -> None:
    tables = value
    if not isinstance(tables, list) or len(tables) != len(_JOB_TABLES):
        raise ValueError("JOB canonical drift requires exactly its eleven tables")
    seen: set[str] = set()
    for index, item in enumerate(tables):
        table = _require_mapping(item, f"DriftSpec variables.tables[{index}]")
        name = table.get("name")
        if not isinstance(name, str) or name not in _JOB_TABLES or name in seen:
            raise ValueError("JOB table names must be unique and canonical")
        seen.add(name)
        keys = {"name", "path", "format", "output_path"}
        if name == "title":
            keys.add("key_column")
        _require_exact_keys(table, keys, f"DriftSpec variables.tables[{index}]")
        suffix = name.upper()
        if (
            table.get("format") != "csv"
            or table.get("path") != f"${{DRIFTBENCH_INPUT_{suffix}}}"
            or table.get("output_path") != f"${{DRIFTBENCH_OUTPUT_{suffix}}}"
            or (name == "title" and table.get("key_column") != "id")
        ):
            raise ValueError(f"JOB table {name!r} has a non-canonical runtime contract")
    if seen != _JOB_TABLES:
        raise ValueError("JOB canonical table coverage is incomplete")


def _validate_job_relationships(value: Any) -> None:
    relationships = value
    if not isinstance(relationships, list) or len(relationships) != len(
        _JOB_RELATIONSHIPS
    ):
        raise ValueError("JOB canonical relationship graph is incomplete")
    seen: set[str] = set()
    for index, item in enumerate(relationships):
        relationship = _require_mapping(
            item, f"DriftSpec variables.relationships[{index}]"
        )
        _require_exact_keys(
            relationship,
            {"name", "fact", "fk", "dim", "pk"},
            f"DriftSpec variables.relationships[{index}]",
        )
        name = relationship.get("name")
        if (
            not isinstance(name, str)
            or name in seen
            or name not in _JOB_RELATIONSHIPS
            or dict(relationship) != _JOB_RELATIONSHIPS[name]
        ):
            raise ValueError("JOB relationship graph differs from the canonical graph")
        seen.add(name)
    if seen != set(_JOB_RELATIONSHIPS):
        raise ValueError("JOB canonical relationship coverage is incomplete")


def _validate_job_propagation(value: Any) -> None:
    propagation = value
    if not isinstance(propagation, list) or len(propagation) != len(
        _JOB_TITLE_RELATIONSHIPS
    ):
        raise ValueError("JOB deletion must propagate across four title relationships")
    seen: set[str] = set()
    for index, item in enumerate(propagation):
        rule = _require_mapping(
            item, f"DriftSpec variables.drift_steps[0].propagate[{index}]"
        )
        _require_exact_keys(
            rule,
            {"relationship", "policy"},
            f"DriftSpec variables.drift_steps[0].propagate[{index}]",
        )
        relationship = rule.get("relationship")
        if (
            not isinstance(relationship, str)
            or relationship in seen
            or relationship not in _JOB_TITLE_RELATIONSHIPS
            or rule.get("policy") != "drop"
        ):
            raise ValueError("JOB deletion propagation must use each title edge once")
        seen.add(relationship)
    if seen != _JOB_TITLE_RELATIONSHIPS:
        raise ValueError("JOB deletion propagation coverage is incomplete")


def drift_parameters(spec: CanonicalSpec) -> dict[str, Any]:
    variables = spec.payload["variables"]
    assert isinstance(variables, Mapping)
    if spec.kind == "query":
        parsed = parse_query_template_mix_spec(spec.payload)
        return {
            "baseline_weights": dict(parsed.baseline_weights),
            "target_weights": dict(parsed.target_weights),
            "sample_size": parsed.sample_size,
        }
    metadata = spec.payload["metadata"]
    assert isinstance(metadata, Mapping)
    comparison = metadata["comparison"]
    assert isinstance(comparison, Mapping)
    if spec.type_triple[-1] == "single_table":
        drifts = variables.get("drifts")
        if (
            not isinstance(drifts, list)
            or len(drifts) != 1
            or not isinstance(drifts[0], Mapping)
        ):
            raise ValueError(
                "canonical single-table DriftSpec requires exactly one drift"
            )
        drift = {
            str(key): value
            for key, value in drifts[0].items()
            if key not in {"name", "output_path"}
        }
        return {
            "table": str(comparison["table"]),
            "column": str(comparison["column"]),
            **drift,
        }
    return {
        "table": str(comparison["table"]),
        "column": str(comparison["column"]),
        "drift_steps": list(variables["drift_steps"]),
        "validate_integrity": bool(variables.get("validate_integrity", True)),
    }


def _validate_effect_policy(
    value: Any, *, allowed_metrics: frozenset[str]
) -> None:
    policy = _require_mapping(value, "DriftSpec effect_policy")
    _require_exact_keys(policy, {"mode", "assertions"}, "DriftSpec effect_policy")
    mode = policy.get("mode")
    if not isinstance(mode, str) or mode not in {"all", "any"}:
        raise ValueError("effect_policy mode must be all or any")
    assertions = policy.get("assertions")
    if not isinstance(assertions, list) or not assertions:
        raise ValueError("effect_policy assertions must be a non-empty list")
    for index, item in enumerate(assertions):
        assertion = _require_mapping(
            item, f"DriftSpec effect_policy.assertions[{index}]"
        )
        _require_exact_keys(
            assertion,
            {"metric", "operator", "threshold"},
            f"DriftSpec effect_policy.assertions[{index}]",
        )
        metric = assertion.get("metric")
        if not isinstance(metric, str) or metric not in allowed_metrics:
            raise ValueError("effect assertion metric is not valid for this drift kind")
        operator = assertion.get("operator")
        if not isinstance(operator, str) or operator not in {"gte", "lte", "eq"}:
            raise ValueError("unsupported effect assertion operator")
        _finite_number(assertion.get("threshold"), "effect assertion threshold")


def _reject_unsafe_runtime_features(value: Any, field: str = "DriftSpec") -> None:
    """Reject executable/live-database inputs before interpreting any schema."""

    if isinstance(value, Mapping):
        for key, item in value.items():
            if not isinstance(key, str):
                raise ValueError(f"{field} contains a non-string key")
            normalized = re.sub(r"[^a-z0-9]", "", key.casefold())
            unsafe = (
                "module" in normalized
                or "callable" in normalized
                or "function" in normalized
                or "dbconfig" in normalized
                or "postgres" in normalized
                or "sqlalchemy" in normalized
                or normalized
                in {
                    "uri",
                    "url",
                    "dsn",
                    "database",
                    "databaseurl",
                    "connection",
                    "connectionstring",
                    "livedb",
                    "host",
                    "hostname",
                    "port",
                    "username",
                    "password",
                }
            )
            if unsafe:
                raise ValueError(
                    f"{field}.{key} uses a forbidden dynamic/database field"
                )
            if "path" in normalized and (
                not isinstance(item, str)
                or _RUNTIME_PLACEHOLDER_RE.fullmatch(item) is None
            ):
                raise ValueError(
                    f"{field}.{key} must use an approved runtime placeholder"
                )
            _reject_unsafe_runtime_features(item, f"{field}.{key}")
        return
    if isinstance(value, (list, tuple)):
        for index, item in enumerate(value):
            _reject_unsafe_runtime_features(item, f"{field}[{index}]")
        return
    if not isinstance(value, str):
        return
    lowered = value.casefold()
    if "://" in lowered or lowered in {"postgres", "postgresql", "live_database"}:
        raise ValueError(f"{field} contains a forbidden live-database/URI value")
    if ("${" in value or "}" in value) and _RUNTIME_PLACEHOLDER_RE.fullmatch(
        value
    ) is None:
        raise ValueError(f"{field} contains an unsupported or partial placeholder")


def _require_mapping(value: Any, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{field} must be a mapping")
    return value


def _require_exact_keys(
    value: Mapping[str, Any], expected: set[str], field: str
) -> None:
    actual = set(value)
    if actual != expected:
        missing = sorted(expected - actual)
        extra = sorted((actual - expected), key=repr)
        raise ValueError(
            f"{field} keys must be exactly {sorted(expected)}; "
            f"missing={missing}, extra={extra}"
        )


def _finite_number(value: Any, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{field} must be numeric")
    number = float(value)
    if not math.isfinite(number):
        raise ValueError(f"{field} must be finite")
    return number


def _nonnegative_weight(value: Any, field: str) -> float:
    weight = _finite_number(value, field)
    if not 0.0 <= weight <= 1.0:
        raise ValueError(f"{field} must be between zero and one")
    return weight


def _require_unit_mass(values: Any, field: str) -> None:
    total = math.fsum(values)
    if not math.isclose(total, 1.0, rel_tol=0.0, abs_tol=1e-9):
        raise ValueError(f"{field} weights must sum to one")


__all__ = [
    "CanonicalSpec",
    "drift_parameters",
    "expected_artifact_keys",
    "load_canonical_spec",
    "validate_canonical_spec",
]
