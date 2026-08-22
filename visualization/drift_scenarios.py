"""Execute canonical tracked DriftSpecs through DriftBench's public API."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

import pandas as pd

from driftbench.api import QueryWorkloadMixResult, run_spec

from .artifacts import ensure_managed_path, semantic_hash, sha256_file
from .benchmarks import PreparedData, PreparedQueries, data_scenario_contract
from .specs import CanonicalSpec


SPEC_EXECUTOR = "driftbench.api.run_spec"
SPEC_EXECUTOR_VERSION = "driftbench.spec-executor/v3"
_PLACEHOLDER_RE = re.compile(r"^\$\{([A-Z][A-Z0-9_]*)\}$")


@dataclass(frozen=True)
class DataDriftExecution:
    baseline: pd.DataFrame
    drifted: pd.DataFrame
    baseline_path: Path
    drifted_path: Path
    execution_sha256: str
    output_sha256: str
    algorithm: str
    integrity: Mapping[str, Any]


@dataclass(frozen=True)
class QueryDriftExecution:
    result: QueryWorkloadMixResult
    execution_sha256: str
    algorithm: str
    output_sha256: str


def execute_data_drift(
    prepared: PreparedData,
    spec: CanonicalSpec,
    workspace_root: Path,
) -> DataDriftExecution:
    if spec.kind != "data" or prepared.benchmark.name != spec.benchmark:
        raise ValueError("prepared data and canonical DriftSpec identity disagree")
    metadata = spec.payload["metadata"]
    comparison = metadata["comparison"]
    table = str(comparison["table"])
    column = str(comparison["column"])
    operation = _data_algorithm(spec.payload)
    contract = data_scenario_contract(spec.benchmark, spec.scenario)
    if (table, column, operation) != (
        contract["table"],
        contract["column"],
        contract["operation"],
    ):
        raise ValueError(
            f"DriftSpec target/operation disagrees with scenario contract for "
            f"{spec.benchmark}/{spec.scenario}"
        )
    if table not in prepared.tables:
        raise RuntimeError(f"prepared data is missing {table}")

    if spec.type_triple[-1] == "single_table":
        output_path = ensure_managed_path(
            workspace_root,
            "data",
            "drifted",
            spec.benchmark,
            spec.scenario,
            f"{table}.csv",
        )
        schema_path = ensure_managed_path(
            workspace_root,
            "cache",
            "schemas",
            spec.benchmark,
            spec.scenario,
            f"{table}.json",
        )
        bindings = {
            "DRIFTBENCH_INPUT": str(prepared.tables[table].resolve()),
            "DRIFTBENCH_OUTPUT": str(output_path.resolve()),
            "DRIFTBENCH_SCHEMA": str(schema_path.resolve()),
        }
        result = run_spec(str(spec.path), bindings=bindings)
        _verify_executor_output(result, {table: output_path})
        integrity: Mapping[str, Any] = {
            "status": "not_applicable",
            "relationships_checked": 0,
        }
    else:
        output_dir = ensure_managed_path(
            workspace_root,
            "data",
            "drifted",
            spec.benchmark,
            spec.scenario,
            "tables",
        )
        variables = spec.payload["variables"]
        table_configs = variables["tables"]
        expected: dict[str, Path] = {}
        bindings: dict[str, str] = {}
        for table_config in table_configs:
            name = str(table_config["name"])
            if name not in prepared.tables:
                raise RuntimeError(f"prepared data is missing {name}")
            output = output_dir / f"{name}.csv"
            expected[name] = output
            bindings[_placeholder(table_config["path"])] = str(
                prepared.tables[name].resolve()
            )
            bindings[_placeholder(table_config["output_path"])] = str(output.resolve())
        result = run_spec(str(spec.path), bindings=bindings)
        _verify_executor_output(result, expected)
        output_path = expected[table]
        integrity = _validate_job_outputs(
            prepared,
            expected,
            spec.payload,
        )

    if not output_path.is_file():
        raise RuntimeError("DriftSpec executor did not create its declared output")
    baseline = pd.read_csv(prepared.tables[table])
    drifted = pd.read_csv(output_path)
    if column not in baseline.columns or column not in drifted.columns:
        raise RuntimeError(f"comparison column {table}.{column} is missing from executor data")
    output_sha = sha256_file(output_path)
    execution_sha = semantic_hash(
        {
            "spec_semantic_sha256": spec.semantic_sha256,
            "inputs": list(prepared.input_files),
            "output_sha256": output_sha,
        }
    )
    drift_type = operation
    return DataDriftExecution(
        baseline=baseline,
        drifted=drifted,
        baseline_path=prepared.tables[table],
        drifted_path=output_path,
        execution_sha256=execution_sha,
        output_sha256=output_sha,
        algorithm=f"driftbench.data-drift-spec/v1:{drift_type}",
        integrity=integrity,
    )


def execute_query_drift(
    prepared: PreparedQueries,
    spec: CanonicalSpec,
    workspace_root: Path,
) -> QueryDriftExecution:
    if spec.kind != "query" or prepared.benchmark.name != spec.benchmark:
        raise ValueError("prepared queries and canonical DriftSpec identity disagree")
    output_path = ensure_managed_path(
        workspace_root,
        "data",
        "query-drift",
        spec.benchmark,
        spec.scenario,
        "result.json",
    )
    result = run_spec(
        str(spec.path),
        bindings={"DRIFTBENCH_OUTPUT": str(output_path.resolve())},
        runtime_inputs={"query_templates": prepared.templates},
    )
    if not isinstance(result, QueryWorkloadMixResult):
        raise RuntimeError("query DriftSpec executor returned an unsupported result")
    if not output_path.is_file():
        raise RuntimeError("query DriftSpec executor did not write its declared result")
    payload = json.loads(output_path.read_text(encoding="utf-8"))
    if payload.get("semantic_hash") != result.semantic_hash:
        raise RuntimeError("query DriftSpec result file disagrees with executor result")
    return QueryDriftExecution(
        result=result,
        execution_sha256=result.semantic_hash,
        algorithm=result.algorithm,
        output_sha256=sha256_file(output_path),
    )


def _verify_executor_output(result: Any, expected: Mapping[str, Path]) -> None:
    for path in expected.values():
        if not path.is_file():
            raise RuntimeError(f"DriftSpec executor output is missing: {path.name}")
    if result is None:
        # Legacy handlers wrote files without a return contract.  V3 core now
        # returns one; retaining this explicit failure makes integration drift visible.
        raise RuntimeError("data DriftSpec executor did not return an output contract")
    outputs = (
        result.get("outputs")
        if isinstance(result, Mapping)
        else getattr(result, "outputs", None)
    )
    if not isinstance(outputs, (list, tuple)):
        raise RuntimeError("data DriftSpec executor output contract is malformed")
    observed: dict[str, Path] = {}
    for descriptor in outputs:
        if not isinstance(descriptor, Mapping):
            raise RuntimeError("data DriftSpec output descriptor is malformed")
        table = descriptor.get("table")
        path = descriptor.get("path")
        if not isinstance(table, str) or not table or not isinstance(path, str) or not path:
            raise RuntimeError("data DriftSpec output descriptor is incomplete")
        if table in observed:
            raise RuntimeError(f"data DriftSpec returned duplicate output for {table}")
        observed[table] = Path(path).resolve()
    wanted = {name: path.resolve() for name, path in expected.items()}
    if observed != wanted:
        raise RuntimeError("data DriftSpec executor output contract disagrees with bindings")


def _validate_job_outputs(
    prepared: PreparedData,
    outputs: Mapping[str, Path],
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    """Prove that a canonical JOB delete_keys spec executed exactly as declared."""

    variables = payload["variables"]
    relationships = variables["relationships"]
    step = variables["drift_steps"][0]
    target = str(step["target"])
    key_column = str(step["key_column"])
    baseline_frames = {
        name: pd.read_csv(path) for name, path in prepared.tables.items()
    }
    drifted_frames = {name: pd.read_csv(path) for name, path in outputs.items()}
    baseline = baseline_frames[target]
    drifted = drifted_frames[target]
    if key_column not in baseline.columns or key_column not in drifted.columns:
        raise RuntimeError("JOB deletion key column is missing")

    baseline_keys = set(baseline[key_column].dropna())
    drifted_keys = set(drifted[key_column].dropna())
    added_keys = drifted_keys - baseline_keys
    deleted_keys = baseline_keys - drifted_keys
    if added_keys:
        raise RuntimeError("JOB deletion introduced undeclared target keys")
    if not deleted_keys:
        raise RuntimeError("JOB deletion DriftSpec deleted no target keys")

    predicate = step["filter"]
    predicate_column = str(predicate["column"])
    eligible_mask = _stratum_mask(baseline[predicate_column], predicate)
    eligible_keys = set(baseline.loc[eligible_mask, key_column].dropna())
    if not deleted_keys <= eligible_keys:
        raise RuntimeError("JOB deletion removed keys outside its declared stratum")
    expected_deleted = int(round(len(eligible_keys) * float(step["fraction"])))
    if len(deleted_keys) != expected_deleted:
        raise RuntimeError(
            "JOB deletion key count disagrees with the declared eligible fraction"
        )

    expected_target = baseline[~baseline[key_column].isin(deleted_keys)].reset_index(
        drop=True
    )
    if not drifted.reset_index(drop=True).equals(expected_target):
        raise RuntimeError("JOB target output is not exactly baseline minus declared keys")

    propagated = {
        str(item["relationship"]): str(item["policy"])
        for item in step["propagate"]
    }
    changed_tables = {target}
    propagation_counts: dict[str, dict[str, int]] = {}
    for relationship in relationships:
        name = str(relationship["name"])
        if name not in propagated:
            continue
        if propagated[name] != "drop":
            raise RuntimeError("canonical JOB deletion propagation must use drop")
        fact = str(relationship["fact"])
        foreign_key = str(relationship["fk"])
        baseline_fact = baseline_frames[fact]
        expected_fact = baseline_fact[
            ~baseline_fact[foreign_key].isin(deleted_keys)
        ].reset_index(drop=True)
        observed_fact = drifted_frames[fact].reset_index(drop=True)
        if not observed_fact.equals(expected_fact):
            raise RuntimeError(
                f"JOB propagated output {fact} is not exactly baseline minus deleted keys"
            )
        changed_tables.add(fact)
        propagation_counts[name] = {
            "baseline_rows": len(baseline_fact),
            "drifted_rows": len(observed_fact),
            "dropped_rows": len(baseline_fact) - len(observed_fact),
        }

    for name, baseline_frame in baseline_frames.items():
        if name in changed_tables:
            continue
        if not drifted_frames[name].reset_index(drop=True).equals(
            baseline_frame.reset_index(drop=True)
        ):
            raise RuntimeError(f"JOB deletion unexpectedly changed unrelated table {name}")

    orphan_counts: dict[str, int] = {}
    for relationship in relationships:
        fact = str(relationship["fact"])
        foreign_key = str(relationship["fk"])
        dimension = str(relationship["dim"])
        primary_key = str(relationship["pk"])
        fact_frame = drifted_frames[fact]
        dimension_frame = drifted_frames[dimension]
        dimension_keys = set(dimension_frame[primary_key].dropna())
        orphan_counts[f"{fact}.{foreign_key}"] = len(
            set(fact_frame[foreign_key].dropna()) - dimension_keys
        )
    orphan_count = sum(orphan_counts.values())
    if orphan_count:
        raise RuntimeError(f"JOB drift left orphan foreign keys: {orphan_counts}")

    stratum = payload["metadata"]["comparison"]["stratum"]
    column = str(stratum["column"])
    baseline_share = float(_stratum_mask(baseline[column], stratum).mean())
    drifted_share = float(_stratum_mask(drifted[column], stratum).mean())
    shift_pp = abs(drifted_share - baseline_share) * 100.0
    reduction_pp = max(0.0, baseline_share - drifted_share) * 100.0
    return {
        "status": "passed",
        "relationships_checked": len(relationships),
        "orphan_counts": orphan_counts,
        "orphan_count": orphan_count,
        "baseline_title_rows": len(baseline),
        "drifted_title_rows": len(drifted),
        "eligible_key_count": len(eligible_keys),
        "expected_deleted_key_count": expected_deleted,
        "deleted_key_count": len(deleted_keys),
        "added_key_count": len(added_keys),
        "out_of_stratum_deleted_key_count": len(deleted_keys - eligible_keys),
        "propagation": propagation_counts,
        "target_stratum": dict(stratum),
        "target_stratum_share": {
            "baseline": baseline_share,
            "drifted": drifted_share,
        },
        "target_stratum_share_shift_pp": shift_pp,
        "target_stratum_share_reduction_pp": reduction_pp,
    }


def _stratum_mask(series: pd.Series, predicate: Mapping[str, Any]) -> pd.Series:
    mask = pd.Series(True, index=series.index)
    if "min" in predicate:
        mask &= series >= predicate["min"]
    if "max" in predicate:
        mask &= series <= predicate["max"]
    return mask


def _placeholder(value: Any) -> str:
    match = _PLACEHOLDER_RE.fullmatch(str(value))
    if match is None:
        raise ValueError(f"runtime-bound path must be one exact placeholder: {value!r}")
    return match.group(1)


def _data_algorithm(payload: Mapping[str, Any]) -> str:
    variables = payload["variables"]
    if payload["type"]["subtype"] == "single_table":
        return str(variables["drifts"][0]["drift_type"])
    return "+".join(str(step["op"]) for step in variables["drift_steps"])


__all__ = [
    "DataDriftExecution",
    "QueryDriftExecution",
    "SPEC_EXECUTOR",
    "SPEC_EXECUTOR_VERSION",
    "execute_data_drift",
    "execute_query_drift",
]
