"""Read-only, deterministic DriftSpec readiness validation.

The shallow validator in :mod:`driftbench.spec.core` intentionally preserves
its historical contract.  This module provides the opt-in, deeper local
preflight used by ``validate-spec --deep``.  A preflight never executes a
handler, generates benchmark artifacts, imports user modules, connects to an
external service, or creates/writes paths declared by a spec.
"""

from __future__ import annotations

import datetime as _datetime
import importlib
import inspect
import json
import math
import os
import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

# Import only built-in handler modules so the registry is complete.  Merely
# importing these modules is the same registration step already performed by
# driftbench.spec.core; no handler is invoked here.
from .registry import get_handler
from .types import data_drift as _data_drift  # noqa: F401
from .types import workload_keylist as _workload_keylist  # noqa: F401
from .types import workload_query_mix as _workload_query_mix  # noqa: F401
from .types import workload_sql_templates as _workload_sql_templates  # noqa: F401
from .types import workload_templates as _workload_templates  # noqa: F401


_PLACEHOLDER_RE = re.compile(r"\$\{[^{}]+\}")
_CHECK_NAMES = (
    "document",
    "structure",
    "handler",
    "parameters",
    "inputs",
    "outputs",
    "benchmark",
)

_SINGLE_DRIFT_TYPES = {
    "outlier_injection",
    "value_skew",
    "vary_cardinality",
    "selective_deletion",
    "insert_records",
    "add_timestamp",
    "concat_csvs",
}
_MULTI_DRIFT_OPS = {
    "delete_keys",
    "reassign_fk",
    "scale_tables",
    "scale_sample",
    "add_dimension_keys",
    "skew_fk",
    "skew_column",
    "insert_outliers",
    "rewrite_columns",
}
_BENCHMARK_MODULES = {
    "tpch": "driftbench.data.tpch",
    "tpcds": "driftbench.data.tpcds",
    "tpcc": "driftbench.data.tpcc",
    "tpcc_skew": "driftbench.data.tpcc_skew",
    "job": "driftbench.data.job",
    "ycsb": "driftbench.data.ycsb",
    "dsb": "driftbench.data.dsb",
    "pgbench": "driftbench.data.pgbench",
    "benchbase": "driftbench.data.benchbase",
}


@dataclass(frozen=True)
class PreflightIssue:
    """One stable, machine-readable preflight finding."""

    severity: str
    code: str
    field: str
    message: str
    hint: str

    def as_dict(self) -> dict[str, str]:
        return {
            "severity": self.severity,
            "code": self.code,
            "field": self.field,
            "message": self.message,
            "hint": self.hint,
        }


@dataclass(frozen=True)
class PreflightCheck:
    """Status of one deterministic validation phase."""

    name: str
    status: str

    def as_dict(self) -> dict[str, str]:
        return {"name": self.name, "status": self.status}


@dataclass(frozen=True)
class PreflightReport:
    """Deep-validation result without retaining potentially secret spec data."""

    spec_path: str
    pattern_id: str
    type_name: str
    declared_outputs: int
    checks: tuple[PreflightCheck, ...]
    issues: tuple[PreflightIssue, ...]

    @property
    def valid(self) -> bool:
        return not any(issue.severity == "error" for issue in self.issues)

    @property
    def locally_ready(self) -> bool:
        # External services are deliberately not probed.  An
        # external_not_checked warning therefore does not negate *local*
        # readiness, while every error does.
        return self.valid

    def as_dict(self) -> dict[str, Any]:
        errors = sum(issue.severity == "error" for issue in self.issues)
        warnings = sum(issue.severity == "warning" for issue in self.issues)
        failed_checks = sum(check.status == "failed" for check in self.checks)
        passed_checks = sum(check.status == "passed" for check in self.checks)
        if errors:
            outcome = "not_ready"
        elif warnings:
            outcome = "ready_with_warnings"
        else:
            outcome = "ready"
        return {
            "ok": self.valid,
            "outcome": outcome,
            "command": "validate-spec",
            "spec_path": self.spec_path,
            "pattern_id": self.pattern_id,
            "type": self.type_name,
            "declared_outputs": self.declared_outputs,
            "mode": "deep",
            "valid": self.valid,
            "locally_ready": self.locally_ready,
            "checks": [check.as_dict() for check in self.checks],
            "issues": [issue.as_dict() for issue in self.issues],
            "summary": {
                "status": outcome,
                "errors": errors,
                "warnings": warnings,
                "checks_passed": passed_checks,
                "checks_failed": failed_checks,
            },
        }


@dataclass(frozen=True)
class _PathUse:
    value: str
    field: str
    expected_type: str
    order: int = 0
    may_be_generated: bool = False
    parse_json: bool = False
    json_contract: str | None = None


class _Context:
    def __init__(self, working_dir: Path) -> None:
        self.working_dir = working_dir
        self._findings: list[tuple[str, PreflightIssue]] = []
        self._statuses = {name: "skipped" for name in _CHECK_NAMES}
        self._current_check = "structure"
        self.inputs: list[_PathUse] = []
        self.outputs: list[_PathUse] = []
        self._path_keys: set[tuple[str, str, str, int, bool]] = set()
        self.generated_input_keys: set[tuple[str, str]] = set()

    def phase(self, name: str, action: Callable[[], Any]) -> Any:
        previous = self._current_check
        self._current_check = name
        start = len(self._findings)
        try:
            result = action()
        finally:
            self._current_check = previous
        new_issues = [item for _, item in self._findings[start:]]
        if any(item.severity == "error" for item in new_issues):
            self._statuses[name] = "failed"
        elif any(item.severity == "warning" for item in new_issues):
            self._statuses[name] = "warning"
        else:
            self._statuses[name] = "passed"
        return result

    def issue(
        self,
        code: str,
        field: str,
        message: str,
        hint: str,
        *,
        severity: str = "error",
    ) -> None:
        self._findings.append(
            (
                self._current_check,
                PreflightIssue(
                    severity=severity,
                    code=code,
                    field=field,
                    message=message,
                    hint=hint,
                ),
            )
        )

    def input_path(
        self,
        value: Any,
        field: str,
        *,
        expected_type: str = "file",
        order: int = 0,
        may_be_generated: bool = False,
        parse_json: bool = False,
        json_contract: str | None = None,
    ) -> None:
        path = self._path_value(value, field)
        if path is None:
            return
        key = ("input", field, path, order, may_be_generated)
        if key in self._path_keys:
            return
        self._path_keys.add(key)
        self.inputs.append(
            _PathUse(
                path,
                field,
                expected_type,
                order,
                may_be_generated,
                parse_json,
                json_contract,
            )
        )

    def output_path(
        self,
        value: Any,
        field: str,
        *,
        expected_type: str = "file",
        order: int = 0,
    ) -> None:
        path = self._path_value(value, field)
        if path is None:
            return
        key = ("output", field, path, order, False)
        if key in self._path_keys:
            return
        self._path_keys.add(key)
        self.outputs.append(_PathUse(path, field, expected_type, order))

    def _path_value(self, value: Any, field: str) -> str | None:
        if value is None:
            self.issue(
                "required_field_missing",
                field,
                "A required path is missing.",
                "Set this field to a non-empty local path.",
            )
            return None
        if not isinstance(value, str):
            self.issue(
                "field_type_invalid",
                field,
                "This path must be a string.",
                "Use a non-empty path string.",
            )
            return None
        if not value.strip():
            self.issue(
                "field_value_invalid",
                field,
                "This path must not be empty.",
                "Use a non-empty local path.",
            )
            return None
        return value

    def checks(self) -> tuple[PreflightCheck, ...]:
        return tuple(
            PreflightCheck(name=name, status=self._statuses[name])
            for name in _CHECK_NAMES
        )

    def issues(self) -> tuple[PreflightIssue, ...]:
        # Deduplicate exact findings and impose a stable order independent of
        # mapping iteration or filesystem enumeration order.
        unique = {item for _, item in self._findings}
        return tuple(
            sorted(
                unique,
                key=lambda item: (
                    0 if item.severity == "error" else 1,
                    item.code,
                    item.field,
                    item.message,
                    item.hint,
                ),
            )
        )


def deep_validate_spec_file(
    spec_path: str | os.PathLike[str],
    *,
    working_dir: str | os.PathLike[str] | None = None,
) -> PreflightReport:
    """Deeply validate a DriftSpec using only read-only local checks.

    Relative resource paths deliberately resolve from ``working_dir`` (the
    current process directory by default), matching handler runtime semantics;
    they are not resolved relative to the spec file.
    """

    display_path = os.fspath(spec_path)
    base = Path.cwd() if working_dir is None else Path(working_dir)
    if not base.is_absolute():
        base = Path.cwd() / base
    ctx = _Context(base)

    payload = ctx.phase("document", lambda: _load_document(ctx, display_path))
    if payload is None:
        return _report(ctx, display_path, None)

    # Preserve the current migration rule without mutating the loaded object.
    spec = dict(payload)
    spec.setdefault("spec_version", 1)
    triple, variables = ctx.phase("structure", lambda: _validate_structure(ctx, spec))

    registered = False
    if triple is not None:
        registered = bool(
            ctx.phase("handler", lambda: _validate_handler_registration(ctx, triple))
        )

    validator = _VALIDATORS.get(triple) if triple is not None else None
    if triple is not None and registered and validator is None:
        ctx.phase(
            "parameters",
            lambda: ctx.issue(
                "preflight_not_supported",
                "type",
                "This registered handler has no deep-validation contract.",
                "Add a pure preflight validator before treating this spec as ready.",
            ),
        )
    elif validator is not None and variables is not None:
        ctx.phase("parameters", lambda: validator(ctx, spec, variables))

    if ctx.inputs:
        ctx.phase("inputs", lambda: _validate_inputs(ctx))
    if ctx.outputs:
        ctx.phase("outputs", lambda: _validate_outputs(ctx))
    ctx.phase("benchmark", lambda: _validate_benchmark(ctx, spec))

    return _report(ctx, display_path, spec)


def _report(
    ctx: _Context, display_path: str, spec: Mapping[str, Any] | None
) -> PreflightReport:
    if spec is None:
        pattern_id = ""
        type_name = ""
        declared_outputs = 0
    else:
        pattern = spec.get("pattern_id", "")
        pattern_id = pattern if isinstance(pattern, str) else ""
        spec_type = spec.get("type")
        if isinstance(spec_type, Mapping):
            parts = [spec_type.get(key) for key in ("family", "category", "subtype")]
            type_name = ".".join(part if isinstance(part, str) else "" for part in parts)
        else:
            type_name = ""
        declared_outputs = _legacy_declared_output_count(spec)
    return PreflightReport(
        spec_path=display_path,
        pattern_id=pattern_id,
        type_name=type_name,
        declared_outputs=declared_outputs,
        checks=ctx.checks(),
        issues=ctx.issues(),
    )


def _load_document(ctx: _Context, display_path: str) -> dict[str, Any] | None:
    if _PLACEHOLDER_RE.search(display_path):
        ctx.issue(
            "unresolved_placeholder",
            "spec_path",
            "The spec path contains an unresolved placeholder.",
            "Bind the placeholder before running deep validation.",
        )
        return None
    path = _resolve_path(ctx, display_path)
    try:
        exists = path.exists()
    except OSError:
        exists = False
    if not exists:
        ctx.issue(
            "input_not_found",
            "spec_path",
            "The DriftSpec file does not exist.",
            "Create the file or pass the correct path from the current working directory.",
        )
        return None
    if not path.is_file():
        ctx.issue(
            "input_type_mismatch",
            "spec_path",
            "The DriftSpec path is not a file.",
            "Pass a YAML file path.",
        )
        return None
    if not os.access(path, os.R_OK):
        ctx.issue(
            "input_not_readable",
            "spec_path",
            "The DriftSpec file is not readable.",
            "Grant read permission and retry.",
        )
        return None
    try:
        with path.open("r", encoding="utf-8") as stream:
            payload = yaml.safe_load(stream)
    except (OSError, UnicodeError):
        ctx.issue(
            "input_not_readable",
            "spec_path",
            "The DriftSpec file could not be read as UTF-8.",
            "Use a readable UTF-8 YAML file.",
        )
        return None
    except yaml.YAMLError:
        ctx.issue(
            "yaml_invalid",
            "spec_path",
            "The DriftSpec contains invalid YAML.",
            "Correct the YAML syntax and retry.",
        )
        return None
    if not isinstance(payload, dict):
        ctx.issue(
            "field_type_invalid",
            "$",
            "The DriftSpec root must be a mapping.",
            "Use YAML key/value pairs at the document root.",
        )
        return None
    return payload


def _validate_structure(
    ctx: _Context, spec: Mapping[str, Any]
) -> tuple[tuple[str, str, str] | None, Mapping[str, Any] | None]:
    version = spec.get("spec_version")
    if isinstance(version, bool) or not isinstance(version, int):
        ctx.issue(
            "field_type_invalid",
            "spec_version",
            "spec_version must be an integer.",
            "Use spec_version: 1.",
        )
    elif version != 1:
        ctx.issue(
            "spec_version_unsupported",
            "spec_version",
            "This DriftSpec version is not supported.",
            "Migrate the spec to spec_version: 1.",
        )

    spec_type = spec.get("type")
    triple: tuple[str, str, str] | None = None
    if not isinstance(spec_type, Mapping):
        code = "required_field_missing" if spec_type is None else "field_type_invalid"
        ctx.issue(
            code,
            "type",
            "type must be a mapping with family, category, and subtype.",
            "Declare all three non-empty type fields.",
        )
    else:
        parts: list[str] = []
        complete = True
        for key in ("family", "category", "subtype"):
            value = spec_type.get(key)
            field = f"type.{key}"
            if value is None:
                ctx.issue(
                    "required_field_missing",
                    field,
                    f"{field} is required.",
                    "Set it to the registered handler component.",
                )
                complete = False
            elif not isinstance(value, str):
                ctx.issue(
                    "field_type_invalid",
                    field,
                    f"{field} must be a string.",
                    "Use a non-empty string.",
                )
                complete = False
            elif not value.strip():
                ctx.issue(
                    "field_value_invalid",
                    field,
                    f"{field} must not be empty.",
                    "Use a registered handler component.",
                )
                complete = False
            else:
                # Runtime registration uses the declared strings verbatim.
                # Preserve surrounding whitespace here so deep validation
                # cannot approve a handler that runtime will not find.
                parts.append(value)
        if complete:
            triple = (parts[0], parts[1], parts[2])

    variables = spec.get("variables")
    if not isinstance(variables, Mapping):
        code = "required_field_missing" if variables is None else "field_type_invalid"
        ctx.issue(
            code,
            "variables",
            "variables must be a mapping.",
            "Declare the fields required by the selected handler.",
        )
        variables = None

    if "seed" in spec:
        seed = spec.get("seed")
        if isinstance(seed, bool) or not isinstance(seed, int):
            ctx.issue(
                "field_type_invalid",
                "seed",
                "seed must be an integer.",
                "Use an integer seed for reproducible execution.",
            )
    return triple, variables


def _validate_handler_registration(
    ctx: _Context, triple: tuple[str, str, str]
) -> bool:
    try:
        get_handler(triple)
    except ValueError:
        ctx.issue(
            "handler_not_registered",
            "type",
            "No runnable handler is registered for this type triple.",
            "Choose a supported type or register its handler before execution.",
        )
        return False
    return True


def _validate_data_single(
    ctx: _Context, spec: Mapping[str, Any], variables: Mapping[str, Any]
) -> None:
    data_source = _required_mapping(ctx, spec, "data_source", "data_source")
    if data_source is not None:
        _validate_data_source_kind(ctx, data_source, "data_source")
        ctx.input_path(data_source.get("path"), "data_source.path")
        _collect_schema_resources(ctx, data_source, "data_source", order=-100)

    _required_string(ctx, variables, "base_table", "variables.base_table")
    drifts = _required_nonempty_list(ctx, variables, "drifts", "variables.drifts")
    if drifts is not None:
        _validate_single_drifts(ctx, drifts, "variables.drifts", order_base=100)


def _validate_data_multi(
    ctx: _Context, spec: Mapping[str, Any], variables: Mapping[str, Any]
) -> None:
    data_source = _optional_mapping(ctx, spec.get("data_source"), "data_source") or {}
    if data_source:
        kind = data_source.get("kind")
        if kind is not None and kind not in {"multi_table", "csv", "parquet", "postgres"}:
            ctx.issue(
                "field_value_invalid",
                "data_source.kind",
                "Unsupported multi-table data source kind.",
                "Use multi_table, csv, parquet, or postgres.",
            )
        _collect_schema_resources(ctx, data_source, "data_source", order=-100)

    tables = _required_nonempty_list(ctx, variables, "tables", "variables.tables")
    if tables is None:
        return
    table_names: set[str] = set()
    table_entries: list[tuple[int, Mapping[str, Any], str]] = []
    global_ddl = variables.get("ddl_path")
    if global_ddl is not None:
        ctx.input_path(global_ddl, "variables.ddl_path")

    for index, value in enumerate(tables):
        field = f"variables.tables[{index}]"
        if not isinstance(value, Mapping):
            ctx.issue(
                "field_type_invalid",
                field,
                "Each table entry must be a mapping.",
                "Declare name, path, and output configuration for each table.",
            )
            continue
        name_value = value.get("name") or value.get("base_table")
        name = _validate_string_value(ctx, name_value, f"{field}.name", required=True)
        if name is None:
            continue
        if name in table_names:
            ctx.issue(
                "duplicate_identifier",
                f"{field}.name",
                "Table names must be unique.",
                "Give every table a distinct name.",
            )
        table_names.add(name)
        table_entries.append((index, value, name))
        ctx.input_path(value.get("path"), f"{field}.path")
        if value.get("ddl_path") is not None:
            ctx.input_path(value.get("ddl_path"), f"{field}.ddl_path")
        if value.get("schema_path") is not None:
            ctx.input_path(
                value.get("schema_path"),
                f"{field}.schema_path",
                parse_json=True,
                json_contract="schema",
            )
        schema_extractor = _optional_mapping(
            ctx, value.get("schema_extractor"), f"{field}.schema_extractor"
        )
        if schema_extractor and schema_extractor.get("schema_output_path") is not None:
            ctx.output_path(
                schema_extractor.get("schema_output_path"),
                f"{field}.schema_extractor.schema_output_path",
                order=-50 + index,
            )

    relationships = variables.get("relationships", [])
    relationship_names: set[str] = set()
    if relationships is None:
        relationships = []
    if not isinstance(relationships, list):
        ctx.issue(
            "field_type_invalid",
            "variables.relationships",
            "relationships must be a list.",
            "Use a list of relationship mappings.",
        )
        relationships = []
    for index, relationship in enumerate(relationships):
        field = f"variables.relationships[{index}]"
        if not isinstance(relationship, Mapping):
            ctx.issue(
                "field_type_invalid",
                field,
                "Each relationship must be a mapping.",
                "Declare name, fact, fk, dim, and pk.",
            )
            continue
        name = _required_string(ctx, relationship, "name", f"{field}.name")
        if name is not None:
            if name in relationship_names:
                ctx.issue(
                    "duplicate_identifier",
                    f"{field}.name",
                    "Relationship names must be unique.",
                    "Give every relationship a distinct name.",
                )
            relationship_names.add(name)
        for key in ("fact", "fk", "dim", "pk"):
            _required_string(ctx, relationship, key, f"{field}.{key}")
        for key in ("fact", "dim"):
            ref = relationship.get(key)
            if isinstance(ref, str) and ref and ref not in table_names:
                ctx.issue(
                    "reference_not_found",
                    f"{field}.{key}",
                    "This relationship references an unknown table.",
                    "Reference one of variables.tables[].name.",
                )

    steps = variables.get("drift_steps")
    if steps:
        if not isinstance(steps, list):
            ctx.issue(
                "field_type_invalid",
                "variables.drift_steps",
                "drift_steps must be a list.",
                "Use a non-empty list of operation mappings.",
            )
            return
        for index, table, _ in table_entries:
            output_field = f"variables.tables[{index}].output_path"
            ctx.output_path(table.get("output_path"), output_field, order=1000 + index)
        _validate_multi_steps(ctx, steps, table_names, relationship_names)
    else:
        for index, table, _ in table_entries:
            field = f"variables.tables[{index}]"
            _required_string(ctx, table, "base_table", f"{field}.base_table")
            drifts = _required_nonempty_list(ctx, table, "drifts", f"{field}.drifts")
            if drifts is not None:
                _validate_single_drifts(
                    ctx, drifts, f"{field}.drifts", order_base=1000 + index * 100
                )


def _validate_keylist(
    ctx: _Context, spec: Mapping[str, Any], variables: Mapping[str, Any]
) -> None:
    data_source = _required_mapping(ctx, spec, "data_source", "data_source")
    if data_source is not None:
        ctx.input_path(data_source.get("path"), "data_source.path")
    if "key_column" in variables:
        _validate_string_value(
            ctx, variables.get("key_column"), "variables.key_column", required=True
        )
    type_format = variables.get("type_format", "u64")
    _enum(ctx, type_format, {"u64", "i64"}, "variables.type_format")
    runs = _required_nonempty_list(
        ctx, variables, "query_runs", "variables.query_runs"
    )
    if runs is None:
        return
    names: set[str] = set()
    for index, run in enumerate(runs):
        field = f"variables.query_runs[{index}]"
        if not isinstance(run, Mapping):
            ctx.issue(
                "field_type_invalid",
                field,
                "Each query run must be a mapping.",
                "Declare its distribution and output path.",
            )
            continue
        _optional_unique_name(ctx, run.get("name"), f"{field}.name", names)
        _enum(ctx, run.get("query_type", "point"), {"point", "range"}, f"{field}.query_type")
        distribution = run.get("distribution", "uniform")
        _enum(
            ctx,
            distribution,
            {"uniform", "hotspot", "zipf"},
            f"{field}.distribution",
        )
        _positive_int(ctx, run.get("count", 10000), f"{field}.count")
        _nonnegative_int(ctx, run.get("r_size", 0), f"{field}.r_size")
        if distribution == "hotspot":
            _fraction(ctx, run.get("hotspot_frac", 0.1), f"{field}.hotspot_frac")
        if distribution == "zipf":
            alpha = _finite_number(ctx, run.get("zipf_alpha", 1.2), f"{field}.zipf_alpha")
            if alpha is not None and alpha <= 1.0:
                ctx.issue(
                    "field_value_invalid",
                    f"{field}.zipf_alpha",
                    "zipf_alpha must be greater than 1.",
                    "Use a finite value greater than 1.",
                )
        ctx.output_path(run.get("output_path"), f"{field}.output_path", order=100 + index)


def _validate_selection_payload(
    ctx: _Context, spec: Mapping[str, Any], variables: Mapping[str, Any]
) -> None:
    control_names = (
        "num_templates",
        "max_predicates",
        "max_payload_columns",
        "join_count",
    )
    _required_string(ctx, variables, "base_table", "variables.base_table")
    data_source = _required_mapping(ctx, spec, "data_source", "data_source")
    schema_path = variables.get("schema_path")
    if schema_path is not None:
        ctx.input_path(
            schema_path,
            "variables.schema_path",
            parse_json=True,
            json_contract="schema",
        )
    elif data_source is not None:
        schema_extractor = _optional_mapping(
            ctx, data_source.get("schema_extractor"), "data_source.schema_extractor"
        ) or {}
        source_type = schema_extractor.get("source_type") or data_source.get("kind")
        if source_type in {"csv", "parquet"}:
            ctx.input_path(data_source.get("path"), "data_source.path")
        elif source_type == "postgres":
            _validate_postgres_source(ctx, data_source, "data_source")
        else:
            ctx.issue(
                "field_value_invalid",
                "data_source.kind",
                "Unsupported selection-template data source kind.",
                "Use csv, parquet, or postgres.",
            )
        if schema_extractor.get("schema_output_path") is not None:
            ctx.output_path(
                schema_extractor.get("schema_output_path"),
                "data_source.schema_extractor.schema_output_path",
                order=-100,
            )

    defaults = (
        _optional_mapping(ctx, variables.get("defaults", {}), "variables.defaults")
        or {}
    )

    candidate_sets_value = variables.get("candidate_sets", {})
    candidate_sets: Mapping[str, Any] = {}
    if not isinstance(candidate_sets_value, Mapping):
        ctx.issue(
            "field_type_invalid",
            "variables.candidate_sets",
            "candidate_sets must be a mapping.",
            "Map each set name to a non-empty table-name list.",
        )
    else:
        candidate_sets = candidate_sets_value
        for name, values in candidate_sets.items():
            field = f"variables.candidate_sets.{name}"
            if not isinstance(name, str) or not name.strip():
                ctx.issue(
                    "field_value_invalid",
                    "variables.candidate_sets",
                    "Candidate-set names must be non-empty strings.",
                    "Use stable string identifiers.",
                )
            _string_list(ctx, values, field, nonempty=True)

    runs = _optional_list(ctx, variables.get("runs", []), "variables.runs") or []
    run_names: set[str] = set()
    run_outputs: dict[str, int] = {}
    for index, run in enumerate(runs):
        field = f"variables.runs[{index}]"
        if not isinstance(run, Mapping):
            ctx.issue(
                "field_type_invalid",
                field,
                "Each template run must be a mapping.",
                "Declare a name and output_path.",
            )
            continue
        name = _required_string(ctx, run, "name", f"{field}.name")
        if name is not None:
            if name in run_names:
                ctx.issue(
                    "duplicate_identifier",
                    f"{field}.name",
                    "Template-run names must be unique.",
                    "Give every run a distinct name.",
                )
            run_names.add(name)
            run_outputs[name] = 100 + index
        ctx.output_path(run.get("output_path"), f"{field}.output_path", order=100 + index)
        if "candidate_tables_ref" in run:
            ref = run.get("candidate_tables_ref")
            if not isinstance(ref, str) or ref not in candidate_sets:
                ctx.issue(
                    "reference_not_found",
                    f"{field}.candidate_tables_ref",
                    "This run references an unknown candidate set.",
                    "Reference a key declared in variables.candidate_sets.",
                )
        if "candidate_tables" in run:
            _string_list(ctx, run.get("candidate_tables"), f"{field}.candidate_tables", nonempty=True)
        merged_controls = dict(defaults)
        merged_controls.update(run)
        for key in control_names:
            if key in merged_controls:
                source_field = (
                    f"{field}.{key}"
                    if key in run
                    else f"variables.defaults.{key}"
                )
                _positive_int(ctx, merged_controls.get(key), source_field)

    query_runs = _optional_list(
        ctx, variables.get("query_runs", []), "variables.query_runs"
    ) or []
    if not runs and not query_runs:
        ctx.issue(
            "required_field_missing",
            "variables.runs",
            "At least one template run or query run is required.",
            "Declare variables.runs or variables.query_runs.",
        )
    query_names: set[str] = set()
    for index, run in enumerate(query_runs):
        field = f"variables.query_runs[{index}]"
        if not isinstance(run, Mapping):
            ctx.issue(
                "field_type_invalid",
                field,
                "Each query run must be a mapping.",
                "Declare template, queries_per_template, and outputs.",
            )
            continue
        _optional_unique_name(ctx, run.get("name"), f"{field}.name", query_names)
        template = _required_string(ctx, run, "template", f"{field}.template")
        if template is not None and template not in run_names:
            ctx.input_path(
                template,
                f"{field}.template",
                order=1000 + index,
                may_be_generated=True,
            )
        _positive_int(
            ctx,
            run.get("queries_per_template", 300),
            f"{field}.queries_per_template",
        )
        _validate_dist_config(ctx, run.get("dist_config", {}), f"{field}.dist_config")
        outputs = _required_nonempty_list(ctx, run, "outputs", f"{field}.outputs")
        if outputs is None:
            continue
        for out_index, output in enumerate(outputs):
            out_field = f"{field}.outputs[{out_index}]"
            if not isinstance(output, Mapping):
                ctx.issue(
                    "field_type_invalid",
                    out_field,
                    "Each output must be a mapping.",
                    "Declare path and optional timestamp configuration.",
                )
                continue
            _enum(
                ctx,
                output.get("type", "workload"),
                {"workload", "temporal"},
                f"{out_field}.type",
            )
            ctx.output_path(
                output.get("path"), f"{out_field}.path", order=2000 + index * 100 + out_index
            )
            if output.get("timestamp") is not None:
                _validate_timestamp(ctx, output.get("timestamp"), f"{out_field}.timestamp")


def _validate_tpch_sql(
    ctx: _Context, spec: Mapping[str, Any], variables: Mapping[str, Any]
) -> None:
    template_dir = variables.get("template_dir")
    ctx.input_path(template_dir, "variables.template_dir", expected_type="directory")
    defaults = _optional_mapping(ctx, variables.get("defaults", {}), "variables.defaults") or {}
    default_ids = variables.get("query_ids")
    if default_ids is not None:
        _query_ids(ctx, default_ids, "variables.query_ids")
    base_params = variables.get("params", {})
    _validate_tpch_params(ctx, base_params, "variables.params")

    runs_value = variables.get("query_runs") or variables.get("runs")
    if runs_value is None:
        ctx.issue(
            "required_field_missing",
            "variables.query_runs",
            "TPC-H requires query_runs or runs.",
            "Declare a non-empty list of query-generation runs.",
        )
        return
    if not isinstance(runs_value, list) or not runs_value:
        ctx.issue(
            "field_type_invalid",
            "variables.query_runs",
            "TPC-H runs must be a non-empty list.",
            "Declare one or more run mappings.",
        )
        return

    names: set[str] = set()
    for index, run in enumerate(runs_value):
        field = f"variables.query_runs[{index}]"
        if not isinstance(run, Mapping):
            ctx.issue(
                "field_type_invalid",
                field,
                "Each TPC-H run must be a mapping.",
                "Declare query IDs, mode, and outputs.",
            )
            continue
        _optional_unique_name(ctx, run.get("name"), f"{field}.name", names)
        ids_value = run.get("query_ids") or default_ids
        query_ids: list[str] = []
        if ids_value is not None:
            query_ids = _query_ids(ctx, ids_value, f"{field}.query_ids") or []
        elif isinstance(template_dir, str) and not _PLACEHOLDER_RE.search(template_dir):
            directory = _resolve_path(ctx, template_dir)
            if directory.is_dir():
                query_ids = sorted(
                    (path.stem for path in directory.glob("*.sql") if path.stem.isdigit()),
                    key=lambda item: int(item),
                )
                if not query_ids:
                    ctx.issue(
                        "input_not_found",
                        "variables.template_dir",
                        "The template directory contains no numeric .sql templates.",
                        "Add selected TPC-H templates or declare query_ids.",
                    )
        if isinstance(template_dir, str) and not _PLACEHOLDER_RE.search(template_dir):
            for qid in query_ids:
                ctx.input_path(
                    os.path.join(template_dir, f"{qid}.sql"),
                    f"{field}.query_ids[{qid}]",
                )

        _positive_int(
            ctx,
            run.get("queries_per_template", defaults.get("queries_per_template", 1)),
            f"{field}.queries_per_template",
        )
        mode = run.get("param_mode", defaults.get("param_mode", "custom"))
        _enum(ctx, mode, {"custom", "qgen"}, f"{field}.param_mode")
        if mode == "qgen":
            scale = _finite_number(
                ctx,
                run.get("qgen_scale", defaults.get("qgen_scale", 1)),
                f"{field}.qgen_scale",
            )
            if scale is not None and scale <= 0:
                ctx.issue(
                    "field_value_invalid",
                    f"{field}.qgen_scale",
                    "qgen_scale must be positive.",
                    "Use a finite value greater than zero.",
                )
            dist_file = (
                run.get("qgen_dist_file")
                or variables.get("qgen_dist_file")
                or defaults.get("qgen_dist_file")
            )
            if dist_file is None and isinstance(template_dir, str) and not _PLACEHOLDER_RE.search(template_dir):
                dist_file = os.path.join(os.path.dirname(template_dir), "dists.dss")
            ctx.input_path(dist_file, f"{field}.qgen_dist_file")
        elif mode == "custom":
            _validate_tpch_params(ctx, run.get("params", {}), f"{field}.params")

        outputs = run.get("outputs")
        if outputs is None:
            ctx.output_path(run.get("output_path"), f"{field}.output_path", order=1000 + index)
        elif not isinstance(outputs, list) or not outputs:
            ctx.issue(
                "field_type_invalid",
                f"{field}.outputs",
                "outputs must be a non-empty list.",
                "Declare output mappings or use output_path.",
            )
        else:
            for out_index, output in enumerate(outputs):
                out_field = f"{field}.outputs[{out_index}]"
                if not isinstance(output, Mapping):
                    ctx.issue(
                        "field_type_invalid",
                        out_field,
                        "Each output must be a mapping.",
                        "Declare a supported type and path.",
                    )
                    continue
                out_type = output.get("type", "workload")
                _enum(
                    ctx,
                    out_type,
                    {"workload", "temporal", "split"},
                    f"{out_field}.type",
                )
                ctx.output_path(
                    output.get("path"),
                    f"{out_field}.path",
                    expected_type="directory" if out_type == "split" else "file",
                    order=2000 + index * 100 + out_index,
                )
                if out_type == "temporal":
                    _validate_timestamp(ctx, output.get("timestamp", {}), f"{out_field}.timestamp")


def _validate_query_mix(
    ctx: _Context, spec: Mapping[str, Any], variables: Mapping[str, Any]
) -> None:
    required = ("template_ids", "baseline", "target", "sample_size", "output_path")
    missing = False
    for key in required:
        if key not in variables or variables.get(key) is None:
            ctx.issue(
                "required_field_missing",
                f"variables.{key}",
                f"variables.{key} is required.",
                "Declare the field required by template_mix.",
            )
            missing = True
    if "seed" not in spec:
        ctx.issue(
            "required_field_missing",
            "seed",
            "template_mix requires a top-level seed.",
            "Set an integer seed.",
        )
        missing = True
    if not missing:
        from driftbench.query_drift import parse_query_template_mix_spec

        try:
            parse_query_template_mix_spec(spec)
        except (TypeError, ValueError) as exc:
            message = str(exc)
            match = re.search(r"(variables(?:\.[A-Za-z_][A-Za-z0-9_]*)*|seed|type)", message)
            field = match.group(1) if match else "variables"
            ctx.issue(
                "field_type_invalid" if isinstance(exc, TypeError) else "field_value_invalid",
                field,
                "The template_mix configuration is invalid.",
                "Correct the referenced template_mix field.",
            )
    if variables.get("output_path") is not None:
        ctx.output_path(variables.get("output_path"), "variables.output_path", order=100)


def _validate_single_drift_arguments(
    ctx: _Context,
    drift: Mapping[str, Any],
    drift_type: str,
    field: str,
) -> None:
    """Reject parameters the selected built-in drift cannot consume.

    Runtime forwards every non-metadata key as a keyword argument.  Deriving
    the accepted names from the built-in method signatures keeps preflight in
    lockstep without executing a handler.  ``selective_deletion`` also has a
    bounded set of supported legacy keywords despite its compatibility
    ``**legacy`` parameter.
    """

    method_name = {
        "outlier_injection": (
            "inject_outliers_from_csv"
            if drift.get("outlier_csv_path")
            else "_inject_outliers"
        ),
        "value_skew": "_inject_skew",
        "vary_cardinality": "_vary_cardinality",
        "selective_deletion": "_delete_records",
        "insert_records": "_insert_records",
        "add_timestamp": "_add_timestamp",
        "concat_csvs": "_concat_csvs",
    }[drift_type]
    method = getattr(_data_drift.SingleTableDriftGenerator, method_name)
    accepted = {
        name
        for name, parameter in inspect.signature(method).parameters.items()
        if name != "self"
        and parameter.kind
        in {inspect.Parameter.POSITIONAL_OR_KEYWORD, inspect.Parameter.KEYWORD_ONLY}
    }
    if drift_type == "selective_deletion":
        accepted.update(
            {
                "filter_column",
                "filter_func",
                "filter_func_name",
                "filter_func_config",
                "filter_op",
                "filter_value",
                "filter_min",
                "filter_max",
            }
        )
    accepted.update({"name", "drift_type", "output_path"})
    if any(key not in accepted for key in drift):
        ctx.issue(
            "parameter_unsupported",
            field,
            "This drift contains a parameter unsupported by its runtime operation.",
            "Remove unsupported parameters or correct misspelled parameter names.",
        )


def _validate_single_drifts(
    ctx: _Context, drifts: list[Any], field: str, *, order_base: int
) -> None:
    names: set[str] = set()
    for index, drift in enumerate(drifts):
        item_field = f"{field}[{index}]"
        if not isinstance(drift, Mapping):
            ctx.issue(
                "field_type_invalid",
                item_field,
                "Each drift must be a mapping.",
                "Declare drift_type, output_path, and operation parameters.",
            )
            continue
        _optional_unique_name(ctx, drift.get("name"), f"{item_field}.name", names)
        drift_type = _required_string(ctx, drift, "drift_type", f"{item_field}.drift_type")
        if drift_type is not None:
            _enum(ctx, drift_type, _SINGLE_DRIFT_TYPES, f"{item_field}.drift_type")
            if drift_type in _SINGLE_DRIFT_TYPES:
                _validate_single_drift_arguments(ctx, drift, drift_type, item_field)
        ctx.output_path(
            drift.get("output_path"), f"{item_field}.output_path", order=order_base + index
        )

        if drift_type == "outlier_injection":
            if drift.get("outlier_csv_path") is not None:
                ctx.input_path(
                    drift.get("outlier_csv_path"), f"{item_field}.outlier_csv_path"
                )
            elif not (drift.get("column") or drift.get("key_column")):
                ctx.issue(
                    "required_field_missing",
                    f"{item_field}.column",
                    "Outlier injection requires column/key_column or outlier_csv_path.",
                    "Declare a target column or a readable outlier CSV.",
                )
            _positive_optional_ints(ctx, drift, item_field, ("n", "inject_count"))
            if drift.get("n_ratio") is not None:
                _fraction(ctx, drift.get("n_ratio"), f"{item_field}.n_ratio")
            direction = drift.get("extreme_direction")
            if direction is not None:
                _enum(ctx, direction, {"high", "low"}, f"{item_field}.extreme_direction")
            extreme_scale = _finite_number(
                ctx,
                drift.get("extreme_scale", 1.0),
                f"{item_field}.extreme_scale",
            )
            if extreme_scale is not None and extreme_scale <= 0:
                ctx.issue(
                    "field_value_invalid",
                    f"{item_field}.extreme_scale",
                    "extreme_scale must be positive.",
                    "Use a finite value greater than zero.",
                )
        elif drift_type == "value_skew":
            _string_list(ctx, drift.get("columns"), f"{item_field}.columns", nonempty=True)
            _fraction(ctx, drift.get("portion", 1.0), f"{item_field}.portion")
            _finite_number(ctx, drift.get("skewness", 2), f"{item_field}.skewness")
        elif drift_type == "vary_cardinality":
            value = _finite_number(ctx, drift.get("scale", 0.1), f"{item_field}.scale")
            if value is not None and value <= 0:
                ctx.issue(
                    "field_value_invalid",
                    f"{item_field}.scale",
                    "scale must be positive.",
                    "Use a finite value greater than zero.",
                )
        elif drift_type in {"selective_deletion", "insert_records"}:
            if drift.get("n") is not None and drift.get("n_ratio") is not None:
                ctx.issue(
                    "field_value_invalid",
                    item_field,
                    "Use n or n_ratio, not both.",
                    "Remove one of the two size controls.",
                )
            if drift.get("n") is not None:
                _positive_int(ctx, drift.get("n"), f"{item_field}.n")
            if drift.get("n_ratio") is not None:
                _fraction(ctx, drift.get("n_ratio"), f"{item_field}.n_ratio")
            if drift_type == "selective_deletion":
                _validate_filter(ctx, drift.get("filter"), f"{item_field}.filter")
                _enum(
                    ctx,
                    drift.get("strategy", "uniform"),
                    {"uniform", "key_weighted", "time_weighted"},
                    f"{item_field}.strategy",
                )
        elif drift_type == "add_timestamp":
            if drift.get("source_path") is not None:
                ctx.input_path(
                    drift.get("source_path"),
                    f"{item_field}.source_path",
                    order=order_base + index,
                    may_be_generated=True,
                )
            _validate_timestamp(ctx, drift, item_field)
        elif drift_type == "concat_csvs":
            paths = drift.get("input_paths")
            if not isinstance(paths, list) or not paths:
                ctx.issue(
                    "required_field_missing",
                    f"{item_field}.input_paths",
                    "concat_csvs requires a non-empty input_paths list.",
                    "Declare one or more readable CSV paths.",
                )
            else:
                for path_index, path in enumerate(paths):
                    ctx.input_path(
                        path,
                        f"{item_field}.input_paths[{path_index}]",
                        order=order_base + index,
                        may_be_generated=True,
                    )


def _validate_multi_steps(
    ctx: _Context,
    steps: list[Any],
    table_names: set[str],
    relationship_names: set[str],
) -> None:
    if not steps:
        ctx.issue(
            "required_field_missing",
            "variables.drift_steps",
            "drift_steps must not be empty.",
            "Declare at least one operation.",
        )
        return
    target_ops = {
        "delete_keys",
        "add_dimension_keys",
        "skew_column",
        "insert_outliers",
        "rewrite_columns",
    }
    relationship_ops = {"reassign_fk", "skew_fk"}
    count_ops = {
        "delete_keys",
        "reassign_fk",
        "add_dimension_keys",
        "skew_fk",
        "skew_column",
        "insert_outliers",
    }
    for index, step in enumerate(steps):
        field = f"variables.drift_steps[{index}]"
        if not isinstance(step, Mapping):
            ctx.issue(
                "field_type_invalid",
                field,
                "Each drift step must be a mapping.",
                "Declare an op and its required references.",
            )
            continue
        op = _required_string(ctx, step, "op", f"{field}.op")
        if op is None:
            continue
        _enum(ctx, op, _MULTI_DRIFT_OPS, f"{field}.op")
        if op in target_ops:
            target = _required_string(ctx, step, "target", f"{field}.target")
            if target is not None and target not in table_names:
                ctx.issue(
                    "reference_not_found",
                    f"{field}.target",
                    "This step references an unknown table.",
                    "Reference one of variables.tables[].name.",
                )
        if op in relationship_ops:
            ref = _required_string(ctx, step, "relationship", f"{field}.relationship")
            if ref is not None and ref not in relationship_names:
                ctx.issue(
                    "reference_not_found",
                    f"{field}.relationship",
                    "This step references an unknown relationship.",
                    "Reference one of variables.relationships[].name.",
                )
        if op in count_ops:
            _count_or_fraction(ctx, step, field)
        if op in {"scale_tables", "scale_sample"}:
            factor = _finite_number(ctx, step.get("factor", 1), f"{field}.factor")
            if factor is not None and factor <= 1:
                ctx.issue(
                    "field_value_invalid",
                    f"{field}.factor",
                    "A scale operation requires factor greater than 1.",
                    "Use a finite value greater than 1.",
                )
            if step.get("tables") is not None:
                refs = _string_list(ctx, step.get("tables"), f"{field}.tables", nonempty=True)
                if refs:
                    for ref in refs:
                        if ref not in table_names:
                            ctx.issue(
                                "reference_not_found",
                                f"{field}.tables",
                                "A scale operation references an unknown table.",
                                "Use names declared in variables.tables.",
                            )
        if op in {"skew_column", "insert_outliers"}:
            _required_string(ctx, step, "column", f"{field}.column")
        if op == "rewrite_columns":
            columns = step.get("columns")
            if not isinstance(columns, Mapping) or not columns:
                ctx.issue(
                    "required_field_missing",
                    f"{field}.columns",
                    "rewrite_columns requires a non-empty columns mapping.",
                    "Declare columns and supported rewrite types.",
                )
            else:
                for name, config in columns.items():
                    if not isinstance(name, str) or not name:
                        ctx.issue(
                            "field_value_invalid",
                            f"{field}.columns",
                            "Rewrite column names must be non-empty strings.",
                            "Use valid column identifiers.",
                        )
                    if isinstance(config, Mapping):
                        _enum(
                            ctx,
                            config.get("type", "template"),
                            {"template", "numeric_jitter", "categorical_resample"},
                            f"{field}.columns.{name}.type",
                        )
        _validate_filter(ctx, step.get("filter"), f"{field}.filter")
        for prop_key in ("propagate",):
            props = step.get(prop_key)
            if props is None:
                continue
            prop_values = props if isinstance(props, list) else [props]
            for prop_index, prop in enumerate(prop_values):
                prop_field = f"{field}.{prop_key}[{prop_index}]"
                if not isinstance(prop, Mapping):
                    ctx.issue(
                        "field_type_invalid",
                        prop_field,
                        "Propagation entries must be mappings.",
                        "Declare relationship and policy.",
                    )
                    continue
                ref = _required_string(ctx, prop, "relationship", f"{prop_field}.relationship")
                if ref is not None and ref not in relationship_names:
                    ctx.issue(
                        "reference_not_found",
                        f"{prop_field}.relationship",
                        "Propagation references an unknown relationship.",
                        "Use a declared relationship name.",
                    )
                policies = {"reassign"} if op == "add_dimension_keys" else {"drop", "reassign"}
                _enum(ctx, prop.get("policy", next(iter(policies))), policies, f"{prop_field}.policy")


def _collect_schema_resources(
    ctx: _Context, data_source: Mapping[str, Any], field: str, *, order: int
) -> None:
    schema_extractor = _optional_mapping(
        ctx, data_source.get("schema_extractor"), f"{field}.schema_extractor"
    )
    if schema_extractor is None:
        return
    source_type = schema_extractor.get("source_type") or data_source.get("kind")
    if source_type is not None:
        _enum(
            ctx,
            source_type,
            {"csv", "parquet", "postgres"},
            f"{field}.schema_extractor.source_type",
        )
    schema_path = schema_extractor.get("schema_output_path")
    if schema_path is not None:
        ctx.output_path(
            schema_path, f"{field}.schema_extractor.schema_output_path", order=order
        )
    if source_type == "postgres":
        _validate_postgres_source(ctx, data_source, field)


def _validate_data_source_kind(
    ctx: _Context, data_source: Mapping[str, Any], field: str
) -> None:
    kind = data_source.get("kind", "csv")
    _enum(ctx, kind, {"csv", "parquet", "postgres"}, f"{field}.kind")


def _validate_postgres_source(
    ctx: _Context, data_source: Mapping[str, Any], field: str
) -> None:
    if data_source.get("uri") and data_source.get("table"):
        return
    _required_string(ctx, data_source, "schema_name", f"{field}.schema_name")
    config = data_source.get("db_config")
    config_path = data_source.get("db_config_path")
    if isinstance(config, Mapping):
        return
    if config is not None and not isinstance(config, Mapping):
        ctx.issue(
            "field_type_invalid",
            f"{field}.db_config",
            "db_config must be a mapping.",
            "Use a mapping or provide db_config_path.",
        )
    if config_path is None:
        ctx.issue(
            "required_field_missing",
            f"{field}.db_config_path",
            "PostgreSQL requires db_config or db_config_path.",
            "Provide a local JSON config; connectivity is checked separately at runtime.",
        )
    else:
        ctx.input_path(config_path, f"{field}.db_config_path", parse_json=True)


def _validate_benchmark(ctx: _Context, spec: Mapping[str, Any]) -> None:
    metadata_value = spec.get("metadata")
    metadata = _optional_mapping(ctx, metadata_value, "metadata") or {}
    source_value = spec.get("data_source")
    data_source = _optional_mapping(ctx, source_value, "data_source") or {}
    meta_benchmark = metadata.get("benchmark")
    source_benchmark = data_source.get("benchmark")

    if meta_benchmark is not None and not isinstance(meta_benchmark, str):
        ctx.issue(
            "field_type_invalid",
            "metadata.benchmark",
            "metadata.benchmark must be a string.",
            "Use a supported benchmark identifier.",
        )
    if source_benchmark is not None and not isinstance(source_benchmark, str):
        ctx.issue(
            "field_type_invalid",
            "data_source.benchmark",
            "data_source.benchmark must be a string.",
            "Use a supported benchmark identifier.",
        )
    if isinstance(meta_benchmark, str) and isinstance(source_benchmark, str):
        if _canonical_benchmark(meta_benchmark) != _canonical_benchmark(source_benchmark):
            ctx.issue(
                "benchmark_mismatch",
                "data_source.benchmark",
                "metadata and data_source declare different benchmarks.",
                "Use the same benchmark identifier in both locations.",
            )

    kind = data_source.get("kind")
    if kind == "benchmark_adapter":
        if not isinstance(source_benchmark, str) or not source_benchmark.strip():
            ctx.issue(
                "required_field_missing",
                "data_source.benchmark",
                "benchmark_adapter requires a benchmark identifier.",
                "Choose one of the installed DriftBench benchmark adapters.",
            )
        else:
            benchmark = _canonical_benchmark(source_benchmark)
            module_name = _BENCHMARK_MODULES.get(benchmark)
            if module_name is None:
                ctx.issue(
                    "benchmark_unsupported",
                    "data_source.benchmark",
                    "The requested benchmark adapter is not supported.",
                    "Choose a benchmark listed in the local benchmark reference.",
                )
            else:
                try:
                    importlib.import_module(module_name)
                except Exception:
                    ctx.issue(
                        "benchmark_adapter_unavailable",
                        "data_source.benchmark",
                        "The benchmark adapter module could not be imported.",
                        "Install the package's local adapter dependencies and retry.",
                    )
                if benchmark in {"benchbase", "pgbench"}:
                    ctx.issue(
                        "external_not_checked",
                        "data_source.benchmark",
                        (
                            "BenchBase Java/runtime readiness was not checked."
                            if benchmark == "benchbase"
                            else "pgbench binary and database readiness were not checked."
                        ),
                        (
                            "Verify the BenchBase JAR and target database before execution."
                            if benchmark == "benchbase"
                            else "Verify pgbench and PostgreSQL connectivity before execution."
                        ),
                        severity="warning",
                    )
    if kind == "postgres" or (
        isinstance(data_source.get("schema_extractor"), Mapping)
        and data_source["schema_extractor"].get("source_type") == "postgres"
    ):
        ctx.issue(
            "external_not_checked",
            "data_source",
            "PostgreSQL connectivity was not checked.",
            "Test credentials and connectivity in the execution environment.",
            severity="warning",
        )


def _validate_inputs(ctx: _Context) -> None:
    output_by_path: dict[str, list[_PathUse]] = {}
    for output in ctx.outputs:
        if _PLACEHOLDER_RE.search(output.value):
            continue
        output_by_path.setdefault(_path_identity(ctx, output.value), []).append(output)

    for use in ctx.inputs:
        if _PLACEHOLDER_RE.search(use.value):
            ctx.issue(
                "unresolved_placeholder",
                use.field,
                "A required input path contains an unresolved placeholder.",
                "Bind the placeholder to a readable local resource.",
            )
            continue
        identity = _path_identity(ctx, use.value)
        producers = output_by_path.get(identity, [])
        if use.may_be_generated and any(output.order < use.order for output in producers):
            ctx.generated_input_keys.add((use.field, identity))
            continue

        path = _resolve_path(ctx, use.value)
        try:
            exists = path.exists()
        except OSError:
            exists = False
        if not exists:
            ctx.issue(
                "input_not_found",
                use.field,
                "A required local input does not exist.",
                "Create or bind the resource relative to the current working directory.",
            )
            continue
        correct_type = path.is_dir() if use.expected_type == "directory" else path.is_file()
        if not correct_type:
            ctx.issue(
                "input_type_mismatch",
                use.field,
                f"This input must be a {use.expected_type}.",
                f"Point the field to a readable {use.expected_type}.",
            )
            continue
        access_mode = os.R_OK | (os.X_OK if use.expected_type == "directory" else 0)
        if not os.access(path, access_mode):
            ctx.issue(
                "input_not_readable",
                use.field,
                "A required local input is not readable.",
                "Grant read permission and retry.",
            )
            continue
        if use.parse_json:
            try:
                with path.open("r", encoding="utf-8") as stream:
                    parsed = json.load(stream)
                if not isinstance(parsed, Mapping):
                    raise ValueError("configuration root is not a mapping")
            except (OSError, UnicodeError, ValueError, json.JSONDecodeError):
                ctx.issue(
                    "config_invalid",
                    use.field,
                    "This JSON configuration is invalid or unreadable.",
                    "Provide a UTF-8 JSON object without exposing secrets in the spec.",
                )
                continue
            if use.json_contract == "schema" and not _has_supported_schema_shape(parsed):
                ctx.issue(
                    "config_invalid",
                    use.field,
                    "This JSON object does not contain a runnable schema structure.",
                    "Provide tables with columns, source.columns, or table plus columns.",
                )


def _has_supported_schema_shape(value: Mapping[str, Any]) -> bool:
    def columns_ready(columns: Any) -> bool:
        return isinstance(columns, Mapping) and bool(columns)

    tables = value.get("tables")
    if isinstance(tables, Mapping) and tables:
        return all(
            isinstance(entry, Mapping) and columns_ready(entry.get("columns"))
            for entry in tables.values()
        )

    source = value.get("source")
    if isinstance(source, Mapping) and columns_ready(source.get("columns")):
        return True

    table = value.get("table")
    return (
        isinstance(table, str)
        and bool(table.strip())
        and columns_ready(value.get("columns"))
    )


def _validate_outputs(ctx: _Context) -> None:
    literal_outputs: list[tuple[_PathUse, str]] = []
    for use in ctx.outputs:
        if _PLACEHOLDER_RE.search(use.value):
            ctx.issue(
                "unresolved_placeholder",
                use.field,
                "An output path contains an unresolved placeholder.",
                "Bind the placeholder before execution.",
            )
            continue
        literal_outputs.append((use, _path_identity(ctx, use.value)))

    by_identity: dict[str, list[_PathUse]] = {}
    for use, identity in literal_outputs:
        by_identity.setdefault(identity, []).append(use)
    for uses in by_identity.values():
        if len(uses) > 1:
            for use in uses:
                ctx.issue(
                    "duplicate_output",
                    use.field,
                    "Multiple declarations resolve to the same output path.",
                    "Give every output a distinct path.",
                )

    input_identities: dict[str, list[_PathUse]] = {}
    for input_use in ctx.inputs:
        if _PLACEHOLDER_RE.search(input_use.value):
            continue
        identity = _path_identity(ctx, input_use.value)
        if (input_use.field, identity) in ctx.generated_input_keys:
            continue
        input_identities.setdefault(identity, []).append(input_use)
    for use, identity in literal_outputs:
        if identity in input_identities:
            ctx.issue(
                "input_output_collision",
                use.field,
                "An output resolves to the same path as a required input.",
                "Choose a different output path to preserve the input.",
            )

    for use, _ in literal_outputs:
        path = _resolve_path(ctx, use.value)
        try:
            exists = path.exists()
        except OSError:
            exists = False
        if exists:
            correct_type = path.is_dir() if use.expected_type == "directory" else path.is_file()
            if not correct_type:
                ctx.issue(
                    "output_type_mismatch",
                    use.field,
                    f"The existing output path is not a {use.expected_type}.",
                    "Remove the type conflict or choose another output path.",
                )
                continue
            ctx.issue(
                "would_overwrite",
                use.field,
                "The output already exists and execution may overwrite it.",
                "Back it up or choose a new path before execution.",
                severity="warning",
            )
            if not os.access(path, os.W_OK):
                ctx.issue(
                    "output_not_writable",
                    use.field,
                    "The existing output is not writable.",
                    "Grant write permission or choose another path.",
                )

        ancestor = path if use.expected_type == "directory" else path.parent
        while not ancestor.exists() and ancestor != ancestor.parent:
            ancestor = ancestor.parent
        if not ancestor.exists() or not ancestor.is_dir():
            ctx.issue(
                "output_parent_invalid",
                use.field,
                "No usable existing output ancestor was found.",
                "Choose a path beneath an existing directory.",
            )
        elif not os.access(ancestor, os.W_OK):
            ctx.issue(
                "output_parent_not_writable",
                use.field,
                "The nearest existing output ancestor is not writable.",
                "Grant write permission or choose another output location.",
            )


def _validate_tpch_params(ctx: _Context, value: Any, field: str) -> None:
    if not isinstance(value, Mapping):
        ctx.issue(
            "field_type_invalid",
            field,
            "TPC-H params must be a mapping.",
            "Map query IDs to parameter definitions.",
        )
        return
    for query_id, definitions in value.items():
        item_field = f"{field}.{query_id}"
        if isinstance(definitions, list):
            if not definitions:
                ctx.issue(
                    "field_value_invalid",
                    item_field,
                    "A parameter choice list must not be empty.",
                    "Declare at least one value or definition.",
                )
            for index, definition in enumerate(definitions):
                _validate_tpch_param_definition(ctx, definition, f"{item_field}[{index}]")
        else:
            _validate_tpch_param_definition(ctx, definitions, item_field)


def _validate_tpch_param_definition(ctx: _Context, value: Any, field: str) -> None:
    if not isinstance(value, Mapping):
        return
    param_type = value.get("type")
    if param_type is None:
        param_type = "choice" if ("values" in value or "choices" in value) else "fixed"
    allowed = {"choice", "fixed", "int_range", "float_range", "date_range", "dss_dist"}
    _enum(ctx, param_type, allowed, f"{field}.type")
    if param_type == "choice":
        choices = value.get("values") or value.get("choices")
        if not isinstance(choices, list) or not choices:
            ctx.issue(
                "required_field_missing",
                f"{field}.values",
                "choice parameters require non-empty values or choices.",
                "Declare at least one choice.",
            )
    elif param_type == "fixed":
        if "value" not in value and not (value.get("values") or value.get("choices")):
            ctx.issue(
                "required_field_missing",
                f"{field}.value",
                "fixed parameters require a value.",
                "Declare value, values, or choices.",
            )
    elif param_type in {"int_range", "float_range"}:
        low = _finite_number(ctx, value.get("min"), f"{field}.min")
        high = _finite_number(ctx, value.get("max"), f"{field}.max")
        if low is not None and high is not None and high < low:
            ctx.issue(
                "field_value_invalid",
                field,
                "Parameter range max must be at least min.",
                "Correct the range bounds.",
            )
    elif param_type == "date_range":
        for key in ("start", "end"):
            text = _validate_string_value(ctx, value.get(key), f"{field}.{key}", required=True)
            if text is not None:
                try:
                    _datetime.date.fromisoformat(text)
                except ValueError:
                    ctx.issue(
                        "field_value_invalid",
                        f"{field}.{key}",
                        "Date range bounds must use ISO dates.",
                        "Use YYYY-MM-DD.",
                    )
    elif param_type == "dss_dist":
        ctx.input_path(value.get("dist_file"), f"{field}.dist_file")
        _validate_string_value(ctx, value.get("dist_name"), f"{field}.dist_name", required=True)


def _validate_dist_config(ctx: _Context, value: Any, field: str) -> None:
    if not isinstance(value, Mapping):
        ctx.issue(
            "field_type_invalid",
            field,
            "dist_config must be a mapping.",
            "Map columns or logical types to distribution mappings.",
        )
        return
    for key, config in value.items():
        config_field = f"{field}.{key}"
        if not isinstance(config, Mapping):
            ctx.issue(
                "field_type_invalid",
                config_field,
                "Each distribution config must be a mapping.",
                "Declare a supported distribution and its parameters.",
            )
            continue
        distribution = config.get("distribution", "uniform")
        _enum(
            ctx,
            distribution,
            {"uniform", "normal", "zipf", "choice", "fixed"},
            f"{config_field}.distribution",
        )
        if distribution == "choice" and not config.get("choices"):
            ctx.issue(
                "required_field_missing",
                f"{config_field}.choices",
                "choice distribution requires choices.",
                "Declare a non-empty choices list.",
            )


def _validate_timestamp(ctx: _Context, value: Any, field: str) -> None:
    if not isinstance(value, Mapping):
        ctx.issue(
            "field_type_invalid",
            field,
            "Timestamp configuration must be a mapping.",
            "Declare pattern, start_time, and queries_per_minute as needed.",
        )
        return
    pattern = value.get("pattern", "uniform")
    _enum(
        ctx,
        pattern,
        {"uniform", "periodic", "bursty", "trend", "long_tail"},
        f"{field}.pattern",
    )
    _positive_int(
        ctx, value.get("queries_per_minute", 60), f"{field}.queries_per_minute"
    )
    start_time = value.get("start_time", "2025-07-01T00:00:00")
    if not isinstance(start_time, str):
        ctx.issue(
            "field_type_invalid",
            f"{field}.start_time",
            "start_time must be an ISO datetime string.",
            "Use an ISO-8601 datetime.",
        )
    else:
        try:
            _datetime.datetime.fromisoformat(start_time)
        except ValueError:
            ctx.issue(
                "field_value_invalid",
                f"{field}.start_time",
                "start_time is not a valid ISO datetime.",
                "Use an ISO-8601 datetime such as 2025-07-01T00:00:00.",
            )


def _validate_filter(ctx: _Context, value: Any, field: str) -> None:
    if value is None:
        return
    if not isinstance(value, Mapping):
        ctx.issue(
            "field_type_invalid",
            field,
            "filter must be a mapping.",
            "Declare a column and a supported condition.",
        )
        return
    _required_string(ctx, value, "column", f"{field}.column")
    op = value.get("op")
    if op is not None:
        _enum(ctx, op, {">", ">=", "<", "<=", "==", "!=", "in", "not_in"}, f"{field}.op")
        if "value" not in value:
            ctx.issue(
                "required_field_missing",
                f"{field}.value",
                "filter.value is required when filter.op is set.",
                "Declare the comparison value.",
            )


def _count_or_fraction(ctx: _Context, value: Mapping[str, Any], field: str) -> None:
    has_count = value.get("count") is not None
    has_fraction = value.get("fraction") is not None
    if has_count == has_fraction:
        ctx.issue(
            "required_field_missing" if not has_count else "field_value_invalid",
            field,
            "Use exactly one of count or fraction.",
            "Declare one size control and remove the other.",
        )
        return
    if has_count:
        _positive_int(ctx, value.get("count"), f"{field}.count")
    else:
        _fraction(ctx, value.get("fraction"), f"{field}.fraction")


def _required_mapping(
    ctx: _Context, mapping: Mapping[str, Any], key: str, field: str
) -> Mapping[str, Any] | None:
    if key not in mapping or mapping.get(key) is None:
        ctx.issue(
            "required_field_missing",
            field,
            f"{field} is required.",
            "Declare a mapping for this field.",
        )
        return None
    return _optional_mapping(ctx, mapping.get(key), field)


def _optional_mapping(
    ctx: _Context, value: Any, field: str
) -> Mapping[str, Any] | None:
    if value is None:
        return None
    if not isinstance(value, Mapping):
        ctx.issue(
            "field_type_invalid",
            field,
            f"{field} must be a mapping.",
            "Use YAML key/value pairs.",
        )
        return None
    return value


def _required_string(
    ctx: _Context, mapping: Mapping[str, Any], key: str, field: str
) -> str | None:
    return _validate_string_value(ctx, mapping.get(key), field, required=True)


def _validate_string_value(
    ctx: _Context, value: Any, field: str, *, required: bool
) -> str | None:
    if value is None:
        if required:
            ctx.issue(
                "required_field_missing",
                field,
                f"{field} is required.",
                "Set it to a non-empty string.",
            )
        return None
    if not isinstance(value, str):
        ctx.issue(
            "field_type_invalid",
            field,
            f"{field} must be a string.",
            "Use a non-empty string.",
        )
        return None
    if not value.strip():
        ctx.issue(
            "field_value_invalid",
            field,
            f"{field} must not be empty.",
            "Use a non-empty string.",
        )
        return None
    return value.strip()


def _required_nonempty_list(
    ctx: _Context, mapping: Mapping[str, Any], key: str, field: str
) -> list[Any] | None:
    if key not in mapping or mapping.get(key) is None:
        ctx.issue(
            "required_field_missing",
            field,
            f"{field} is required.",
            "Declare a non-empty list.",
        )
        return None
    value = mapping.get(key)
    if not isinstance(value, list):
        ctx.issue(
            "field_type_invalid",
            field,
            f"{field} must be a list.",
            "Declare a non-empty YAML list.",
        )
        return None
    if not value:
        ctx.issue(
            "field_value_invalid",
            field,
            f"{field} must not be empty.",
            "Declare at least one item.",
        )
        return None
    return value


def _optional_list(ctx: _Context, value: Any, field: str) -> list[Any] | None:
    if value is None:
        return None
    if not isinstance(value, list):
        ctx.issue(
            "field_type_invalid",
            field,
            f"{field} must be a list.",
            "Use a YAML list.",
        )
        return None
    return value


def _string_list(
    ctx: _Context, value: Any, field: str, *, nonempty: bool
) -> list[str] | None:
    if not isinstance(value, list) or (nonempty and not value):
        ctx.issue(
            "field_type_invalid" if value is not None else "required_field_missing",
            field,
            f"{field} must be {'a non-empty' if nonempty else 'a'} string list.",
            "Use a YAML list of non-empty strings.",
        )
        return None
    if not all(isinstance(item, str) and item.strip() for item in value):
        ctx.issue(
            "field_value_invalid",
            field,
            f"{field} must contain only non-empty strings.",
            "Remove empty or non-string entries.",
        )
        return None
    normalized = [item.strip() for item in value]
    if len(set(normalized)) != len(normalized):
        ctx.issue(
            "duplicate_identifier",
            field,
            f"{field} must not contain duplicates.",
            "Keep each identifier once.",
        )
    return normalized


def _query_ids(ctx: _Context, value: Any, field: str) -> list[str] | None:
    if not isinstance(value, (list, tuple)) or not value:
        ctx.issue(
            "field_type_invalid",
            field,
            "query_ids must be a non-empty list.",
            "Declare TPC-H query IDs from 1 through 22.",
        )
        return None
    result: list[str] = []
    for index, item in enumerate(value):
        text = str(item) if isinstance(item, int) and not isinstance(item, bool) else item
        if not isinstance(text, str) or not text.isdigit() or not 1 <= int(text) <= 22:
            ctx.issue(
                "field_value_invalid",
                f"{field}[{index}]",
                "TPC-H query IDs must be integers or digit strings from 1 through 22.",
                "Use one of the standard 22 TPC-H query IDs.",
            )
            continue
        result.append(text)
    if len(set(result)) != len(result):
        ctx.issue(
            "duplicate_identifier",
            field,
            "query_ids must be unique.",
            "Keep each query ID once per run.",
        )
    return result


def _optional_unique_name(
    ctx: _Context, value: Any, field: str, names: set[str]
) -> None:
    if value is None:
        return
    name = _validate_string_value(ctx, value, field, required=False)
    if name is None:
        return
    if name in names:
        ctx.issue(
            "duplicate_identifier",
            field,
            "Names in this list must be unique.",
            "Give every item a distinct name.",
        )
    names.add(name)


def _enum(ctx: _Context, value: Any, allowed: set[str], field: str) -> None:
    if not isinstance(value, str):
        ctx.issue(
            "field_type_invalid",
            field,
            f"{field} must be a string.",
            f"Choose one of: {', '.join(sorted(allowed))}.",
        )
    elif value not in allowed:
        ctx.issue(
            "field_value_invalid",
            field,
            f"{field} uses an unsupported value.",
            f"Choose one of: {', '.join(sorted(allowed))}.",
        )


def _finite_number(ctx: _Context, value: Any, field: str) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        ctx.issue(
            "field_type_invalid",
            field,
            f"{field} must be a number.",
            "Use a finite numeric value.",
        )
        return None
    converted = float(value)
    if not math.isfinite(converted):
        ctx.issue(
            "field_value_invalid",
            field,
            f"{field} must be finite.",
            "Use a finite numeric value.",
        )
        return None
    return converted


def _positive_int(ctx: _Context, value: Any, field: str) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int):
        ctx.issue(
            "field_type_invalid",
            field,
            f"{field} must be an integer.",
            "Use an integer greater than zero.",
        )
        return None
    if value <= 0:
        ctx.issue(
            "field_value_invalid",
            field,
            f"{field} must be positive.",
            "Use an integer greater than zero.",
        )
        return None
    return value


def _nonnegative_int(ctx: _Context, value: Any, field: str) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int):
        ctx.issue(
            "field_type_invalid",
            field,
            f"{field} must be an integer.",
            "Use a non-negative integer.",
        )
        return None
    if value < 0:
        ctx.issue(
            "field_value_invalid",
            field,
            f"{field} must not be negative.",
            "Use a non-negative integer.",
        )
        return None
    return value


def _fraction(ctx: _Context, value: Any, field: str) -> float | None:
    number = _finite_number(ctx, value, field)
    if number is not None and not 0 < number <= 1:
        ctx.issue(
            "field_value_invalid",
            field,
            f"{field} must be in (0, 1].",
            "Use a fraction greater than zero and at most one.",
        )
        return None
    return number


def _positive_optional_ints(
    ctx: _Context, mapping: Mapping[str, Any], field: str, keys: Sequence[str]
) -> None:
    for key in keys:
        if mapping.get(key) is not None:
            _positive_int(ctx, mapping.get(key), f"{field}.{key}")


def _canonical_benchmark(value: str) -> str:
    return value.strip().lower().replace("-", "_")


def _resolve_path(ctx: _Context, value: str) -> Path:
    path = Path(value)
    if not path.is_absolute():
        path = ctx.working_dir / path
    return path


def _path_identity(ctx: _Context, value: str) -> str:
    path = _resolve_path(ctx, value)
    # Existing hard links and symlinks must compare as the same resource;
    # otherwise an apparently distinct output can overwrite an input.  For a
    # path that does not yet exist, retain a normalized real-path identity so
    # duplicate future outputs are still detected lexically.
    try:
        stat_result = path.stat()
        if stat_result.st_ino:
            return f"inode:{stat_result.st_dev}:{stat_result.st_ino}"
    except OSError:
        pass
    return os.path.normcase(os.path.realpath(os.path.abspath(os.fspath(path))))


def _legacy_declared_output_count(spec: Mapping[str, Any]) -> int:
    found: set[str] = set()

    def walk(value: Any) -> None:
        if isinstance(value, Mapping):
            for key, item in value.items():
                if key == "output_path" and isinstance(item, str):
                    found.add(item)
                else:
                    walk(item)
        elif isinstance(value, list):
            for item in value:
                walk(item)

    walk(spec)
    return len(found)


_VALIDATORS: dict[
    tuple[str, str, str],
    Callable[[_Context, Mapping[str, Any], Mapping[str, Any]], None],
] = {
    ("data", "drift", "single_table"): _validate_data_single,
    ("data", "drift", "multi_table"): _validate_data_multi,
    ("workload", "templates", "selection_payload"): _validate_selection_payload,
    ("workload", "sql_templates", "tpch"): _validate_tpch_sql,
    ("workload", "keylist", "single_table"): _validate_keylist,
    ("workload", "drift", "template_mix"): _validate_query_mix,
}


__all__ = [
    "PreflightCheck",
    "PreflightIssue",
    "PreflightReport",
    "deep_validate_spec_file",
]
