from __future__ import annotations

import hashlib
import json
import math
import os
import re
from dataclasses import dataclass, fields, is_dataclass
from pathlib import Path
from typing import Any, Mapping

from driftbench import __version__
from driftbench.data.tpch_param_specs import (
    TPCHParamSpecsIdentityError,
    canonicalize_tpch_param_specs,
)

from .errors import CacheConfigurationError


REMOTE_CACHE_SCHEMA = "driftbench-artifact-cache"
REMOTE_CACHE_VERSION = "v1"
REMOTE_MANIFEST_SCHEMA = "driftbench.remote-cache-manifest/v1"
PRODUCER_CONTRACT_REVISION = "cache5-20260823"


def _producer() -> dict[str, str]:
    return {
        "distribution": "driftbench-db",
        "version": __version__,
        "contract_revision": PRODUCER_CONTRACT_REVISION,
        "runtime_family": "windows" if os.name == "nt" else "posix",
    }


@dataclass(frozen=True)
class _AdapterRule:
    benchmark: str
    artifact_type: str
    revision: str
    allowed: bool = True
    reason: str = ""


_RULES: dict[str, _AdapterRule] = {
    "driftbench.data.tpch.TPCHData": _AdapterRule("tpch", "data", "3", False, "TPC-H data is external/licensed or dbgen-produced"),
    "driftbench.data.tpch.TPCHQueries": _AdapterRule("tpch", "queries", "5"),
    "driftbench.data.tpcds.TPCDSData": _AdapterRule("tpcds", "data", "3"),
    "driftbench.data.tpcds.TPCDSQueries": _AdapterRule("tpcds", "queries", "3", False, "the generated sample XML contains connection placeholders"),
    "driftbench.data.tpcc.TPCCData": _AdapterRule("tpcc", "data", "4"),
    "driftbench.data.tpcc.TPCCQueries": _AdapterRule("tpcc", "queries", "3"),
    "driftbench.data.tpcc_skew.TPCCSkewData": _AdapterRule("tpcc_skew", "data", "4"),
    "driftbench.data.tpcc_skew.TPCCSkewQueries": _AdapterRule("tpcc_skew", "queries", "3"),
    "driftbench.data.ycsb.YCSBData": _AdapterRule("ycsb", "data", "4"),
    "driftbench.data.ycsb.YCSBQueries": _AdapterRule("ycsb", "queries", "4", False, "the generated sample XML contains connection placeholders"),
    "driftbench.data.dsb.DSBData": _AdapterRule("dsb", "data", "3"),
    "driftbench.data.dsb.DSBQueries": _AdapterRule("dsb", "queries", "3"),
    "driftbench.data.job.JOBData": _AdapterRule("job", "data", "3"),
    "driftbench.data.job.JOBQueries": _AdapterRule("job", "queries", "3"),
    "driftbench.data.pgbench.PgBenchData": _AdapterRule("pgbench", "data", "3"),
    "driftbench.data.pgbench.PgBenchQueries": _AdapterRule("pgbench", "queries", "3"),
    "driftbench.data.benchbase.BenchBaseData": _AdapterRule("benchbase", "data", "3", False, "BenchBase artifacts are external-driver configuration"),
    "driftbench.data.benchbase.BenchBaseQueries": _AdapterRule("benchbase", "queries", "3", False, "BenchBase artifacts are external-driver configuration"),
}


@dataclass(frozen=True)
class ArtifactIdentity:
    benchmark: str
    artifact_type: str
    generator: str
    generator_id: str
    generator_revision: str
    parameters: dict[str, Any]
    fingerprint: str

    def canonical(self) -> dict[str, Any]:
        return {
            "schema": REMOTE_CACHE_SCHEMA,
            "version": REMOTE_CACHE_VERSION,
            "producer": _producer(),
            "benchmark": self.benchmark,
            "artifact_type": self.artifact_type,
            "generator": self.generator,
            "generator_revision": self.generator_revision,
            "parameters": self.parameters,
        }


def _type_name(adapter: object) -> str:
    adapter_type = type(adapter)
    return f"{adapter_type.__module__}.{adapter_type.__qualname__}"


def _normalize(value: Any) -> Any:
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise CacheConfigurationError("artifact parameters must be finite")
        return int(value) if value.is_integer() else value
    if isinstance(value, Path):
        raise CacheConfigurationError(
            "external filesystem paths are not eligible for the shared remote cache"
        )
    if isinstance(value, Mapping):
        return {
            str(key): _normalize(item)
            for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
        }
    if isinstance(value, (list, tuple)):
        return [_normalize(item) for item in value]
    if isinstance(value, (set, frozenset)):
        normalized = [_normalize(item) for item in value]
        return sorted(
            normalized,
            key=lambda item: json.dumps(
                item,
                ensure_ascii=True,
                allow_nan=False,
                separators=(",", ":"),
                sort_keys=True,
            ),
        )
    if hasattr(value, "__fspath__"):
        raise CacheConfigurationError(
            "external filesystem paths are not eligible for the shared remote cache"
        )
    try:
        return [_normalize(item) for item in value]
    except TypeError as exc:
        raise CacheConfigurationError(
            f"unsupported artifact parameter type: {type(value).__name__}"
        ) from exc


def _validate_tpch_custom_param_specs(
    value: Any,
) -> None:
    """Default-deny external inputs and containers with unstable semantics."""
    try:
        canonicalize_tpch_param_specs(value, require_remote_safe=True)
    except TPCHParamSpecsIdentityError as exc:
        raise CacheConfigurationError(str(exc)) from exc


def _require_real(
    value: Any,
    *,
    field: str,
    minimum: float | None = None,
    maximum: float | None = None,
) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise CacheConfigurationError(f"{field} must be a finite number")
    number = float(value)
    if not math.isfinite(number):
        raise CacheConfigurationError(f"{field} must be a finite number")
    if minimum is not None and number < minimum:
        raise CacheConfigurationError(f"{field} is below the supported range")
    if maximum is not None and number > maximum:
        raise CacheConfigurationError(f"{field} is above the supported range")
    return number


def _require_int(
    value: Any,
    *,
    field: str,
    minimum: int | None = None,
) -> int:
    if type(value) is not int:
        raise CacheConfigurationError(f"{field} must be an integer")
    if minimum is not None and value < minimum:
        raise CacheConfigurationError(f"{field} is below the supported range")
    return value


def _require_optional_path(value: Any, *, field: str) -> None:
    if value is not None and not isinstance(value, (str, Path)):
        raise CacheConfigurationError(f"{field} must be a filesystem path string")


def _normalize_tpch_query_ids(adapter: object) -> None:
    query_ids = getattr(adapter, "query_ids")
    if query_ids is None:
        return
    if isinstance(query_ids, (str, bytes, Mapping)):
        raise CacheConfigurationError("TPC-H query_ids must be an iterable of IDs")
    try:
        raw_ids = list(query_ids)
    except TypeError as exc:
        raise CacheConfigurationError(
            "TPC-H query_ids must be an iterable of IDs"
        ) from exc
    if not raw_ids:
        raise CacheConfigurationError("TPC-H query_ids must not be empty")

    normalized: list[str] = []
    for item in raw_ids:
        if isinstance(item, bool) or not isinstance(item, (int, str)):
            raise CacheConfigurationError(
                "TPC-H query_ids must contain integer IDs from 1 through 22"
            )
        text = str(item).strip()
        if not re.fullmatch(r"(?:[1-9]|1[0-9]|2[0-2])", text):
            raise CacheConfigurationError(
                "TPC-H query_ids must contain integer IDs from 1 through 22"
            )
        normalized.append(str(int(text)))
    if len(set(normalized)) != len(normalized):
        raise CacheConfigurationError("TPC-H query_ids must be unique")
    if isinstance(query_ids, (set, frozenset)):
        normalized.sort(key=int)
    setattr(adapter, "query_ids", tuple(normalized))


def validate_adapter_parameters(adapter: object) -> None:
    """Validate request-visible adapter values before identity or remote access."""

    type_name = _type_name(adapter)
    try:
        if type_name == "driftbench.data.tpch.TPCHData":
            scale = getattr(adapter, "scale_factor")
            if isinstance(scale, bool) or not isinstance(scale, (str, int, float)):
                raise CacheConfigurationError(
                    "TPC-H scale_factor must be a positive decimal value"
                )
            try:
                numeric_scale = float(str(scale).strip())
            except (TypeError, ValueError) as exc:
                raise CacheConfigurationError(
                    "TPC-H scale_factor must be a positive decimal value"
                ) from exc
            if not math.isfinite(numeric_scale) or numeric_scale <= 0:
                raise CacheConfigurationError(
                    "TPC-H scale_factor must be a positive decimal value"
                )
            if getattr(adapter, "mode") not in {"copy", "generate"}:
                raise CacheConfigurationError(
                    "TPC-H data mode must be one of: copy, generate"
                )
            _require_optional_path(getattr(adapter, "source_dir"), field="source_dir")
            _require_optional_path(getattr(adapter, "dbgen_path"), field="dbgen_path")

        elif type_name == "driftbench.data.tpch.TPCHQueries":
            if getattr(adapter, "mode") not in {"qgen", "custom"}:
                raise CacheConfigurationError(
                    "TPC-H query mode must be one of: qgen, custom"
                )
            _normalize_tpch_query_ids(adapter)
            _require_optional_path(
                getattr(adapter, "template_dir"), field="template_dir"
            )
            _require_optional_path(
                getattr(adapter, "qgen_dist_file"), field="qgen_dist_file"
            )
            _require_int(
                getattr(adapter, "queries_per_template"),
                field="queries_per_template",
                minimum=1,
            )
            _require_int(getattr(adapter, "seed"), field="seed")
            if type(getattr(adapter, "shuffle")) is not bool:
                raise CacheConfigurationError("shuffle must be a boolean")
            if getattr(adapter, "param_specs") is not None and type(
                getattr(adapter, "param_specs")
            ) is not dict:
                raise CacheConfigurationError("param_specs must be a plain mapping")
            if getattr(adapter, "mode") == "custom":
                _validate_tpch_custom_param_specs(getattr(adapter, "param_specs"))
            _require_real(getattr(adapter, "scale"), field="scale", minimum=1e-12)

        elif type_name in {
            "driftbench.data.tpcds.TPCDSData",
            "driftbench.data.tpcc.TPCCData",
            "driftbench.data.dsb.DSBData",
            "driftbench.data.job.JOBData",
            "driftbench.data.pgbench.PgBenchData",
        }:
            _require_real(
                getattr(adapter, "scale_factor"),
                field="scale_factor",
                minimum=1e-12,
            )

        elif type_name in {
            "driftbench.data.tpcc_skew.TPCCSkewData",
            "driftbench.data.tpcc_skew.TPCCSkewQueries",
        }:
            _require_real(
                getattr(adapter, "scale_factor"),
                field="scale_factor",
                minimum=1e-12,
            )
            _require_real(
                getattr(adapter, "hot_warehouse_fraction"),
                field="hot_warehouse_fraction",
                minimum=1e-12,
                maximum=1.0,
            )
            _require_real(
                getattr(adapter, "skew_factor"),
                field="skew_factor",
                minimum=1e-12,
            )

        elif type_name in {
            "driftbench.data.ycsb.YCSBData",
            "driftbench.data.ycsb.YCSBQueries",
        }:
            from driftbench.data.ycsb import _effective_record_count

            _effective_record_count(
                getattr(adapter, "scale_factor"), getattr(adapter, "record_count")
            )
            if type_name.endswith("YCSBQueries"):
                workload = getattr(adapter, "workload")
                if not isinstance(workload, str) or workload.upper() not in {
                    "A",
                    "B",
                    "C",
                    "D",
                    "E",
                    "F",
                }:
                    raise CacheConfigurationError(
                        "YCSB workload must be one of: A, B, C, D, E, F"
                    )
                _require_int(
                    getattr(adapter, "run_seconds"),
                    field="run_seconds",
                    minimum=1,
                )
                _require_int(
                    getattr(adapter, "target_rate"),
                    field="target_rate",
                    minimum=0,
                )

        elif type_name == "driftbench.data.pgbench.PgBenchQueries":
            if getattr(adapter, "workload") not in {
                "tpcb",
                "simple_update",
                "select_only",
            }:
                raise CacheConfigurationError(
                    "pgbench workload must be one of: tpcb, simple_update, select_only"
                )
            _require_int(getattr(adapter, "clients"), field="clients", minimum=1)
            _require_int(getattr(adapter, "duration"), field="duration", minimum=1)
            _require_int(getattr(adapter, "rate"), field="rate", minimum=0)
    except CacheConfigurationError:
        raise
    except (OverflowError, TypeError, ValueError) as exc:
        raise CacheConfigurationError(
            "artifact request parameters are invalid"
        ) from exc


def _effective_parameters(adapter: object, type_name: str) -> dict[str, Any]:
    if not is_dataclass(adapter):
        raise CacheConfigurationError("remote cache adapters must be registered dataclasses")

    values = {
        field.name: getattr(adapter, field.name)
        for field in fields(adapter)
        if field.init and field.name not in {"benchmark", "artifact_type"}
    }

    if type_name in {
        "driftbench.data.tpcc.TPCCData",
        "driftbench.data.tpcc.TPCCQueries",
        "driftbench.data.tpcc_skew.TPCCSkewData",
        "driftbench.data.tpcc_skew.TPCCSkewQueries",
    } and "scale_factor" in values:
        values["scale_factor"] = max(1, int(round(float(values["scale_factor"]))))
    elif type_name in {
        "driftbench.data.tpcds.TPCDSData",
        "driftbench.data.dsb.DSBData",
        "driftbench.data.job.JOBData",
        "driftbench.data.pgbench.PgBenchData",
    }:
        values["scale_factor"] = max(1, int(round(float(values["scale_factor"]))))

    if type_name in {
        "driftbench.data.ycsb.YCSBData",
        "driftbench.data.ycsb.YCSBQueries",
    }:
        from driftbench.data.ycsb import _effective_record_count

        scale, records = _effective_record_count(
            values.get("scale_factor"), values.get("record_count")
        )
        values["scale_factor"] = scale
        values["record_count"] = records
        if "workload" in values:
            values["workload"] = str(values["workload"]).upper()

    if type_name == "driftbench.data.tpch.TPCHQueries":
        if values.get("template_dir") is not None or values.get("qgen_dist_file") is not None:
            raise CacheConfigurationError(
                "TPC-H queries with external template or distribution paths are not remote-cache eligible"
            )
        query_ids = values.get("query_ids")
        values["query_ids"] = (
            [str(index) for index in range(1, 23)]
            if query_ids is None
            else [str(item) for item in query_ids]
        )
        adapter.query_ids = tuple(values["query_ids"])
        values["template_dir"] = None
        if values.get("mode") == "qgen":
            values.pop("param_specs", None)
            values["qgen_dist_file"] = None
        else:
            values.pop("qgen_dist_file", None)
            values.pop("scale", None)
            try:
                values["param_specs"] = canonicalize_tpch_param_specs(
                    values.get("param_specs") or {},
                    require_remote_safe=True,
                )
            except TPCHParamSpecsIdentityError as exc:
                raise CacheConfigurationError(str(exc)) from exc

    return _normalize(values)


def identity_for(adapter: object) -> ArtifactIdentity:
    type_name = _type_name(adapter)
    rule = _RULES.get(type_name)
    if rule is None:
        raise CacheConfigurationError(
            f"adapter {type_name} is not in the remote-cache allowlist"
        )
    if not rule.allowed:
        raise CacheConfigurationError(
            f"adapter {type_name} is not remote-cache eligible: {rule.reason}"
        )
    if getattr(adapter, "benchmark", None) != rule.benchmark or getattr(
        adapter, "artifact_type", None
    ) != rule.artifact_type:
        raise CacheConfigurationError("adapter benchmark/artifact_type identity was overridden")

    validate_adapter_parameters(adapter)
    parameters = _effective_parameters(adapter, type_name)
    canonical = {
        "schema": REMOTE_CACHE_SCHEMA,
        "version": REMOTE_CACHE_VERSION,
        "producer": _producer(),
        "benchmark": rule.benchmark,
        "artifact_type": rule.artifact_type,
        "generator": type_name,
        "generator_revision": rule.revision,
        "parameters": parameters,
    }
    encoded = json.dumps(
        canonical,
        ensure_ascii=True,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    fingerprint = hashlib.sha256(encoded).hexdigest()
    slug = re.sub(r"[^a-z0-9]+", "-", type_name.lower()).strip("-")
    return ArtifactIdentity(
        benchmark=rule.benchmark,
        artifact_type=rule.artifact_type,
        generator=type_name,
        generator_id=f"{slug}-r{rule.revision}",
        generator_revision=rule.revision,
        parameters=parameters,
        fingerprint=fingerprint,
    )
