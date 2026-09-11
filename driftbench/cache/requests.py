from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from driftbench.data.dsb import DSBData, DSBQueries
from driftbench.data.job import JOBData, JOBQueries
from driftbench.data.pgbench import PgBenchData, PgBenchQueries
from driftbench.data.tpcc import TPCCData, TPCCQueries
from driftbench.data.tpcc_skew import TPCCSkewData, TPCCSkewQueries
from driftbench.data.tpcds import TPCDSData, TPCDSQueries
from driftbench.data.tpch import TPCHData, TPCHQueries
from driftbench.data.ycsb import YCSBData, YCSBQueries
from driftbench.data.sysbench import SysbenchData, SysbenchQueries
from driftbench.data.ssb import SSBData, SSBQueries
from driftbench.data.ldbc import LDBCData, LDBCQueries

from .errors import CacheConfigurationError
from .identity import validate_adapter_parameters
from .models import AzureHNSCacheConfig, RemoteCacheMode


_YAML_LIMIT_BYTES = 64 * 1024
_REQUEST_SCHEMA = "driftbench.artifact-request/v1"
_AZURE_CONFIG_SCHEMA = "driftbench.azure-hns-cache/v1"


class _UniqueKeyLoader(yaml.SafeLoader):
    pass


def _construct_unique_mapping(
    loader: _UniqueKeyLoader,
    node: yaml.MappingNode,
    deep: bool = False,
) -> dict[Any, Any]:
    mapping: dict[Any, Any] = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        if key in mapping:
            raise CacheConfigurationError("YAML contains a duplicate mapping key")
        mapping[key] = loader.construct_object(value_node, deep=deep)
    return mapping


_UniqueKeyLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
    _construct_unique_mapping,
)


@dataclass(frozen=True)
class ArtifactRequest:
    adapter: Any
    benchmark: str
    artifact_type: str
    force: bool


@dataclass(frozen=True)
class _FactoryRule:
    adapter_type: type[Any]
    parameters: frozenset[str]


_REGISTRY: dict[tuple[str, str], _FactoryRule] = {
    ("tpch", "data"): _FactoryRule(TPCHData, frozenset({"scale_factor", "source_dir", "mode", "dbgen_path"})),
    ("tpch", "queries"): _FactoryRule(TPCHQueries, frozenset({"query_ids", "template_dir", "mode", "queries_per_template", "seed", "shuffle", "param_specs", "qgen_dist_file", "scale"})),
    ("tpcds", "data"): _FactoryRule(TPCDSData, frozenset({"scale_factor"})),
    ("tpcds", "queries"): _FactoryRule(TPCDSQueries, frozenset()),
    ("tpcc", "data"): _FactoryRule(TPCCData, frozenset({"scale_factor"})),
    ("tpcc", "queries"): _FactoryRule(TPCCQueries, frozenset()),
    ("tpcc_skew", "data"): _FactoryRule(TPCCSkewData, frozenset({"scale_factor", "hot_warehouse_fraction", "skew_factor"})),
    ("tpcc_skew", "queries"): _FactoryRule(TPCCSkewQueries, frozenset({"scale_factor", "hot_warehouse_fraction", "skew_factor"})),
    ("ycsb", "data"): _FactoryRule(YCSBData, frozenset({"scale_factor", "record_count"})),
    ("ycsb", "queries"): _FactoryRule(YCSBQueries, frozenset({"workload", "run_seconds", "target_rate", "scale_factor", "record_count"})),
    ("dsb", "data"): _FactoryRule(DSBData, frozenset({"scale_factor"})),
    ("dsb", "queries"): _FactoryRule(DSBQueries, frozenset()),
    ("job", "data"): _FactoryRule(JOBData, frozenset({"scale_factor"})),
    ("job", "queries"): _FactoryRule(JOBQueries, frozenset()),
    ("pgbench", "data"): _FactoryRule(PgBenchData, frozenset({"scale_factor"})),
    ("pgbench", "queries"): _FactoryRule(PgBenchQueries, frozenset({"workload", "clients", "duration", "rate"})),
    ("sysbench", "data"): _FactoryRule(SysbenchData, frozenset({"tables", "table_size", "db_driver", "seed"})),
    ("sysbench", "queries"): _FactoryRule(SysbenchQueries, frozenset({"workload", "tables", "table_size", "threads", "duration", "rate", "db_driver", "distribution", "seed"})),
    ("ssb", "data"): _FactoryRule(SSBData, frozenset({"source_dir"})),
    ("ssb", "queries"): _FactoryRule(SSBQueries, frozenset({"query_ids"})),
    ("ldbc", "data"): _FactoryRule(LDBCData, frozenset({"source_dir", "format", "benchmark_version", "date_format"})),
    ("ldbc", "queries"): _FactoryRule(LDBCQueries, frozenset({"parameters_dir", "driver_config", "updates_dir", "read_only"})),
}


def _read_yaml(path_value: str | Path) -> Any:
    candidate = Path(path_value).expanduser()
    try:
        if candidate.is_symlink():
            raise CacheConfigurationError("configuration path must be a non-symlink regular file")
        path = candidate.resolve(strict=True)
        if not path.is_file():
            raise CacheConfigurationError("configuration path must be a non-symlink regular file")
        size = path.stat().st_size
        if size < 1 or size > _YAML_LIMIT_BYTES:
            raise CacheConfigurationError("configuration file must be between 1 byte and 64 KiB")
        text = path.read_text(encoding="utf-8")
    except CacheConfigurationError:
        raise
    except (OSError, UnicodeError) as exc:
        raise CacheConfigurationError("configuration file could not be read") from exc
    try:
        return yaml.load(text, Loader=_UniqueKeyLoader)
    except CacheConfigurationError:
        raise
    except yaml.YAMLError as exc:
        raise CacheConfigurationError("configuration file is not valid safe YAML") from exc


def _require_shape(
    payload: Any,
    *,
    allowed: frozenset[str],
    required: frozenset[str],
    label: str,
) -> dict[str, Any]:
    if not isinstance(payload, dict) or not all(isinstance(key, str) for key in payload):
        raise CacheConfigurationError(f"{label} must be a YAML mapping with string keys")
    unknown = sorted(set(payload) - allowed)
    missing = sorted(required - set(payload))
    if unknown:
        raise CacheConfigurationError(f"{label} contains unknown fields: {', '.join(unknown)}")
    if missing:
        raise CacheConfigurationError(f"{label} is missing required fields: {', '.join(missing)}")
    return payload


def load_artifact_request(path: str | Path) -> ArtifactRequest:
    payload = _require_shape(
        _read_yaml(path),
        allowed=frozenset({"schema", "benchmark", "artifact_type", "parameters", "force"}),
        required=frozenset({"schema", "benchmark", "artifact_type"}),
        label="artifact request",
    )
    if payload["schema"] != _REQUEST_SCHEMA:
        raise CacheConfigurationError(f"artifact request schema must be {_REQUEST_SCHEMA}")
    benchmark = payload["benchmark"]
    artifact_type = payload["artifact_type"]
    if not isinstance(benchmark, str) or not isinstance(artifact_type, str):
        raise CacheConfigurationError("benchmark and artifact_type must be strings")
    rule = _REGISTRY.get((benchmark, artifact_type))
    if rule is None:
        raise CacheConfigurationError("benchmark/artifact_type is not in the public request registry")
    parameters = payload.get("parameters", {})
    if not isinstance(parameters, dict) or not all(isinstance(key, str) for key in parameters):
        raise CacheConfigurationError("artifact request parameters must be a mapping")
    unknown = sorted(set(parameters) - rule.parameters)
    if unknown:
        raise CacheConfigurationError(
            "artifact request contains unknown parameters: " + ", ".join(unknown)
        )
    force = payload.get("force", False)
    if type(force) is not bool:
        raise CacheConfigurationError("artifact request force must be a boolean")
    try:
        adapter = rule.adapter_type(**parameters)
    except (TypeError, ValueError) as exc:
        raise CacheConfigurationError("artifact request parameters are invalid") from exc
    validate_adapter_parameters(adapter)
    return ArtifactRequest(
        adapter=adapter,
        benchmark=benchmark,
        artifact_type=artifact_type,
        force=force,
    )


def load_azure_cache_config(
    path: str | Path,
    *,
    mode: RemoteCacheMode | str,
    credential_env_file: str | Path | None,
) -> AzureHNSCacheConfig:
    payload = _require_shape(
        _read_yaml(path),
        allowed=frozenset({"schema", "account_url", "file_system", "prefix"}),
        required=frozenset({"schema", "account_url", "file_system"}),
        label="Azure cache config",
    )
    if payload["schema"] != _AZURE_CONFIG_SCHEMA:
        raise CacheConfigurationError(
            f"Azure cache config schema must be {_AZURE_CONFIG_SCHEMA}"
        )
    if not all(
        isinstance(payload.get(name, ""), str)
        for name in ("account_url", "file_system", "prefix")
    ):
        raise CacheConfigurationError("Azure cache config values must be strings")
    config = AzureHNSCacheConfig(
        account_url=payload["account_url"],
        file_system=payload["file_system"],
        prefix=payload.get("prefix", ""),
        mode=mode,
        credential_env_file=credential_env_file,
    )
    config.validate_enabled()
    return config
