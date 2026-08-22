"""Benchmark registry, adapter preparation, and query normalization."""

from __future__ import annotations

import csv
import importlib
import json
import os
import re
from dataclasses import dataclass
from functools import lru_cache
from importlib import resources
from pathlib import Path
from typing import Any, Mapping

import yaml
import pandas as pd

from driftbench.data.base import GenerationResult
from driftbench.api import QueryTemplate

from .artifacts import ensure_managed_path, file_descriptor, load_json, sha256_file


BENCHMARK_ORDER = (
    "tpch",
    "tpcds",
    "tpcc",
    "tpcc_skew",
    "job",
    "ycsb",
    "dsb",
    "pgbench",
)
TPCH_TABLES = frozenset(
    {
        "customer",
        "lineitem",
        "nation",
        "orders",
        "part",
        "partsupp",
        "region",
        "supplier",
    }
)
_SCENARIO_ID_RE = re.compile(r"^[a-z0-9]+(?:_[a-z0-9]+)*$")
_DATA_OPERATIONS = frozenset(
    {"outlier_injection", "value_skew", "vary_cardinality", "delete_keys"}
)


class PrerequisiteError(RuntimeError):
    """A local/offline prerequisite is missing."""


@dataclass(frozen=True)
class BenchmarkDefinition:
    name: str
    title: str
    description: str
    adapter_module: str
    adapter: str
    scale: Mapping[str, Any]
    table: str
    column: str
    query_capabilities: Mapping[str, str]
    limitations: str


@dataclass(frozen=True)
class PreparedData:
    benchmark: BenchmarkDefinition
    generation: GenerationResult
    tables: Mapping[str, Path]
    input_files: tuple[Mapping[str, Any], ...]


@dataclass(frozen=True)
class PreparedQueries:
    benchmark: BenchmarkDefinition
    templates: tuple[QueryTemplate, ...]
    input_files: tuple[Mapping[str, Any], ...]
    profile_weights: Mapping[str, Mapping[str, float]]


@lru_cache(maxsize=1)
def benchmark_config() -> dict[str, Any]:
    return _load_yaml("benchmarks.yaml")


@lru_cache(maxsize=1)
def scenario_config() -> dict[str, Any]:
    payload = _load_yaml("drift_scenarios.yaml")
    if payload.get("schema_version") != 3:
        raise ValueError("drift scenario registry must use schema_version 3")
    for kind in ("data", "query"):
        by_benchmark = payload.get(kind)
        if not isinstance(by_benchmark, Mapping) or tuple(by_benchmark) != BENCHMARK_ORDER:
            raise ValueError(f"{kind} scenario registry must contain the ordered eight benchmarks")
        for benchmark in BENCHMARK_ORDER:
            entries = by_benchmark[benchmark]
            expected_count = 3 if kind == "data" else 2
            if not isinstance(entries, Mapping) or len(entries) != expected_count:
                raise ValueError(
                    f"{benchmark}/{kind} must define exactly {expected_count} scenarios"
                )
            for scenario, entry in entries.items():
                if not isinstance(scenario, str) or not _SCENARIO_ID_RE.fullmatch(scenario):
                    raise ValueError(f"unsafe scenario ID: {scenario!r}")
                expected_keys = (
                    {"spec", "rationale", "target", "operation"}
                    if kind == "data"
                    else {"spec", "rationale"}
                )
                if not isinstance(entry, Mapping) or set(entry) != expected_keys:
                    raise ValueError(
                        f"{benchmark}/{kind}/{scenario} registry entry must contain "
                        f"exactly {sorted(expected_keys)}"
                    )
                spec = str(entry["spec"])
                expected = f"specs/{kind}/{benchmark}/{scenario}.yaml"
                if spec != expected:
                    raise ValueError(
                        f"{benchmark}/{kind}/{scenario} must use canonical spec path {expected}"
                    )
                if not str(entry["rationale"]).strip():
                    raise ValueError(f"{benchmark}/{kind}/{scenario} rationale is empty")
                if kind == "data":
                    target = entry["target"]
                    if (
                        not isinstance(target, Mapping)
                        or set(target) != {"table", "column"}
                        or not all(
                            isinstance(target.get(key), str)
                            and bool(str(target[key]).strip())
                            for key in ("table", "column")
                        )
                    ):
                        raise ValueError(
                            f"{benchmark}/data/{scenario} target must contain table and column"
                        )
                    if entry["operation"] not in _DATA_OPERATIONS:
                        raise ValueError(
                            f"{benchmark}/data/{scenario} uses an unsupported operation"
                        )
    return payload


def scenario_entries(kind: str, benchmark: str) -> tuple[tuple[str, Mapping[str, Any]], ...]:
    if kind not in {"data", "query"}:
        raise ValueError(f"unsupported visualization kind: {kind}")
    get_benchmark(benchmark)
    entries = scenario_config()[kind][benchmark]
    return tuple((str(scenario), dict(entry)) for scenario, entry in entries.items())


def get_scenario_entry(kind: str, benchmark: str, scenario: str) -> Mapping[str, Any]:
    for candidate, entry in scenario_entries(kind, benchmark):
        if candidate == scenario:
            return entry
    raise ValueError(f"unsupported scenario: {benchmark}/{kind}/{scenario}")


def data_scenario_contract(benchmark: str, scenario: str) -> dict[str, str]:
    """Return the scenario-keyed target/operation allowlist entry."""

    entry = get_scenario_entry("data", benchmark, scenario)
    target = entry["target"]
    assert isinstance(target, Mapping)
    return {
        "table": str(target["table"]),
        "column": str(target["column"]),
        "operation": str(entry["operation"]),
    }


def registry() -> dict[str, BenchmarkDefinition]:
    payload = benchmark_config()
    order = tuple(payload.get("order", ()))
    entries = payload.get("benchmarks", {})
    if order != BENCHMARK_ORDER or set(entries) != set(BENCHMARK_ORDER):
        raise ValueError("benchmark registry must contain exactly the supported eight benchmarks")
    result: dict[str, BenchmarkDefinition] = {}
    for name in BENCHMARK_ORDER:
        entry = entries[name]
        result[name] = BenchmarkDefinition(
            name=name,
            title=str(entry["title"]),
            description=str(entry["description"]),
            adapter_module=str(entry["adapter_module"]),
            adapter=str(entry["adapter"]),
            scale=dict(entry.get("scale", {})),
            table=str(entry["data"]["table"]),
            column=str(entry["data"]["column"]),
            query_capabilities=dict(entry["query_capabilities"]),
            limitations=str(entry["limitations"]),
        )
    return result


def get_benchmark(name: str) -> BenchmarkDefinition:
    try:
        return registry()[name]
    except KeyError as exc:
        raise ValueError(f"unsupported benchmark: {name}") from exc


def preflight_data(
    benchmark: str,
    workspace_root: Path | None = None,
    *,
    offline: bool,
    force: bool = False,
) -> None:
    get_benchmark(benchmark)
    if benchmark != "tpch":
        return
    source_value = os.environ.get("DRIFTBENCH_TPCH_SOURCE_DIR", "").strip()
    if source_value:
        _tpch_source_dir(source_value)
        return
    if (
        not force
        and workspace_root is not None
        and _load_tpch_adapter_result(workspace_root)
    ):
        return
    mode = "offline " if offline else ""
    force_note = " when --force is used" if force else ""
    raise PrerequisiteError(
        f"TPC-H {mode}data generation requires "
        "DRIFTBENCH_TPCH_SOURCE_DIR pointing to all eight local SF0.01 .tbl "
        f"files{force_note}, or a content-verified managed cache without --force"
    )


def prepare_data(
    benchmark: str,
    workspace_root: Path,
    *,
    seed: int,
    force: bool,
    offline: bool,
) -> PreparedData:
    del seed  # Public data adapters currently own their fixed baseline seed.
    definition = get_benchmark(benchmark)
    preflight_data(benchmark, workspace_root, offline=offline, force=force)
    module = importlib.import_module(definition.adapter_module)
    adapter_root = ensure_managed_path(workspace_root, "data", "adapters")

    if benchmark == "tpch":
        source_value = os.environ.get("DRIFTBENCH_TPCH_SOURCE_DIR", "").strip()
        if source_value:
            adapter = module.data(
                scale_factor="0.01",
                source_dir=_tpch_source_dir(source_value),
                mode="copy",
            )
            generation = adapter.generate(output_dir=adapter_root, force=force)
        else:
            generation = _load_tpch_adapter_result(workspace_root)
            if generation is None:  # pragma: no cover - preflight owns this branch
                raise PrerequisiteError(
                    "TPC-H data generation requires a local source or valid cache"
                )
    elif benchmark == "tpcds":
        adapter = module.data(scale_factor=1)
    elif benchmark == "tpcc":
        adapter = module.data(scale_factor=1)
    elif benchmark == "tpcc_skew":
        adapter = module.data(
            scale_factor=1, hot_warehouse_fraction=0.2, skew_factor=0.99
        )
    elif benchmark == "job":
        adapter = module.data(scale_factor=1)
    elif benchmark == "ycsb":
        adapter = module.data(scale_factor=1, record_count=1000)
    elif benchmark == "dsb":
        adapter = module.data(scale_factor=1)
    elif benchmark == "pgbench":
        adapter = module.data(scale_factor=1)
    else:  # pragma: no cover - registry validation protects this branch
        raise ValueError(f"unsupported benchmark: {benchmark}")

    if benchmark != "tpch":
        generation = adapter.generate(output_dir=adapter_root, force=force)
    generation = _as_csv_cached(generation, force=force)
    tables = {
        path.stem: path
        for path in generation.files
        if path.suffix.lower() == ".csv" and path.is_file()
    }
    target_tables = {
        data_scenario_contract(benchmark, scenario)["table"]
        for scenario, _ in scenario_entries("data", benchmark)
    }
    missing_tables = sorted(target_tables - set(tables))
    if missing_tables:
        raise RuntimeError(
            f"{benchmark} adapter did not produce required target tables {missing_tables}"
        )
    for scenario, _ in scenario_entries("data", benchmark):
        contract = data_scenario_contract(benchmark, scenario)
        columns = set(pd.read_csv(tables[contract["table"]], nrows=0).columns)
        if contract["column"] not in columns:
            raise RuntimeError(
                f"{benchmark} adapter target {contract['table']}.{contract['column']} "
                f"for {scenario} is missing"
            )
    descriptors = tuple(
        file_descriptor(tables[name], workspace_root) for name in sorted(tables)
    )
    return PreparedData(definition, generation, tables, descriptors)


def prepare_queries(
    benchmark: str,
    workspace_root: Path,
    *,
    seed: int,
    force: bool,
    offline: bool,
) -> PreparedQueries:
    del offline  # Every query adapter used here is local and deterministic.
    definition = get_benchmark(benchmark)
    module = importlib.import_module(definition.adapter_module)
    base_root = ensure_managed_path(workspace_root, "cache", "adapters", "query")
    profile_weights: dict[str, Mapping[str, float]] = {}
    consumed: list[Path] = []

    if benchmark == "tpch":
        result = module.queries(
            query_ids=range(1, 23),
            mode="qgen",
            queries_per_template=1,
            seed=seed,
            shuffle=True,
            scale=0.01,
        ).generate(output_dir=base_root / "tpch", force=force)
        csv_path = _find_file(result, "tpch_queries.csv")
        consumed.append(csv_path)
        templates_by_id: dict[str, QueryTemplate] = {}
        with csv_path.open(encoding="utf-8", newline="") as stream:
            for row in csv.DictReader(stream):
                template_id = f"q{int(row['query_id'])}"
                templates_by_id[template_id] = QueryTemplate(template_id, row["sql"])
        templates = tuple(
            templates_by_id[f"q{index}"] for index in range(1, 23)
        )
    elif benchmark == "tpcds":
        result = module.queries().generate(output_dir=base_root / "tpcds", force=force)
        ids_path = _find_file(result, "query_ids.txt")
        consumed.append(ids_path)
        templates = tuple(
            QueryTemplate(line.strip(), None, {"artifact": "query_id"})
            for line in ids_path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        )
    elif benchmark in {"tpcc", "tpcc_skew"}:
        query_root = base_root / benchmark
        if benchmark == "tpcc":
            adapter = module.queries()
        else:
            adapter = module.queries(
                scale_factor=1, hot_warehouse_fraction=0.2, skew_factor=0.99
            )
        result = adapter.generate(output_dir=query_root, force=force)
        metadata = _read_metadata(result)
        consumed.append(result.metadata)
        names = tuple(str(name) for name in metadata["transaction_types"])
        templates = tuple(
            QueryTemplate(name, _read_named_sql(result, f"{name}.sql", consumed))
            for name in names
        )
    elif benchmark == "job":
        result = module.queries().generate(output_dir=base_root / "job", force=force)
        metadata = _read_metadata(result)
        consumed.append(result.metadata)
        names = tuple(str(name) for name in metadata["query_families"])
        templates = tuple(
            QueryTemplate(name, _read_named_sql(result, f"{name}.sql", consumed))
            for name in names
        )
    elif benchmark == "ycsb":
        manifests: dict[str, dict[str, Any]] = {}
        for profile in ("A", "C", "E"):
            result = module.queries(workload=profile).generate(
                output_dir=base_root / "ycsb" / f"profile_{profile.lower()}",
                force=force,
            )
            manifests[profile] = _read_metadata(result)
            consumed.append(result.metadata)
        operation_ids = tuple(sorted(manifests["A"]["weights"]))
        for profile in ("C", "E"):
            if set(operation_ids) != set(manifests[profile]["weights"]):
                raise RuntimeError(
                    "YCSB public adapter profiles expose inconsistent operations"
                )
        templates = tuple(
            QueryTemplate(operation, None, {"artifact": "operation"})
            for operation in operation_ids
        )
        for profile, metadata in manifests.items():
            profile_weights[profile] = {
                operation: float(metadata["weights"][operation])
                for operation in operation_ids
            }
    elif benchmark == "dsb":
        result = module.queries().generate(output_dir=base_root / "dsb", force=force)
        sql_paths = sorted(
            path for path in result.files if path.suffix.lower() == ".sql"
        )
        consumed.extend(sql_paths)
        templates = tuple(
            QueryTemplate(path.stem, path.read_text(encoding="utf-8"))
            for path in sql_paths
        )
    elif benchmark == "pgbench":
        templates_list: list[QueryTemplate] = []
        for workload in ("tpcb", "simple_update", "select_only"):
            result = module.queries(workload=workload).generate(
                output_dir=base_root / "pgbench" / workload,
                force=force,
            )
            sql_path = _find_file(result, f"pgbench_{workload}.sql")
            consumed.append(sql_path)
            templates_list.append(
                QueryTemplate(workload, sql_path.read_text(encoding="utf-8"))
            )
        templates = tuple(templates_list)
    else:  # pragma: no cover
        raise ValueError(f"unsupported benchmark: {benchmark}")

    if not templates:
        raise RuntimeError(f"{benchmark} query adapter produced no templates")
    descriptors = tuple(
        file_descriptor(path, workspace_root)
        for path in sorted(set(consumed), key=lambda item: item.as_posix())
    )
    return PreparedQueries(definition, templates, descriptors, profile_weights)


def resolve_query_weights(
    prepared: PreparedQueries, config: Mapping[str, Any]
) -> tuple[dict[str, float], dict[str, float]]:
    ids = tuple(template.template_id for template in prepared.templates)
    baseline = _expand_weight_config(
        ids, config["baseline"], prepared.profile_weights, "baseline"
    )
    target = _expand_weight_config(
        ids, config["target"], prepared.profile_weights, "target"
    )
    return baseline, target


def _expand_weight_config(
    ids: tuple[str, ...],
    config: Mapping[str, Any],
    profiles: Mapping[str, Mapping[str, float]],
    label: str,
) -> dict[str, float]:
    if config.get("mode") == "uniform":
        return {template_id: 1.0 / len(ids) for template_id in ids}
    if "adapter_profile" in config:
        profile = str(config["adapter_profile"])
        if profile not in profiles:
            raise ValueError(f"missing adapter profile {profile!r} for {label}")
        return {template_id: float(profiles[profile][template_id]) for template_id in ids}
    if "weights" in config:
        weights = {str(key): float(value) for key, value in config["weights"].items()}
        if set(weights) != set(ids):
            raise ValueError(f"{label} weights do not match adapter template IDs")
        return {template_id: weights[template_id] for template_id in ids}
    if "focus" in config:
        focus = {str(key): float(value) for key, value in config["focus"].items()}
        unknown = set(focus) - set(ids)
        if unknown:
            raise ValueError(f"{label} focus contains unknown IDs: {sorted(unknown)}")
        remaining = [template_id for template_id in ids if template_id not in focus]
        remaining_total = float(config.get("remaining_total", 0.0))
        if not remaining and remaining_total:
            raise ValueError(f"{label} has remaining mass but no remaining templates")
        each = remaining_total / len(remaining) if remaining else 0.0
        return {
            template_id: focus.get(template_id, each) for template_id in ids
        }
    raise ValueError(f"unsupported {label} weight configuration")


def _load_yaml(name: str) -> dict[str, Any]:
    text = resources.files("visualization").joinpath("configs", name).read_text(
        encoding="utf-8"
    )
    payload = yaml.safe_load(text)
    if not isinstance(payload, dict):
        raise ValueError(f"configuration must be a mapping: {name}")
    return payload


def _tpch_source_dir(value: str) -> Path:
    source = Path(value).expanduser().resolve()
    missing = sorted(
        table for table in TPCH_TABLES if not (source / f"{table}.tbl").is_file()
    )
    if not source.is_dir() or missing:
        raise PrerequisiteError(
            "DRIFTBENCH_TPCH_SOURCE_DIR must contain all eight SF0.01 .tbl files; "
            f"missing: {missing}"
        )
    return source


def _load_tpch_adapter_result(workspace_root: Path) -> GenerationResult | None:
    """Load a content-verified public Adapter result without its original source."""

    adapter_root = ensure_managed_path(workspace_root, "data", "adapters")
    manifest_path = adapter_root / "tpch" / "data" / "sf_0.01" / "tpch_data_manifest.json"
    if not manifest_path.is_file():
        return None
    try:
        payload = load_json(manifest_path)
        if (
            payload.get("benchmark") != "tpch"
            or payload.get("artifact_type") != "data"
            or str(payload.get("scale_factor")) != "0.01"
        ):
            return None
        paths = payload.get("files")
        cache = payload.get("cache")
        descriptors = cache.get("artifacts") if isinstance(cache, Mapping) else None
        if (
            not isinstance(paths, list)
            or not paths
            or not isinstance(descriptors, list)
            or len(paths) != len(descriptors)
        ):
            return None
        files: list[Path] = []
        for relative, descriptor in zip(paths, descriptors):
            if (
                not isinstance(relative, str)
                or not isinstance(descriptor, Mapping)
                or descriptor.get("path") != relative
            ):
                return None
            candidate = (adapter_root / Path(relative)).resolve()
            candidate.relative_to(adapter_root.resolve())
            if (
                not candidate.is_file()
                or candidate.stat().st_size != descriptor.get("bytes")
                or sha256_file(candidate) != descriptor.get("sha256")
            ):
                return None
            files.append(candidate)
        if {path.stem for path in files} != TPCH_TABLES:
            return None
        return GenerationResult(
            benchmark="tpch",
            artifact_type="data",
            output_dir=adapter_root,
            files=files,
            metadata=manifest_path,
        )
    except Exception:
        return None


def _as_csv_cached(result: GenerationResult, *, force: bool) -> GenerationResult:
    convertible = [path for path in result.files if path.suffix.lower() in {".tbl", ".dat"}]
    if not convertible:
        return result
    if not force and all(
        path.with_suffix(".csv").is_file()
        and path.with_suffix(".csv").stat().st_mtime_ns >= path.stat().st_mtime_ns
        for path in convertible
    ):
        converted = [
            path.with_suffix(".csv") if path.suffix.lower() in {".tbl", ".dat"} else path
            for path in result.files
        ]
        return GenerationResult(
            benchmark=result.benchmark,
            artifact_type=result.artifact_type,
            output_dir=result.output_dir,
            files=converted,
            metadata=result.metadata,
        )
    return result.as_csv()


def _find_file(result: GenerationResult, name: str) -> Path:
    matches = [path for path in result.files if path.name == name]
    if len(matches) != 1:
        raise RuntimeError(
            f"{result.benchmark} adapter expected exactly one {name!r}, found {len(matches)}"
        )
    return matches[0]


def _read_named_sql(result: GenerationResult, name: str, consumed: list[Path]) -> str:
    path = _find_file(result, name)
    consumed.append(path)
    return path.read_text(encoding="utf-8")


def _read_metadata(result: GenerationResult) -> dict[str, Any]:
    payload = json.loads(result.metadata.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise RuntimeError(f"invalid adapter manifest: {result.metadata}")
    return payload


__all__ = [
    "BENCHMARK_ORDER",
    "BenchmarkDefinition",
    "PreparedData",
    "PreparedQueries",
    "PrerequisiteError",
    "benchmark_config",
    "data_scenario_contract",
    "get_benchmark",
    "get_scenario_entry",
    "preflight_data",
    "prepare_data",
    "prepare_queries",
    "registry",
    "resolve_query_weights",
    "scenario_config",
    "scenario_entries",
]
