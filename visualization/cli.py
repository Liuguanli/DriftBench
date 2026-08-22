"""Command-line orchestration for the DriftBench Visualization Gallery."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any, Literal, Mapping, Sequence

from driftbench import __version__ as DRIFTBENCH_VERSION

from . import VISUALIZATION_SCHEMA_VERSION
from .artifacts import (
    ensure_managed_path,
    file_descriptor,
    load_json,
    semantic_hash,
    utc_timestamp,
)
from .benchmarks import (
    BENCHMARK_ORDER,
    PreparedData,
    PreparedQueries,
    PrerequisiteError,
    get_benchmark,
    preflight_data,
    prepare_data,
    prepare_queries,
    scenario_entries,
)
from .distributions import (
    analysis_metadata,
    summarize_data_distribution,
    summarize_query_distribution,
)
from .drift_scenarios import (
    SPEC_EXECUTOR,
    SPEC_EXECUTOR_VERSION,
    execute_data_drift,
    execute_query_drift,
)
from .effects import EffectAssertionError, effect_label, evaluate_effect
from .gallery import build_gallery, is_cache_hit, write_manifest
from .plots import (
    MissingOptionalDependency,
    plot_data_comparison,
    plot_query_comparison,
    renderer_metadata,
    require_matplotlib,
)
from .provenance import (
    CACHE_SCHEMA,
    cache_fingerprint,
    configuration_hash_for_spec,
    manifest_semantic_hash,
    resolved_spec_hash,
)
from .specs import CanonicalSpec, drift_parameters, load_canonical_spec


EXIT_OK = 0
EXIT_USAGE = 2
EXIT_PREREQUISITE = 3
EXIT_FAILURE = 4
GenerationAction = Literal["Generated", "Migrated", "Reused"]


class ConfigurationError(ValueError):
    pass


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m visualization.cli",
        description="Generate reproducible DriftBench data/query drift comparisons.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    prepare_parser = subparsers.add_parser("prepare", help="prepare adapter artifacts")
    _add_benchmark_argument(prepare_parser)
    _add_kind_argument(prepare_parser)
    prepare_parser.add_argument("--seed", type=int, default=42)
    prepare_parser.add_argument("--force", action="store_true")
    prepare_parser.add_argument("--offline", action="store_true")
    _add_output_argument(prepare_parser)

    generate_parser = subparsers.add_parser(
        "generate", help="generate figures, DriftSpecs, and manifests"
    )
    _add_benchmark_argument(generate_parser)
    _add_kind_argument(generate_parser)
    generate_parser.add_argument("--scenario", default="all")
    generate_parser.add_argument("--seed", type=int, default=42)
    generate_parser.add_argument("--force", action="store_true")
    generate_parser.add_argument("--offline", action="store_true")
    generate_parser.add_argument("--sample-size", type=int, default=1000)
    _add_output_argument(generate_parser)

    gallery_parser = subparsers.add_parser(
        "build-gallery", help="validate manifests and build GALLERY.md"
    )
    _add_output_argument(gallery_parser)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        output_root = Path(args.output_dir).expanduser().resolve()
        if args.command == "prepare":
            return _prepare_command(args, output_root)
        if args.command == "generate":
            return _generate_command(args, output_root)
        if args.command == "build-gallery":
            gallery = build_gallery(output_root)
            print(f"Gallery written: {gallery}")
            return EXIT_OK
        raise ConfigurationError(f"unknown command: {args.command}")
    except ConfigurationError as exc:
        print(f"configuration error: {exc}", file=sys.stderr)
        return EXIT_USAGE
    except (PrerequisiteError, MissingOptionalDependency) as exc:
        print(f"prerequisite error: {exc}", file=sys.stderr)
        return EXIT_PREREQUISITE
    except Exception as exc:
        print(f"visualization error: {exc}", file=sys.stderr)
        return EXIT_FAILURE


def _prepare_command(args: argparse.Namespace, output_root: Path) -> int:
    benchmarks = _selected_benchmarks(args.benchmark)
    kinds = _selected_kinds(args.kind)
    _validate_seed(args.seed)
    _preflight_all(
        benchmarks,
        kinds,
        output_root,
        offline=args.offline,
        force=args.force,
        plotting=False,
    )
    failures: list[tuple[str, str, Exception]] = []
    for benchmark in benchmarks:
        for kind in kinds:
            try:
                _prepare_one(
                    benchmark,
                    kind,
                    output_root,
                    seed=args.seed,
                    force=args.force,
                    offline=args.offline,
                )
                print(f"Prepared {benchmark}/{kind}")
            except Exception as exc:
                failures.append((benchmark, kind, exc))
                print(f"FAILED {benchmark}/{kind}: {exc}", file=sys.stderr)
    if failures:
        if all(isinstance(item[2], PrerequisiteError) for item in failures):
            return EXIT_PREREQUISITE
        return EXIT_FAILURE
    return EXIT_OK


def _generate_command(args: argparse.Namespace, output_root: Path) -> int:
    benchmarks = _selected_benchmarks(args.benchmark)
    kinds = _selected_kinds(args.kind)
    _validate_seed(args.seed)
    if args.sample_size <= 0:
        raise ConfigurationError("--sample-size must be positive")
    targets = _planned_targets(benchmarks, kinds, args.scenario)
    _preflight_all(
        benchmarks,
        kinds,
        output_root,
        offline=args.offline,
        force=args.force,
        plotting=True,
    )

    failures: list[tuple[str, str, str, Exception]] = []
    by_preparation: dict[tuple[str, str], list[str]] = {}
    for benchmark, kind, scenario in targets:
        by_preparation.setdefault((benchmark, kind), []).append(scenario)

    for (benchmark, kind), scenarios in by_preparation.items():
        try:
            prepared = _prepare_one(
                benchmark,
                kind,
                output_root,
                seed=args.seed,
                force=args.force,
                offline=args.offline,
            )
        except Exception as exc:
            for scenario in scenarios:
                failures.append((benchmark, kind, scenario, exc))
                print(f"FAILED {benchmark}/{kind}/{scenario}: {exc}", file=sys.stderr)
            continue

        for scenario in scenarios:
            try:
                action = _generate_one(
                    benchmark,
                    kind,
                    scenario,
                    output_root,
                    seed=args.seed,
                    sample_size=args.sample_size,
                    force=args.force,
                    offline=args.offline,
                    prepared=prepared,
                )
                print(f"{action} {benchmark}/{kind}/{scenario}")
            except Exception as exc:
                failures.append((benchmark, kind, scenario, exc))
                print(f"FAILED {benchmark}/{kind}/{scenario}: {exc}", file=sys.stderr)
    if failures:
        if all(
            isinstance(item[3], (PrerequisiteError, MissingOptionalDependency))
            for item in failures
        ):
            return EXIT_PREREQUISITE
        return EXIT_FAILURE
    return EXIT_OK


def _generate_one(
    benchmark: str,
    kind: str,
    scenario: str,
    output_root: Path,
    *,
    seed: int,
    sample_size: int,
    force: bool,
    offline: bool,
    prepared: PreparedData | PreparedQueries | None = None,
) -> GenerationAction:
    definition = get_benchmark(benchmark)
    spec = load_canonical_spec(
        output_root,
        kind=kind,
        benchmark=benchmark,
        scenario=scenario,
    )
    _validate_canonical_runtime(spec, seed=seed, sample_size=sample_size)
    figure_path = ensure_managed_path(
        output_root, "figures", kind, benchmark, f"{scenario}.png"
    )
    manifest_path = ensure_managed_path(
        output_root, "manifests", kind, benchmark, f"{scenario}.json"
    )

    if prepared is None:
        prepared = _prepare_one(
            benchmark,
            kind,
            output_root,
            seed=seed,
            force=force,
            offline=offline,
        )
    if prepared.benchmark.name != benchmark:
        raise ConfigurationError("prepared adapter result does not match target benchmark")

    analysis = analysis_metadata()
    render = renderer_metadata()
    config_hash = configuration_hash_for_spec(
        spec,
        definition=definition,
        analysis=analysis,
        render=render,
    )
    fingerprint = cache_fingerprint(
        driftbench_version=DRIFTBENCH_VERSION,
        benchmark=benchmark,
        kind=kind,
        scenario=scenario,
        seed=seed,
        sample_size=sample_size,
        config_sha256=config_hash,
        spec_descriptor=spec.descriptor,
        analysis=analysis,
        render=render,
        inputs=prepared.input_files,
    )
    if not force and is_cache_hit(
        manifest_path,
        output_root,
        fingerprint=fingerprint,
    ):
        return "Reused"
    if not force and _migrate_legacy_manifest(
        manifest_path,
        output_root,
        fingerprint=fingerprint,
        config_hash=config_hash,
        analysis=analysis,
        render=render,
        inputs=prepared.input_files,
    ):
        return "Migrated"

    policy = spec.payload["effect_policy"]
    if kind == "data":
        if not isinstance(prepared, PreparedData):
            raise ConfigurationError("data generation requires PreparedData")
        execution = execute_data_drift(prepared, spec, output_root)
        comparison = spec.payload["metadata"]["comparison"]
        statistics = summarize_data_distribution(
            execution.baseline,
            execution.drifted,
            column=str(comparison["column"]),
            sample_size=sample_size,
            seed=seed,
        )
        effect = evaluate_effect(policy, statistics, integrity=execution.integrity)
        plot_data_comparison(
            statistics,
            benchmark=benchmark,
            benchmark_title=definition.title,
            scenario=scenario,
            seed=seed,
            output_path=figure_path,
            drift_type=execution.algorithm.rsplit(":", 1)[-1],
            effect_label=effect_label(effect),
        )
        capabilities: Mapping[str, Any] = {
            "distribution_comparison": "supported",
            "row_count": "supported",
            "integrity": execution.integrity,
        }
        execution_sha = execution.execution_sha256
        algorithm = execution.algorithm
        resolved_parameters = drift_parameters(spec)
        execution_output_sha = execution.output_sha256
    else:
        if not isinstance(prepared, PreparedQueries):
            raise ConfigurationError("query generation requires PreparedQueries")
        execution = execute_query_drift(prepared, spec, output_root)
        statistics = summarize_query_distribution(
            execution.result,
            capabilities=definition.query_capabilities,
        )
        effect = evaluate_effect(policy, statistics)
        plot_query_comparison(
            statistics,
            benchmark=benchmark,
            benchmark_title=definition.title,
            scenario=scenario,
            seed=seed,
            output_path=figure_path,
            drift_type="template_mix",
            effect_label=effect_label(effect),
        )
        capabilities = dict(definition.query_capabilities)
        execution_sha = execution.execution_sha256
        algorithm = execution.algorithm
        resolved_parameters = {
            "baseline_weights": dict(execution.result.baseline_weights),
            "target_weights": dict(execution.result.target_weights),
            "sample_size": execution.result.sample_size,
        }
        execution_output_sha = execution.output_sha256

    figure = file_descriptor(figure_path, output_root)
    resolved_spec_semantic = resolved_spec_hash(
        spec_semantic_sha256=spec.semantic_sha256,
        seed=seed,
        sample_size=sample_size,
        inputs=prepared.input_files,
        resolved_parameters=resolved_parameters,
    )
    reproduce = (
        f"python -m visualization.cli generate --benchmark {benchmark} "
        f"--kind {kind} --scenario {scenario} --seed {seed} "
        f"--sample-size {sample_size}"
        + (" --offline" if offline else "")
    )
    manifest = {
        "schema_version": VISUALIZATION_SCHEMA_VERSION,
        "benchmark": benchmark,
        "kind": kind,
        "scenario": scenario,
        "rationale": spec.rationale,
        "adapter": definition.adapter,
        "driftbench_version": DRIFTBENCH_VERSION,
        "seed": seed,
        "scale": dict(definition.scale),
        "sample_size": sample_size,
        "drift_parameters": resolved_parameters,
        "analysis": analysis,
        "statistics": statistics,
        "comparison_metrics": statistics["comparison_metrics"],
        "effect": effect,
        "capabilities": capabilities,
        "render": render,
        "figure": figure,
        "drift_spec": {
            **dict(spec.descriptor),
            "semantic_sha256": spec.semantic_sha256,
            "resolved_semantic_sha256": resolved_spec_semantic,
            "type": {
                "family": spec.type_triple[0],
                "category": spec.type_triple[1],
                "subtype": spec.type_triple[2],
            },
        },
        "execution": {
            "status": "supported",
            "engine": SPEC_EXECUTOR,
            "engine_version": SPEC_EXECUTOR_VERSION,
            "algorithm": algorithm,
            "semantic_sha256": execution_sha,
            "output_sha256": execution_output_sha,
        },
        "input_files": list(prepared.input_files),
        "config_sha256": config_hash,
        "generated_at": utc_timestamp(),
        "reproduce": reproduce,
        "limitations": definition.limitations,
        "cache": {"schema": CACHE_SCHEMA, "fingerprint": fingerprint},
    }
    manifest["semantic_sha256"] = manifest_semantic_hash(manifest)
    write_manifest(manifest_path, manifest, output_root)
    return "Generated"


def _migrate_legacy_manifest(
    manifest_path: Path,
    output_root: Path,
    *,
    fingerprint: str,
    config_hash: str,
    analysis: Mapping[str, Any],
    render: Mapping[str, Any],
    inputs: Sequence[Mapping[str, Any]],
) -> bool:
    """Upgrade a content-verified v3 manifest without touching its Spec or PNG."""

    if not manifest_path.is_file():
        return False
    try:
        legacy = load_json(manifest_path)
        cache = legacy.get("cache")
        if (
            isinstance(legacy.get("schema_version"), bool)
            or legacy.get("schema_version") != 3
            or not isinstance(cache, Mapping)
            or cache.get("schema") != "driftbench.visualization-cache/v3"
            or semantic_hash(legacy.get("analysis")) != semantic_hash(analysis)
            or semantic_hash(legacy.get("render")) != semantic_hash(render)
            or legacy.get("input_files") != list(inputs)
            or legacy.get("semantic_sha256") != manifest_semantic_hash(legacy)
        ):
            return False
        migrated = dict(legacy)
        migrated["schema_version"] = VISUALIZATION_SCHEMA_VERSION
        migrated["config_sha256"] = config_hash
        migrated["cache"] = {
            "schema": CACHE_SCHEMA,
            "fingerprint": fingerprint,
        }
        migrated["semantic_sha256"] = manifest_semantic_hash(migrated)
        write_manifest(manifest_path, migrated, output_root)
        return True
    except (KeyError, TypeError, ValueError):
        return False


def _prepare_one(
    benchmark: str,
    kind: str,
    output_root: Path,
    *,
    seed: int,
    force: bool,
    offline: bool,
) -> PreparedData | PreparedQueries:
    if kind == "data":
        return prepare_data(
            benchmark,
            output_root,
            seed=seed,
            force=force,
            offline=offline,
        )
    return prepare_queries(
        benchmark,
        output_root,
        seed=seed,
        force=force,
        offline=offline,
    )


def _planned_targets(
    benchmarks: tuple[str, ...],
    kinds: tuple[str, ...],
    requested: str | None,
) -> tuple[tuple[str, str, str], ...]:
    value = "all" if requested in {None, "all"} else str(requested)
    targets: list[tuple[str, str, str]] = []
    incompatible: list[str] = []
    for benchmark in benchmarks:
        for kind in kinds:
            ids = tuple(scenario for scenario, _ in scenario_entries(kind, benchmark))
            selected = ids if value == "all" else tuple(
                scenario for scenario in ids if scenario == value
            )
            if not selected:
                incompatible.append(f"{benchmark}/{kind}")
            targets.extend((benchmark, kind, scenario) for scenario in selected)
    if incompatible:
        raise ConfigurationError(
            f"--scenario {value!r} is not valid for: {', '.join(incompatible)}"
        )
    return tuple(targets)


def _validate_canonical_runtime(
    spec: CanonicalSpec, *, seed: int, sample_size: int
) -> None:
    if spec.payload["seed"] != seed:
        raise ConfigurationError(
            f"canonical DriftSpec {spec.scenario} requires --seed {spec.payload['seed']}"
        )
    if sample_size != 1000:
        raise ConfigurationError("canonical Gallery generation requires --sample-size 1000")
    if spec.kind == "query" and spec.payload["variables"]["sample_size"] != sample_size:
        raise ConfigurationError("query DriftSpec sample_size disagrees with CLI")


def _preflight_all(
    benchmarks: tuple[str, ...],
    kinds: tuple[str, ...],
    output_root: Path,
    *,
    offline: bool,
    force: bool,
    plotting: bool,
) -> None:
    if plotting:
        require_matplotlib()
    if "data" in kinds:
        for benchmark in benchmarks:
            preflight_data(benchmark, output_root, offline=offline, force=force)


def _validate_seed(seed: int) -> None:
    if isinstance(seed, bool) or not isinstance(seed, int):
        raise ConfigurationError("--seed must be an integer")


def _selected_benchmarks(value: str) -> tuple[str, ...]:
    return BENCHMARK_ORDER if value == "all" else (value,)


def _selected_kinds(value: str) -> tuple[str, ...]:
    return ("data", "query") if value == "all" else (value,)


def _add_benchmark_argument(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--benchmark", required=True, choices=[*BENCHMARK_ORDER, "all"]
    )


def _add_kind_argument(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--kind", choices=["data", "query", "all"], default="all")


def _add_output_argument(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--output-dir",
        default=str(Path.cwd() / "visualization"),
        help="managed output root (default: ./visualization)",
    )


if __name__ == "__main__":
    raise SystemExit(main())
