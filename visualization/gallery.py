"""Strict manifests and deterministic 40-scenario Markdown Gallery generation."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Mapping

from driftbench.query_drift import QUERY_MIX_ALGORITHM

from . import VISUALIZATION_SCHEMA_VERSION
from .artifacts import (
    atomic_write_json,
    atomic_write_text,
    is_sha256,
    load_json,
    reject_machine_paths,
    semantic_hash,
    sha256_file,
    validate_relative_posix,
)
from .benchmarks import (
    BENCHMARK_ORDER,
    get_benchmark,
    get_scenario_entry,
    registry,
    scenario_entries,
)
from .distributions import ANALYSIS_SCHEMA
from .drift_scenarios import SPEC_EXECUTOR, SPEC_EXECUTOR_VERSION
from .effects import EffectAssertionError, evaluate_effect
from .plots import RENDERER_SCHEMA
from .provenance import (
    CACHE_SCHEMA,
    cache_fingerprint,
    configuration_hash,
    manifest_semantic_hash,
    resolved_spec_hash,
)
from .specs import drift_parameters, expected_artifact_keys, load_canonical_spec


_UTC_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")
class ManifestError(ValueError):
    pass


def write_manifest(path: Path, payload: Mapping[str, Any], output_root: Path) -> None:
    validate_manifest(dict(payload), output_root, verify_figure=True)
    atomic_write_json(path, payload)


def validate_manifest(
    payload: Mapping[str, Any], output_root: Path, *, verify_figure: bool
) -> None:
    required = {
        "schema_version",
        "benchmark",
        "kind",
        "scenario",
        "rationale",
        "adapter",
        "driftbench_version",
        "seed",
        "scale",
        "sample_size",
        "drift_parameters",
        "analysis",
        "statistics",
        "comparison_metrics",
        "effect",
        "capabilities",
        "render",
        "figure",
        "drift_spec",
        "execution",
        "input_files",
        "config_sha256",
        "semantic_sha256",
        "generated_at",
        "reproduce",
        "limitations",
        "cache",
    }
    if set(payload) != required:
        missing = sorted(required - set(payload))
        extra = sorted(set(payload) - required)
        raise ManifestError(f"manifest fields mismatch; missing={missing}, extra={extra}")
    if payload["schema_version"] != VISUALIZATION_SCHEMA_VERSION:
        raise ManifestError("unsupported visualization manifest schema")
    benchmark = str(payload["benchmark"])
    kind = str(payload["kind"])
    scenario = str(payload["scenario"])
    if benchmark not in BENCHMARK_ORDER or kind not in {"data", "query"}:
        raise ManifestError("manifest benchmark/kind is not registered")
    valid_scenarios = {value for value, _ in scenario_entries(kind, benchmark)}
    if scenario not in valid_scenarios:
        raise ManifestError("manifest scenario is not registered")
    entry = get_scenario_entry(kind, benchmark, scenario)
    if payload["seed"] != 42 or payload["sample_size"] != 1000:
        raise ManifestError("canonical manifest requires seed 42 and sample_size 1000")
    if not isinstance(payload["generated_at"], str) or not _UTC_RE.fullmatch(
        payload["generated_at"]
    ):
        raise ManifestError("generated_at must be UTC with a Z suffix")
    for field in ("config_sha256", "semantic_sha256"):
        if not is_sha256(payload[field]):
            raise ManifestError(f"{field} must be a lowercase SHA-256")

    analysis = payload["analysis"]
    if not isinstance(analysis, Mapping) or analysis.get("schema") != ANALYSIS_SCHEMA:
        raise ManifestError("analysis metadata has an unsupported schema")
    analysis_config = analysis.get("config")
    if not isinstance(analysis_config, Mapping) or semantic_hash(
        analysis_config
    ) != analysis.get("config_sha256"):
        raise ManifestError("analysis config/hash is invalid")
    if analysis.get("basis") != "observed_sample":
        raise ManifestError("analysis basis must be observed_sample")

    statistics = payload["statistics"]
    comparison_metrics = payload["comparison_metrics"]
    if (
        not isinstance(statistics, Mapping)
        or not isinstance(comparison_metrics, Mapping)
        or statistics.get("comparison_metrics") != comparison_metrics
    ):
        raise ManifestError("comparison_metrics must match statistics")

    render = payload["render"]
    if not isinstance(render, Mapping) or render.get("schema") != RENDERER_SCHEMA:
        raise ManifestError("render metadata has an unsupported schema")
    if render.get("pixel_size") != [1600, 1000] or render.get("dpi") != 100:
        raise ManifestError("render dimensions must be 1600x1000 at 100 DPI")
    plot_config = render.get("plot_config")
    if not isinstance(plot_config, Mapping) or semantic_hash(
        plot_config
    ) != render.get("plot_config_sha256"):
        raise ManifestError("render plot config/hash is invalid")

    _validate_file_descriptor(
        payload["figure"], output_root, expected_suffix=".png", verify=verify_figure
    )
    expected_figure = f"figures/{kind}/{benchmark}/{scenario}.png"
    if payload["figure"]["path"] != expected_figure:
        raise ManifestError("figure path does not match manifest identity")

    drift_spec = payload["drift_spec"]
    if not isinstance(drift_spec, Mapping):
        raise ManifestError("drift_spec must be an object")
    descriptor = {key: drift_spec.get(key) for key in ("path", "bytes", "sha256")}
    _validate_file_descriptor(
        descriptor, output_root, expected_suffix=".yaml", verify=True
    )
    expected_spec = f"specs/{kind}/{benchmark}/{scenario}.yaml"
    if drift_spec.get("path") != expected_spec:
        raise ManifestError("DriftSpec path does not match manifest identity")
    for field in ("semantic_sha256", "resolved_semantic_sha256"):
        if not is_sha256(drift_spec.get(field)):
            raise ManifestError(f"DriftSpec {field} is invalid")
    canonical_spec = load_canonical_spec(
        output_root,
        kind=kind,
        benchmark=benchmark,
        scenario=scenario,
    )
    if canonical_spec.descriptor != descriptor:
        raise ManifestError("DriftSpec file descriptor does not match source")
    if canonical_spec.semantic_sha256 != drift_spec["semantic_sha256"]:
        raise ManifestError("DriftSpec semantic hash does not match")
    expected_type = dict(
        zip(("family", "category", "subtype"), canonical_spec.type_triple)
    )
    if drift_spec.get("type") != expected_type:
        raise ManifestError("DriftSpec type triple does not match source")

    execution = payload["execution"]
    if not isinstance(execution, Mapping) or execution.get("status") != "supported":
        raise ManifestError("execution must be supported")
    if execution.get("engine") != SPEC_EXECUTOR or execution.get(
        "engine_version"
    ) != SPEC_EXECUTOR_VERSION:
        raise ManifestError("execution engine identity is invalid")
    for field in ("semantic_sha256", "output_sha256"):
        if not is_sha256(execution.get(field)):
            raise ManifestError(f"execution {field} is invalid")
    expected_algorithm = (
        QUERY_MIX_ALGORITHM
        if kind == "query"
        else f"driftbench.data-drift-spec/v1:{entry['operation']}"
    )
    if execution.get("algorithm") != expected_algorithm:
        raise ManifestError("execution algorithm does not match canonical operation")

    input_files = payload["input_files"]
    if not isinstance(input_files, list) or not input_files:
        raise ManifestError("input_files must be a non-empty list")
    for item in input_files:
        _validate_descriptor_shape(item)

    definition = get_benchmark(benchmark)
    if payload["rationale"] != entry["rationale"]:
        raise ManifestError("manifest rationale does not match scenario registry")
    if payload["adapter"] != definition.adapter:
        raise ManifestError("manifest adapter does not match benchmark registry")
    if payload["scale"] != dict(definition.scale):
        raise ManifestError("manifest scale does not match benchmark registry")
    if payload["limitations"] != definition.limitations:
        raise ManifestError("manifest limitations do not match benchmark registry")

    capabilities = payload["capabilities"]
    if not isinstance(capabilities, Mapping):
        raise ManifestError("manifest capabilities must be an object")
    if kind == "query" and capabilities != definition.query_capabilities:
        raise ManifestError("query capabilities do not match benchmark registry")
    expected_parameters = drift_parameters(canonical_spec)
    if payload["drift_parameters"] != expected_parameters:
        raise ManifestError(
            f"{kind} drift parameters do not match canonical DriftSpec"
        )
    if kind == "data":
        if capabilities.get("distribution_comparison") != "supported" or capabilities.get(
            "row_count"
        ) != "supported":
            raise ManifestError("data capabilities are incomplete")
        if not isinstance(capabilities.get("integrity"), Mapping):
            raise ManifestError("data integrity evidence is missing")

    effect = payload["effect"]
    _validate_effect(effect)
    try:
        expected_effect = evaluate_effect(
            canonical_spec.payload["effect_policy"],
            statistics,
            integrity=(capabilities.get("integrity") if kind == "data" else None),
        )
    except EffectAssertionError as exc:
        raise ManifestError(f"manifest effect evidence fails canonical policy: {exc}") from exc
    if effect != expected_effect:
        raise ManifestError("manifest effect does not exactly match canonical policy/evidence")

    expected_config_hash = configuration_hash(
        definition=definition,
        kind=kind,
        scenario=scenario,
        spec_descriptor=descriptor,
        spec_semantic_sha256=drift_spec["semantic_sha256"],
        effect_policy=canonical_spec.payload["effect_policy"],
        analysis=analysis,
        render=render,
    )
    if payload["config_sha256"] != expected_config_hash:
        raise ManifestError("config_sha256 does not match manifest inputs")
    expected_resolved = resolved_spec_hash(
        spec_semantic_sha256=drift_spec["semantic_sha256"],
        seed=payload["seed"],
        sample_size=payload["sample_size"],
        inputs=input_files,
        resolved_parameters=payload["drift_parameters"],
    )
    if drift_spec["resolved_semantic_sha256"] != expected_resolved:
        raise ManifestError("resolved DriftSpec semantic hash does not match")
    cache = payload["cache"]
    if not isinstance(cache, Mapping) or cache.get("schema") != CACHE_SCHEMA:
        raise ManifestError("cache schema is invalid")
    expected_fingerprint = cache_fingerprint(
        driftbench_version=payload["driftbench_version"],
        benchmark=benchmark,
        kind=kind,
        scenario=scenario,
        seed=payload["seed"],
        sample_size=payload["sample_size"],
        config_sha256=payload["config_sha256"],
        spec_descriptor=descriptor,
        analysis=analysis,
        render=render,
        inputs=input_files,
    )
    if cache.get("fingerprint") != expected_fingerprint:
        raise ManifestError("cache fingerprint does not match inputs/spec/config")
    if f"--scenario {scenario}" not in str(payload["reproduce"]):
        raise ManifestError("reproduce command must select the exact scenario")
    expected_semantic = manifest_semantic_hash(payload)
    if payload["semantic_sha256"] != expected_semantic:
        raise ManifestError("semantic_sha256 does not match manifest semantics")
    reject_machine_paths(payload)


def is_cache_hit(
    manifest_path: Path,
    output_root: Path,
    *,
    fingerprint: str,
) -> bool:
    if not manifest_path.is_file():
        return False
    try:
        payload = load_json(manifest_path)
        validate_manifest(payload, output_root, verify_figure=True)
        return payload["cache"]["fingerprint"] == fingerprint
    except Exception:
        return False


def build_gallery(output_root: Path) -> Path:
    keys = expected_artifact_keys()
    expected = set(keys)
    actual_specs = _artifact_key_set(output_root / "specs", ".yaml")
    actual_figures = _artifact_key_set(output_root / "figures", ".png")
    actual_manifests = _artifact_key_set(output_root / "manifests", ".json")
    for label, actual in (
        ("DriftSpec", actual_specs),
        ("figure", actual_figures),
        ("manifest", actual_manifests),
    ):
        if actual != expected:
            missing = sorted(expected - actual)
            extra = sorted(actual - expected)
            raise ManifestError(f"{label} set mismatch; missing={missing}, extra={extra}")

    manifests: dict[tuple[str, str, str], dict[str, Any]] = {}
    for kind, benchmark, scenario in keys:
        path = output_root / "manifests" / kind / benchmark / f"{scenario}.json"
        payload = load_json(path)
        validate_manifest(payload, output_root, verify_figure=True)
        manifests[(kind, benchmark, scenario)] = payload

    lines = [
        "# DriftBench Visualization Gallery",
        "",
        "Canonical seed `42`, observed sample size `1,000`. Every entry below is a "
        "validated one-to-one PNG ↔ manifest ↔ executable DriftSpec result; no target "
        "database performance test is run.",
        "",
        "## Canonical verdict and trace matrix",
        "",
        "| Benchmark | Data scenarios | Query scenarios | Verdict |",
        "|---|---|---|---|",
    ]
    for benchmark in BENCHMARK_ORDER:
        data_ids = [value for value, _ in scenario_entries("data", benchmark)]
        query_ids = [value for value, _ in scenario_entries("query", benchmark)]
        lines.append(
            f"| `{benchmark}` | {', '.join(f'`{value}`' for value in data_ids)} | "
            f"{', '.join(f'`{value}`' for value in query_ids)} | 5/5 PASS |"
        )
    lines.extend(
        [
            "",
            "## Reading the diagnostics",
            "",
            "- Numeric dashboards combine shared-bin distributions, ECDF or log-tail CCDF, "
            "quantile shifts, row scale, KS-D, W₁, and a visible effect verdict.",
            "- Categorical/query dashboards use full-support JSD/TVD, ranked movers, "
            "concentration, entropy/effective count, and shared Baseline/Drifted scales.",
            "- Predicate selectivity and temporal arrival metrics remain Unsupported; SQL "
            "complexity is lexical only when public adapter SQL exists.",
            "",
        ]
    )

    definitions = registry()
    for benchmark in BENCHMARK_ORDER:
        definition = definitions[benchmark]
        lines.extend([f"## {definition.title} (`{benchmark}`)", "", definition.description, ""])
        for kind, heading in (("data", "Data Drift"), ("query", "Query Drift")):
            lines.extend([f"### {heading}", ""])
            for scenario, _ in scenario_entries(kind, benchmark):
                manifest = manifests[(kind, benchmark, scenario)]
                manifest_path = f"manifests/{kind}/{benchmark}/{scenario}.json"
                lines.extend(
                    [
                        f"#### `{scenario}` — PASS",
                        "",
                        str(manifest["rationale"]),
                        "",
                        f"![{definition.title} {kind} drift {scenario}]({manifest['figure']['path']})",
                        "",
                        f"- DriftSpec: [`{manifest['drift_spec']['path']}`]({manifest['drift_spec']['path']})",
                        f"- Manifest: [`{manifest_path}`]({manifest_path})",
                        f"- Configuration: `{_compact_json(manifest['drift_parameters'])}`",
                        f"- Effect: {_effect_summary(manifest['effect'])}",
                        f"- Seed/sample: `{manifest['seed']}` / `{manifest['sample_size']}`",
                        f"- Reproduce: `{manifest['reproduce']}`",
                        "",
                    ]
                )
        lines.extend([f"**Current limitations:** {definition.limitations}", ""])

    gallery_path = output_root / "GALLERY.md"
    atomic_write_text(gallery_path, "\n".join(lines).rstrip() + "\n")
    return gallery_path


def gallery_semantic_hash(output_root: Path) -> str:
    return semantic_hash((output_root / "GALLERY.md").read_text(encoding="utf-8"))


def _validate_file_descriptor(
    value: Any,
    output_root: Path,
    *,
    expected_suffix: str,
    verify: bool,
) -> None:
    _validate_descriptor_shape(value)
    path_value = str(value["path"])
    if not path_value.endswith(expected_suffix):
        raise ManifestError(f"artifact must end in {expected_suffix}")
    if not verify:
        return
    path = (output_root / path_value).resolve()
    try:
        path.relative_to(output_root.resolve())
    except ValueError as exc:
        raise ManifestError("artifact escapes output root") from exc
    if not path.is_file() or path.stat().st_size != value["bytes"]:
        raise ManifestError("artifact is missing or byte size differs")
    if sha256_file(path) != value["sha256"]:
        raise ManifestError("artifact SHA-256 differs")


def _validate_descriptor_shape(value: Any) -> None:
    if not isinstance(value, Mapping) or set(value) != {"path", "bytes", "sha256"}:
        raise ManifestError("artifact descriptor requires path, bytes, and sha256")
    validate_relative_posix(str(value["path"]))
    if not is_sha256(value["sha256"]):
        raise ManifestError("artifact descriptor SHA-256 is invalid")
    if isinstance(value["bytes"], bool) or not isinstance(value["bytes"], int) or value[
        "bytes"
    ] <= 0:
        raise ManifestError("artifact descriptor bytes must be positive")


def _validate_effect(value: Any) -> None:
    if not isinstance(value, Mapping):
        raise ManifestError("effect must be an object")
    if value.get("status") != "supported" or value.get("passed") is not True:
        raise ManifestError("canonical effect must be supported and pass")
    if value.get("verdict") != "PASS" or value.get("mode") not in {"all", "any"}:
        raise ManifestError("effect verdict/mode is invalid")
    assertions = value.get("assertions")
    if not isinstance(assertions, list) or not assertions:
        raise ManifestError("effect assertions are missing")
    passes = []
    for assertion in assertions:
        if not isinstance(assertion, Mapping) or set(assertion) != {
            "metric",
            "operator",
            "threshold",
            "observed",
            "passed",
        }:
            raise ManifestError("effect assertion shape is invalid")
        passes.append(assertion["passed"] is True)
    expected = all(passes) if value["mode"] == "all" else any(passes)
    if not expected:
        raise ManifestError("effect assertion verdict is inconsistent")


def _artifact_key_set(root: Path, suffix: str) -> set[tuple[str, str, str]]:
    if not root.is_dir():
        return set()
    result: set[tuple[str, str, str]] = set()
    for path in root.rglob(f"*{suffix}"):
        relative = path.relative_to(root)
        if len(relative.parts) != 3:
            raise ManifestError(f"unexpected artifact path: {relative.as_posix()}")
        kind, benchmark, name = relative.parts
        result.add((kind, benchmark, Path(name).stem))
    return result


def _compact_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _effect_summary(effect: Mapping[str, Any]) -> str:
    evidence = []
    for item in effect["assertions"]:
        marker = "✓" if item["passed"] else "alternative"
        evidence.append(
            f"{item['metric']} `{_metric(item['observed'])}` {item['operator']} "
            f"`{_metric(item['threshold'])}` ({marker})"
        )
    return f"**{effect['verdict']}** — " + "; ".join(evidence)


def _metric(value: Any) -> str:
    if value is None:
        return "n/a"
    number = float(value)
    return str(int(number)) if number.is_integer() else f"{number:.4f}"


__all__ = [
    "ManifestError",
    "build_gallery",
    "gallery_semantic_hash",
    "is_cache_hit",
    "validate_manifest",
    "write_manifest",
]
