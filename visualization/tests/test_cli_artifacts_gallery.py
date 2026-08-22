from __future__ import annotations

import contextlib
import io
import json
import os
import re
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from driftbench.api import QueryTemplate
from driftbench.data import GenerationResult
from driftbench.query_drift import QUERY_MIX_ALGORITHM
from visualization import VISUALIZATION_SCHEMA_VERSION
from visualization.artifacts import file_descriptor, semantic_hash
from visualization.benchmarks import (
    PreparedQueries,
    PrerequisiteError,
    get_benchmark,
    get_scenario_entry,
    prepare_data,
)
from visualization.drift_scenarios import SPEC_EXECUTOR, SPEC_EXECUTOR_VERSION
from visualization.distributions import analysis_metadata
from visualization.effects import evaluate_effect
from visualization.cli import (
    EXIT_FAILURE,
    EXIT_OK,
    EXIT_PREREQUISITE,
    EXIT_USAGE,
    _generate_one,
    main,
)
from visualization.gallery import (
    build_gallery,
    is_cache_hit,
    validate_manifest,
    write_manifest,
)
from visualization.plots import renderer_metadata
from visualization.provenance import (
    CACHE_SCHEMA,
    cache_fingerprint,
    configuration_hash_for_spec,
    manifest_semantic_hash,
    resolved_spec_hash,
)
from visualization.specs import (
    drift_parameters,
    expected_artifact_keys,
    load_canonical_spec,
)


REPO_ROOT = Path(__file__).resolve().parents[2]


class CliExitAndFlagTests(unittest.TestCase):
    def _main(self, argv: list[str]) -> tuple[int, str, str]:
        stdout = io.StringIO()
        stderr = io.StringIO()
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            code = main(argv)
        return code, stdout.getvalue(), stderr.getvalue()

    def test_invalid_configuration_returns_usage_exit(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            with mock.patch("visualization.cli.require_matplotlib") as require:
                code, _, stderr = self._main(
                    [
                        "generate",
                        "--benchmark",
                        "tpch",
                        "--kind",
                        "query",
                        "--sample-size",
                        "0",
                        "--output-dir",
                        temporary,
                    ]
                )
        self.assertEqual(code, EXIT_USAGE)
        self.assertIn("--sample-size must be positive", stderr)
        require.assert_not_called()

    def test_missing_offline_tpch_source_returns_prerequisite_exit(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            with mock.patch.dict(os.environ, {}, clear=False):
                os.environ.pop("DRIFTBENCH_TPCH_SOURCE_DIR", None)
                with mock.patch("visualization.cli.require_matplotlib"):
                    code, _, stderr = self._main(
                        [
                            "generate",
                            "--benchmark",
                            "tpch",
                            "--kind",
                            "data",
                            "--offline",
                            "--output-dir",
                            temporary,
                        ]
                    )
        self.assertEqual(code, EXIT_PREREQUISITE)
        self.assertIn("prerequisite error", stderr)
        self.assertIn("DRIFTBENCH_TPCH_SOURCE_DIR", stderr)

    def test_runtime_error_returns_failure_exit(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            with (
                mock.patch("visualization.cli.require_matplotlib"),
                mock.patch(
                    "visualization.cli._generate_one",
                    side_effect=RuntimeError("deliberate execution failure"),
                ),
            ):
                code, _, stderr = self._main(
                    [
                        "generate",
                        "--benchmark",
                        "tpcds",
                        "--kind",
                        "query",
                        "--output-dir",
                        temporary,
                    ]
                )
        self.assertEqual(code, EXIT_FAILURE)
        self.assertIn("FAILED tpcds/query", stderr)

    def test_prepare_forwards_force_and_offline(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            with (
                mock.patch("visualization.cli.preflight_data") as preflight,
                mock.patch("visualization.cli.prepare_data") as prepare,
            ):
                code, stdout, stderr = self._main(
                    [
                        "prepare",
                        "--benchmark",
                        "tpcc",
                        "--kind",
                        "data",
                        "--seed",
                        "17",
                        "--force",
                        "--offline",
                        "--output-dir",
                        temporary,
                    ]
                )

        self.assertEqual(code, EXIT_OK, stderr)
        self.assertIn("Prepared tpcc/data", stdout)
        preflight.assert_called_once_with(
            "tpcc", Path(temporary), offline=True, force=True
        )
        prepare.assert_called_once()
        self.assertEqual(prepare.call_args.args[0], "tpcc")
        self.assertEqual(prepare.call_args.kwargs["seed"], 17)
        self.assertIs(prepare.call_args.kwargs["force"], True)
        self.assertIs(prepare.call_args.kwargs["offline"], True)

    def test_all_mode_collects_every_target_and_fails_on_partial_result(self) -> None:
        calls: list[tuple[str, str, str]] = []

        def generate_one(
            benchmark: str, kind: str, scenario: str, *args, **kwargs
        ) -> str:
            del args, kwargs
            calls.append((benchmark, kind, scenario))
            if (benchmark, kind, scenario) == (
                "tpch",
                "data",
                "price_outliers",
            ):
                raise RuntimeError("one target failed")
            return "Generated"

        with tempfile.TemporaryDirectory() as temporary:
            with (
                mock.patch("visualization.cli.require_matplotlib"),
                mock.patch("visualization.cli.preflight_data"),
                mock.patch("visualization.cli._prepare_one", return_value=mock.Mock()),
                mock.patch("visualization.cli._generate_one", side_effect=generate_one),
            ):
                code, _, stderr = self._main(
                    [
                        "generate",
                        "--benchmark",
                        "all",
                        "--kind",
                        "all",
                        "--offline",
                        "--output-dir",
                        temporary,
                    ]
                )

        self.assertEqual(code, EXIT_FAILURE)
        expected_calls = tuple(
            (benchmark, kind, scenario)
            for kind, benchmark, scenario in expected_artifact_keys()
        )
        self.assertEqual(tuple(calls), expected_calls)
        self.assertEqual(len(calls), 40)
        self.assertEqual(len(set(calls)), 40)
        self.assertIn("FAILED tpch/data/price_outliers", stderr)

    def test_all_mode_reports_migration_generation_and_reuse_counts(self) -> None:
        first_actions = ["Migrated"] * 32 + ["Generated"] * 8
        with tempfile.TemporaryDirectory() as temporary:
            with (
                mock.patch("visualization.cli.require_matplotlib"),
                mock.patch("visualization.cli.preflight_data"),
                mock.patch("visualization.cli._prepare_one", return_value=mock.Mock()),
                mock.patch(
                    "visualization.cli._generate_one", side_effect=first_actions
                ) as generate,
            ):
                arguments = [
                    "generate",
                    "--benchmark",
                    "all",
                    "--kind",
                    "all",
                    "--offline",
                    "--output-dir",
                    temporary,
                ]
                first_code, first_stdout, first_stderr = self._main(arguments)
                generate.side_effect = None
                generate.return_value = "Reused"
                second_code, second_stdout, second_stderr = self._main(arguments)

        self.assertEqual(first_code, EXIT_OK, first_stderr)
        self.assertEqual(second_code, EXIT_OK, second_stderr)
        first_lines = first_stdout.splitlines()
        second_lines = second_stdout.splitlines()
        self.assertEqual(sum(line.startswith("Migrated ") for line in first_lines), 32)
        self.assertEqual(sum(line.startswith("Generated ") for line in first_lines), 8)
        self.assertEqual(sum(line.startswith("Reused ") for line in first_lines), 0)
        self.assertEqual(sum(line.startswith("Reused ") for line in second_lines), 40)


class CliCacheTests(unittest.TestCase):
    def test_valid_cache_skips_drift_and_plot_while_force_regenerates(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            input_path = root / "cache" / "fixture" / "queries.sql"
            input_path.parent.mkdir(parents=True)
            input_path.write_text("SELECT 1;\n", encoding="utf-8")
            templates = tuple(
                QueryTemplate(f"q{index}", f"SELECT {index};")
                for index in range(1, 23)
            )
            prepared = PreparedQueries(
                benchmark=get_benchmark("tpch"),
                templates=templates,
                input_files=(file_descriptor(input_path, root),),
                profile_weights={},
            )

            def fake_plot(*args, **kwargs) -> None:
                del args
                output_path = Path(kwargs["output_path"])
                output_path.parent.mkdir(parents=True, exist_ok=True)
                output_path.write_bytes(b"\x89PNG\r\n\x1a\ncache-contract")

            with mock.patch(
                "visualization.cli.plot_query_comparison", side_effect=fake_plot
            ) as plot:
                generated = _generate_one(
                    "tpch",
                    "query",
                    "hotset_concentration",
                    root,
                    seed=42,
                    sample_size=1000,
                    force=False,
                    offline=True,
                    prepared=prepared,
                )
                reused = _generate_one(
                    "tpch",
                    "query",
                    "hotset_concentration",
                    root,
                    seed=42,
                    sample_size=1000,
                    force=False,
                    offline=True,
                    prepared=prepared,
                )
                forced = _generate_one(
                    "tpch",
                    "query",
                    "hotset_concentration",
                    root,
                    seed=42,
                    sample_size=1000,
                    force=True,
                    offline=True,
                    prepared=prepared,
                )

            self.assertEqual(generated, "Generated")
            self.assertEqual(reused, "Reused")
            self.assertEqual(forced, "Generated")
            self.assertEqual(plot.call_count, 2)

            manifest_path = (
                root
                / "manifests"
                / "query"
                / "tpch"
                / "hotset_concentration.json"
            )
            payload = json.loads(manifest_path.read_text(encoding="utf-8"))
            self.assertTrue(
                is_cache_hit(
                    manifest_path,
                    root,
                    fingerprint=payload["cache"]["fingerprint"],
                )
            )
            figure_path = root / payload["figure"]["path"]
            figure_path.write_bytes(figure_path.read_bytes() + b"tampered")
            self.assertFalse(
                is_cache_hit(
                    manifest_path,
                    root,
                    fingerprint=payload["cache"]["fingerprint"],
                )
            )

    def test_v3_manifest_migrates_without_rewriting_spec_or_figure(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            input_path = root / "cache" / "fixture" / "queries.sql"
            input_path.parent.mkdir(parents=True)
            input_path.write_text("SELECT 1;\n", encoding="utf-8")
            prepared = PreparedQueries(
                benchmark=get_benchmark("tpch"),
                templates=tuple(
                    QueryTemplate(f"q{index}", f"SELECT {index};")
                    for index in range(1, 23)
                ),
                input_files=(file_descriptor(input_path, root),),
                profile_weights={},
            )

            def fake_plot(*args, **kwargs) -> None:
                del args
                output_path = Path(kwargs["output_path"])
                output_path.parent.mkdir(parents=True, exist_ok=True)
                output_path.write_bytes(b"\x89PNG\r\n\x1a\nlegacy-migration")

            with mock.patch(
                "visualization.cli.plot_query_comparison", side_effect=fake_plot
            ) as plot:
                generated = _generate_one(
                    "tpch",
                    "query",
                    "hotset_concentration",
                    root,
                    seed=42,
                    sample_size=1000,
                    force=False,
                    offline=True,
                    prepared=prepared,
                )
                manifest_path = (
                    root
                    / "manifests"
                    / "query"
                    / "tpch"
                    / "hotset_concentration.json"
                )
                legacy = json.loads(manifest_path.read_text(encoding="utf-8"))
                figure_path = root / legacy["figure"]["path"]
                spec_path = root / legacy["drift_spec"]["path"]
                figure_bytes = figure_path.read_bytes()
                spec_bytes = spec_path.read_bytes()
                legacy["schema_version"] = 3
                legacy["cache"] = {
                    "schema": "driftbench.visualization-cache/v3",
                    "fingerprint": "0" * 64,
                }
                legacy["semantic_sha256"] = manifest_semantic_hash(legacy)
                manifest_path.write_text(
                    json.dumps(legacy, indent=2, sort_keys=True) + "\n",
                    encoding="utf-8",
                )

                migrated = _generate_one(
                    "tpch",
                    "query",
                    "hotset_concentration",
                    root,
                    seed=42,
                    sample_size=1000,
                    force=False,
                    offline=True,
                    prepared=prepared,
                )
                reused = _generate_one(
                    "tpch",
                    "query",
                    "hotset_concentration",
                    root,
                    seed=42,
                    sample_size=1000,
                    force=False,
                    offline=True,
                    prepared=prepared,
                )

            migrated_payload = json.loads(
                manifest_path.read_text(encoding="utf-8")
            )
            self.assertEqual(generated, "Generated")
            self.assertEqual(migrated, "Migrated")
            self.assertEqual(reused, "Reused")
            self.assertEqual(plot.call_count, 1)
            self.assertEqual(figure_path.read_bytes(), figure_bytes)
            self.assertEqual(spec_path.read_bytes(), spec_bytes)
            self.assertEqual(
                migrated_payload["schema_version"], VISUALIZATION_SCHEMA_VERSION
            )
            self.assertEqual(migrated_payload["cache"]["schema"], CACHE_SCHEMA)
            self.assertEqual(
                migrated_payload["semantic_sha256"],
                manifest_semantic_hash(migrated_payload),
            )

class TPCHSourceAndForceContractTests(unittest.TestCase):
    def _cached_generation(self, root: Path) -> GenerationResult:
        adapter_root = root / "data" / "adapters"
        csv_path = adapter_root / "tpch" / "data" / "sf_0.01" / "lineitem.csv"
        csv_path.parent.mkdir(parents=True)
        csv_path.write_text(
            "l_orderkey,l_extendedprice\n1,100.0\n2,200.0\n",
            encoding="utf-8",
        )
        metadata = csv_path.with_name("tpch_data_manifest.json")
        metadata.write_text("{}\n", encoding="utf-8")
        return GenerationResult(
            benchmark="tpch",
            artifact_type="data",
            output_dir=adapter_root,
            files=[csv_path],
            metadata=metadata,
        )

    def test_env_absent_cache_is_reusable_without_force_but_force_requires_source(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            cached = self._cached_generation(root)
            with mock.patch.dict(os.environ, {}, clear=False):
                os.environ.pop("DRIFTBENCH_TPCH_SOURCE_DIR", None)
                with mock.patch(
                    "visualization.benchmarks._load_tpch_adapter_result",
                    return_value=cached,
                ):
                    prepared = prepare_data(
                        "tpch", root, seed=42, force=False, offline=True
                    )
                    self.assertEqual(prepared.generation, cached)
                    with self.assertRaises(PrerequisiteError):
                        prepare_data(
                            "tpch", root, seed=42, force=True, offline=True
                        )

    def test_no_source_and_no_cache_never_falls_back_to_dbgen(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            with mock.patch.dict(os.environ, {}, clear=False):
                os.environ.pop("DRIFTBENCH_TPCH_SOURCE_DIR", None)
                with (
                    mock.patch(
                        "visualization.benchmarks._load_tpch_adapter_result",
                        return_value=None,
                    ),
                    mock.patch(
                        "visualization.benchmarks.importlib.import_module",
                        side_effect=AssertionError(
                            "TPC-H local-source contract must not enter dbgen mode"
                        ),
                    ) as import_module,
                ):
                    for offline in (False, True):
                        with self.subTest(offline=offline):
                            with self.assertRaises(PrerequisiteError):
                                prepare_data(
                                    "tpch",
                                    root,
                                    seed=42,
                                    force=False,
                                    offline=offline,
                                )
                import_module.assert_not_called()


class ManifestAndGalleryTests(unittest.TestCase):
    def _manifest(
        self,
        root: Path,
        *,
        benchmark: str,
        kind: str,
        scenario: str,
        input_descriptor: dict[str, object],
    ) -> dict[str, object]:
        definition = get_benchmark(benchmark)
        spec = load_canonical_spec(
            root,
            kind=kind,
            benchmark=benchmark,
            scenario=scenario,
        )
        figure_path = root / "figures" / kind / benchmark / f"{scenario}.png"
        figure_path.parent.mkdir(parents=True, exist_ok=True)
        figure_path.write_bytes(
            b"\x89PNG\r\n\x1a\n" + f"{benchmark}/{kind}/{scenario}".encode("ascii")
        )
        analysis = analysis_metadata()
        render = renderer_metadata()
        integrity = {
            "status": "passed" if benchmark == "job" else "not_applicable",
            "relationships_checked": 7 if benchmark == "job" else 0,
            "orphan_count": 0,
            "target_stratum_share_shift_pp": 20.0,
            "target_stratum_share_reduction_pp": 20.0,
        }
        if kind == "data":
            policy_metrics = {
                str(assertion["metric"])
                for assertion in spec.payload["effect_policy"]["assertions"]
            }
            if "row_growth_rate" in policy_metrics:
                row_rate = 0.5 if scenario == "record_cardinality_growth" else 0.1
            elif "row_reduction_rate" in policy_metrics:
                row_rate = -0.5
            elif "absolute_row_rate" in policy_metrics:
                row_rate = -0.5
            else:
                row_rate = 0.0
            comparison_metrics = {
                "status": "supported",
                "row_rate": row_rate,
                "ks_distance": 0.5,
                "wasserstein_distance": 1.0,
                "normalized_wasserstein_p95_p05": 0.5,
                "tail_gain_over_baseline_p99": 0.2,
                "jensen_shannon_divergence_bits": 0.5,
                "total_variation_distance": 0.5,
                "max_mover": {"delta_percentage_points": 40.0},
            }
            statistics = {
                "status": "supported",
                "distribution_type": "numeric",
                "sample_count": {"baseline": 10, "drifted": 10},
                "row_count": {"baseline": 10, "drifted": 11},
                "comparison_metrics": comparison_metrics,
            }
        else:
            comparison_metrics = {
                "status": "supported",
                "jensen_shannon_divergence_bits": 0.2,
                "total_variation_distance": 0.3,
                "effective_count": {"baseline": 3.0, "drifted": 2.0},
                "max_mover": {
                    "template_id": "q1",
                    "delta_percentage_points": 40.0,
                },
            }
            statistics = {
                "status": "supported",
                "sample_count": {"baseline": 10, "drifted": 10},
                "comparison_metrics": comparison_metrics,
            }
        effect = evaluate_effect(
            spec.payload["effect_policy"],
            statistics,
            integrity=integrity if kind == "data" else None,
        )
        parameters = drift_parameters(spec)
        inputs = [input_descriptor]
        config_sha = configuration_hash_for_spec(
            spec,
            definition=definition,
            analysis=analysis,
            render=render,
        )
        resolved_spec_sha = resolved_spec_hash(
            spec_semantic_sha256=spec.semantic_sha256,
            seed=42,
            sample_size=1000,
            inputs=inputs,
            resolved_parameters=parameters,
        )
        execution_sha = semantic_hash(
            {"benchmark": benchmark, "kind": kind, "scenario": scenario, "fixture": True}
        )
        execution_output_sha = semantic_hash(
            {"execution": execution_sha, "output": "fixture"}
        )
        fingerprint = cache_fingerprint(
            driftbench_version="test-version",
            benchmark=benchmark,
            kind=kind,
            scenario=scenario,
            seed=42,
            sample_size=1000,
            config_sha256=config_sha,
            spec_descriptor=spec.descriptor,
            analysis=analysis,
            render=render,
            inputs=inputs,
        )
        payload = {
            "schema_version": VISUALIZATION_SCHEMA_VERSION,
            "benchmark": benchmark,
            "kind": kind,
            "scenario": scenario,
            "rationale": spec.rationale,
            "adapter": definition.adapter,
            "driftbench_version": "test-version",
            "seed": 42,
            "scale": dict(definition.scale),
            "sample_size": 1000,
            "drift_parameters": parameters,
            "analysis": analysis,
            "statistics": statistics,
            "comparison_metrics": comparison_metrics,
            "effect": effect,
            "capabilities": (
                {
                    "distribution_comparison": "supported",
                    "row_count": "supported",
                    "integrity": integrity,
                }
                if kind == "data"
                else dict(definition.query_capabilities)
            ),
            "render": render,
            "figure": file_descriptor(figure_path, root),
            "drift_spec": {
                **dict(spec.descriptor),
                "semantic_sha256": spec.semantic_sha256,
                "resolved_semantic_sha256": resolved_spec_sha,
                "type": dict(
                    zip(("family", "category", "subtype"), spec.type_triple)
                ),
            },
            "execution": {
                "status": "supported",
                "engine": SPEC_EXECUTOR,
                "engine_version": SPEC_EXECUTOR_VERSION,
                "algorithm": (
                    QUERY_MIX_ALGORITHM
                    if kind == "query"
                    else "driftbench.data-drift-spec/v1:"
                    + str(get_scenario_entry(kind, benchmark, scenario)["operation"])
                ),
                "semantic_sha256": execution_sha,
                "output_sha256": execution_output_sha,
            },
            "input_files": inputs,
            "config_sha256": config_sha,
            "generated_at": "2026-08-19T00:00:00Z",
            "reproduce": (
                "python -m visualization.cli generate "
                f"--benchmark {benchmark} --kind {kind} --scenario {scenario} "
                "--seed 42 --sample-size 1000"
            ),
            "limitations": definition.limitations,
            "cache": {
                "schema": CACHE_SCHEMA,
                "fingerprint": fingerprint,
            },
        }
        payload["semantic_sha256"] = manifest_semantic_hash(payload)
        return payload

    def test_gallery_has_exactly_forty_traceable_artifact_links(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            input_path = root / "cache" / "fixture" / "input.txt"
            input_path.parent.mkdir(parents=True)
            input_path.write_text("fixture\n", encoding="utf-8")
            input_descriptor = file_descriptor(input_path, root)

            for kind, benchmark, scenario in expected_artifact_keys():
                payload = self._manifest(
                    root,
                    benchmark=benchmark,
                    kind=kind,
                    scenario=scenario,
                    input_descriptor=input_descriptor,
                )
                manifest_path = (
                    root / "manifests" / kind / benchmark / f"{scenario}.json"
                )
                write_manifest(manifest_path, payload, root)

            gallery_path = build_gallery(root)
            text = gallery_path.read_text(encoding="utf-8")
            image_links = re.findall(r"!\[[^]]+\]\(([^)]+)\)", text)
            spec_links = re.findall(r"^- DriftSpec: .*\]\(([^)]+)\)$", text, re.MULTILINE)
            manifest_links = re.findall(
                r"^- Manifest: .*\]\(([^)]+)\)$", text, re.MULTILINE
            )
            expected_images = {
                f"figures/{kind}/{benchmark}/{scenario}.png"
                for kind, benchmark, scenario in expected_artifact_keys()
            }
            expected_specs = {
                f"specs/{kind}/{benchmark}/{scenario}.yaml"
                for kind, benchmark, scenario in expected_artifact_keys()
            }
            expected_manifests = {
                f"manifests/{kind}/{benchmark}/{scenario}.json"
                for kind, benchmark, scenario in expected_artifact_keys()
            }
            self.assertEqual(set(image_links), expected_images)
            self.assertEqual(set(spec_links), expected_specs)
            self.assertEqual(set(manifest_links), expected_manifests)
            self.assertEqual(len(image_links), 40)
            self.assertEqual(len(spec_links), 40)
            self.assertEqual(len(manifest_links), 40)
            self.assertEqual(len(re.findall(r"\n## .+ \(`", text)), 8)
            self.assertEqual(text.count("5/5 PASS"), 8)
            self.assertIn("Reading the diagnostics", text)
            self.assertIn("KS-D", text)
            self.assertIn("JSD", text)
            for link in image_links + spec_links + manifest_links:
                with self.subTest(link=link):
                    self.assertNotIn("\\", link)
                    self.assertFalse(Path(link).is_absolute())
                    self.assertTrue((root / link).is_file())

    def test_manifest_rejects_absolute_paths_at_any_depth(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            input_path = root / "cache" / "fixture" / "input.txt"
            input_path.parent.mkdir(parents=True)
            input_path.write_text("fixture\n", encoding="utf-8")
            payload = self._manifest(
                root,
                benchmark="tpch",
                kind="query",
                scenario="hotset_concentration",
                input_descriptor=file_descriptor(input_path, root),
            )
            unsafe_values = (
                r"C:\Users\someone\benchmark-data",
                "/home/someone/benchmark-data",
                r"\\server\share\benchmark-data",
            )
            for unsafe in unsafe_values:
                candidate = dict(payload)
                candidate["scale"] = {"source": unsafe}
                with self.subTest(path=unsafe):
                    with self.assertRaises(ValueError):
                        validate_manifest(candidate, root, verify_figure=True)

    def test_stale_cache_and_semantically_tampered_manifests_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            input_path = root / "cache" / "fixture" / "input.txt"
            input_path.parent.mkdir(parents=True)
            input_path.write_text("fixture\n", encoding="utf-8")
            payload = self._manifest(
                root,
                benchmark="tpch",
                kind="query",
                scenario="hotset_concentration",
                input_descriptor=file_descriptor(input_path, root),
            )

            legacy = json.loads(json.dumps(payload))
            legacy["schema_version"] = 2
            with self.assertRaisesRegex(ValueError, "schema"):
                validate_manifest(legacy, root, verify_figure=True)

            stale_cache = json.loads(json.dumps(payload))
            stale_cache["cache"]["schema"] = "driftbench.visualization-cache/v2"
            with self.assertRaisesRegex(ValueError, "cache schema"):
                validate_manifest(stale_cache, root, verify_figure=True)

            tampered = json.loads(json.dumps(payload))
            tampered["comparison_metrics"]["total_variation_distance"] = 0.99
            tampered["statistics"]["comparison_metrics"][
                "total_variation_distance"
            ] = 0.99
            with self.assertRaisesRegex(ValueError, "effect|semantic_sha256"):
                validate_manifest(tampered, root, verify_figure=True)

            query_parameters = json.loads(json.dumps(payload))
            query_parameters["drift_parameters"]["target_weights"]["q1"] = 0.0
            query_parameters["drift_spec"]["resolved_semantic_sha256"] = (
                resolved_spec_hash(
                    spec_semantic_sha256=query_parameters["drift_spec"][
                        "semantic_sha256"
                    ],
                    seed=query_parameters["seed"],
                    sample_size=query_parameters["sample_size"],
                    inputs=query_parameters["input_files"],
                    resolved_parameters=query_parameters["drift_parameters"],
                )
            )
            query_parameters["semantic_sha256"] = manifest_semantic_hash(
                query_parameters
            )
            with self.assertRaisesRegex(ValueError, "query drift parameters"):
                validate_manifest(query_parameters, root, verify_figure=True)

            wrong_algorithm = json.loads(json.dumps(payload))
            wrong_algorithm["execution"]["algorithm"] = "fixture:query"
            wrong_algorithm["semantic_sha256"] = manifest_semantic_hash(
                wrong_algorithm
            )
            with self.assertRaisesRegex(ValueError, "execution algorithm"):
                validate_manifest(wrong_algorithm, root, verify_figure=True)


class GitIgnoreContractTests(unittest.TestCase):
    def _check_ignore(self, relative_path: str, *extra: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ["git", "check-ignore", *extra, "--no-index", relative_path],
            cwd=REPO_ROOT,
            capture_output=True,
            check=False,
            text=True,
            timeout=30,
        )

    def test_data_and_cache_payloads_are_ignored_but_gitkeep_files_are_not(self) -> None:
        ignored = (
            "visualization/data/adapters/tpch/lineitem.csv",
            "visualization/data/drifted/tpch/outlier_injection/lineitem.csv",
            "visualization/cache/adapters/query/tpch/tpch_queries.sql",
            "visualization/cache/downloads/archive.bin",
        )
        for relative_path in ignored:
            with self.subTest(path=relative_path):
                completed = self._check_ignore(relative_path, "--verbose")
                self.assertEqual(
                    completed.returncode,
                    0,
                    completed.stderr or f"not ignored: {relative_path}",
                )
                self.assertIn(".gitignore", completed.stdout)

        keep_files = (
            "visualization/data/.gitkeep",
            "visualization/cache/.gitkeep",
        )
        for relative_path in keep_files:
            with self.subTest(path=relative_path):
                self.assertTrue((REPO_ROOT / relative_path).is_file())
                completed = self._check_ignore(relative_path, "--quiet")
                self.assertEqual(
                    completed.returncode,
                    1,
                    completed.stderr or f"unexpectedly ignored: {relative_path}",
                )


if __name__ == "__main__":
    unittest.main()
