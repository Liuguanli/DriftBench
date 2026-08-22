from __future__ import annotations

import csv
import hashlib
import json
import re
import subprocess
import sys
import tempfile
import unittest
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

from driftbench.api import QueryTemplate, QueryWorkloadMixResult, run_spec
from driftbench.core.temporal.time_stamp_generator import generate_timestamps
from driftbench.core.workload.tpch_sql_generator import (
    generate_tpch_queries_indexed_qgen,
)
from driftbench.data.tpch import queries as tpch_queries
from visualization.artifacts import semantic_hash
from visualization.specs import validate_canonical_spec


REPO_ROOT = Path(__file__).resolve().parents[1]
EXAMPLES_ROOT = REPO_ROOT / "driftspec" / "examples"
PAPER_ROOT = REPO_ROOT / "driftspec" / "examples" / "paper"
TPCH_RESOURCE_ROOT = REPO_ROOT / "driftbench" / "data" / "resources" / "tpch"
TPCH_TEMPLATE_DIR = TPCH_RESOURCE_ROOT / "queries"
TPCH_DISTS_FILE = TPCH_RESOURCE_ROOT / "dists.dss"


@dataclass(frozen=True)
class PaperExample:
    filename: str
    canonical: str
    kind: str
    benchmark: str
    scenario: str


EXAMPLES = (
    PaperExample(
        "data_value_skew.yaml",
        "visualization/specs/data/tpch/price_skew.yaml",
        "data",
        "tpch",
        "price_skew",
    ),
    PaperExample(
        "data_outlier_injection.yaml",
        "visualization/specs/data/tpcds/price_outliers.yaml",
        "data",
        "tpcds",
        "price_outliers",
    ),
    PaperExample(
        "data_cardinality_change.yaml",
        "visualization/specs/data/ycsb/record_cardinality_growth.yaml",
        "data",
        "ycsb",
        "record_cardinality_growth",
    ),
    PaperExample(
        "data_fk_safe_selective_deletion.yaml",
        "visualization/specs/data/job/post_2000_title_deletion.yaml",
        "data",
        "job",
        "post_2000_title_deletion",
    ),
    PaperExample(
        "query_template_mix.yaml",
        "visualization/specs/query/tpch/complexity_mix_shift.yaml",
        "query",
        "tpch",
        "complexity_mix_shift",
    ),
)

FLAGSHIP_EXAMPLES = (
    "paper_tpch_data_drift.yaml",
    "paper_tpch_query_workload_drift.yaml",
)


def _load_yaml(path: Path) -> dict[str, Any]:
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise AssertionError(f"expected a YAML mapping: {path}")
    return payload


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _aggregate_hash(paths: dict[str, Path]) -> str:
    digest = hashlib.sha256()
    for name, path in sorted(paths.items()):
        digest.update(name.encode("utf-8"))
        digest.update(b"\0")
        digest.update(path.read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()


def _validate_with_cli(spec_path: Path) -> dict[str, Any]:
    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "driftbench.cli",
            "validate-spec",
            str(spec_path.relative_to(REPO_ROOT)),
            "--json",
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    if completed.returncode != 0:
        raise AssertionError(completed.stderr)
    payload = json.loads(completed.stdout)
    if not payload.get("ok"):
        raise AssertionError(payload)
    return payload


def _assert_readme_links_to(
    test_case: unittest.TestCase,
    readme_path: Path,
    target_path: Path,
) -> None:
    text = readme_path.read_text(encoding="utf-8")
    candidates = [
        link.split("#", 1)[0]
        for link in re.findall(r"\[[^]]+\]\(([^)]+)\)", text)
        if Path(link.split("#", 1)[0]).name == target_path.name
    ]
    test_case.assertTrue(candidates, f"{readme_path} does not link to {target_path}")
    for candidate in candidates:
        test_case.assertFalse(Path(candidate).is_absolute())
        resolved = (readme_path.parent / candidate).resolve()
        test_case.assertEqual(resolved, target_path.resolve())
        test_case.assertTrue(resolved.is_file())


class PaperDriftSpecContractTests(unittest.TestCase):
    def test_inventory_comments_and_canonical_semantics(self) -> None:
        canonical_names = {example.filename for example in EXAMPLES}
        self.assertEqual(
            {path.name for path in PAPER_ROOT.glob("*.yaml")},
            canonical_names,
        )

        for filename in sorted(canonical_names):
            with self.subTest(comments=filename):
                paper_path = PAPER_ROOT / filename
                lines = paper_path.read_text(encoding="utf-8").splitlines()
                self.assertGreaterEqual(len(lines), 6)
                for index, line in enumerate(lines[:5], start=1):
                    self.assertTrue(line.startswith(f"# {index}. "))
                    line.encode("ascii")
                self.assertLessEqual(sum(bool(line.strip()) for line in lines), 70)

        for example in EXAMPLES:
            with self.subTest(canonical_semantics=example.filename):
                paper_path = PAPER_ROOT / example.filename
                canonical_path = REPO_ROOT / example.canonical
                paper = _load_yaml(paper_path)
                canonical = _load_yaml(canonical_path)
                self.assertEqual(paper, canonical)
                self.assertEqual(semantic_hash(paper), semantic_hash(canonical))
                self.assertEqual(paper["seed"], 42)
                validate_canonical_spec(
                    paper,
                    kind=example.kind,
                    benchmark=example.benchmark,
                    scenario=example.scenario,
                )

        for filename in FLAGSHIP_EXAMPLES:
            with self.subTest(flagship_comments=filename):
                spec_path = EXAMPLES_ROOT / filename
                text = spec_path.read_text(encoding="utf-8")
                lines = text.splitlines()
                for index, line in enumerate(lines[:5], start=1):
                    self.assertTrue(line.startswith(f"# {index}. "))
                    line.encode("ascii")
                self.assertIn("# Baseline phase", text)
                self.assertIn("# Drifted phase", text)
                self.assertNotRegex(text, r"[A-Za-z]:[\\/]")
                self.assertNotIn("file://", text)

    def test_readme_traceability_links_and_limitations(self) -> None:
        readme_path = PAPER_ROOT / "README.md"
        text = readme_path.read_text(encoding="utf-8")
        normalized_text = " ".join(text.split())
        links = re.findall(r"\[[^]]+\]\(([^)]+)\)", text)
        for link in links:
            with self.subTest(link=link):
                relative = link.split("#", 1)[0]
                self.assertFalse(Path(relative).is_absolute())
                resolved = (PAPER_ROOT / relative).resolve()
                self.assertTrue(resolved.is_relative_to(REPO_ROOT))
                self.assertTrue(resolved.is_file())

        for example in EXAMPLES:
            self.assertIn(example.filename, text)
            self.assertIn(example.canonical, text)
        for required in (
            "not environment-variable",
            "run-yaml",
            "broader or legacy demos",
            "predicate selectivity",
            "query_template_mix.yaml",
            "observed traffic",
            "database execution",
            "database loadability",
            "key integrity",
            "performance",
            "causality",
        ):
            self.assertIn(required, normalized_text)

        for filename in FLAGSHIP_EXAMPLES:
            target = EXAMPLES_ROOT / filename
            for source in (
                REPO_ROOT / "README.md",
                EXAMPLES_ROOT / "README.md",
                PAPER_ROOT / "README.md",
            ):
                with self.subTest(source=source, target=filename):
                    _assert_readme_links_to(self, source, target)

    def test_indexing_guide_marks_sketches_as_non_executable(self) -> None:
        guide_path = REPO_ROOT / "docs" / "driftbench_indexing_guide.md"
        text = guide_path.read_text(encoding="utf-8")
        normalized_text = " ".join(text.split())
        self.assertIn("Conceptual notation only", normalized_text)
        self.assertIn("not registered DriftSpec handler", normalized_text)
        self.assertIn("contracts", normalized_text)
        self.assertNotIn("run-yaml driftspec/examples/base_", text)
        for filename in FLAGSHIP_EXAMPLES:
            _assert_readme_links_to(self, guide_path, EXAMPLES_ROOT / filename)

    def test_flagship_specs_validate_through_cli(self) -> None:
        expected_types = {
            "paper_tpch_data_drift.yaml": "data.drift.single_table",
            "paper_tpch_query_workload_drift.yaml": "workload.sql_templates.tpch",
        }
        for filename, expected_type in expected_types.items():
            with self.subTest(spec=filename):
                payload = _validate_with_cli(EXAMPLES_ROOT / filename)
                self.assertEqual(payload["type"], expected_type)


class PaperSingleTableExecutionTests(unittest.TestCase):
    def test_all_single_table_examples_execute_deterministically(self) -> None:
        cases = (
            (
                "data_value_skew.yaml",
                "lineitem",
                pd.DataFrame(
                    {
                        "l_orderkey": range(1, 101),
                        "l_extendedprice": [float(value) for value in range(10, 110)],
                    }
                ),
                "l_extendedprice",
                100,
            ),
            (
                "data_outlier_injection.yaml",
                "item",
                pd.DataFrame(
                    {
                        "i_item_sk": range(1, 101),
                        "i_current_price": [float(value) for value in range(1, 101)],
                    }
                ),
                "i_current_price",
                110,
            ),
            (
                "data_cardinality_change.yaml",
                "usertable",
                pd.DataFrame(
                    {
                        "YCSB_KEY": [f"user{value:04d}" for value in range(100)],
                        "FIELD0": [float(value) for value in range(100)],
                    }
                ),
                "FIELD0",
                150,
            ),
        )

        for filename, table, baseline, comparison_column, expected_rows in cases:
            with self.subTest(example=filename), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                input_path = root / f"{table}.csv"
                baseline.to_csv(input_path, index=False)
                outputs: list[Path] = []

                for run_number in (1, 2):
                    run_root = root / f"run-{run_number}"
                    output_path = run_root / f"{table}.csv"
                    schema_path = run_root / f"{table}.schema.json"
                    result = run_spec(
                        PAPER_ROOT / filename,
                        bindings={
                            "DRIFTBENCH_INPUT": input_path.resolve(),
                            "DRIFTBENCH_SCHEMA": schema_path.resolve(),
                            "DRIFTBENCH_OUTPUT": output_path.resolve(),
                        },
                    )
                    self.assertEqual(len(result["outputs"]), 1)
                    self.assertEqual(
                        Path(result["outputs"][0]["path"]).resolve(),
                        output_path.resolve(),
                    )
                    self.assertTrue(schema_path.is_file())
                    self.assertTrue(output_path.is_file())
                    outputs.append(output_path)

                self.assertEqual(_sha256(outputs[0]), _sha256(outputs[1]))
                drifted = pd.read_csv(outputs[0])
                self.assertEqual(len(drifted), expected_rows)
                self.assertIn(comparison_column, drifted.columns)

                if filename == "data_value_skew.yaml":
                    self.assertFalse(
                        baseline[comparison_column].equals(drifted[comparison_column])
                    )
                elif filename == "data_outlier_injection.yaml":
                    self.assertGreater(
                        drifted[comparison_column].max(),
                        baseline[comparison_column].max(),
                    )


class PaperFlagshipDataExecutionTests(unittest.TestCase):
    def test_tpch_data_drift_executes_twice_with_exact_phase_semantics(self) -> None:
        baseline_input = pd.DataFrame(
            {
                "l_orderkey": range(1, 101),
                "l_quantity": [float(value % 50 + 1) for value in range(100)],
                "l_extendedprice": [float(value * 100) for value in range(1, 101)],
                "l_shipmode": ["AIR", "RAIL", "SHIP", "TRUCK"] * 25,
            }
        )
        spec_path = EXAMPLES_ROOT / "paper_tpch_data_drift.yaml"

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            input_path = root / "lineitem.csv"
            baseline_input.to_csv(input_path, index=False)
            run_outputs: list[dict[str, Path]] = []
            run_results: list[dict[str, Any]] = []

            for run_number in (1, 2):
                run_root = root / f"run-{run_number}"
                outputs = {
                    "baseline": run_root / "baseline.csv",
                    "skew": run_root / "skew.csv",
                    "drifted": run_root / "drifted.csv",
                    "combined": run_root / "combined.csv",
                }
                result = run_spec(
                    spec_path,
                    bindings={
                        "DRIFTBENCH_INPUT": input_path.resolve(),
                        "DRIFTBENCH_SCHEMA": (run_root / "lineitem.schema.json").resolve(),
                        "DRIFTBENCH_BASELINE_OUTPUT": outputs["baseline"].resolve(),
                        "DRIFTBENCH_SKEW_INTERMEDIATE": outputs["skew"].resolve(),
                        "DRIFTBENCH_DRIFTED_OUTPUT": outputs["drifted"].resolve(),
                        "DRIFTBENCH_COMBINED_OUTPUT": outputs["combined"].resolve(),
                    },
                )
                self.assertEqual(
                    [item["name"] for item in result["outputs"]],
                    [
                        "baseline_day1_uniform",
                        "price_skew_intermediate",
                        "drifted_day2_periodic",
                        "combined_two_day_timeline",
                    ],
                )
                self.assertEqual(
                    [item["rows"] for item in result["outputs"]],
                    [100, 100, 100, 200],
                )
                self.assertEqual(
                    [item["drift_type"] for item in result["outputs"]],
                    ["add_timestamp", "value_skew", "add_timestamp", "concat_csvs"],
                )
                self.assertTrue(all(path.is_file() for path in outputs.values()))
                run_outputs.append(outputs)
                run_results.append(result)

            self.assertEqual(
                _aggregate_hash(run_outputs[0]),
                _aggregate_hash(run_outputs[1]),
            )
            for name in ("baseline", "skew", "drifted", "combined"):
                self.assertEqual(
                    _sha256(run_outputs[0][name]),
                    _sha256(run_outputs[1][name]),
                )

            source = pd.read_csv(input_path)
            baseline = pd.read_csv(run_outputs[0]["baseline"])
            skew = pd.read_csv(run_outputs[0]["skew"])
            drifted = pd.read_csv(run_outputs[0]["drifted"])
            combined = pd.read_csv(run_outputs[0]["combined"])
            self.assertEqual(len(baseline), len(baseline_input))
            self.assertEqual(len(skew), len(baseline_input))
            self.assertEqual(len(drifted), len(baseline_input))
            self.assertEqual(len(combined), len(baseline_input) * 2)
            pd.testing.assert_frame_equal(
                baseline.drop(columns="event_timestamp"),
                source,
            )
            pd.testing.assert_frame_equal(
                skew.drop(columns="l_extendedprice"),
                source.drop(columns="l_extendedprice"),
            )
            self.assertEqual(
                int((skew["l_extendedprice"] != source["l_extendedprice"]).sum()),
                80,
            )
            pd.testing.assert_frame_equal(
                drifted.drop(columns="event_timestamp"),
                skew,
            )

            self.assertEqual(
                baseline["event_timestamp"].tolist(),
                generate_timestamps(
                    count=100,
                    start_time="2025-07-01T00:00:00",
                    pattern="uniform",
                    queries_per_minute=60,
                ),
            )
            self.assertEqual(
                drifted["event_timestamp"].tolist(),
                generate_timestamps(
                    count=100,
                    start_time="2025-07-02T00:00:00",
                    pattern="periodic",
                    queries_per_minute=180,
                ),
            )

            expected_combined = (
                pd.concat([baseline, drifted], ignore_index=True)
                .sort_values("event_timestamp")
                .reset_index(drop=True)
            )
            pd.testing.assert_frame_equal(combined, expected_combined)
            self.assertTrue(pd.to_datetime(combined["event_timestamp"]).is_monotonic_increasing)

            for result, outputs in zip(run_results, run_outputs):
                self.assertEqual(
                    [Path(item["path"]).resolve() for item in result["outputs"]],
                    [path.resolve() for path in outputs.values()],
                )


class PaperJobExecutionTests(unittest.TestCase):
    def test_fk_safe_deletion_is_deterministic_and_leaves_no_orphans(self) -> None:
        title_ids = list(range(1, 21))
        inputs = {
            "cast_info": pd.DataFrame(
                {"movie_id": title_ids, "person_id": [1] * len(title_ids)}
            ),
            "company_name": pd.DataFrame({"id": [1]}),
            "company_type": pd.DataFrame({"id": [1]}),
            "info_type": pd.DataFrame({"id": [1]}),
            "keyword": pd.DataFrame({"id": [1]}),
            "kind_type": pd.DataFrame({"id": [1]}),
            "movie_companies": pd.DataFrame(
                {"movie_id": title_ids, "company_id": [1] * len(title_ids)}
            ),
            "movie_info": pd.DataFrame(
                {"movie_id": title_ids, "info_type_id": [1] * len(title_ids)}
            ),
            "movie_keyword": pd.DataFrame(
                {"movie_id": title_ids, "keyword_id": [1] * len(title_ids)}
            ),
            "name": pd.DataFrame({"id": [1]}),
            "title": pd.DataFrame(
                {
                    "id": title_ids,
                    "production_year": list(range(1991, 2011)),
                }
            ),
        }
        spec_path = PAPER_ROOT / "data_fk_safe_selective_deletion.yaml"
        payload = _load_yaml(spec_path)

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            input_paths: dict[str, Path] = {}
            for name, frame in inputs.items():
                path = root / "input" / f"{name}.csv"
                path.parent.mkdir(parents=True, exist_ok=True)
                frame.to_csv(path, index=False)
                input_paths[name] = path

            run_outputs: list[dict[str, Path]] = []
            for run_number in (1, 2):
                outputs = {
                    name: root / f"run-{run_number}" / f"{name}.csv"
                    for name in inputs
                }
                bindings: dict[str, Path] = {}
                for name in inputs:
                    suffix = name.upper()
                    bindings[f"DRIFTBENCH_INPUT_{suffix}"] = input_paths[name].resolve()
                    bindings[f"DRIFTBENCH_OUTPUT_{suffix}"] = outputs[name].resolve()
                result = run_spec(spec_path, bindings=bindings)
                self.assertTrue(result["integrity_validated"])
                self.assertEqual(len(result["outputs"]), len(inputs))
                self.assertTrue(all(path.is_file() for path in outputs.values()))
                run_outputs.append(outputs)

            self.assertEqual(
                _aggregate_hash(run_outputs[0]),
                _aggregate_hash(run_outputs[1]),
            )
            drifted = {
                name: pd.read_csv(path) for name, path in run_outputs[0].items()
            }

            baseline_title = inputs["title"]
            drifted_title = drifted["title"]
            deleted = set(baseline_title["id"]) - set(drifted_title["id"])
            eligible = set(
                baseline_title.loc[baseline_title["production_year"] >= 2001, "id"]
            )
            self.assertEqual(len(deleted), round(len(eligible) * 0.40))
            self.assertTrue(deleted <= eligible)

            for relationship in payload["variables"]["relationships"]:
                fact = drifted[relationship["fact"]]
                dimension = drifted[relationship["dim"]]
                missing = set(fact[relationship["fk"]].dropna()) - set(
                    dimension[relationship["pk"]].dropna()
                )
                self.assertEqual(missing, set(), relationship["name"])

            for fact_name in (
                "cast_info",
                "movie_info",
                "movie_companies",
                "movie_keyword",
            ):
                self.assertTrue(set(drifted[fact_name]["movie_id"]).isdisjoint(deleted))


class PaperQueryExecutionTests(unittest.TestCase):
    def test_real_tpch_templates_produce_a_deterministic_mix(self) -> None:
        spec_path = PAPER_ROOT / "query_template_mix.yaml"
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            generation = tpch_queries(
                query_ids=range(1, 23),
                mode="qgen",
                queries_per_template=1,
                seed=42,
                shuffle=False,
                scale=0.01,
            ).generate(output_dir=root / "adapter", force=True)
            csv_path = next(
                Path(path)
                for path in generation.files
                if Path(path).name == "tpch_queries.csv"
            )
            templates_by_id: dict[str, QueryTemplate] = {}
            with csv_path.open(encoding="utf-8", newline="") as stream:
                for row in csv.DictReader(stream):
                    template_id = f"q{int(row['query_id'])}"
                    templates_by_id[template_id] = QueryTemplate(
                        template_id,
                        row["sql"],
                    )
            templates = tuple(
                templates_by_id[f"q{index}"] for index in range(1, 23)
            )

            results: list[QueryWorkloadMixResult] = []
            outputs: list[Path] = []
            for run_number in (1, 2):
                output_path = root / f"run-{run_number}" / "query-mix.json"
                result = run_spec(
                    spec_path,
                    bindings={"DRIFTBENCH_OUTPUT": output_path.resolve()},
                    runtime_inputs={"query_templates": templates},
                )
                self.assertIsInstance(result, QueryWorkloadMixResult)
                self.assertTrue(output_path.is_file())
                results.append(result)
                outputs.append(output_path)

            self.assertEqual(results[0].semantic_hash, results[1].semantic_hash)
            self.assertEqual(_sha256(outputs[0]), _sha256(outputs[1]))
            result = results[0]
            self.assertEqual(result.sample_size, 1000)
            self.assertAlmostEqual(result.target_weights["q19"], 0.30)
            self.assertAlmostEqual(result.target_weights["q2"], 0.30)
            self.assertAlmostEqual(result.target_weights["q21"], 0.30)
            baseline_counts = Counter(item.template_id for item in result.baseline)
            drifted_counts = Counter(item.template_id for item in result.drifted)
            self.assertEqual(sum(baseline_counts.values()), 1000)
            self.assertEqual(sum(drifted_counts.values()), 1000)
            self.assertGreater(
                sum(drifted_counts[name] for name in ("q19", "q2", "q21")),
                sum(baseline_counts[name] for name in ("q19", "q2", "q21")),
            )

    def test_flagship_tpch_query_workload_executes_with_exact_phases(self) -> None:
        spec_path = EXAMPLES_ROOT / "paper_tpch_query_workload_drift.yaml"
        self.assertTrue((TPCH_TEMPLATE_DIR / "1.sql").is_file())
        self.assertTrue(TPCH_DISTS_FILE.is_file())

        payload = _load_yaml(spec_path)
        self.assertEqual(payload["seed"], 42)
        self.assertEqual(
            payload["variables"]["defaults"],
            {"param_mode": "qgen", "queries_per_template": 20, "shuffle": False},
        )
        declared_runs = payload["variables"]["query_runs"]
        self.assertEqual(
            [run["query_ids"] for run in declared_runs],
            [["1", "3", "6", "11", "14"], ["2", "5", "8", "11", "21"]],
        )
        self.assertEqual([run["qgen_scale"] for run in declared_runs], [0.01, 1.0])

        phase_config = {
            "baseline": {
                "query_ids": ["1", "3", "6", "11", "14"],
                "scale": 0.01,
                "start_time": "2025-07-01T00:00:00",
                "pattern": "uniform",
                "queries_per_minute": 60,
                "q11_fraction": "0.0100000000",
            },
            "drifted": {
                "query_ids": ["2", "5", "8", "11", "21"],
                "scale": 1.0,
                "start_time": "2025-07-02T00:00:00",
                "pattern": "periodic",
                "queries_per_minute": 180,
                "q11_fraction": "0.0001000000",
            },
        }
        expected_entries = {
            name: generate_tpch_queries_indexed_qgen(
                template_dir=str(TPCH_TEMPLATE_DIR),
                query_ids=config["query_ids"],
                queries_per_template=20,
                seed=42,
                shuffle=False,
                dist_file=str(TPCH_DISTS_FILE),
                scale=config["scale"],
            )
            for name, config in phase_config.items()
        }

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            run_outputs: list[dict[str, Path]] = []
            run_frames: list[dict[str, pd.DataFrame]] = []

            for run_number in (1, 2):
                run_root = root / f"run-{run_number}"
                outputs = {
                    "baseline": run_root / "baseline.csv",
                    "drifted": run_root / "drifted.csv",
                }
                result = run_spec(
                    spec_path,
                    bindings={
                        "DRIFTBENCH_TPCH_TEMPLATE_DIR": TPCH_TEMPLATE_DIR.resolve(),
                        "DRIFTBENCH_TPCH_DISTS_FILE": TPCH_DISTS_FILE.resolve(),
                        "DRIFTBENCH_BASELINE_OUTPUT": outputs["baseline"].resolve(),
                        "DRIFTBENCH_DRIFTED_OUTPUT": outputs["drifted"].resolve(),
                    },
                )
                self.assertIsNone(result)
                self.assertTrue(all(path.is_file() for path in outputs.values()))

                frames = {name: pd.read_csv(path) for name, path in outputs.items()}
                for name, frame in frames.items():
                    self.assertEqual(list(frame.columns), ["timestamp", "sql"])
                    self.assertEqual(len(frame), 100)
                    self.assertEqual(
                        frame["sql"].tolist(),
                        [entry["sql"] for entry in expected_entries[name]],
                    )
                    config = phase_config[name]
                    self.assertEqual(
                        frame["timestamp"].tolist(),
                        generate_timestamps(
                            count=100,
                            start_time=config["start_time"],
                            pattern=config["pattern"],
                            queries_per_minute=config["queries_per_minute"],
                        ),
                    )

                    query_counts = Counter(
                        entry["query_id"] for entry in expected_entries[name]
                    )
                    self.assertEqual(
                        query_counts,
                        Counter({query_id: 20 for query_id in config["query_ids"]}),
                    )
                    self.assertEqual(
                        [entry["query_id"] for entry in expected_entries[name]],
                        [
                            query_id
                            for query_id in config["query_ids"]
                            for _ in range(20)
                        ],
                    )
                    q11_sql = [
                        entry["sql"]
                        for entry in expected_entries[name]
                        if entry["query_id"] == "11"
                    ]
                    self.assertEqual(len(q11_sql), 20)
                    self.assertTrue(
                        all(config["q11_fraction"] in sql for sql in q11_sql)
                    )
                self.assertFalse(
                    frames["baseline"]["sql"].equals(frames["drifted"]["sql"])
                )
                run_outputs.append(outputs)
                run_frames.append(frames)

            self.assertEqual(
                _aggregate_hash(run_outputs[0]),
                _aggregate_hash(run_outputs[1]),
            )
            for name in ("baseline", "drifted"):
                self.assertEqual(
                    _sha256(run_outputs[0][name]),
                    _sha256(run_outputs[1][name]),
                )
                self.assertTrue(
                    run_frames[0][name]["sql"].equals(run_frames[1][name]["sql"])
                )
                self.assertTrue(
                    run_frames[0][name]["timestamp"].equals(
                        run_frames[1][name]["timestamp"]
                    )
                )


if __name__ == "__main__":
    unittest.main()
