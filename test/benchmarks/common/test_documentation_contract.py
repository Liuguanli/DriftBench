import json
import re
import unittest

from ..helpers import REPO_ROOT


class VersionConsistencyTests(unittest.TestCase):
    def test_benchmark_documentation_contracts_stay_aligned(self) -> None:
        root = REPO_ROOT
        readme = (root / "README.md").read_text(encoding="utf-8")
        reference = (root / "docs" / "benchmark_reference.md").read_text(
            encoding="utf-8"
        )
        target_contract = (
            root / "docs" / "benchmark_target_contract.md"
        ).read_text(encoding="utf-8")
        testing_guide = (
            root / "docs" / "benchmark_testing_guide.html"
        ).read_text(encoding="utf-8")

        adapter_section = readme.split(
            "## Benchmark Adapters (`driftbench.data`)", 1
        )[1].split("### Generate data and queries", 1)[0]
        adapter_rows = re.findall(r"^\| `[^`]+` \|", adapter_section, re.MULTILINE)
        self.assertEqual(len(adapter_rows), 9)
        self.assertIn(
            "| `tpcds` | OLAP / Decision support | `.dat` (pipe-delimited) "
            "| 5 synthetic |",
            readme,
        )
        self.assertIn("Complete reference for all 9 benchmark adapters", reference)
        self.assertIn("| TPC-DS | Medium (5 synthetic; 24 full) |", reference)
        self.assertNotIn("26 full", reference)

        links = {
            "README.md": (
                readme,
                (
                    "docs/benchmark_reference.md",
                    "docs/benchmark_target_contract.md",
                    "docs/benchmark_testing_guide.html",
                ),
            ),
            "benchmark_reference.md": (
                reference,
                ("../README.md", "benchmark_target_contract.md", "benchmark_testing_guide.html"),
            ),
            "benchmark_target_contract.md": (
                target_contract,
                ("../README.md", "benchmark_reference.md", "benchmark_testing_guide.html"),
            ),
            "benchmark_testing_guide.html": (
                testing_guide,
                ("../README.md", "benchmark_reference.md", "benchmark_target_contract.md"),
            ),
        }
        for document, (text, expected_links) in links.items():
            with self.subTest(document=document):
                for expected_link in expected_links:
                    self.assertIn(expected_link, text)

        for document, text in (("README", readme), ("testing guide", testing_guide)):
            with self.subTest(integration_note=document):
                normalized = re.sub(r"\s+", " ", text)
                self.assertIn(
                    "five real PostgreSQL 16 integration tests", normalized
                )
                self.assertIn("DRIFTBENCH_REQUIRE_PG_INTEGRATION", normalized)
                self.assertIn("not exactly", normalized)
                self.assertIn("no mocks or skips", normalized)
                self.assertIn("benchmark-regression-pgbench.yml", normalized)

        for expected in (
            "driftbench benchmark verify --bundle benchmark-artifacts/results --json",
            '"outcome": "partial_failure"',
            '"outcome": "verification_error"',
            '"outcome": "threshold_failed"',
            "reports target execution and artifact collection only",
        ):
            self.assertIn(expected, target_contract)

        for expected in (
            "driftbench benchmark verify --bundle benchmark-artifacts/results --json",
            "verification_error",
            "threshold_failed",
            "without PostgreSQL",
            "benchmark_target_contract.md#metrics-and-real-database-evidence",
        ):
            self.assertIn(expected, testing_guide)

        json_examples = re.findall(
            r"```json\n(.*?)\n```", target_contract, re.DOTALL
        )
        self.assertEqual(len(json_examples), 3)
        for example in json_examples:
            self.assertIsInstance(json.loads(example), dict)

    def test_generation_and_conformance_boundaries_are_explicit(self) -> None:
        root = REPO_ROOT
        readme = (root / "README.md").read_text(encoding="utf-8")
        reference = (root / "docs" / "benchmark_reference.md").read_text(
            encoding="utf-8"
        )
        examples_guide = (
            root / "driftspec" / "examples" / "README.md"
        ).read_text(encoding="utf-8")
        paper_guide = (
            root / "driftspec" / "examples" / "paper" / "README.md"
        ).read_text(encoding="utf-8")

        canonical_anchor = (
            "benchmark_reference.md#provenance-conformance-and-execution-boundaries"
        )
        self.assertIn("## Provenance, Conformance, and Execution Boundaries", reference)
        for document, text in (
            ("README", readme),
            ("examples guide", examples_guide),
            ("paper guide", paper_guide),
        ):
            with self.subTest(canonical_boundary_link=document):
                self.assertIn(canonical_anchor, text)

        normalized = re.sub(r"\s+", " ", reference).lower()
        for expected in (
            "synthetic fixtures and workload artifacts",
            "not official, audited, or benchmark-spec-compliant implementations",
            "must not be reported as official tpc, ycsb, job, dsb, or benchbase scores",
            "explicit `dbgen_path`, `path`, the repository-local",
            "repository-local",
            "user cache",
            "`electrum/tpch-dbgen`",
            "unpinned revision",
            "`dbgen` binary sha-256",
            "python qgen-style",
            "does not execute the native or official `qgen` binary",
            "`yaml.safe_load()`",
            "exact scalar-string `${name}` bindings, including mapping keys",
            "`migrate_spec()`",
            "shallow `validate_spec()`",
            "python `random` and numpy",
            "(family, category, subtype)",
            "`runtime_inputs`",
            "postgresql schema sources",
            "configured `output_path`",
            "current working directory, not from the yaml file's directory",
            "`benchmark_adapter` does not generate data",
            "from driftbench.data.ycsb import data; data(scale_factor=1).generate(output_dir=\"artifacts\")`",
            "does not create a benchmark dataset or a performance baseline",
            "does not automatically create a separate `baseline.json`",
            "11 tables",
            "7 single-column relationships",
            "4 declared relationships",
            "declared relationships and direct propagation",
            "composite fks",
            "recursive cascade propagation",
            "only real database gate",
            "postgresql 16 `select-only`",
            "three paired rounds",
            "five opt-in integration tests",
        ):
            with self.subTest(canonical_fact=expected):
                self.assertIn(expected, normalized)

        normalized_examples = re.sub(r"\s+", " ", examples_guide).lower()
        for expected in (
            "`benchmark_adapter` does not generate data",
            "it does not create benchmark tables or measure a performance baseline",
            "does not automatically create a separate `baseline.json`",
            "does not accept placeholder bindings",
            "current working directory, not the yaml file's directory",
            "from driftbench.data.ycsb import data",
        ):
            self.assertIn(expected, normalized_examples)

        normalized_paper = re.sub(r"\s+", " ", paper_guide).lower()
        for expected in (
            "7 single-column relationships",
            "4 declared incoming `title` relationships",
            "does not discover a database schema",
            "`run_spec()` does not invoke the tpc-h adapter",
            "it does not create benchmark data or a measured performance baseline",
            "does not automatically create a separate `baseline.json`",
        ):
            self.assertIn(expected, normalized_paper)

        self.assertNotIn("22 sql via qgen", readme.lower())
        self.assertNotIn("generates parameterized sql via qgen", reference.lower())
        self.assertNotIn("driftbench.data.<benchmark>.generate()", reference)
        self.assertNotIn("driftbench.data.<benchmark>.generate()", examples_guide)


if __name__ == "__main__":
    unittest.main()
