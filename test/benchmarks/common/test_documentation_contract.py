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


if __name__ == "__main__":
    unittest.main()
