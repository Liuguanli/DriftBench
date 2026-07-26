from __future__ import annotations

import json
import re
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from driftbench.data.base import (
    SUPPORT_CONTRACT_VERSION,
    SUPPORT_PROFILES,
    BenchmarkArtifact,
    assert_referential_integrity,
    assert_row_count_law,
    find_optional_binary,
    get_support_profile,
)
from driftbench.data.benchbase import BenchBaseData, BenchBaseQueries
from driftbench.data.dsb import DSBData, DSBQueries
from driftbench.data.job import JOBData, JOBQueries
from driftbench.data.pgbench import PgBenchData, PgBenchQueries
from driftbench.data.tpcc import TPCCData, TPCCQueries
from driftbench.data.tpcc_skew import TPCCSkewData, TPCCSkewQueries
from driftbench.data.tpcds import TPCDSData, TPCDSQueries
from driftbench.data.tpch import TPCHData, TPCHQueries
from driftbench.data.ycsb import YCSBData, YCSBQueries


REPO_ROOT = Path(__file__).resolve().parents[1]


class AdapterSupportManifestTests(unittest.TestCase):
    def _tiny_files(
        self,
        adapter: object,
        out_dir: Path,
        *args: object,
    ) -> list[Path]:
        path = out_dir / "tiny.csv"
        path.write_text("id\n1\n", encoding="utf-8")
        return [path]

    def _assert_support(
        self,
        metadata: Path,
        benchmark: str,
        artifact_type: str,
        registry_mode: str | None = None,
    ) -> None:
        payload = json.loads(metadata.read_text(encoding="utf-8"))
        support = payload["support"]
        profile = get_support_profile(benchmark, artifact_type, registry_mode)
        self.assertEqual(support["contract_version"], SUPPORT_CONTRACT_VERSION)
        self.assertEqual(support["tier"], profile.tier)
        self.assertEqual(support["tier_name"], {
            0: "illustrative",
            1: "synthetic-conformant",
            2: "executable",
            3: "official-tool/spec-traceable",
        }[profile.tier])
        self.assertEqual(support["mode"], profile.mode)
        for count_group in ("official", "shipped"):
            self.assertEqual(
                set(support[count_group]),
                {"table_count", "query_count", "transaction_count"},
            )
            for value in support[count_group].values():
                self.assertTrue(value is None or isinstance(value, int))
        self.assertIn("TPC/YCSB", support["compliance_disclaimer"])

    def test_all_nine_adapters_generate_supported_data_and_query_manifests(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "tpch_source"
            source.mkdir()
            (source / "customer.tbl").write_text("1|customer|\n", encoding="utf-8")

            data_cases = [
                (TPCHData(source_dir=source), "tpch", "copy"),
                (TPCDSData(scale_factor=1), "tpcds", None),
                (TPCCData(scale_factor=1), "tpcc", None),
                (TPCCSkewData(scale_factor=1), "tpcc_skew", None),
                (JOBData(scale_factor=1), "job", None),
                (YCSBData(record_count=1), "ycsb", None),
                (DSBData(scale_factor=1), "dsb", None),
                (PgBenchData(scale_factor=1), "pgbench", None),
                (BenchBaseData(benchmark_name="tpcc"), "benchbase", None),
            ]
            query_cases = [
                (TPCHQueries(query_ids=[1]), "tpch", "qgen"),
                (TPCDSQueries(), "tpcds", None),
                (TPCCQueries(), "tpcc", None),
                (TPCCSkewQueries(), "tpcc_skew", None),
                (JOBQueries(), "job", None),
                (YCSBQueries(), "ycsb", None),
                (DSBQueries(), "dsb", None),
                (PgBenchQueries(), "pgbench", None),
                (BenchBaseQueries(benchmark_name="tpcc"), "benchbase", None),
            ]

            heavy_generators = [
                (TPCDSData, "_generate_synth"),
                (TPCCData, "_generate_synth"),
                (JOBData, "_generate_synth"),
                (YCSBData, "_generate_synth"),
                (DSBData, "_generate_synth"),
                (PgBenchData, "_generate_synth"),
            ]
            patches = [
                patch.object(owner, method, autospec=True, side_effect=self._tiny_files)
                for owner, method in heavy_generators
            ]
            for active_patch in patches:
                active_patch.start()
                self.addCleanup(active_patch.stop)

            for adapter, benchmark, mode in data_cases:
                with self.subTest(benchmark=benchmark, artifact_type="data"):
                    result = adapter.generate(output_dir=root / "artifacts")
                    self._assert_support(result.metadata, benchmark, "data", mode)

            for adapter, benchmark, mode in query_cases:
                with self.subTest(benchmark=benchmark, artifact_type="queries"):
                    result = adapter.generate(output_dir=root / "artifacts")
                    self._assert_support(result.metadata, benchmark, "queries", mode)

            benchbase_payload = json.loads(query_cases[-1][0].generate(
                output_dir=root / "artifacts"
            ).metadata.read_text(encoding="utf-8"))
            self.assertEqual(benchbase_payload["support"]["shipped"]["transaction_count"], 5)

    def test_existing_manifest_is_upgraded_without_rewriting_artifact(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            artifact = root / "ycsb" / "data" / "usertable.csv"
            artifact.parent.mkdir(parents=True)
            artifact.write_text("sentinel", encoding="utf-8")
            manifest = artifact.parent / "ycsb_data_manifest.json"
            manifest.write_text(
                json.dumps(
                    {
                        "benchmark": "ycsb",
                        "artifact_type": "data",
                        "files": ["ycsb/data/usertable.csv"],
                    }
                ),
                encoding="utf-8",
            )

            result = YCSBData(record_count=1).generate(output_dir=root)

            self.assertEqual(artifact.read_text(encoding="utf-8"), "sentinel")
            self.assertIn("support", json.loads(result.metadata.read_text(encoding="utf-8")))

    def test_unknown_adapter_manifest_fails_instead_of_omitting_support(self) -> None:
        artifact = BenchmarkArtifact()
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaisesRegex(KeyError, "No default support mode"):
                artifact._write_json(
                    Path(tmp) / "unknown_data_manifest.json",
                    {"benchmark": "unknown", "artifact_type": "data", "files": ["x"]},
                )


class AdapterSupportHelperTests(unittest.TestCase):
    def test_referential_integrity_accepts_scalar_and_composite_keys(self) -> None:
        assert_referential_integrity(
            [{"parent_id": 1}, {"parent_id": None}],
            [{"id": 1}],
            child_key="parent_id",
            parent_key="id",
        )
        assert_referential_integrity(
            [{"warehouse": 1, "district": 2}],
            [{"w": 1, "d": 2}],
            child_key=("warehouse", "district"),
            parent_key=("w", "d"),
        )

    def test_referential_integrity_reports_orphans_and_bad_columns(self) -> None:
        with self.assertRaisesRegex(AssertionError, "orders_customer.*orphan"):
            assert_referential_integrity(
                [{"customer_id": 2}],
                [{"id": 1}],
                child_key="customer_id",
                parent_key="id",
                relationship="orders_customer",
            )
        with self.assertRaisesRegex(KeyError, "missing key column"):
            assert_referential_integrity(
                [{"wrong": 1}],
                [{"id": 1}],
                child_key="customer_id",
                parent_key="id",
            )

    def test_row_count_law_success_and_failures(self) -> None:
        assert_row_count_law(20, 10 * 2, label="district")
        with self.assertRaisesRegex(AssertionError, "expected 20, got 19"):
            assert_row_count_law(19, 20, label="district")
        with self.assertRaisesRegex(TypeError, "actual_count must be an integer"):
            assert_row_count_law(19.0, 20)
        with self.assertRaisesRegex(ValueError, "expected_count must be non-negative"):
            assert_row_count_law(19, -1)

    def test_optional_binary_discovery_present_and_missing_without_network(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            candidate = Path(tmp) / "tool"
            candidate.write_text("binary", encoding="utf-8")
            with patch("driftbench.data.base.shutil.which", return_value=None):
                self.assertEqual(
                    find_optional_binary("tool", candidate_paths=[candidate]),
                    candidate.resolve(),
                )
                self.assertIsNone(
                    find_optional_binary("missing", candidate_paths=[Path(tmp) / "absent"])
                )

    def test_optional_binary_prefers_path(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path_binary = Path(tmp) / "path-tool"
            path_binary.write_text("binary", encoding="utf-8")
            with patch(
                "driftbench.data.base.shutil.which",
                return_value=str(path_binary),
            ):
                self.assertEqual(
                    find_optional_binary(("tool.exe", "tool")),
                    path_binary.resolve(),
                )


class AdapterSupportDocumentationTests(unittest.TestCase):
    def test_contract_registry_table_matches_code(self) -> None:
        text = (REPO_ROOT / "docs" / "adapter_support_contract.md").read_text(
            encoding="utf-8"
        )
        row_pattern = re.compile(
            r"^\| `(?P<benchmark>[^`]+)` \| "
            r"(?P<artifact>data|queries) \| "
            r"(?P<registry>`[^`]+`|default) \| "
            r"(?P<tier>[0-3]) \| `(?P<mode>[^`]+)` \|",
            re.MULTILINE,
        )
        documented = {}
        for match in row_pattern.finditer(text):
            registry = match.group("registry")
            registry_mode = None if registry == "default" else registry.strip("`")
            documented[
                (match.group("benchmark"), match.group("artifact"), registry_mode)
            ] = (int(match.group("tier")), match.group("mode"))

        expected = {
            key: (profile.tier, profile.mode)
            for key, profile in SUPPORT_PROFILES.items()
        }
        self.assertEqual(documented, expected)

    def test_benchmark_reference_covers_all_adapters_and_registered_claims(self) -> None:
        text = (REPO_ROOT / "docs" / "benchmark_reference.md").read_text(
            encoding="utf-8"
        )
        self.assertIn("all 9 benchmark adapters", text)
        for heading in (
            "TPC-H",
            "TPC-DS",
            "TPC-C",
            "TPC-C Skew",
            "JOB (Join Order Benchmark)",
            "YCSB",
            "DSB",
            "pgbench",
            "BenchBase",
        ):
            self.assertIn(f"## {heading}", text)
        for profile in SUPPORT_PROFILES.values():
            self.assertIn(f"Tier {profile.tier}", text)
            self.assertIn(f"`{profile.mode}`", text)


if __name__ == "__main__":
    unittest.main()
