import hashlib
import json
import os
from contextlib import ExitStack
from pathlib import Path
import re
import socket
import subprocess
import tempfile
import unittest
from unittest import mock
import urllib.request

import yaml

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - Python 3.10
    import tomli as tomllib

from driftbench.data.dsb import queries as dsb_queries
from driftbench.data.job import queries as job_queries
from driftbench.data.pgbench import queries as pgbench_queries
from driftbench.data.tpcc import queries as tpcc_queries
from driftbench.data.tpcc_skew import queries as tpcc_skew_queries
from driftbench.data.tpcds import queries as tpcds_queries
from driftbench.data.tpch import queries as tpch_queries
from driftbench.data.ycsb import queries as ycsb_queries

from ..helpers import REPO_ROOT


AGENT_NAMES = (
    "user_industry_vendor",
    "user_newcomer",
    "user_researcher",
)
SKILL_NAME = "driftbench-real-user-validation"
SKILL_ROOT = REPO_ROOT / ".agents" / "skills" / SKILL_NAME
WORKFLOW = SKILL_ROOT / "references" / "workflow.md"

JOB_FILES = {
    "1a_keyword_filter.sql",
    "2a_company_movies.sql",
    "3a_movie_info_filter.sql",
    "4a_cast_keyword.sql",
    "5a_company_country_cast.sql",
    "6a_info_company.sql",
    "7a_keyword_count.sql",
    "8a_actor_productivity.sql",
    "9a_multi_keyword_movie.sql",
    "10a_full_join.sql",
    "11a_company_keyword_year.sql",
    "12a_cast_info_selective.sql",
    "13a_movie_info_aggregate.sql",
    "14a_company_info_cast.sql",
    "15a_keyword_year_range.sql",
    "16a_actor_company.sql",
    "17a_selective_cast_keyword.sql",
    "18a_movie_info_keyword.sql",
    "19a_company_output_volume.sql",
    "20a_full_eight_table.sql",
    "job_all_queries.sql",
}


def _source_snapshot() -> dict[str, str]:
    source_root = REPO_ROOT / "driftbench" / "data"
    return {
        path.relative_to(REPO_ROOT).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted(source_root.glob("*.py"))
    }


def _worktree_snapshot() -> str:
    completed = subprocess.run(
        ["git", "status", "--porcelain=v1", "--untracked-files=all"],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    return completed.stdout


def _is_ignored(relative_path: str) -> bool:
    completed = subprocess.run(
        ["git", "check-ignore", "--no-index", "-q", relative_path],
        cwd=REPO_ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode not in (0, 1):
        raise AssertionError(completed.stderr)
    return completed.returncode == 0


def _assert_contained(test: unittest.TestCase, path: Path, root: Path) -> Path:
    resolved = Path(path).resolve()
    try:
        resolved.relative_to(root)
    except ValueError:
        test.fail(f"Path escaped owned temporary directory: {resolved}")
    return resolved


class RealUserAgentStaticContractTests(unittest.TestCase):
    def test_exactly_three_project_agents_are_safe_and_distinct(self) -> None:
        agent_dir = REPO_ROOT / ".codex" / "agents"
        agent_files = sorted(agent_dir.glob("*.toml"))
        self.assertEqual([path.stem for path in agent_files], list(AGENT_NAMES))

        parsed = [tomllib.loads(path.read_text(encoding="utf-8")) for path in agent_files]
        self.assertEqual({item["name"] for item in parsed}, set(AGENT_NAMES))
        self.assertEqual(len({item["name"] for item in parsed}), 3)

        forbidden_keys = {"model", "mcp_servers", "approval_policy", "network_access"}
        for path, config in zip(agent_files, parsed):
            with self.subTest(agent=path.stem):
                self.assertTrue({"name", "description", "developer_instructions"} <= config.keys())
                self.assertEqual(config["sandbox_mode"], "workspace-write")
                self.assertTrue(forbidden_keys.isdisjoint(config))
                instructions = config["developer_instructions"]
                self.assertIn(f"${SKILL_NAME}", instructions)
                self.assertIn("BLOCKED", instructions)
                self.assertIn("Do not edit product code", instructions)
                self.assertIn("architecture", instructions)
                self.assertIn("test/repro", instructions)
                self.assertIn("final-integration", instructions)
                self.assertIn("advisory", instructions.lower())
                self.assertNotRegex(instructions, r"[A-Za-z]:\\\\|/(?:home|Users)/")

        instructions_by_name = {
            config["name"]: config["developer_instructions"].lower() for config in parsed
        }
        self.assertIn("reproducibility", instructions_by_name["user_researcher"])
        self.assertIn("provenance", instructions_by_name["user_researcher"])
        self.assertIn("machine-readable", instructions_by_name["user_industry_vendor"])
        self.assertIn("operational safety", instructions_by_name["user_industry_vendor"])
        self.assertIn("documented entry points", instructions_by_name["user_newcomer"])
        self.assertIn("copy-paste", instructions_by_name["user_newcomer"])

    def test_skill_metadata_and_instruction_only_layout(self) -> None:
        skill_text = (SKILL_ROOT / "SKILL.md").read_text(encoding="utf-8")
        _, frontmatter_text, body = skill_text.split("---", 2)
        frontmatter = yaml.safe_load(frontmatter_text)
        self.assertEqual(frontmatter["name"], SKILL_NAME)
        self.assertIn("eight local query/workload artifact generators", frontmatter["description"])
        self.assertIn("references/workflow.md", body)
        self.assertIn("PASS", body)
        self.assertIn("FAIL", body)
        self.assertIn("BLOCKED", body)
        self.assertIn("Never say \"all benchmarks validated.\"", body)
        self.assertIn("all eight query/workload artifact generators succeeded", body)
        self.assertIn("cannot replace the architecture gate", body)

        actual_files = {
            path.relative_to(SKILL_ROOT).as_posix()
            for path in SKILL_ROOT.rglob("*")
            if path.is_file()
        }
        self.assertEqual(
            actual_files,
            {"SKILL.md", "agents/openai.yaml", "references/workflow.md"},
        )
        self.assertFalse((SKILL_ROOT / "scripts").exists())

        metadata = yaml.safe_load(
            (SKILL_ROOT / "agents" / "openai.yaml").read_text(encoding="utf-8")
        )
        self.assertEqual(set(metadata), {"interface"})
        interface = metadata["interface"]
        self.assertEqual(
            set(interface), {"display_name", "short_description", "default_prompt"}
        )
        self.assertTrue(interface["display_name"].strip())
        self.assertGreaterEqual(len(interface["short_description"]), 25)
        self.assertLessEqual(len(interface["short_description"]), 64)
        self.assertIn(f"${SKILL_NAME}", interface["default_prompt"])

    def test_workflow_has_fixed_calls_evidence_and_claim_boundaries(self) -> None:
        workflow = WORKFLOW.read_text(encoding="utf-8")
        for benchmark in ("TPC-H", "TPC-DS", "TPC-C", "TPC-C Skew", "JOB", "YCSB", "DSB", "pgbench"):
            with self.subTest(benchmark=benchmark):
                self.assertIn(f"| {benchmark} |", workflow)

        exact_calls = (
            'queries(query_ids=[1, 6], queries_per_template=1, mode="qgen", seed=42, shuffle=False)',
            "from driftbench.data.tpcds import queries",
            "from driftbench.data.tpcc import queries",
            "from driftbench.data.tpcc_skew import queries",
            "from driftbench.data.job import queries",
            'queries(workload="B", run_seconds=1, target_rate=10)',
            "from driftbench.data.dsb import queries",
            'queries(workload="select_only", clients=1, duration=1, rate=0)',
            ".generate(output_dir=<owned-temp>/<id>, force=False)",
        )
        for expected in exact_calls:
            self.assertIn(expected, workflow)

        for expected in (
            "Overall status",
            "Source state",
            "Safety facts",
            "Benchmark results",
            "Persona observations",
            "Limitations",
            "expected artifacts",
            "observed artifacts",
            "all eight benchmark rows",
            "Never report “all benchmarks validated.”",
            "database execution",
            "official benchmark conformance",
        ):
            self.assertIn(expected, workflow)

    def test_gitignore_exposes_only_approved_agents_and_skill(self) -> None:
        visible = (
            ".codex/agents/user_researcher.toml",
            ".codex/agents/user_industry_vendor.toml",
            ".codex/agents/user_newcomer.toml",
            ".agents/skills/driftbench-real-user-validation/SKILL.md",
            ".agents/skills/driftbench-real-user-validation/references/workflow.md",
            ".agents/skills/driftbench-real-user-validation/agents/openai.yaml",
        )
        hidden = (
            ".codex/config.toml",
            ".codex/agents/unrelated.toml",
            ".agents/skills/unrelated/SKILL.md",
            "agents/unrelated.toml",
            "skills/unrelated/SKILL.md",
        )
        for path in visible:
            with self.subTest(visible=path):
                self.assertFalse(_is_ignored(path))
        for path in hidden:
            with self.subTest(hidden=path):
                self.assertTrue(_is_ignored(path))


class RealUserAgentSmokeContractTests(unittest.TestCase):
    def _cases(self):
        tpcc_files = {
            "delivery.sql",
            "new_order.sql",
            "order_status.sql",
            "payment.sql",
            "stock_level.sql",
            "tpcc_all_transactions.sql",
        }
        tpcc_skew_files = (tpcc_files - {"tpcc_all_transactions.sql"}) | {
            "tpcc_skew_all_transactions.sql"
        }
        return (
            {
                "name": "tpch",
                "factory": lambda: tpch_queries(
                    query_ids=[1, 6], queries_per_template=1, mode="qgen", seed=42, shuffle=False
                ),
                "files": {"tpch_queries.csv", "tpch_queries.sql"},
                "metadata": "tpch_queries_manifest.json",
                "markers": {
                    "tpch_queries.csv": ("query_id,index,sql",),
                    "tpch_queries.sql": ("-- TPCH Q1 instance 1", "-- TPCH Q6 instance 1"),
                },
            },
            {
                "name": "tpcds",
                "factory": tpcds_queries,
                "files": {"query_ids.txt", "sample_tpcds_config.xml"},
                "metadata": "tpcds_queries_manifest.json",
                "markers": {
                    "query_ids.txt": ("query01", "query99"),
                    "sample_tpcds_config.xml": ("<parameters>", "<type>POSTGRES</type>"),
                },
            },
            {
                "name": "tpcc",
                "factory": tpcc_queries,
                "files": tpcc_files,
                "metadata": "tpcc_queries_manifest.json",
                "markers": {
                    "delivery.sql": ("TPC-C Transaction 4",),
                    "new_order.sql": ("TPC-C Transaction 1",),
                    "order_status.sql": ("TPC-C Transaction 3",),
                    "payment.sql": ("TPC-C Transaction 2",),
                    "stock_level.sql": ("TPC-C Transaction 5",),
                    "tpcc_all_transactions.sql": ("-- === NEW_ORDER ===",),
                },
            },
            {
                "name": "tpcc_skew",
                "factory": lambda: tpcc_skew_queries(
                    scale_factor=2, hot_warehouse_fraction=.5, skew_factor=.99
                ),
                "files": tpcc_skew_files,
                "metadata": "tpcc_skew_queries_manifest.json",
                "markers": {
                    name: ("TPC-C Skew: Zipf alpha=0.99",)
                    for name in tpcc_skew_files
                }
                | {"tpcc_skew_all_transactions.sql": ("-- === NEW_ORDER ===",)},
            },
            {
                "name": "job",
                "factory": job_queries,
                "files": JOB_FILES,
                "metadata": "job_queries_manifest.json",
                "markers": {
                    name: ("-- JOB",) for name in JOB_FILES if name != "job_all_queries.sql"
                }
                | {
                    "job_all_queries.sql": (
                        "-- === 1A_KEYWORD_FILTER ===",
                        "-- === 20A_FULL_EIGHT_TABLE ===",
                    )
                },
            },
            {
                "name": "ycsb",
                "factory": lambda: ycsb_queries(workload="B", run_seconds=1, target_rate=10),
                "files": {"sample_ycsb_config.xml", "workload_b.properties"},
                "metadata": "ycsb_queries_manifest.json",
                "markers": {
                    "sample_ycsb_config.xml": ("<parameters>",),
                    "workload_b.properties": ("maxexecutiontime=1", "target=10"),
                },
            },
            {
                "name": "dsb",
                "factory": dsb_queries,
                "files": {
                    "q1_revenue_by_year.sql",
                    "q2_revenue_by_region.sql",
                    "q3_margin_trend.sql",
                },
                "metadata": "dsb_queries_manifest.json",
                "markers": {
                    "q1_revenue_by_year.sql": ("SUM(lo.revenue)",),
                    "q2_revenue_by_region.sql": ("c.region",),
                    "q3_margin_trend.sql": ("lo.revenue - lo.supply_cost",),
                },
            },
            {
                "name": "pgbench",
                "factory": lambda: pgbench_queries(
                    workload="select_only", clients=1, duration=1, rate=0
                ),
                "files": {"pgbench_select_only.sql", "run_pgbench.sh"},
                "metadata": "pgbench_queries_manifest.json",
                "markers": {
                    "pgbench_select_only.sql": ("pgbench select-only transaction",),
                    "run_pgbench.sh": ("#!/usr/bin/env bash", "pgbench -c 1 -T 1"),
                },
            },
        )

    def test_all_eight_public_query_generators_are_safe_and_ready(self) -> None:
        before_worktree = _worktree_snapshot()
        before_sources = _source_snapshot()
        temp_root = None

        try:
            with tempfile.TemporaryDirectory(prefix="driftbench-real-user-") as temporary:
                temp_root = Path(temporary).resolve()
                self.assertFalse(temp_root == REPO_ROOT or REPO_ROOT in temp_root.parents)

                for case in self._cases():
                    with self.subTest(benchmark=case["name"]):
                        artifact = case["factory"]()
                        self.assertEqual(artifact.artifact_type, "queries")

                        blocked = AssertionError(
                            f"{case['name']} attempted a forbidden external action"
                        )
                        with ExitStack() as guards:
                            for target in (
                                "subprocess.run",
                                "subprocess.Popen",
                                "subprocess.call",
                                "subprocess.check_call",
                                "subprocess.check_output",
                                "os.system",
                                "socket.create_connection",
                                "urllib.request.urlopen",
                            ):
                                guards.enter_context(mock.patch(target, side_effect=blocked))
                            result = artifact.generate(
                                output_dir=temp_root / case["name"], force=False
                            )

                        self.assertEqual(result.artifact_type, "queries")
                        _assert_contained(self, result.output_dir, temp_root)
                        metadata_path = _assert_contained(self, result.metadata, temp_root)
                        self.assertEqual(metadata_path.name, case["metadata"])
                        self.assertTrue(metadata_path.is_file())

                        managed = {
                            _assert_contained(self, path, temp_root).name: Path(path).resolve()
                            for path in result.files
                        }
                        self.assertEqual(set(managed), case["files"])
                        self.assertEqual(len(result.files), len(case["files"]))

                        manifest = json.loads(metadata_path.read_text(encoding="utf-8"))
                        self.assertEqual(manifest["benchmark"], case["name"])
                        self.assertEqual(manifest["artifact_type"], "queries")
                        self.assertEqual(
                            {Path(path).name for path in manifest["files"]}, case["files"]
                        )
                        cache = manifest["cache"]
                        self.assertEqual(cache["schema"], "driftbench.benchmark-cache")
                        self.assertEqual(cache["version"], 3)
                        self.assertRegex(cache["fingerprint"], r"^[0-9a-f]{64}$")
                        canonical_cache = {
                            key: cache[key]
                            for key in ("schema", "version", "generator", "parameters")
                        }
                        canonical_bytes = json.dumps(
                            canonical_cache,
                            ensure_ascii=True,
                            allow_nan=False,
                            separators=(",", ":"),
                            sort_keys=True,
                        ).encode("utf-8")
                        self.assertEqual(
                            cache["fingerprint"], hashlib.sha256(canonical_bytes).hexdigest()
                        )
                        if case["name"] == "tpch":
                            self.assertEqual(cache["parameters"]["query_ids"], ["1", "6"])
                            self.assertEqual(cache["parameters"]["mode"], "qgen")
                            self.assertEqual(cache["parameters"]["queries_per_template"], 1)
                            self.assertEqual(cache["parameters"]["seed"], 42)
                            self.assertIs(cache["parameters"]["shuffle"], False)

                        manifest_paths = {
                            _assert_contained(
                                self, Path(result.output_dir) / relative_path, temp_root
                            )
                            for relative_path in manifest["files"]
                        }
                        self.assertEqual(manifest_paths, set(managed.values()))
                        descriptors = {
                            Path(item["path"]).name: item for item in cache["artifacts"]
                        }
                        self.assertEqual(set(descriptors), case["files"])
                        self.assertEqual(
                            manifest["files"], [item["path"] for item in cache["artifacts"]]
                        )

                        for name, path in managed.items():
                            descriptor = descriptors[name]
                            payload = path.read_bytes()
                            self.assertEqual(descriptor["bytes"], len(payload))
                            self.assertEqual(
                                descriptor["sha256"], hashlib.sha256(payload).hexdigest()
                            )
                            text = payload.decode("utf-8")
                            for marker in case["markers"][name]:
                                self.assertIn(marker, text)
        finally:
            after_sources = _source_snapshot()
            after_worktree = _worktree_snapshot()

        self.assertIsNotNone(temp_root)
        self.assertFalse(temp_root.exists())
        self.assertEqual(after_sources, before_sources)
        self.assertEqual(after_worktree, before_worktree)


if __name__ == "__main__":
    unittest.main()
