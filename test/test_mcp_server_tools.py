import os
import shutil
import unittest
from pathlib import Path

from driftbench_mcp.server import ToolExecutionError, call_tool


REPO_ROOT = Path(__file__).resolve().parents[1]
VALID_SPEC = "driftspec/examples/demo_data_single.yaml"
TRACE_INPUT = "driftspec/trace_inputs/trace_data_mock.csv"


class DriftbenchMCPToolTests(unittest.TestCase):
    def setUp(self) -> None:
        self.catalog_root = REPO_ROOT / "tmp" / "mcp_catalog_test"
        self.shared_dir = self.catalog_root / "shared_specs"
        self.catalog_file = self.catalog_root / "public_specs_catalog.json"
        self.catalog_root.mkdir(parents=True, exist_ok=True)
        os.environ["DRIFTBENCH_MCP_CATALOG_PATH"] = str(self.catalog_file.relative_to(REPO_ROOT))
        os.environ["DRIFTBENCH_MCP_SHARED_SPECS_DIR"] = str(self.shared_dir.relative_to(REPO_ROOT))

    def tearDown(self) -> None:
        os.environ.pop("DRIFTBENCH_MCP_CATALOG_PATH", None)
        os.environ.pop("DRIFTBENCH_MCP_SHARED_SPECS_DIR", None)
        if self.catalog_root.exists():
            shutil.rmtree(self.catalog_root)

    def test_health(self) -> None:
        result = call_tool("driftbench_health", {})
        self.assertTrue(result["ok"])
        self.assertIn("repo_root", result)

    def test_validate_spec(self) -> None:
        result = call_tool("validate_spec", {"spec_path": VALID_SPEC})
        self.assertTrue(result["ok"])
        self.assertIn("type", result)
        self.assertIsInstance(result["declared_outputs"], list)

    def test_dry_run_spec(self) -> None:
        result = call_tool("dry_run_spec", {"spec_path": VALID_SPEC})
        self.assertTrue(result["ok"])
        self.assertTrue(result["would_execute"])
        self.assertIn("variables_keys", result)

    def test_list_outputs(self) -> None:
        result = call_tool("list_outputs", {"root": "output", "limit": 5})
        self.assertTrue(result["ok"])
        self.assertLessEqual(result["count"], 5)

    def test_trace_to_spec(self) -> None:
        out_dir = REPO_ROOT / "tmp" / "mcp_test_specs"
        out_file = out_dir / "generated_trace.yaml"
        out_dir.mkdir(parents=True, exist_ok=True)
        try:
            result = call_tool(
                "trace_to_spec",
                {
                    "trace_path": TRACE_INPUT,
                    "output_path": str(out_file.relative_to(REPO_ROOT)),
                },
            )
            self.assertTrue(result["ok"])
            self.assertTrue(out_file.exists())
        finally:
            if out_dir.exists():
                shutil.rmtree(out_dir)

    def test_unknown_tool_raises(self) -> None:
        with self.assertRaises(ToolExecutionError):
            call_tool("does_not_exist", {})

    def test_save_and_list_public_specs(self) -> None:
        saved = call_tool(
            "save_spec",
            {
                "spec_path": VALID_SPEC,
                "spec_id": "demo-public-spec",
                "title": "Demo Public Spec",
                "description": "shared for tests",
                "tags": ["demo", "test"],
                "owner": "ci",
            },
        )
        self.assertTrue(saved["ok"])
        self.assertEqual(saved["spec"]["id"], "demo-public-spec")
        self.assertTrue((self.shared_dir / "demo-public-spec.yaml").exists())
        self.assertTrue(self.catalog_file.exists())

        listed = call_tool("list_public_specs", {"tag": "demo"})
        self.assertTrue(listed["ok"])
        self.assertEqual(listed["count"], 1)
        self.assertEqual(listed["specs"][0]["id"], "demo-public-spec")

    def test_import_spec_and_run_execute_false(self) -> None:
        call_tool(
            "save_spec",
            {
                "spec_path": VALID_SPEC,
                "spec_id": "importable-spec",
                "title": "Importable Spec",
                "tags": ["import"],
            },
        )
        imported = call_tool(
            "import_spec_and_run",
            {
                "spec_id": "importable-spec",
                "target_path": "tmp/imported_specs/imported-spec.yaml",
                "execute": False,
                "overwrite": True,
            },
        )
        self.assertTrue(imported["ok"])
        self.assertEqual(imported["run"]["executed"], False)
        imported_path = REPO_ROOT / imported["imported_spec_path"]
        self.assertTrue(imported_path.exists())
        imported_path.unlink(missing_ok=True)


if __name__ == "__main__":
    unittest.main()
