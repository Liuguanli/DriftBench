import importlib
import json
import os
import shutil
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
VALID_SPEC = REPO_ROOT / "driftspec" / "examples" / "demo_data_single.yaml"


class PublicSpecsServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp_root = REPO_ROOT / "tmp" / "public_specs_service_test"
        self.shared_dir = self.tmp_root / "shared_specs"
        self.state_dir = self.tmp_root / "state"
        self.catalog_path = self.state_dir / "public_specs_catalog.json"
        self.tmp_root.mkdir(parents=True, exist_ok=True)
        self.shared_dir.mkdir(parents=True, exist_ok=True)
        self.state_dir.mkdir(parents=True, exist_ok=True)

        os.environ["DRIFTBENCH_MCP_SHARED_SPECS_DIR"] = str(self.shared_dir.relative_to(REPO_ROOT))
        os.environ["DRIFTBENCH_MCP_CATALOG_PATH"] = str(self.catalog_path.relative_to(REPO_ROOT))

        # Reload module so it picks up test-specific env vars.
        import driftbench_service.server as service_module

        self.svc = importlib.reload(service_module)

        shared_spec = self.shared_dir / "demo-public-spec.yaml"
        shutil.copy2(VALID_SPEC, shared_spec)
        catalog_payload = {
            "version": 1,
            "updated_at": "2026-05-08T00:00:00Z",
            "specs": [
                {
                    "id": "demo-public-spec",
                    "title": "Demo Public Spec",
                    "description": "shared spec for endpoint tests",
                    "tags": ["demo", "test"],
                    "owner": "ci",
                    "shared_path": shared_spec.relative_to(REPO_ROOT).as_posix(),
                    "updated_at": "2026-05-08T00:00:00Z",
                }
            ],
        }
        self.catalog_path.write_text(json.dumps(catalog_payload, ensure_ascii=False, indent=2), encoding="utf-8")

    def tearDown(self) -> None:
        os.environ.pop("DRIFTBENCH_MCP_SHARED_SPECS_DIR", None)
        os.environ.pop("DRIFTBENCH_MCP_CATALOG_PATH", None)
        if self.tmp_root.exists():
            shutil.rmtree(self.tmp_root)

    def test_get_public_specs(self) -> None:
        specs = self.svc.list_public_specs(tag="demo", limit=100)
        self.assertEqual(len(specs), 1)
        self.assertEqual(specs[0]["id"], "demo-public-spec")
        self.assertEqual(specs[0]["spec_version"], 1)

    def test_import_run_execute_false(self) -> None:
        target_path = "tmp/public_specs_service_test/imported/imported_from_public.yaml"
        data = self.svc.import_public_spec(
            spec_id="demo-public-spec",
            target_path=target_path,
            overwrite=True,
        )
        imported = REPO_ROOT / data["imported_spec_path"]
        self.assertTrue(imported.exists())


if __name__ == "__main__":
    unittest.main()
