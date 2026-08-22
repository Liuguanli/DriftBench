import json
import tempfile
import unittest
from pathlib import Path

from driftbench.orchestrate import load_benchmark_targets, orchestrate_targets
from ..helpers import REPO_ROOT


class OrchestrateTargetsExamplesTests(unittest.TestCase):
    def test_tpch_adapter_demo_targets_load(self) -> None:
        cfg = REPO_ROOT / "driftspec" / "examples" / "adapters" / "benchmark_target_tpch_demo.yaml"
        targets = load_benchmark_targets(cfg)
        self.assertEqual(len(targets), 1)
        t = targets[0]
        self.assertEqual(t.name, "tpch-adapter-demo")
        self.assertTrue(t.workdir.exists())
        self.assertIn("queries/*.sql", t.output_globs)

    def test_trace_adapter_demo_targets_load(self) -> None:
        cfg = REPO_ROOT / "driftspec" / "examples" / "adapters" / "benchmark_target_trace_demo.yaml"
        targets = load_benchmark_targets(cfg)
        self.assertEqual(len(targets), 1)
        t = targets[0]
        self.assertEqual(t.name, "trace-adapter-demo")
        self.assertTrue(t.workdir.exists())
        self.assertIn("*.csv", t.output_globs)

    def test_demos_are_orchestratable_in_plan_mode(self) -> None:
        spec = REPO_ROOT / "driftspec" / "examples" / "demo_data_single.yaml"
        tpch_cfg = REPO_ROOT / "driftspec" / "examples" / "adapters" / "benchmark_target_tpch_demo.yaml"
        trace_cfg = REPO_ROOT / "driftspec" / "examples" / "adapters" / "benchmark_target_trace_demo.yaml"
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            m1 = tmp / "tpch_manifest.json"
            m2 = tmp / "trace_manifest.json"
            out1 = orchestrate_targets(spec_path=spec, targets_file=tpch_cfg, manifest_path=m1, execute=False)
            out2 = orchestrate_targets(spec_path=spec, targets_file=trace_cfg, manifest_path=m2, execute=False)
            self.assertEqual(out1["summary"]["total_targets"], 1)
            self.assertEqual(out2["summary"]["total_targets"], 1)
            self.assertEqual(out1["summary"]["planned"], 1)
            self.assertEqual(out2["summary"]["planned"], 1)
            self.assertTrue(m1.exists())
            self.assertTrue(m2.exists())

    def test_local_example_executes_successfully(self) -> None:
        spec = REPO_ROOT / "driftspec" / "examples" / "demo_data_single.yaml"
        cfg = (
            REPO_ROOT
            / "driftspec"
            / "examples"
            / "adapters"
            / "benchmark_target_local_example.yaml"
        )

        targets = load_benchmark_targets(cfg)
        self.assertEqual(len(targets), 1)
        self.assertEqual(targets[0].name, "driftbench-self-check")
        self.assertEqual(targets[0].workdir, REPO_ROOT.resolve())

        with tempfile.TemporaryDirectory() as tmpdir:
            manifest_path = Path(tmpdir) / "local_example_manifest.json"
            result = orchestrate_targets(
                spec_path=spec,
                targets_file=cfg,
                manifest_path=manifest_path,
                execute=True,
            )

            self.assertTrue(result["ok"])
            self.assertEqual(result["outcome"], "completed")
            self.assertTrue(result["execute"])
            self.assertEqual(result["summary"]["total_targets"], 1)
            self.assertEqual(result["summary"]["completed"], 1)
            self.assertEqual(result["summary"]["failed"], 0)
            self.assertEqual(result["summary"]["planned"], 0)
            self.assertGreaterEqual(result["summary"]["duration_seconds"], 0)

            self.assertEqual(len(result["targets"]), 1)
            target_result = result["targets"][0]
            self.assertEqual(target_result["target"], "driftbench-self-check")
            self.assertEqual(target_result["status"], "completed")
            self.assertIsNone(target_result["setup"])
            self.assertEqual(target_result["artifacts"], [])
            self.assertEqual(target_result["run"]["returncode"], 0)

            command_result = json.loads(target_result["run"]["stdout"])
            self.assertTrue(command_result["ok"])
            self.assertEqual(command_result["command"], "validate-spec")
            self.assertEqual(Path(command_result["spec_path"]), spec.resolve())

            self.assertTrue(manifest_path.exists())
            persisted = json.loads(manifest_path.read_text(encoding="utf-8"))
            self.assertTrue(persisted["ok"])
            self.assertEqual(persisted["outcome"], "completed")
            self.assertEqual(persisted["targets"][0]["status"], "completed")


if __name__ == "__main__":
    unittest.main()
