import tempfile
import unittest
from pathlib import Path

from driftbench.orchestrate import load_benchmark_targets, orchestrate_targets


REPO_ROOT = Path(__file__).resolve().parents[1]


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


if __name__ == "__main__":
    unittest.main()

