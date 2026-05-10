import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
VALID_SPEC = "driftspec/examples/demo_data_single.yaml"
TRACE_INPUT = "driftspec/trace_inputs/trace_data_mock.csv"


def run_cli(*args: str) -> subprocess.CompletedProcess:
    cmd = [sys.executable, "-m", "driftbench.cli", *args]
    return subprocess.run(
        cmd,
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
        check=False,
    )


class DriftbenchCLITests(unittest.TestCase):
    def test_validate_spec_json_success(self) -> None:
        proc = run_cli("validate-spec", VALID_SPEC, "--json")
        self.assertEqual(proc.returncode, 0, msg=proc.stderr)
        payload = json.loads(proc.stdout)
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["command"], "validate-spec")
        self.assertIn("type", payload)

    def test_dry_run_json_success(self) -> None:
        proc = run_cli("dry-run", VALID_SPEC, "--json")
        self.assertEqual(proc.returncode, 0, msg=proc.stderr)
        payload = json.loads(proc.stdout)
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["command"], "dry-run")
        self.assertIn("declared_outputs", payload)
        self.assertIn("would_execute", payload)

    def test_list_outputs_json_success(self) -> None:
        proc = run_cli("list-outputs", "--root", "output", "--limit", "5", "--json")
        self.assertEqual(proc.returncode, 0, msg=proc.stderr)
        payload = json.loads(proc.stdout)
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["command"], "list-outputs")
        self.assertLessEqual(payload["count"], 5)
        self.assertIsInstance(payload["paths"], list)

    def test_validate_spec_missing_file_returns_validation_error(self) -> None:
        proc = run_cli("validate-spec", "driftspec/examples/does_not_exist.yaml")
        self.assertEqual(proc.returncode, 3)
        self.assertIn("[VALIDATION ERROR]", proc.stderr)

    def test_trace_to_spec_success(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            out_path = Path(tmpdir) / "generated_from_trace.yaml"
            proc = run_cli("trace-to-spec", TRACE_INPUT, str(out_path))
            self.assertEqual(proc.returncode, 0, msg=proc.stderr)
            self.assertTrue(out_path.exists())

    def test_trace_to_spec_with_output_flag_success(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            out_path = Path(tmpdir) / "generated_from_trace_flag.yaml"
            proc = run_cli("trace-to-spec", TRACE_INPUT, "--output", str(out_path))
            self.assertEqual(proc.returncode, 0, msg=proc.stderr)
            self.assertTrue(out_path.exists())

    def test_init_agent_dry_run(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            out_dir = Path(tmpdir) / "driftbench-agent"
            proc = run_cli("init-agent", "--output", str(out_dir), "--dry-run")
            self.assertEqual(proc.returncode, 0, msg=proc.stderr)
            self.assertIn("[DRY-RUN]", proc.stdout)
            self.assertIn("AGENTS.md", proc.stdout)
            self.assertFalse(out_dir.exists())

    def test_init_agent_generates_expected_structure(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            out_dir = Path(tmpdir) / "driftbench-agent"
            proc = run_cli("init-agent", "--output", str(out_dir))
            self.assertEqual(proc.returncode, 0, msg=proc.stderr)
            self.assertTrue((out_dir / "AGENTS.md").exists())
            self.assertTrue((out_dir / "README.md").exists())
            self.assertTrue((out_dir / "skills" / "driftbench" / "SKILL.md").exists())
            self.assertTrue((out_dir / "references" / "cli-commands.md").exists())
            self.assertTrue((out_dir / "references" / "spec-guidelines.md").exists())
            self.assertTrue((out_dir / "examples" / "workload-drift.yaml").exists())
            self.assertTrue((out_dir / "examples" / "data-drift.yaml").exists())

    def test_init_agent_non_empty_dir_requires_force(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            out_dir = Path(tmpdir) / "driftbench-agent"
            out_dir.mkdir(parents=True, exist_ok=True)
            (out_dir / "custom.txt").write_text("hello", encoding="utf-8")
            proc = run_cli("init-agent", "--output", str(out_dir))
            self.assertEqual(proc.returncode, 3)
            self.assertIn("Output directory already exists and is not empty", proc.stderr)

    def test_init_agent_force_overwrites_managed_files_only(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            out_dir = Path(tmpdir) / "driftbench-agent"
            proc1 = run_cli("init-agent", "--output", str(out_dir))
            self.assertEqual(proc1.returncode, 0, msg=proc1.stderr)

            agents_path = out_dir / "AGENTS.md"
            agents_path.write_text("modified", encoding="utf-8")
            custom_path = out_dir / "custom.txt"
            custom_path.write_text("keep-me", encoding="utf-8")

            proc2 = run_cli("init-agent", "--output", str(out_dir), "--force")
            self.assertEqual(proc2.returncode, 0, msg=proc2.stderr)

            agents_text = agents_path.read_text(encoding="utf-8")
            self.assertIn("DriftBench-specific guidance for coding agents", agents_text)
            self.assertEqual(custom_path.read_text(encoding="utf-8"), "keep-me")


if __name__ == "__main__":
    unittest.main()
