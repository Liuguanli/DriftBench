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


if __name__ == "__main__":
    unittest.main()

