import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

import yaml


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

    def test_orchestrate_dry_run_generates_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            target_a = tmp / "target_a"
            target_b = tmp / "target_b"
            target_a.mkdir(parents=True, exist_ok=True)
            target_b.mkdir(parents=True, exist_ok=True)

            targets_yaml = tmp / "benchmark_targets.yaml"
            manifest_out = tmp / "manifest.json"
            targets_yaml.write_text(
                yaml.safe_dump(
                    {
                        "targets": [
                            {
                                "name": "a",
                                "workdir": str(target_a),
                                "run_command": "echo run-a",
                                "output_globs": ["**/*.txt"],
                            },
                            {
                                "name": "b",
                                "workdir": str(target_b),
                                "run_command": "echo run-b",
                                "output_globs": ["**/*.txt"],
                            },
                        ]
                    },
                    sort_keys=False,
                ),
                encoding="utf-8",
            )

            proc = run_cli(
                "orchestrate",
                "--spec",
                VALID_SPEC,
                "--targets",
                str(targets_yaml),
                "--manifest-out",
                str(manifest_out),
                "--json",
            )
            self.assertEqual(proc.returncode, 0, msg=proc.stderr)
            payload = json.loads(proc.stdout)
            self.assertEqual(payload["command"], "orchestrate")
            self.assertFalse(payload["execute"])
            self.assertTrue(manifest_out.exists())

            manifest = json.loads(manifest_out.read_text(encoding="utf-8"))
            self.assertEqual(manifest["summary"]["total_targets"], 2)
            self.assertEqual(manifest["summary"]["planned"], 2)

    def test_orchestrate_execute_runs_commands_and_collects_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            target_a = tmp / "target_exec_a"
            target_b = tmp / "target_exec_b"
            target_a.mkdir(parents=True, exist_ok=True)
            target_b.mkdir(parents=True, exist_ok=True)

            targets_yaml = tmp / "benchmark_targets_exec.yaml"
            manifest_out = tmp / "manifest_exec.json"
            targets_yaml.write_text(
                yaml.safe_dump(
                    {
                        "targets": [
                            {
                                "name": "exec-a",
                                "workdir": str(target_a),
                                "setup_command": "python -c \"from pathlib import Path; Path('setup_ok.txt').write_text('ok', encoding='utf-8')\"",
                                "run_command": "python -c \"from pathlib import Path; Path('artifact_a.txt').write_text('a', encoding='utf-8')\"",
                                "output_globs": ["**/*.txt"],
                            },
                            {
                                "name": "exec-b",
                                "workdir": str(target_b),
                                "run_command": "python -c \"from pathlib import Path; Path('artifact_b.txt').write_text('b', encoding='utf-8')\"",
                                "output_globs": ["**/*.txt"],
                            },
                        ]
                    },
                    sort_keys=False,
                ),
                encoding="utf-8",
            )

            proc = run_cli(
                "orchestrate",
                "--spec",
                VALID_SPEC,
                "--targets",
                str(targets_yaml),
                "--manifest-out",
                str(manifest_out),
                "--execute",
                "--json",
            )
            self.assertEqual(proc.returncode, 0, msg=proc.stderr)
            manifest = json.loads(manifest_out.read_text(encoding="utf-8"))

            self.assertEqual(manifest["summary"]["total_targets"], 2)
            self.assertEqual(manifest["summary"]["completed"], 2)
            self.assertEqual(manifest["summary"]["failed"], 0)
            statuses = {x["target"]: x["status"] for x in manifest["targets"]}
            self.assertEqual(statuses["exec-a"], "completed")
            self.assertEqual(statuses["exec-b"], "completed")

            self.assertTrue((target_a / "artifact_a.txt").exists())
            self.assertTrue((target_b / "artifact_b.txt").exists())

    def test_orchestrate_invalid_targets_returns_validation_error(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            targets_yaml = tmp / "invalid_targets.yaml"
            targets_yaml.write_text("targets: []\n", encoding="utf-8")
            manifest_out = tmp / "manifest_invalid.json"
            proc = run_cli(
                "orchestrate",
                "--spec",
                VALID_SPEC,
                "--targets",
                str(targets_yaml),
                "--manifest-out",
                str(manifest_out),
            )
            self.assertEqual(proc.returncode, 3)
            self.assertIn("[VALIDATION ERROR]", proc.stderr)

    def test_bootstrap_dataset_from_preset(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            out_dir = Path(tmpdir) / "bootstrap"
            proc = run_cli(
                "bootstrap",
                "dataset",
                "--source",
                "census_original",
                "--output-dir",
                str(out_dir),
                "--json",
            )
            self.assertEqual(proc.returncode, 0, msg=proc.stderr)
            payload = json.loads(proc.stdout)
            self.assertEqual(payload["command"], "bootstrap dataset")
            self.assertEqual(payload["source_kind"], "preset")
            self.assertTrue(Path(payload["dataset_path"]).exists())
            self.assertTrue(Path(payload["schema_path"]).exists())

    def test_bootstrap_dataset_from_local_with_checksum(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            src = tmp / "input.csv"
            src.write_text("a,b\n1,2\n3,4\n", encoding="utf-8")
            out_dir = tmp / "out"

            import hashlib

            digest = hashlib.sha256(src.read_bytes()).hexdigest()
            proc = run_cli(
                "bootstrap",
                "dataset",
                "--source",
                str(src),
                "--output-dir",
                str(out_dir),
                "--checksum",
                f"sha256:{digest}",
                "--json",
            )
            self.assertEqual(proc.returncode, 0, msg=proc.stderr)
            payload = json.loads(proc.stdout)
            self.assertEqual(payload["source_kind"], "local")
            self.assertEqual(payload["sha256"], digest)

    def test_bootstrap_dataset_checksum_mismatch_returns_validation_error(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            src = tmp / "input.csv"
            src.write_text("x,y\n10,20\n", encoding="utf-8")
            out_dir = tmp / "out"
            proc = run_cli(
                "bootstrap",
                "dataset",
                "--source",
                str(src),
                "--output-dir",
                str(out_dir),
                "--checksum",
                "sha256:1111111111111111111111111111111111111111111111111111111111111111",
            )
            self.assertEqual(proc.returncode, 3)
            self.assertIn("[VALIDATION ERROR]", proc.stderr)


if __name__ == "__main__":
    unittest.main()
