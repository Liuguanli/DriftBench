from __future__ import annotations

from contextlib import ExitStack
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import MagicMock, patch
import tempfile


ROOT = Path(__file__).resolve().parents[1]


def _load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


DEMO = _load("learning_demo", ROOT / "scripts/run_learning_demo.py")
CONVERTER = _load("learning_converter", ROOT / "docs/examples/run-guide/prepare_lineitem.py")


class LearningDemoTests(unittest.TestCase):
    def test_documented_artifact_requests_use_supported_contracts(self):
        from driftbench.cache.requests import load_artifact_request

        guide = ROOT / "docs/examples/run-guide"
        for name in ("tpch-data-generate", "tpch-data-local-dbgen", "tpch-data-copy"):
            request = load_artifact_request(guide / f"{name}.yaml")
            self.assertEqual((request.benchmark, request.artifact_type), ("tpch", "data"))
            self.assertEqual(float(request.adapter.scale_factor), 0.001)
        with tempfile.TemporaryDirectory() as temporary:
            for name, count in (("tpch-queries", 1), ("tpch-queries-qgen", 44)):
                request = load_artifact_request(guide / f"{name}.yaml")
                result = request.adapter.generate(Path(temporary) / name)
                metadata = json.loads(result.metadata.read_text())
                self.assertEqual(metadata["count"], count)
                self.assertTrue(all(path.is_file() for path in result.files))

    def test_actual_sqlite_pipeline_and_reproducible_artifacts(self):
        with tempfile.TemporaryDirectory() as temporary:
            first_root = Path(temporary) / "first"
            second_root = Path(temporary) / "second"
            first = DEMO.run_demo("sqlite", first_root)
            second = DEMO.run_demo("sqlite", second_root)
            self.assertFalse(first["official_tpch"])
            self.assertEqual(first["environment"]["engine"], "sqlite")
            self.assertEqual([phase["loaded_rows"] for phase in first["phases"]], [12, 12])
            self.assertEqual([len(phase["executions"]) for phase in first["phases"]], [4, 4])
            baseline = first["phases"][0]["executions"][0]
            drifted = first["phases"][1]["executions"][0]
            self.assertEqual(baseline["columns"], ["revenue"])
            self.assertAlmostEqual(baseline["rows"][0][0], 468.0)
            self.assertNotEqual(baseline["rows"], drifted["rows"])
            self.assertEqual(first["artifact_sha256"], second["artifact_sha256"])
            self.assertTrue((first_root / "teaching.sqlite").is_file())
            commands = json.loads((first_root / "commands.json").read_text())
            self.assertEqual(len(commands), 8)
            self.assertTrue(all(command["returncode"] == 0 for command in commands))
            self.assertEqual(len(list((first_root / "source").glob("*.tbl"))), 8)

    def test_nonempty_output_preserves_existing_files(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            sentinel = root / "keep.txt"
            sentinel.write_text("existing user data")
            with patch.object(DEMO, "_prepare") as prepare:
                with self.assertRaisesRegex(DEMO.DemoError, "new or empty"):
                    DEMO.run_demo("sqlite", root)
                prepare.assert_not_called()
            self.assertEqual(sentinel.read_text(), "existing user data")
            self.assertEqual(list(root.iterdir()), [sentinel])

    def test_preparation_failure_records_error_without_starting_database(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "attempt"
            with patch.object(DEMO, "_prepare", side_effect=DEMO.DemoError("fixture rejected")), patch.object(DEMO, "_postgres") as postgres:
                with self.assertRaisesRegex(DEMO.DemoError, "fixture rejected"):
                    DEMO.run_demo("postgres", root)
                postgres.assert_not_called()
            failure = json.loads((root / "failure.json").read_text())
            self.assertEqual(failure["status"], "error")
            self.assertFalse((root / "results.json").exists())

    def test_cli_process_failures_retain_argv_and_partial_output(self):
        failures = (
            subprocess.TimeoutExpired("python", 60, output=b"partial stdout\n", stderr=b"partial stderr\n"),
            OSError("process could not start"),
        )
        for failure in failures:
            with self.subTest(failure=type(failure).__name__), patch.object(DEMO.subprocess, "run", side_effect=failure):
                commands = []
                with self.assertRaises(DEMO.CommandFailure):
                    DEMO._cli(["dry-run", "example.yaml"], commands)
                self.assertEqual(len(commands), 1)
                self.assertEqual(commands[0]["argv"][-2:], ["dry-run", "example.yaml"])
                self.assertIsNone(commands[0]["returncode"])
                if isinstance(failure, subprocess.TimeoutExpired):
                    self.assertEqual(commands[0]["stdout"], "partial stdout\n")
                    self.assertEqual(commands[0]["stderr"], "partial stderr\n")
                    self.assertEqual(commands[0]["error_kind"], "timeout")
                else:
                    self.assertEqual(commands[0]["error_kind"], "os_error")

    def test_engine_must_be_explicit_before_output_mutation(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "uncreated"
            process = subprocess.run(
                [sys.executable, str(ROOT / "scripts/run_learning_demo.py"), "--output-dir", str(root)],
                capture_output=True, text=True, check=False,
            )
            self.assertEqual(process.returncode, 2)
            self.assertIn("--engine", process.stderr)
            self.assertFalse(root.exists())

    def test_converter_rejects_invalid_rows_before_writing(self):
        with tempfile.TemporaryDirectory() as temporary:
            source = Path(temporary) / "lineitem.tbl"
            target = Path(temporary) / "new" / "lineitem.csv"
            source.write_text("1|2|\n")
            with self.assertRaisesRegex(ValueError, "expected 16"):
                CONVERTER.convert_lineitem(source, target)
            self.assertFalse(target.parent.exists())

    def test_cleanup_does_not_remove_a_container_with_wrong_identity(self):
        container_id = "a" * 64
        for info in (container_id + "|someone-else", "b" * 64 + "|our-token"):
            with self.subTest(info=info), patch.object(DEMO, "_docker", return_value=SimpleNamespace(returncode=0, stdout=info)) as docker:
                with self.assertRaisesRegex(DEMO.DemoError, "Refusing cleanup"):
                    DEMO._remove_owned_container("our-name", "our-token", container_id)
                self.assertEqual(docker.call_count, 1)
                self.assertEqual(docker.call_args.args[0][0], "inspect")

    def test_cleanup_removes_only_verified_container_id_and_verifies_absence(self):
        container_id = "a" * 64
        answers = [
            SimpleNamespace(returncode=0, stdout=container_id + "|our-token"),
            SimpleNamespace(returncode=0, stdout=container_id),
            SimpleNamespace(returncode=0, stdout=""),
        ]
        with patch.object(DEMO, "_docker", side_effect=answers) as docker:
            DEMO._remove_owned_container("our-name", "our-token", container_id)
        self.assertEqual(docker.call_args_list[1].args[0], ["rm", "--force", container_id])

    def test_postgres_context_attempts_cleanup_after_close_startup_or_execution_error(self):
        container_id = "a" * 64
        for failure_at in ("close", "startup", "execution"):
            with self.subTest(failure_at=failure_at), ExitStack() as stack:
                connection = MagicMock()
                cursor = connection.cursor.return_value.__enter__.return_value
                cursor.fetchone.return_value = ("PostgreSQL test double",)
                if failure_at == "close":
                    connection.close.side_effect = RuntimeError("close failed")
                elif failure_at == "startup":
                    cursor.execute.side_effect = RuntimeError("startup failed")
                answers = [
                    SimpleNamespace(returncode=0, stdout="29.7.2", stderr=""),
                    SimpleNamespace(returncode=0, stdout="sha256:test", stderr=""),
                    SimpleNamespace(returncode=0, stdout=container_id, stderr=""),
                    SimpleNamespace(returncode=0, stdout="127.0.0.1:54321", stderr=""),
                ]
                stack.enter_context(patch.object(DEMO.shutil, "which", return_value="docker"))
                stack.enter_context(patch.object(DEMO, "_DOCKER_CONTEXT", None))
                stack.enter_context(patch.object(DEMO, "_select_local_docker_context", return_value="local-test"))
                docker = stack.enter_context(patch.object(DEMO, "_docker", side_effect=answers))
                stack.enter_context(patch.object(DEMO.uuid, "uuid4", return_value=SimpleNamespace(hex="our-token")))
                stack.enter_context(patch("psycopg2.connect", return_value=connection))
                cleanup = stack.enter_context(patch.object(DEMO, "_remove_owned_container"))
                environment = None
                with self.assertRaisesRegex(RuntimeError, f"{failure_at} failed"):
                    with DEMO._postgres() as (_, environment):
                        if failure_at == "execution":
                            raise RuntimeError("execution failed")
                connection.close.assert_called_once()
                cleanup.assert_called_once_with("driftbench-learning-our-token", "our-token", container_id)
                self.assertEqual(docker.call_args_list[2].args[0][-1], "sha256:test")
                if environment is not None:
                    self.assertTrue(environment["container_removed"])

    def test_remote_docker_context_is_rejected_before_engine_action(self):
        answers = [
            SimpleNamespace(returncode=0, stdout="production\n"),
            SimpleNamespace(returncode=0, stdout="ssh://remote.example\n"),
        ]
        with patch.object(DEMO, "_command", side_effect=answers):
            with self.assertRaisesRegex(DEMO.DemoError, "local Docker context"):
                DEMO._select_local_docker_context()


if __name__ == "__main__":
    unittest.main()
