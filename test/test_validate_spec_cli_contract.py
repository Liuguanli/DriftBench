import contextlib
import io
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import yaml

from driftbench import cli


REPO_ROOT = Path(__file__).resolve().parents[1]
DEMO_SPEC = "driftspec/examples/demo_data_single.yaml"


def run_cli(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "driftbench.cli", *args],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )


class DeepValidationCLIContractTests(unittest.TestCase):
    def test_deep_success_preserves_fields_and_adds_readiness_report(self) -> None:
        proc = run_cli("validate-spec", DEMO_SPEC, "--deep", "--json")
        self.assertEqual(proc.returncode, 0, msg=proc.stderr)
        self.assertEqual(proc.stderr, "")
        payload = json.loads(proc.stdout)
        for field in (
            "ok",
            "command",
            "spec_path",
            "pattern_id",
            "type",
            "declared_outputs",
        ):
            self.assertIn(field, payload)
        self.assertEqual(payload["mode"], "deep")
        self.assertTrue(payload["valid"])
        self.assertTrue(payload["locally_ready"])
        self.assertIsInstance(payload["checks"], list)
        self.assertIsInstance(payload["issues"], list)
        self.assertIsInstance(payload["summary"], dict)

    def test_expected_failure_is_one_json_document_on_stdout(self) -> None:
        proc = run_cli(
            "validate-spec", "driftspec/examples/not-present.yaml", "--deep", "--json"
        )
        self.assertEqual(proc.returncode, 3)
        self.assertEqual(proc.stderr, "")
        payload = json.loads(proc.stdout)
        self.assertFalse(payload["ok"])
        self.assertEqual(payload["outcome"], "not_ready")
        self.assertEqual(payload["issues"][0]["code"], "input_not_found")

    def test_argument_failure_is_also_one_json_document(self) -> None:
        proc = run_cli("validate-spec", "--deep", "--json")
        self.assertEqual(proc.returncode, 3)
        self.assertEqual(proc.stderr, "")
        payload = json.loads(proc.stdout)
        self.assertEqual(payload["mode"], "deep")
        self.assertEqual(payload["issues"][0]["code"], "cli_argument_invalid")

    def test_deep_json_argument_errors_redact_unparsed_option_values(self) -> None:
        secret = "super-secret-database-password"
        proc = run_cli(
            "validate-spec", "--deep", "--json", "--token", secret
        )
        self.assertEqual(proc.returncode, 3)
        self.assertEqual(proc.stderr, "")
        payload = json.loads(proc.stdout)
        self.assertEqual(payload["spec_path"], "")
        self.assertNotIn(secret, proc.stdout)

    def test_deep_json_argument_error_honors_argparse_abbreviations(self) -> None:
        proc = run_cli("validate-spec", "--de", "--j")
        self.assertEqual(proc.returncode, 3)
        self.assertEqual(proc.stderr, "")
        payload = json.loads(proc.stdout)
        self.assertEqual(payload["issues"][0]["code"], "cli_argument_invalid")

    def test_malformed_yaml_is_validation_error_without_traceback(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "broken.yaml"
            path.write_text("type: [unterminated\n", encoding="utf-8")
            proc = run_cli("validate-spec", str(path), "--deep", "--json")
        self.assertEqual(proc.returncode, 3)
        self.assertEqual(proc.stderr, "")
        payload = json.loads(proc.stdout)
        self.assertEqual(payload["issues"][0]["code"], "yaml_invalid")
        self.assertNotIn("Traceback", proc.stdout)

    def test_unknown_handler_is_expected_validation_error(self) -> None:
        spec = {
            "spec_version": 1,
            "seed": 42,
            "type": {"family": "x", "category": "y", "subtype": "z"},
            "variables": {},
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "unknown.yaml"
            path.write_text(yaml.safe_dump(spec), encoding="utf-8")
            proc = run_cli("validate-spec", str(path), "--deep", "--json")
        self.assertEqual(proc.returncode, 3)
        self.assertEqual(proc.stderr, "")
        payload = json.loads(proc.stdout)
        self.assertIn(
            "handler_not_registered", {issue["code"] for issue in payload["issues"]}
        )

    def test_internal_validator_error_uses_exit_four_and_redacts_exception(self) -> None:
        stdout = io.StringIO()
        stderr = io.StringIO()
        secret = "super-secret-database-password"
        args = SimpleNamespace(spec="spec.yaml", deep=True, json=True)
        with mock.patch.object(
            cli, "deep_validate_spec_file", side_effect=RuntimeError(secret)
        ), contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            exit_code = cli._cmd_validate_spec(args)
        self.assertEqual(exit_code, 4)
        self.assertEqual(stderr.getvalue(), "")
        payload = json.loads(stdout.getvalue())
        self.assertEqual(payload["issues"][0]["code"], "validator_internal_error")
        self.assertNotIn(secret, stdout.getvalue())

    def test_shallow_mode_contract_remains_unchanged(self) -> None:
        proc = run_cli("validate-spec", DEMO_SPEC, "--json")
        self.assertEqual(proc.returncode, 0, msg=proc.stderr)
        payload = json.loads(proc.stdout)
        self.assertNotIn("mode", payload)
        self.assertEqual(
            set(payload),
            {
                "ok",
                "command",
                "spec_path",
                "pattern_id",
                "type",
                "declared_outputs",
            },
        )

    def test_non_deep_argument_errors_keep_stock_argparse_contract(self) -> None:
        cases = (
            (),
            ("run-yaml",),
            ("validate-spec", "--json"),
            ("list-outputs", "--limit", "not-an-integer"),
            ("unknown-command",),
        )
        for arguments in cases:
            with self.subTest(arguments=arguments):
                proc = run_cli(*arguments)
                self.assertEqual(proc.returncode, 2)
                self.assertEqual(proc.stdout, "")
                self.assertIn("usage:", proc.stderr.lower())


if __name__ == "__main__":
    unittest.main()
