from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

import yaml

from driftbench.cache.errors import CacheConfigurationError
from driftbench.cache.identity import identity_for
from driftbench.cache.requests import load_artifact_request


class ExtensionRequestTests(unittest.TestCase):
    def test_all_six_public_factory_routes_keep_remote_cache_disabled(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            for benchmark in ("sysbench", "ssb", "ldbc"):
                for artifact_type in ("data", "queries"):
                    with self.subTest(benchmark=benchmark, artifact_type=artifact_type):
                        path = root / "request.yaml"
                        path.write_text(yaml.safe_dump({
                            "schema": "driftbench.artifact-request/v1",
                            "benchmark": benchmark, "artifact_type": artifact_type,
                        }), encoding="utf-8")
                        request = load_artifact_request(path)
                        self.assertEqual(request.adapter.benchmark, benchmark)
                        self.assertEqual(request.adapter.artifact_type, artifact_type)
                        with self.assertRaisesRegex(CacheConfigurationError, "remote-cache allowlist"):
                            identity_for(request.adapter)
                        self.assertEqual(list(root.iterdir()), [path])

    def test_public_request_cannot_inject_adapter_identity_or_arbitrary_options(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "request.yaml"
            for benchmark in ("sysbench", "ssb", "ldbc"):
                for key in ("benchmark", "artifact_type", "shell_command"):
                    with self.subTest(benchmark=benchmark, key=key):
                        path.write_text(yaml.safe_dump({
                            "schema": "driftbench.artifact-request/v1", "benchmark": benchmark,
                            "artifact_type": "queries", "parameters": {key: "injected"},
                        }), encoding="utf-8")
                        with self.assertRaisesRegex(CacheConfigurationError, "unknown parameters"):
                            load_artifact_request(path)

    def test_cli_offline_exports_and_reuses_native_plans_and_sql(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            for benchmark, parameters in (("sysbench", {"duration": 1}), ("ssb", {"query_ids": ["1.1", "3.2"]})):
                request = root / "request.yaml"
                request.write_text(yaml.safe_dump({
                    "schema": "driftbench.artifact-request/v1", "benchmark": benchmark,
                    "artifact_type": "queries", "parameters": parameters,
                }), encoding="utf-8")
                for outcome in ("disabled_generated", "disabled_local_cache"):
                    process = subprocess.run([
                        sys.executable, "-m", "driftbench.cli", "cache", "materialize",
                        "--request", str(request), "--output-dir", str(root / benchmark),
                        "--cache-mode", "off", "--json",
                    ], capture_output=True, text=True, check=False)
                    self.assertEqual(process.returncode, 0, process.stderr + process.stdout)
                    payload = json.loads(process.stdout)
                    self.assertTrue(payload["ok"])
                    self.assertEqual(payload["benchmark"], benchmark)
                    self.assertEqual(payload["cache_outcome"], outcome)
                    self.assertEqual(payload["remote_entry_status"], "not_checked")
                    self.assertEqual(process.stderr, "")
                    self.assertTrue(all(Path(path).is_file() for path in payload["files"]))

    def test_missing_ldbc_inputs_fail_as_one_machine_readable_error(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            request = root / "request.yaml"
            request.write_text("schema: driftbench.artifact-request/v1\nbenchmark: ldbc\nartifact_type: queries\n", encoding="utf-8")
            process = subprocess.run([
                sys.executable, "-m", "driftbench.cli", "cache", "materialize",
                "--request", str(request), "--output-dir", str(root / "out"), "--json",
            ], capture_output=True, text=True, check=False)
            self.assertEqual(process.returncode, 4)
            payload = json.loads(process.stdout)
            self.assertEqual(payload["outcome"], "runtime_error")
            self.assertEqual(process.stderr, "")
            self.assertFalse(list((root / "out").rglob("*manifest.json")))
