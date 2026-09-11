from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from driftbench.cache.errors import CacheConfigurationError
from driftbench.cache.requests import load_artifact_request, load_azure_cache_config


class RequestConfigTests(unittest.TestCase):
    def _write(self, root: Path, name: str, text: str) -> Path:
        path = root / name
        path.write_text(text, encoding="utf-8")
        return path

    def test_request_uses_registry_and_rejects_unknown_fields(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            valid = self._write(
                root,
                "request.yaml",
                "schema: driftbench.artifact-request/v1\n"
                "benchmark: ycsb\n"
                "artifact_type: data\n"
                "parameters:\n"
                "  scale_factor: 2\n",
            )
            request = load_artifact_request(valid)
            self.assertEqual(request.adapter.scale_factor, 2)

            invalid = self._write(
                root,
                "invalid.yaml",
                valid.read_text(encoding="utf-8") + "  dynamic_import: evil.module\n",
            )
            with self.assertRaises(CacheConfigurationError):
                load_artifact_request(invalid)

    def test_request_rejects_duplicate_keys_and_conflicting_ycsb_count(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            duplicate = self._write(
                root,
                "duplicate.yaml",
                "schema: driftbench.artifact-request/v1\n"
                "benchmark: ycsb\nbenchmark: tpcc\nartifact_type: data\n",
            )
            conflict = self._write(
                root,
                "conflict.yaml",
                "schema: driftbench.artifact-request/v1\n"
                "benchmark: ycsb\nartifact_type: data\n"
                "parameters:\n  scale_factor: 2\n  record_count: 1000\n",
            )
            for path in (duplicate, conflict):
                with self.subTest(path=path.name), self.assertRaises(
                    CacheConfigurationError
                ):
                    load_artifact_request(path)

    def test_azure_yaml_is_non_secret_and_strict(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            valid = self._write(
                root,
                "azure.yaml",
                "schema: driftbench.azure-hns-cache/v1\n"
                "account_url: https://account.dfs.core.windows.net\n"
                "file_system: driftbench-cache\n"
                "prefix: team-a\n",
            )
            config = load_azure_cache_config(
                valid, mode="read", credential_env_file=".private/azure.env"
            )
            self.assertEqual(config.prefix, "team-a")

            secret = self._write(
                root,
                "secret.yaml",
                valid.read_text(encoding="utf-8") + "account_key: private\n",
            )
            with self.assertRaises(CacheConfigurationError):
                load_azure_cache_config(secret, mode="read", credential_env_file=None)

    def test_request_and_azure_config_reject_symlink_paths(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            request = self._write(
                root,
                "request.yaml",
                "schema: driftbench.artifact-request/v1\n"
                "benchmark: ycsb\nartifact_type: data\n",
            )
            azure = self._write(
                root,
                "azure.yaml",
                "schema: driftbench.azure-hns-cache/v1\n"
                "account_url: https://account.dfs.core.windows.net\n"
                "file_system: driftbench-cache\n",
            )
            request_link = root / "request-link.yaml"
            azure_link = root / "azure-link.yaml"
            broken_link = root / "broken.yaml"
            try:
                request_link.symlink_to(request)
                azure_link.symlink_to(azure)
                broken_link.symlink_to(root / "missing.yaml")
            except OSError as exc:
                self.skipTest(f"symlinks unavailable: {exc}")

            with self.assertRaises(CacheConfigurationError):
                load_artifact_request(request_link)
            with self.assertRaises(CacheConfigurationError):
                load_azure_cache_config(
                    azure_link, mode="read", credential_env_file=None
                )
            with self.assertRaises(CacheConfigurationError):
                load_artifact_request(broken_link)


if __name__ == "__main__":
    unittest.main()
