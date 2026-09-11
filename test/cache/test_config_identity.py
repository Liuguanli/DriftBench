from __future__ import annotations

import json
import subprocess
import sys
import unittest
from pathlib import Path

from driftbench.cache.errors import CacheConfigurationError
from driftbench.cache.identity import identity_for
from driftbench.cache.models import (
    AzureHNSCacheConfig,
    RemoteCacheMode,
    validate_posix_relative_path,
)
from driftbench.data.tpch import TPCHQueries
from driftbench.data.tpch_param_specs import TPCH_PARAM_SPECS_IDENTITY_SCHEMA
from driftbench.data.ycsb import YCSBData


class CacheConfigIdentityTests(unittest.TestCase):
    def test_off_configuration_is_offline_and_may_be_empty(self) -> None:
        config = AzureHNSCacheConfig(mode="off")
        config.validate_enabled()
        self.assertIs(config.normalized_mode(), RemoteCacheMode.OFF)

    def test_enabled_configuration_rejects_secret_bearing_or_non_dfs_urls(self) -> None:
        invalid = (
            "http://account.dfs.core.windows.net",
            "https://user:secret@account.dfs.core.windows.net",
            "https://account.dfs.core.windows.net/container",
            "https://account.dfs.core.windows.net?sig=secret",
        )
        for account_url in invalid:
            with self.subTest(account_url=account_url), self.assertRaises(
                CacheConfigurationError
            ):
                AzureHNSCacheConfig(
                    account_url=account_url,
                    file_system="driftbench-cache",
                    mode="read",
                ).validate_enabled()

    def test_path_validator_rejects_aliases_and_traversal(self) -> None:
        invalid = (
            "/absolute",
            "../escape",
            "a/../b",
            "a\\b",
            "C:/drive",
            "//server/share",
            "a/%2e%2e/b",
            "a//b",
            "a/./b",
            "a\x00b",
        )
        for value in invalid:
            with self.subTest(value=repr(value)), self.assertRaises(
                CacheConfigurationError
            ):
                validate_posix_relative_path(value, field="test")

    def test_identity_is_deterministic_and_tracks_effective_record_count(self) -> None:
        first = identity_for(YCSBData(scale_factor=2))
        equivalent = identity_for(YCSBData(scale_factor=2.0, record_count=2000))
        changed = identity_for(YCSBData(scale_factor=3))
        self.assertEqual(first.fingerprint, equivalent.fingerprint)
        self.assertNotEqual(first.fingerprint, changed.fingerprint)
        self.assertEqual(first.parameters["record_count"], 2000)
        encoded = json.dumps(first.canonical(), sort_keys=True)
        self.assertNotIn(str(Path.cwd().resolve()), encoded)

    def test_external_tpch_paths_are_not_remote_identity_inputs(self) -> None:
        with self.assertRaises(CacheConfigurationError):
            identity_for(TPCHQueries(template_dir=Path.cwd()))

    def test_plain_tpch_custom_parameters_remain_remote_identity_inputs(self) -> None:
        identity = identity_for(
            TPCHQueries(
                query_ids=[1],
                mode="custom",
                param_specs={"1": ({"type": "fixed", "value": "BUILDING"},)},
                shuffle=False,
            )
        )
        self.assertEqual(
            identity.parameters["param_specs"]["schema"],
            TPCH_PARAM_SPECS_IDENTITY_SCHEMA,
        )
        self.assertEqual(identity.parameters["param_specs"]["root"]["type"], "dict")

    def test_tpch_custom_identity_preserves_generator_visible_python_semantics(self) -> None:
        ordered_ab = {"a": 1, "b": 2}
        ordered_ba = {"b": 2, "a": 1}
        cases = {
            "list versus tuple": (
                {"1": [[1, 2]]},
                {"1": [(1, 2)]},
            ),
            "integer versus integral float": (
                {"1": [1]},
                {"1": [1.0]},
            ),
            "mapping insertion order": (
                {"1": [{"type": "fixed", "value": ordered_ab}]},
                {"1": [{"type": "fixed", "value": ordered_ba}]},
            ),
            "integer versus string mapping key": (
                {"1": [{"type": "fixed", "value": {1: "x"}}]},
                {"1": [{"type": "fixed", "value": {"1": "x"}}]},
            ),
        }
        for label, (first_specs, second_specs) in cases.items():
            with self.subTest(case=label):
                first = identity_for(
                    TPCHQueries(
                        query_ids=[1],
                        mode="custom",
                        param_specs=first_specs,
                        shuffle=False,
                    )
                )
                second = identity_for(
                    TPCHQueries(
                        query_ids=[1],
                        mode="custom",
                        param_specs=second_specs,
                        shuffle=False,
                    )
                )
                self.assertNotEqual(first.parameters, second.parameters)
                self.assertNotEqual(first.fingerprint, second.fingerprint)
                self.assertEqual(first.generator_revision, "5")
                self.assertEqual(second.generator_revision, "5")

    def test_root_import_does_not_import_adls_sdk(self) -> None:
        proc = subprocess.run(
            [
                sys.executable,
                "-c",
                (
                    "import json, sys; import driftbench; "
                    "print(json.dumps({'loaded': 'azure.storage.filedatalake' in sys.modules}))"
                ),
            ],
            check=False,
            capture_output=True,
            text=True,
        )
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertEqual(json.loads(proc.stdout), {"loaded": False})


if __name__ == "__main__":
    unittest.main()
