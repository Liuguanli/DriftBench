from __future__ import annotations

import copy
import json
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from driftbench import catalog
from driftbench.cache.models import AzureHNSCacheConfig
from scripts import export_azure_catalog as exporter
from test.catalog_fixtures import CONFIG, STAMP, Inventory, encode


class CatalogExportTests(unittest.TestCase):
    def setUp(self) -> None:
        self.store = Inventory()
        self.root = self.store.cache()

    def build(self) -> dict:
        return exporter.build_snapshot(CONFIG, self.store.listing(), self.store.read, observed_at=STAMP)

    def test_cache_dataset_and_real_query_records_without_payload_downloads(self) -> None:
        self.store.dataset()
        self.store.cache(benchmark="pgbench", kind="queries")
        result = self.build()
        self.assertEqual(len(result["entries"]), 3)
        tpch = next(item for item in result["entries"] if item["benchmark"] == "tpch")
        self.assertEqual(tpch["file_count"], 8)
        self.assertEqual(tpch["formats"], ["tbl"])
        self.assertEqual(tpch["parameters"], {"scale_factor": "0.01"})
        self.assertEqual(result["observed_at"], STAMP)
        self.assertEqual(len({item["id"] for item in result["entries"]}), 3)
        serialized = json.dumps(result)
        self.assertNotIn("DO-NOT-PUBLISH", serialized)
        self.assertNotIn("SECRET-BUILD-COMMAND", serialized)
        self.assertTrue(all(limit <= exporter._METADATA_LIMIT + 1 for _, limit in self.store.reads))

    def test_distinct_parameters_preserve_distinct_entries_and_ids_are_stable(self) -> None:
        self.store.cache(parameters={"scale_factor": None, "record_count": 1000})
        first = self.build()
        second = exporter.build_snapshot(CONFIG, reversed(self.store.listing()), self.store.read, observed_at=STAMP)
        self.assertEqual(first, second)
        self.assertEqual(len(first["entries"]), 2)

    def test_unknown_parameters_are_not_published(self) -> None:
        self.store = Inventory()
        self.store.cache(parameters={"scale_factor": 1, "credential": "SECRET", "source_dir": "C:\\private"})
        serialized = json.dumps(self.build())
        self.assertNotIn("SECRET", serialized)
        self.assertNotIn("source_dir", serialized)
        self.assertNotIn("credential", serialized)

    def test_known_parameter_cannot_leak_a_path_or_signed_url(self) -> None:
        self.store = Inventory()
        self.store.cache(parameters={"scale_factor": "https://host/?sig=SECRET"})
        with self.assertRaises(catalog.CatalogError):
            self.build()

    def test_uncommitted_drift_and_unknown_namespaces_are_explicitly_excluded(self) -> None:
        self.store.files.pop(self.root + "/_COMMITTED.json")
        self.store.files["team/drifts/v1/ycsb/_COMMITTED.json"] = b"not read"
        self.store.files["team/other/_COMMITTED.json"] = b"not read"
        self.assertEqual(self.build()["entries"], [])
        self.assertEqual(self.store.reads, [])

    def test_prefix_boundary_and_duplicate_listing_are_rejected_before_reads(self) -> None:
        for listing in ([("team-other/object.json", 1)], self.store.listing() * 2):
            with self.subTest(listing=listing[0][0]):
                with self.assertRaises(catalog.CatalogError):
                    exporter.build_snapshot(CONFIG, listing, self.store.read, observed_at=STAMP)
        self.assertEqual(self.store.reads, [])

    def test_incomplete_pagination_never_returns_partial_catalog(self) -> None:
        def broken_listing():
            yield from self.store.listing()
            raise OSError("listing interrupted")

        with self.assertRaisesRegex(OSError, "interrupted"):
            exporter.build_snapshot(CONFIG, broken_listing(), self.store.read, observed_at=STAMP)
        self.assertEqual(self.store.reads, [])

    def test_missing_object_wrong_size_and_bad_metadata_hash_fail(self) -> None:
        original = copy.deepcopy(self.store.files)
        payload = next(iter(self.store.payloads))
        for alteration in ("missing", "size", "hash"):
            with self.subTest(alteration=alteration):
                self.store.files = copy.deepcopy(original)
                if alteration == "missing":
                    del self.store.files[payload]
                elif alteration == "size":
                    self.store.files[payload] += b"more"
                else:
                    self.store.files[self.root + "/manifest.json"] += b" "
                with self.assertRaises(catalog.CatalogError):
                    self.build()

    def test_invalid_cache_descriptor_paths_roles_hashes_and_sizes(self) -> None:
        mutations = (
            lambda value: value["files"][0].update(path="../escape.csv"),
            lambda value: value["files"][0].update(path="job/data/wrong.csv"),
            lambda value: value["files"][0].update(role="unknown"),
            lambda value: value["files"][0].update(bytes=True),
            lambda value: value["files"][0].update(sha256="x" * 64),
            lambda value: value["files"].append(value["files"][0]),
            lambda value: value.update(version=True),
        )
        for change in mutations:
            with self.subTest(change=change):
                self.store = Inventory()
                self.root = self.store.cache()
                self.store.change_cache_manifest(self.root, change)
                with self.assertRaises(catalog.CatalogError):
                    self.build()

    def test_dataset_commit_and_provenance_must_agree(self) -> None:
        root = self.store.dataset()
        marker = root + "/_COMMITTED.json"
        original = self.store.files[marker]
        for field, value in (("state", "STAGING"), ("object_count", 1), ("bundle_fingerprint", "0" * 64)):
            with self.subTest(field=field):
                commit = json.loads(original)
                commit[field] = value
                self.store.files[marker] = encode(commit)
                with self.assertRaises(catalog.CatalogError):
                    self.build()

    def test_metadata_and_inventory_limits(self) -> None:
        with patch.object(exporter, "_METADATA_LIMIT", 1):
            with self.assertRaisesRegex(catalog.CatalogError, "read-size"):
                self.build()
        with patch.object(exporter, "_OBJECT_LIMIT", 1):
            with self.assertRaisesRegex(catalog.CatalogError, "object limit"):
                self.build()

    def test_metadata_short_read_and_growth_are_rejected(self) -> None:
        for result in (b"", b"x" * 10000):
            with self.subTest(length=len(result)):
                with self.assertRaisesRegex(catalog.CatalogError, "read was incomplete"):
                    exporter.build_snapshot(CONFIG, self.store.listing(), lambda path, limit: result)

    def test_off_or_write_modes_do_not_access_azure(self) -> None:
        for mode in ("off", "read-write"):
            with self.subTest(mode=mode):
                config = AzureHNSCacheConfig(account_url=CONFIG.account_url, file_system=CONFIG.file_system,
                                             prefix=CONFIG.prefix, mode=mode)
                with self.assertRaises(catalog.CatalogError):
                    exporter.build_snapshot(config, self.store.listing(), self.store.read)
        self.assertEqual(self.store.reads, [])

    def test_atomic_write_preserves_old_snapshot_on_failure_and_cleans_owned_temp(self) -> None:
        snapshot = self.build()
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "catalog.json"
            output.write_text("old", encoding="utf-8")
            broken = copy.deepcopy(snapshot)
            broken["entries"][0]["total_bytes"] += 1
            with self.assertRaises(catalog.CatalogError):
                exporter.write_snapshot(broken, output)
            self.assertEqual(output.read_text(), "old")
            with patch.object(Path, "replace", side_effect=OSError("replace denied")):
                with self.assertRaises(OSError):
                    exporter.write_snapshot(snapshot, output)
            self.assertEqual(output.read_text(), "old")
            self.assertEqual([path.name for path in Path(directory).iterdir()], ["catalog.json"])
            exporter.write_snapshot(snapshot, output)
            self.assertEqual(json.loads(output.read_text()), snapshot)

    def test_azure_wrapper_is_read_only_and_preserves_output_on_listing_failure(self) -> None:
        class AzureFailure(Exception):
            pass

        file_system = MagicMock(spec=["get_paths", "get_file_client"])
        file_system.get_paths.side_effect = lambda **kwargs: iter([
            SimpleNamespace(name=path, content_length=size, is_directory=False)
            for path, size in self.store.listing()
        ])

        def client(path):
            def download(*, offset, length):
                self.assertEqual(offset, 0)
                return SimpleNamespace(readall=lambda: self.store.read(path, length))
            return SimpleNamespace(download_file=download)

        file_system.get_file_client.side_effect = client
        service = MagicMock(spec=["__enter__", "__exit__", "get_file_system_client"])
        service.__enter__.return_value = service
        service.get_file_system_client.return_value = file_system
        constructor = MagicMock(return_value=service)
        modules = {
            "azure.core.exceptions": SimpleNamespace(AzureError=AzureFailure),
            "azure.storage.filedatalake": SimpleNamespace(DataLakeServiceClient=constructor),
        }
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "catalog.json"
            with patch.dict(sys.modules, modules), patch.object(exporter, "build_azure_credential") as credential:
                result = exporter.export_snapshot(CONFIG, output)
                self.assertEqual(json.loads(output.read_text()), result)
                credential.assert_called_once_with(None)
                file_system.get_paths.assert_called_once_with(path="team", recursive=True)
                previous = output.read_bytes()
                file_system.get_paths.side_effect = AzureFailure("private error details")
                with self.assertRaisesRegex(catalog.CatalogError, "credentials, read permissions"):
                    exporter.export_snapshot(CONFIG, output)
                self.assertEqual(output.read_bytes(), previous)


if __name__ == "__main__":
    unittest.main()
