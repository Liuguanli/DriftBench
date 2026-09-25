from __future__ import annotations

import copy
import io
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from driftbench import catalog
from scripts.export_azure_catalog import build_snapshot
from test.catalog_fixtures import CONFIG, OFFLINE_READER_SCRIPT, STAMP, Inventory


class CatalogTests(unittest.TestCase):
    def test_packaged_inventory_is_actual_data_not_generator_capabilities(self) -> None:
        metadata = catalog.info()
        entries = catalog.list()
        self.assertEqual(metadata["counts"], {"data": 10, "queries": 0})
        self.assertEqual(len(entries), 10)
        self.assertEqual(metadata["benchmarks"],
                         ["dsb", "job", "pgbench", "tpcc", "tpcc_skew", "tpcds", "tpch", "ycsb"])
        self.assertEqual(len(catalog.list(benchmark="ycsb")), 2)
        self.assertEqual(catalog.list(artifact_type="queries"), [])
        self.assertEqual(catalog.list(benchmark="sysbench"), [])
        self.assertFalse(metadata["live"])
        self.assertEqual(metadata["freshness"], "packaged-snapshot")
        tpch_entries = catalog.list(benchmark="tpch", artifact_type="data")
        self.assertEqual(len(tpch_entries), 2)
        tpch_by_scale = {entry["parameters"]["scale_factor"]: entry for entry in tpch_entries}
        self.assertEqual(set(tpch_by_scale), {"0.01", "10"})
        self.assertEqual(tpch_by_scale["0.01"]["total_bytes"], 10_579_624)
        self.assertEqual(tpch_by_scale["10"]["total_bytes"], 11_232_136_268)
        expected_tables = {"customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"}
        for scale, tpch in tpch_by_scale.items():
            with self.subTest(scale=scale):
                self.assertEqual(tpch["layout"], "immutable-dataset-v1")
                self.assertEqual(tpch["file_count"], 8)
                self.assertEqual(tpch["formats"], ["tbl"])
                self.assertIn(f"/sf_{scale}/", tpch["path"])
                self.assertEqual(
                    {item["path"] for item in tpch["files"]},
                    {f"tpch/data/sf_{scale}/{table}.tbl" for table in expected_tables},
                )
                self.assertEqual(catalog.get(tpch["id"]), tpch)
        json.dumps({"metadata": metadata, "entries": entries}, allow_nan=False)

    def test_results_are_ordered_and_deeply_independent(self) -> None:
        first = catalog.list()
        self.assertEqual([entry["id"] for entry in first], sorted(entry["id"] for entry in first))
        original = copy.deepcopy(first[0])
        first[0]["files"][0]["bytes"] = -1
        first[0]["parameters"]["scale_factor"] = -1
        self.assertEqual(catalog.get(original["id"]), original)
        metadata = catalog.info()
        metadata["source"]["prefix"] = "changed"
        self.assertNotEqual(catalog.info()["source"]["prefix"], "changed")

    def test_filters_combine_and_query_records_are_supported_when_present(self) -> None:
        store = Inventory()
        store.cache()
        store.cache(benchmark="pgbench", kind="queries")
        snapshot = build_snapshot(CONFIG, store.listing(), store.read, observed_at=STAMP)
        with patch.object(catalog, "_load", side_effect=lambda: copy.deepcopy(snapshot)):
            self.assertEqual(len(catalog.list(artifact_type="queries")), 1)
            self.assertEqual(catalog.list(benchmark="ycsb", artifact_type="queries"), [])
            self.assertEqual(catalog.info()["counts"], {"data": 1, "queries": 1})

    def test_invalid_filters_and_unknown_ids(self) -> None:
        for benchmark in ("", "TPC-H", "../tpch", 1, [], {}):
            with self.subTest(benchmark=benchmark):
                with self.assertRaises(ValueError):
                    catalog.list(benchmark=benchmark)
        for kind in ("", "drift", True, [], {}):
            with self.subTest(kind=kind):
                with self.assertRaises(ValueError):
                    catalog.list(artifact_type=kind)
        with self.assertRaises(KeyError):
            catalog.get("unknown")
        for value in ("", None, 123):
            with self.assertRaises(ValueError):
                catalog.get(value)

    def test_corrupt_snapshot_never_becomes_empty_success(self) -> None:
        original = catalog._load()
        mutations = (
            lambda value: value.update(schema="future"),
            lambda value: value.update(observed_at="not-a-timestamp"),
            lambda value: value["source"].update(account_url="https://host/?sig=SECRET"),
            lambda value: value["entries"].append(value["entries"][0]),
            lambda value: value["entries"][0].update(file_count=True),
            lambda value: value["entries"][0].update(total_bytes=0),
            lambda value: value["entries"][0].update(path="../outside"),
            lambda value: value["entries"][0]["files"][0].update(path="../outside"),
            lambda value: value["entries"][0]["parameters"].update(secret="SECRET"),
        )
        for mutate in mutations:
            with self.subTest(mutation=mutate):
                payload = copy.deepcopy(original)
                mutate(payload)
                with self.assertRaises(catalog.CatalogError):
                    catalog._validate(payload)
        for value in (b"not json", b"{}", b'{"entries":1,"entries":2}', b'{"value":NaN}'):
            with self.subTest(value=value):
                with self.assertRaises(catalog.CatalogError):
                    catalog._validate(catalog._decode(value, "test snapshot"))
        with tempfile.TemporaryDirectory() as directory:
            with patch.object(catalog.resources, "files", return_value=Path(directory)):
                with self.assertRaisesRegex(catalog.CatalogError, "missing"):
                    catalog.list()

    def test_import_is_passive_and_browsing_needs_no_azure_or_network(self) -> None:
        environment = dict(os.environ)
        environment.pop("PYTHONPATH", None)
        proc = subprocess.run([sys.executable, "-B", "-c", OFFLINE_READER_SCRIPT],
                              cwd=Path(__file__).resolve().parents[1],
                              env=environment, capture_output=True, text=True, check=False)
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertEqual(proc.stdout, "")
        self.assertEqual(proc.stderr, "")

    def test_public_readers_normalize_url_and_json_parser_value_errors(self) -> None:
        invalid_source = catalog._load()
        invalid_source["source"]["account_url"] = "https://[invalid"
        corrupt_resources = (
            json.dumps(invalid_source).encode("utf-8"),
            b'{"oversized_integer":' + b"9" * 5000 + b"}",
        )
        for encoded in corrupt_resources:
            with self.subTest(size=len(encoded)):
                with patch.object(catalog.resources, "files") as resources:
                    resources.return_value.joinpath.return_value.open.side_effect = (
                        lambda mode: io.BytesIO(encoded)
                    )
                    for reader in (catalog.list, catalog.info, lambda: catalog.get("any-id")):
                        with self.assertRaises(catalog.CatalogError):
                            reader()


if __name__ == "__main__":
    unittest.main()
