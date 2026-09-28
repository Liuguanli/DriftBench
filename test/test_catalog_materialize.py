from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import copy
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import threading
import unittest
from unittest.mock import patch

from driftbench import catalog
from driftbench.cache import datasets, service
from driftbench.cache.errors import (
    CacheAuthenticationError,
    CacheCollisionError,
    CacheConfigurationError,
    CacheIntegrityError,
    CacheTransportError,
    RemoteObjectNotFound,
)
from scripts.export_azure_catalog import build_snapshot
from test.catalog_fixtures import CONFIG, STAMP, Inventory, encode, sha


ROOT = Path(__file__).resolve().parents[1]


class ReadBackend:
    def __init__(self, store):
        self.store = store
        self.reads = []

    def iter_bytes(self, path):
        self.reads.append(path)
        if path not in self.store.files:
            raise RemoteObjectNotFound("fixture object missing")
        value = self.store.files[path]
        for offset in range(0, len(value), 31):
            yield value[offset:offset + 31]


class CatalogMaterializeTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="catalog-data-")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.store = Inventory()
        self.remote = self.store.dataset()
        self.snapshot = build_snapshot(CONFIG, self.store.listing(), self.store.read, observed_at=STAMP)
        self.entry = self.snapshot["entries"][0]
        self.backend = ReadBackend(self.store)
        self.loader = patch.object(catalog, "_load", side_effect=lambda: copy.deepcopy(self.snapshot))
        self.loader.start()
        self.addCleanup(self.loader.stop)

    def materialize(self, name="cache", **kwargs):
        return catalog.materialize(self.entry["id"], cache_dir=self.root / name, **kwargs)

    def test_first_population_and_every_offline_hit_verify_all_bytes(self):
        with patch.object(datasets, "_build_backend", return_value=self.backend) as factory:
            first = self.materialize()
        self.assertEqual(first["cache_outcome"], "downloaded")
        self.assertEqual(first["downloaded_payload_bytes"], self.entry["total_bytes"])
        self.assertEqual(len(self.backend.reads), 10)
        self.assertEqual(set(self.backend.reads),
                         {f"{self.remote}/{name}" for name in (
                             "_COMMITTED.json", "provenance_manifest.json",
                             *(item["path"] for item in self.entry["files"]),
                         )})
        self.assertEqual(factory.call_args.args[0].normalized_mode().value, "read")
        self.assertNotIn("DO-NOT-PUBLISH", json.dumps(first))
        self.assertNotIn("SECRET-BUILD-COMMAND", json.dumps(first))
        self.assertNotIn("credential_env_file", json.dumps(first))
        self.assertEqual(first["materialized_bytes"], sum(
            path.stat().st_size for path in Path(first["local_path"]).rglob("*") if path.is_file()
        ))
        with patch.object(datasets, "_build_backend", side_effect=AssertionError("no Azure client")), \
                patch.object(socket.socket, "connect", side_effect=AssertionError("no network")), \
                patch.object(subprocess, "Popen", side_effect=AssertionError("no credential process")):
            second = self.materialize(credential_env_file=self.root / "must-not-read.env")
        self.assertEqual(second["cache_outcome"], "hit")
        self.assertEqual(second["local_path"], first["local_path"])
        self.assertEqual(second["binding_sha256"], first["binding_sha256"])
        self.assertEqual(second["downloaded_payload_bytes"], 0)
        self.assertEqual(second["downloaded_metadata_bytes"], 0)
        self.assertFalse(second["remote_checked"])
        self.assertEqual(len(self.backend.reads), 10)
        self.assertEqual(second["file_count"], 8)
        for descriptor, filename in zip(self.entry["files"], second["files"]):
            self.assertEqual(sha(Path(filename).read_bytes()), descriptor["sha256"])

    def test_fresh_process_hit_needs_no_azure_import_or_credential_file(self):
        with patch.object(datasets, "_build_backend", return_value=self.backend):
            self.materialize()
        snapshot_path = self.root / "snapshot.json"
        snapshot_path.write_bytes(encode(self.snapshot))
        script = r'''
import json
from pathlib import Path
import platform
import sys
from unittest.mock import patch
platform.machine()
def guard(event, args):
    if event == "import" and (args[0] == "azure" or args[0].startswith("azure.")):
        raise AssertionError("Azure import forbidden on a hit")
    if event in {"socket.connect", "subprocess.Popen", "os.system"}:
        raise AssertionError("External action forbidden on a hit")
    if event == "open" and str(args[0]).endswith(".env"):
        raise AssertionError("Credential file read forbidden on a hit")
sys.addaudithook(guard)
from driftbench import catalog
snapshot = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
with patch.object(catalog, "_load", return_value=snapshot):
    result = catalog.materialize(snapshot["entries"][0]["id"], cache_dir=sys.argv[2],
                                 credential_env_file="must-not-read.env")
assert result["cache_outcome"] == "hit"
assert result["downloaded_payload_bytes"] == result["downloaded_metadata_bytes"] == 0
assert not any(name == "azure" or name.startswith("azure.") for name in sys.modules)
print(result["cache_outcome"])
'''
        result = subprocess.run(
            [sys.executable, "-B", "-c", script, str(snapshot_path), str(self.root / "cache")],
            cwd=self.root, env=dict(os.environ, PYTHONPATH=str(ROOT), PYTHONDONTWRITEBYTECODE="1"),
            capture_output=True, text=True, timeout=120,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "hit")

    def test_unsupported_unsafe_and_oversize_requests_do_not_construct_clients(self):
        with patch.object(datasets, "_build_backend", side_effect=AssertionError("no Azure client")):
            for limit in (0, -1, True, 0.5, None, self.entry["total_bytes"]):
                with self.subTest(limit=limit), self.assertRaises(CacheConfigurationError):
                    self.materialize(max_bytes=limit)
            for directory in ("", Path.home(), Path(Path.home().anchor), ROOT, ROOT / "cache"):
                with self.subTest(directory=str(directory)), self.assertRaises(CacheConfigurationError):
                    catalog.materialize(self.entry["id"], cache_dir=directory)
            self.assertFalse((self.root / "cache").exists())
            with self.assertRaises(KeyError):
                catalog.materialize("unknown", cache_dir=self.root / "unknown")
            unsafe_paths = ["tpch/data/CON/x.tbl", "tpch/data/nation.tbl."]
            unsafe_paths.extend(f"tpch/data/sf_0.01/nation{character}.tbl" for character in '?*|<>"')
            for path in unsafe_paths:
                with self.subTest(path=path):
                    original = self.entry["files"][0]["path"]
                    self.entry["files"][0]["path"] = path
                    try:
                        with patch.object(datasets, "_ensure_safe_directory", side_effect=AssertionError("no filesystem writes")), \
                                self.assertRaises(CacheConfigurationError):
                            self.materialize()
                    finally:
                        self.entry["files"][0]["path"] = original
            self.entry["layout"] = "artifact-cache-v1"
            with self.assertRaises(CacheConfigurationError):
                self.materialize()
            self.entry["layout"] = "immutable-dataset-v1"
            self.entry["artifact_type"] = "queries"
            with self.assertRaises(CacheConfigurationError):
                self.materialize()

    def test_forbidden_directory_aliases_fail_before_any_writes_or_clients(self):
        forbidden = [ROOT, ROOT / "unused-cache", Path.home()]
        if os.name == "nt":
            for directory in (ROOT, Path.home()):
                aliases = [Path(str(directory) + "."), Path(str(directory) + " "),
                           Path("\\\\?\\" + str(directory))]
                for alias in aliases:
                    self.assertTrue(alias.samefile(directory))
                    forbidden.append(alias)
                    if directory == ROOT:
                        forbidden.append(alias / "unused-cache")
        with patch.object(datasets, "_ensure_safe_directory", side_effect=AssertionError("no filesystem writes")), \
                patch.object(datasets, "_exclusive_lock", side_effect=AssertionError("no lock file")), \
                patch.object(datasets, "_build_backend", side_effect=AssertionError("no Azure client")):
            for directory in forbidden:
                with self.subTest(directory=str(directory)), self.assertRaises(CacheConfigurationError):
                    catalog.materialize(self.entry["id"], cache_dir=directory)
        if os.name == "nt":
            with patch.object(datasets, "_build_backend", return_value=self.backend):
                first = self.materialize()
            with patch.object(datasets, "_build_backend", side_effect=AssertionError("no second download")):
                result = catalog.materialize(self.entry["id"], cache_dir=Path(str(self.root / "cache") + "."))
            self.assertEqual(result["cache_outcome"], "hit")
            self.assertEqual(result["local_path"], first["local_path"])

    def test_aggregate_limit_includes_remote_metadata_and_local_binding(self):
        with patch.object(datasets, "_build_backend", return_value=self.backend):
            first = self.materialize()
            exact = first["materialized_bytes"]
            self.assertEqual(self.materialize("exact", max_bytes=exact)["materialized_bytes"], exact)
            with self.assertRaises(CacheIntegrityError):
                self.materialize("too-small", max_bytes=exact - 1)
        self.assertFalse(any((self.root / "too-small").glob("dataset-*")))
        self.assertFalse(any((self.root / "too-small").glob(".driftbench-dataset-pull-*")))
        with patch.object(datasets, "_build_backend", side_effect=AssertionError("no Azure client")):
            with self.assertRaises(CacheIntegrityError):
                self.materialize(max_bytes=exact - 1)

    def test_source_binding_changes_never_reuse_another_source(self):
        with patch.object(datasets, "_build_backend", return_value=self.backend):
            first = self.materialize()
            self.snapshot["observed_at"] = "2026-09-26T03:00:00Z"
            self.assertEqual(self.materialize()["cache_outcome"], "hit")
            self.snapshot["source"]["account_url"] = "https://anotheraccount.dfs.core.windows.net"
            second = self.materialize()
        self.assertEqual(second["cache_outcome"], "downloaded")
        self.assertNotEqual(first["binding_sha256"], second["binding_sha256"])
        self.assertTrue(Path(first["local_path"]).is_dir())
        self.assertTrue(Path(second["local_path"]).is_dir())

    def test_changed_descriptors_get_a_distinct_cache_binding(self):
        with patch.object(datasets, "_build_backend", return_value=self.backend):
            first = self.materialize()
            self.store.dataset(table_data={"lineitem": b"2|changed|\n"})
            self.snapshot = build_snapshot(CONFIG, self.store.listing(), self.store.read, observed_at=STAMP)
            self.entry = self.snapshot["entries"][0]
            second = self.materialize()
        self.assertEqual(first["entry_id"], second["entry_id"])
        self.assertNotEqual(first["binding_sha256"], second["binding_sha256"])
        self.assertEqual(second["cache_outcome"], "downloaded")
        self.assertTrue(Path(first["local_path"]).is_dir())

    def test_missing_truncated_changed_payloads_and_auth_fail_without_publication(self):
        path = f"{self.remote}/{self.entry['files'][0]['path']}"
        original = self.store.files[path]
        for name, content, error in (
            ("missing", None, RemoteObjectNotFound),
            ("truncated", original[:-1], CacheIntegrityError),
            ("changed", b"x" + original[1:], CacheIntegrityError),
            ("oversize", original + b"x", CacheIntegrityError),
        ):
            with self.subTest(fault=name):
                if content is None:
                    self.store.files.pop(path)
                else:
                    self.store.files[path] = content
                try:
                    with patch.object(datasets, "_build_backend", return_value=self.backend):
                        with self.assertRaises(error):
                            self.materialize(name)
                    self.assertEqual({p.name for p in (self.root / name).iterdir()}, {".locks"})
                finally:
                    self.store.files[path] = original
        with patch.object(datasets, "_build_backend", side_effect=CacheAuthenticationError("auth rejected")):
            with self.assertRaises(CacheAuthenticationError):
                self.materialize("auth")
        self.assertEqual({p.name for p in (self.root / "auth").iterdir()}, {".locks"})
        read = self.backend.iter_bytes

        def interrupted(remote_path):
            if remote_path == path:
                yield b"1"
                raise CacheTransportError("payload transfer interrupted")
            yield from read(remote_path)

        with patch.object(datasets, "_build_backend", return_value=self.backend), \
                patch.object(self.backend, "iter_bytes", side_effect=interrupted):
            with self.assertRaisesRegex(CacheTransportError, "interrupted"):
                self.materialize("transport")
        self.assertEqual({p.name for p in (self.root / "transport").iterdir()}, {".locks"})

    def test_pinned_hashes_do_not_replace_metadata_structure_and_cross_checks(self):
        original_files = dict(self.store.files)
        original_entry = copy.deepcopy(self.entry)
        for name in ("commit-state", "provenance-payload", "commit-descriptor", "metadata-hash"):
            with self.subTest(fault=name):
                self.store.files = dict(original_files)
                self.entry.clear()
                self.entry.update(copy.deepcopy(original_entry))
                commit = json.loads(self.store.files[f"{self.remote}/_COMMITTED.json"])
                provenance = json.loads(self.store.files[f"{self.remote}/provenance_manifest.json"])
                if name == "commit-state":
                    commit["state"] = "STAGING"
                elif name == "provenance-payload":
                    provenance["artifacts"][0]["bytes"] += 1
                elif name == "commit-descriptor":
                    commit["objects"][-1]["bytes"] += 1
                encoded = encode(provenance)
                self.store.files[f"{self.remote}/provenance_manifest.json"] = encoded
                self.entry["manifest_sha256"] = sha(encoded)
                commit["objects"][0].update(bytes=len(encoded), sha256=sha(encoded))
                marker = encode(commit)
                self.store.files[f"{self.remote}/_COMMITTED.json"] = marker
                self.entry["commit_sha256"] = sha(marker)
                if name == "metadata-hash":
                    self.store.files[f"{self.remote}/provenance_manifest.json"] += b" "
                with patch.object(datasets, "_build_backend", return_value=self.backend):
                    with self.assertRaises(CacheIntegrityError):
                        self.materialize(name)
                self.assertFalse(any((self.root / name).glob("dataset-*")))

    def test_invalid_existing_cache_is_not_repaired_or_overwritten(self):
        for name in ("payload", "missing", "binding", "metadata", "extra-file", "extra-directory"):
            with self.subTest(fault=name):
                with patch.object(datasets, "_build_backend", return_value=self.backend):
                    result = self.materialize(name)
                directory = Path(result["local_path"])
                payload = Path(result["files"][0])
                if name == "payload":
                    payload.write_bytes(b"x" + payload.read_bytes()[1:])
                elif name == "missing":
                    payload.unlink()
                elif name == "binding":
                    (directory / "_LOCAL_CACHE.json").write_bytes(b"{}")
                elif name == "metadata":
                    (directory / "provenance_manifest.json").write_bytes(b"{}")
                elif name == "extra-file":
                    (directory / "my-file.txt").write_text("keep me", encoding="utf-8")
                else:
                    (directory / "my-directory").mkdir()
                before = {str(p): p.read_bytes() for p in directory.rglob("*") if p.is_file()}
                with patch.object(datasets, "_build_backend", side_effect=AssertionError("no repair")):
                    with self.assertRaises(CacheIntegrityError):
                        self.materialize(name)
                self.assertEqual(before, {str(p): p.read_bytes() for p in directory.rglob("*") if p.is_file()})

    def test_interrupted_publish_and_collision_clean_only_the_owned_stage(self):
        with patch.object(datasets, "_build_backend", return_value=self.backend):
            with patch.object(Path, "rename", side_effect=OSError("publication interrupted")):
                with self.assertRaisesRegex(OSError, "interrupted"):
                    self.materialize("interrupted")
            self.assertEqual({p.name for p in (self.root / "interrupted").iterdir()}, {".locks"})

            def collision(stage, destination):
                destination.mkdir()
                (destination / "user.txt").write_text("preserve", encoding="utf-8")
                raise FileExistsError("concurrent destination")

            with patch.object(Path, "rename", new=collision):
                with self.assertRaises(CacheCollisionError):
                    self.materialize("collision")
            records = list((self.root / "collision").glob("dataset-*/user.txt"))
            self.assertEqual(len(records), 1)
            self.assertEqual(records[0].read_text(encoding="utf-8"), "preserve")
            self.assertFalse(any((self.root / "collision").glob(".driftbench-dataset-pull-*")))

    def test_concurrent_calls_publish_once_and_waiter_verifies_a_hit(self):
        entered, release = threading.Event(), threading.Event()
        backend = self.backend
        original = backend.iter_bytes

        def slow_read(path):
            if not entered.is_set():
                entered.set()
                if not release.wait(5):
                    raise AssertionError("test did not release the download")
            yield from original(path)

        with patch.object(backend, "iter_bytes", side_effect=slow_read), \
                patch.object(datasets, "_build_backend", return_value=backend) as factory, \
                ThreadPoolExecutor(max_workers=2) as executor:
            first = executor.submit(self.materialize)
            self.assertTrue(entered.wait(5))
            second = executor.submit(self.materialize)
            release.set()
            results = [first.result(timeout=15), second.result(timeout=15)]
        self.assertEqual(sorted(result["cache_outcome"] for result in results), ["downloaded", "hit"])
        self.assertEqual(results[0]["local_path"], results[1]["local_path"])
        self.assertEqual(factory.call_count, 1)
        self.assertEqual(len(backend.reads), 10)

    def test_link_boundaries_and_lock_timeout_are_explicit_failures(self):
        cache_dir = self.root / "linked"
        cache_dir.mkdir()
        is_link = service._is_link_like
        with patch.object(service, "_is_link_like", side_effect=lambda path: path == cache_dir or is_link(path)), \
                patch.object(datasets, "_build_backend", side_effect=AssertionError("no Azure client")):
            with self.assertRaises(CacheIntegrityError):
                self.materialize("linked")
        root = self.root / "locked"
        binding = datasets._binding(self.entry, self.snapshot["source"])
        lock = root / ".locks" / (sha(binding) + ".lock")
        with patch.object(service, "_LOCK_TIMEOUT_SECONDS", 0), service._exclusive_lock(lock), \
                patch.object(datasets, "_build_backend", side_effect=AssertionError("no Azure client")):
            with self.assertRaises(CacheTransportError):
                self.materialize("locked")


if __name__ == "__main__":
    unittest.main()
