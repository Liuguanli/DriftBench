from __future__ import annotations

import hashlib
import json
import os
import stat
import tempfile
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path
from unittest import mock

from driftbench import AzureHNSCacheConfig, RemoteCacheMode, materialize_artifacts
from driftbench.cache import identity as identity_module
from driftbench.cache.errors import (
    CacheAuthorizationError,
    CacheCollisionError,
    CacheConfigurationError,
    CacheCredentialError,
    CacheIntegrityError,
    CacheTransportError,
    RemoteObjectConflict,
)
from driftbench.cache.identity import identity_for
from driftbench.data.benchbase import BenchBaseData
from driftbench.data.dsb import DSBData, DSBQueries
from driftbench.data.job import JOBData, JOBQueries
from driftbench.data.pgbench import PgBenchData, PgBenchQueries
from driftbench.data.tpcc import TPCCData, TPCCQueries
from driftbench.data.tpcc_skew import TPCCSkewData, TPCCSkewQueries
from driftbench.data.tpcds import TPCDSData
from driftbench.data.tpch import TPCHQueries
from driftbench.data.ycsb import YCSBData, YCSBQueries

from .fakes import FakeRemoteBackend


def _config(mode: RemoteCacheMode) -> AzureHNSCacheConfig:
    return AzureHNSCacheConfig(
        account_url="https://unittest.dfs.core.windows.net",
        file_system="driftbench-cache",
        prefix="team-a",
        mode=mode,
    )


def _entry(backend: FakeRemoteBackend) -> str:
    manifests = [path for path in backend.files if path.endswith("/manifest.json")]
    assert len(manifests) == 1
    return manifests[0].removesuffix("/manifest.json")


class RemoteCacheServiceTests(unittest.TestCase):
    def test_off_is_fully_local_and_never_touches_backend(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            backend = FakeRemoteBackend()
            backend.read_error = AssertionError("off mode touched remote storage")
            result = materialize_artifacts(
                adapter=YCSBData(record_count=2),
                output_dir=tmp,
                remote_cache=AzureHNSCacheConfig(mode="off"),
                _backend=backend,
            )
            self.assertEqual(result.cache_outcome, "disabled_generated")
            self.assertEqual(result.materialization_source, "generated")
            self.assertEqual(result.remote_entry_status, "not_checked")
            self.assertFalse(result.force)
            self.assertEqual(backend.calls, [])

            reused = materialize_artifacts(
                adapter=YCSBData(record_count=2),
                output_dir=tmp,
                remote_cache=AzureHNSCacheConfig(mode="off"),
                _backend=backend,
            )
            self.assertEqual(reused.cache_outcome, "disabled_local_cache")
            self.assertEqual(reused.materialization_source, "local_cache")
            self.assertEqual(reused.remote_entry_status, "not_checked")
            self.assertEqual(backend.calls, [])

    def test_read_miss_generates_without_remote_writes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            backend = FakeRemoteBackend()
            result = materialize_artifacts(
                adapter=YCSBData(record_count=2),
                output_dir=tmp,
                remote_cache=_config(RemoteCacheMode.READ),
                _backend=backend,
            )
            self.assertEqual(result.cache_outcome, "miss_generated")
            self.assertEqual(result.materialization_source, "generated")
            self.assertEqual(result.remote_entry_status, "miss")
            self.assertTrue(result.files[0].is_file())
            self.assertEqual(
                [operation for operation, _ in backend.calls if operation.startswith("upload")],
                [],
            )

            refreshed = materialize_artifacts(
                adapter=YCSBData(record_count=2),
                output_dir=tmp,
                remote_cache=_config(RemoteCacheMode.READ),
                _backend=backend,
            )
            self.assertEqual(refreshed.cache_outcome, "miss_generated")
            self.assertEqual(refreshed.materialization_source, "generated")
            self.assertEqual(refreshed.remote_entry_status, "miss")

    def test_same_output_subtree_materializations_are_serialized(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            state_lock = threading.Lock()
            active = 0
            maximum_active = 0
            original_generate = YCSBData.generate

            def tracked_generate(adapter, *args, **kwargs):
                nonlocal active, maximum_active
                with state_lock:
                    active += 1
                    maximum_active = max(maximum_active, active)
                try:
                    time.sleep(0.1)
                    return original_generate(adapter, *args, **kwargs)
                finally:
                    with state_lock:
                        active -= 1

            def materialize_one(_index: int):
                return materialize_artifacts(
                    adapter=YCSBData(record_count=2),
                    output_dir=tmp,
                    remote_cache=_config(RemoteCacheMode.READ),
                    _backend=FakeRemoteBackend(),
                )

            with mock.patch.object(YCSBData, "generate", tracked_generate):
                with ThreadPoolExecutor(max_workers=2) as executor:
                    results = list(executor.map(materialize_one, range(2)))

            self.assertEqual(maximum_active, 1)
            self.assertEqual(
                {result.materialization_source for result in results},
                {"generated"},
            )

    def test_off_same_output_subtree_materializations_are_serialized(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            state_lock = threading.Lock()
            start = threading.Barrier(2)
            active = 0
            maximum_active = 0
            original_generate = YCSBData.generate
            backend = FakeRemoteBackend()
            backend.read_error = AssertionError("off mode touched remote storage")

            def tracked_generate(adapter, *args, **kwargs):
                nonlocal active, maximum_active
                with state_lock:
                    active += 1
                    maximum_active = max(maximum_active, active)
                try:
                    time.sleep(0.1)
                    return original_generate(adapter, *args, **kwargs)
                finally:
                    with state_lock:
                        active -= 1

            def materialize_one(_index: int):
                start.wait(timeout=5)
                return materialize_artifacts(
                    adapter=YCSBData(record_count=2),
                    output_dir=tmp,
                    remote_cache=AzureHNSCacheConfig(mode=RemoteCacheMode.OFF),
                    _backend=backend,
                )

            with mock.patch.object(YCSBData, "generate", tracked_generate):
                with ThreadPoolExecutor(max_workers=2) as executor:
                    results = list(executor.map(materialize_one, range(2)))

            self.assertEqual(maximum_active, 1)
            self.assertEqual(
                {result.materialization_source for result in results},
                {"generated", "local_cache"},
            )
            self.assertEqual(backend.calls, [])

    def test_read_write_miss_then_exact_hit_without_generation(self) -> None:
        with tempfile.TemporaryDirectory() as first_tmp, tempfile.TemporaryDirectory() as hit_tmp:
            backend = FakeRemoteBackend()
            first = materialize_artifacts(
                adapter=YCSBData(record_count=2),
                output_dir=first_tmp,
                remote_cache=_config(RemoteCacheMode.READ_WRITE),
                _backend=backend,
            )
            self.assertEqual(first.cache_outcome, "miss_generated_uploaded")
            self.assertEqual(first.materialization_source, "generated")
            self.assertEqual(first.remote_entry_status, "uploaded")
            self.assertTrue(any(op == "rename" for op, _ in backend.calls))

            with mock.patch.object(
                YCSBData,
                "generate",
                side_effect=AssertionError("remote hit regenerated locally"),
            ):
                hit = materialize_artifacts(
                    adapter=YCSBData(record_count=2),
                    output_dir=hit_tmp,
                    remote_cache=_config(RemoteCacheMode.READ_WRITE),
                    _backend=backend,
                )
            self.assertEqual(hit.cache_outcome, "hit")
            self.assertEqual(hit.materialization_source, "remote_cache")
            self.assertEqual(hit.remote_entry_status, "hit")
            self.assertFalse(hit.force)
            self.assertEqual(hit.files[0].read_bytes(), first.files[0].read_bytes())
            self.assertTrue(hit.metadata.is_file())

    def test_revision_only_remote_miss_forces_fresh_generation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            backend = FakeRemoteBackend()
            adapter = YCSBData(record_count=2)
            first = materialize_artifacts(
                adapter=adapter,
                output_dir=tmp,
                remote_cache=_config(RemoteCacheMode.READ_WRITE),
                _backend=backend,
            )
            type_name = f"{type(adapter).__module__}.{type(adapter).__qualname__}"
            current_rule = identity_module._RULES[type_name]
            generate_forces: list[bool] = []
            original_generate = YCSBData.generate

            def tracked_generate(current, *args, **kwargs):
                generate_forces.append(kwargs["force"])
                return original_generate(current, *args, **kwargs)

            with (
                mock.patch.dict(
                    identity_module._RULES,
                    {type_name: replace(current_rule, revision="revision-only-bump")},
                ),
                mock.patch.object(YCSBData, "generate", tracked_generate),
            ):
                second = materialize_artifacts(
                    adapter=YCSBData(record_count=2),
                    output_dir=tmp,
                    remote_cache=_config(RemoteCacheMode.READ_WRITE),
                    _backend=backend,
                )

            self.assertEqual(generate_forces, [True])
            self.assertEqual(second.cache_outcome, "miss_generated_uploaded")
            self.assertEqual(second.materialization_source, "generated")
            self.assertNotEqual(second.remote_path, first.remote_path)

    def test_producer_only_remote_miss_forces_fresh_generation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            backend = FakeRemoteBackend()
            first = materialize_artifacts(
                adapter=YCSBData(record_count=2),
                output_dir=tmp,
                remote_cache=_config(RemoteCacheMode.READ_WRITE),
                _backend=backend,
            )
            generate_forces: list[bool] = []
            original_generate = YCSBData.generate

            def tracked_generate(current, *args, **kwargs):
                generate_forces.append(kwargs["force"])
                return original_generate(current, *args, **kwargs)

            with (
                mock.patch.object(
                    identity_module,
                    "PRODUCER_CONTRACT_REVISION",
                    "producer-only-bump",
                ),
                mock.patch.object(YCSBData, "generate", tracked_generate),
            ):
                second = materialize_artifacts(
                    adapter=YCSBData(record_count=2),
                    output_dir=tmp,
                    remote_cache=_config(RemoteCacheMode.READ_WRITE),
                    _backend=backend,
                )

            self.assertEqual(generate_forces, [True])
            self.assertEqual(second.cache_outcome, "miss_generated_uploaded")
            self.assertEqual(second.materialization_source, "generated")
            self.assertNotEqual(second.remote_path, first.remote_path)

    def test_forced_generation_rejects_reported_local_reuse(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            adapter = YCSBData(record_count=2)
            stale = replace(adapter.generate(output_dir=tmp), reused_local=True)
            backend = FakeRemoteBackend()

            with mock.patch.object(YCSBData, "generate", return_value=stale):
                with self.assertRaisesRegex(
                    CacheIntegrityError,
                    "forced local generation unexpectedly reported local cache reuse",
                ):
                    materialize_artifacts(
                        adapter=adapter,
                        output_dir=tmp,
                        remote_cache=_config(RemoteCacheMode.READ_WRITE),
                        _backend=backend,
                    )

            self.assertFalse(
                any(operation.startswith("upload") for operation, _ in backend.calls)
            )

    @unittest.skipIf(os.name == "nt", "requires case-distinct POSIX filenames")
    def test_remote_hit_does_not_fold_case_distinct_extra_local_file(self) -> None:
        with tempfile.TemporaryDirectory() as first_tmp, tempfile.TemporaryDirectory() as hit_tmp:
            backend = FakeRemoteBackend()
            materialize_artifacts(
                adapter=YCSBData(record_count=2),
                output_dir=first_tmp,
                remote_cache=_config(RemoteCacheMode.READ_WRITE),
                _backend=backend,
            )
            materialize_artifacts(
                adapter=YCSBData(record_count=2),
                output_dir=hit_tmp,
                remote_cache=_config(RemoteCacheMode.READ),
                _backend=backend,
            )

            declared = Path(hit_tmp) / "ycsb" / "data" / "usertable.csv"
            extra = declared.with_name("USERTABLE.CSV")
            extra.write_text("unexpected\n", encoding="utf-8")
            self.assertTrue(declared.is_file())
            self.assertTrue(extra.is_file())

            refreshed = materialize_artifacts(
                adapter=YCSBData(record_count=2),
                output_dir=hit_tmp,
                remote_cache=_config(RemoteCacheMode.READ),
                _backend=backend,
            )

            self.assertEqual(refreshed.materialization_source, "remote_cache")
            self.assertTrue(declared.is_file())
            self.assertFalse(extra.exists())

    def test_corruption_fails_closed_without_generation(self) -> None:
        with tempfile.TemporaryDirectory() as first_tmp, tempfile.TemporaryDirectory() as hit_tmp:
            backend = FakeRemoteBackend()
            materialize_artifacts(
                adapter=YCSBData(record_count=2),
                output_dir=first_tmp,
                remote_cache=_config(RemoteCacheMode.READ_WRITE),
                _backend=backend,
            )
            entry = _entry(backend)
            artifact = next(
                path for path in backend.files if path.startswith(entry + "/artifacts/")
                and path.endswith("usertable.csv")
            )
            payload = bytearray(backend.files[artifact])
            payload[-1] ^= 0x01
            backend.files[artifact] = bytes(payload)

            with mock.patch.object(
                YCSBData,
                "generate",
                side_effect=AssertionError("corruption triggered generation"),
            ):
                with self.assertRaises(CacheIntegrityError):
                    materialize_artifacts(
                        adapter=YCSBData(record_count=2),
                        output_dir=hit_tmp,
                        remote_cache=_config(RemoteCacheMode.READ),
                        _backend=backend,
                    )

    def test_oversized_remote_stream_stops_before_full_payload_is_consumed(self) -> None:
        with tempfile.TemporaryDirectory() as first_tmp, tempfile.TemporaryDirectory() as hit_tmp:
            backend = FakeRemoteBackend()
            materialize_artifacts(
                adapter=YCSBData(record_count=2),
                output_dir=first_tmp,
                remote_cache=_config(RemoteCacheMode.READ_WRITE),
                _backend=backend,
            )
            entry = _entry(backend)
            artifact = next(
                path
                for path in backend.files
                if path.startswith(entry + "/artifacts/")
                and path.endswith("usertable.csv")
            )
            declared_size = len(backend.files[artifact])
            oversized = b"x" * (declared_size * 4 + 2)
            backend.files[artifact] = oversized
            backend.bytes_yielded[artifact] = 0

            with self.assertRaises(CacheIntegrityError):
                materialize_artifacts(
                    adapter=YCSBData(record_count=2),
                    output_dir=hit_tmp,
                    remote_cache=_config(RemoteCacheMode.READ),
                    _backend=backend,
                )
            self.assertLess(backend.bytes_yielded[artifact], len(oversized))
            self.assertFalse(
                any(path.name.startswith(".driftbench-cache-pull-") for path in Path(hit_tmp).glob("*"))
            )

    @unittest.skipIf(os.name == "nt", "POSIX executable bits are not represented on Windows")
    def test_remote_hit_restores_pgbench_script_executable_bit(self) -> None:
        with tempfile.TemporaryDirectory() as first_tmp, tempfile.TemporaryDirectory() as hit_tmp:
            backend = FakeRemoteBackend()
            first = materialize_artifacts(
                adapter=PgBenchQueries(workload="select_only", clients=1, duration=1),
                output_dir=first_tmp,
                remote_cache=_config(RemoteCacheMode.READ_WRITE),
                _backend=backend,
            )
            first_script = next(path for path in first.files if path.name == "run_pgbench.sh")
            self.assertTrue(first_script.stat().st_mode & stat.S_IXUSR)

            hit = materialize_artifacts(
                adapter=PgBenchQueries(workload="select_only", clients=1, duration=1),
                output_dir=hit_tmp,
                remote_cache=_config(RemoteCacheMode.READ),
                _backend=backend,
            )
            hit_script = next(path for path in hit.files if path.name == "run_pgbench.sh")
            self.assertTrue(hit_script.stat().st_mode & stat.S_IXUSR)
            self.assertEqual(hit.materialization_source, "remote_cache")

    def test_auth_authorization_and_transport_never_become_misses(self) -> None:
        errors = (
            CacheCredentialError("credential unavailable"),
            CacheAuthorizationError("forbidden"),
            CacheTransportError("network unavailable"),
        )
        for error in errors:
            with self.subTest(error=type(error).__name__), tempfile.TemporaryDirectory() as tmp:
                backend = FakeRemoteBackend()
                backend.read_error = error
                with mock.patch.object(
                    YCSBData,
                    "generate",
                    side_effect=AssertionError("remote error triggered generation"),
                ):
                    with self.assertRaises(type(error)):
                        materialize_artifacts(
                            adapter=YCSBData(record_count=2),
                            output_dir=tmp,
                            remote_cache=_config(RemoteCacheMode.READ_WRITE),
                            _backend=backend,
                        )

    def test_identical_rename_loser_is_idempotent_and_never_overwrites(self) -> None:
        with tempfile.TemporaryDirectory() as first_tmp, tempfile.TemporaryDirectory() as second_tmp:
            backend = FakeRemoteBackend()
            first = materialize_artifacts(
                adapter=YCSBData(record_count=2),
                output_dir=first_tmp,
                remote_cache=_config(RemoteCacheMode.READ_WRITE),
                _backend=backend,
            )
            snapshot = dict(backend.files)
            second = materialize_artifacts(
                adapter=YCSBData(record_count=2),
                output_dir=second_tmp,
                remote_cache=_config(RemoteCacheMode.READ_WRITE),
                force=True,
                _backend=backend,
            )
            self.assertEqual(second.cache_outcome, "concurrent_identical")
            self.assertEqual(second.materialization_source, "generated")
            self.assertEqual(second.remote_entry_status, "concurrent_identical")
            self.assertTrue(second.force)
            self.assertEqual(backend.files, snapshot)
            self.assertEqual(second.files[0].read_bytes(), first.files[0].read_bytes())

    def test_force_read_skips_remote_probe_and_reports_provenance(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            backend = FakeRemoteBackend()
            backend.read_error = AssertionError("force read probed remote storage")
            result = materialize_artifacts(
                adapter=YCSBData(record_count=2),
                output_dir=tmp,
                remote_cache=_config(RemoteCacheMode.READ),
                force=True,
                _backend=backend,
            )
            self.assertEqual(result.cache_outcome, "force_generated")
            self.assertEqual(result.materialization_source, "generated")
            self.assertEqual(result.remote_entry_status, "not_checked")
            self.assertTrue(result.force)
            self.assertEqual(backend.calls, [])

    def test_summary_exposes_complete_cache_provenance(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            result = materialize_artifacts(
                adapter=YCSBData(record_count=2),
                output_dir=tmp,
                remote_cache=AzureHNSCacheConfig(mode="off"),
            )
            summary = result.summary()
            self.assertEqual(summary["cache_mode"], "off")
            self.assertEqual(summary["cache_outcome"], "disabled_generated")
            self.assertEqual(summary["materialization_source"], "generated")
            self.assertEqual(summary["remote_entry_status"], "not_checked")
            self.assertFalse(summary["force"])
            self.assertIsNone(summary["remote_path"])
            self.assertEqual(summary["warnings"], [])

    def test_python_api_rejects_non_boolean_force_before_remote_access(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            backend = FakeRemoteBackend()
            with self.assertRaises(CacheConfigurationError):
                materialize_artifacts(
                    adapter=YCSBData(record_count=2),
                    output_dir=tmp,
                    remote_cache=_config(RemoteCacheMode.READ),
                    force=1,  # type: ignore[arg-type]
                    _backend=backend,
                )
            self.assertEqual(backend.calls, [])

    def test_divergent_rename_winner_is_collision(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            backend = FakeRemoteBackend()

            def install_divergent_winner(source: str, destination: str) -> None:
                backend.before_rename = None
                source_manifest_path = source + "/manifest.json"
                manifest = json.loads(backend.files[source_manifest_path])
                manifest["files"][0]["bytes"] += 1
                encoded = (
                    json.dumps(
                        manifest,
                        ensure_ascii=True,
                        allow_nan=False,
                        separators=(",", ":"),
                        sort_keys=True,
                    )
                    + "\n"
                ).encode("utf-8")
                backend.files[destination + "/manifest.json"] = encoded
                backend.files[destination + "/_COMMITTED.json"] = (
                    json.dumps(
                        {
                            "schema": "driftbench.remote-cache-commit/v1",
                            "fingerprint": manifest["identity"]["fingerprint"],
                            "manifest_sha256": hashlib.sha256(encoded).hexdigest(),
                        },
                        ensure_ascii=True,
                        allow_nan=False,
                        separators=(",", ":"),
                        sort_keys=True,
                    )
                    + "\n"
                ).encode("utf-8")
                raise RemoteObjectConflict("simulated rename race")

            backend.before_rename = install_divergent_winner
            with self.assertRaises(CacheCollisionError):
                materialize_artifacts(
                    adapter=YCSBData(record_count=2),
                    output_dir=tmp,
                    remote_cache=_config(RemoteCacheMode.READ_WRITE),
                    _backend=backend,
                )

    def test_upload_failure_keeps_local_output_and_cleans_only_owned_staging(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            backend = FakeRemoteBackend()
            backend.upload_error = CacheTransportError("injected upload failure")
            output = Path(tmp)
            with self.assertRaises(CacheTransportError):
                materialize_artifacts(
                    adapter=YCSBData(record_count=2),
                    output_dir=output,
                    remote_cache=_config(RemoteCacheMode.READ_WRITE),
                    _backend=backend,
                )
            self.assertTrue((output / "ycsb" / "data" / "usertable.csv").is_file())
            self.assertFalse(any("/_staging/" in path for path in backend.files))

    def test_rename_failure_cleans_staging_without_creating_final_entry(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            backend = FakeRemoteBackend()
            backend.rename_error = CacheTransportError("injected rename failure")
            with self.assertRaises(CacheTransportError):
                materialize_artifacts(
                    adapter=YCSBData(record_count=2),
                    output_dir=tmp,
                    remote_cache=_config(RemoteCacheMode.READ_WRITE),
                    _backend=backend,
                )
            self.assertFalse(any("/_staging/" in path for path in backend.files))
            self.assertFalse(any(path.endswith("/_COMMITTED.json") for path in backend.files))

    def test_remote_traversal_is_rejected_before_download(self) -> None:
        with tempfile.TemporaryDirectory() as first_tmp, tempfile.TemporaryDirectory() as hit_tmp:
            backend = FakeRemoteBackend()
            materialize_artifacts(
                adapter=YCSBData(record_count=2),
                output_dir=first_tmp,
                remote_cache=_config(RemoteCacheMode.READ_WRITE),
                _backend=backend,
            )
            entry = _entry(backend)
            manifest_path = entry + "/manifest.json"
            manifest = json.loads(backend.files[manifest_path])
            manifest["files"][0]["path"] = "../escape.csv"
            encoded = (
                json.dumps(manifest, ensure_ascii=True, allow_nan=False, separators=(",", ":"), sort_keys=True)
                + "\n"
            ).encode("utf-8")
            backend.files[manifest_path] = encoded
            backend.files[entry + "/_COMMITTED.json"] = (
                json.dumps(
                    {
                        "schema": "driftbench.remote-cache-commit/v1",
                        "fingerprint": manifest["identity"]["fingerprint"],
                        "manifest_sha256": hashlib.sha256(encoded).hexdigest(),
                    },
                    ensure_ascii=True,
                    allow_nan=False,
                    separators=(",", ":"),
                    sort_keys=True,
                )
                + "\n"
            ).encode("utf-8")
            with self.assertRaises(CacheIntegrityError):
                materialize_artifacts(
                    adapter=YCSBData(record_count=2),
                    output_dir=hit_tmp,
                    remote_cache=_config(RemoteCacheMode.READ),
                    _backend=backend,
                )
            self.assertFalse((Path(hit_tmp).parent / "escape.csv").exists())

    def test_default_deny_eligibility_precedes_remote_access(self) -> None:
        for adapter in (BenchBaseData(), YCSBQueries()):
            with self.subTest(adapter=type(adapter).__name__), tempfile.TemporaryDirectory() as tmp:
                backend = FakeRemoteBackend()
                with self.assertRaises(CacheConfigurationError):
                    materialize_artifacts(
                        adapter=adapter,
                        output_dir=tmp,
                        remote_cache=_config(RemoteCacheMode.READ_WRITE),
                        _backend=backend,
                    )
                self.assertEqual(backend.calls, [])

    def test_nested_tpch_custom_distribution_paths_are_rejected_before_io(self) -> None:
        class HashableDict(dict):
            __hash__ = object.__hash__

        class CustomIterable:
            def __iter__(self):
                raise AssertionError("custom param_specs iterable was consumed")

        class ListSubclass(list):
            pass

        class ExplosiveEquality:
            def __eq__(self, _other):
                raise RuntimeError(private_dist_file)

        class ExplosiveKey:
            def __init__(self, target: str):
                self.target = target

            def __hash__(self):
                return hash(self.target)

            def __eq__(self, _other):
                raise RuntimeError(private_dist_file)

        class ExplosiveMeta(type):
            def __hash__(self):
                raise RuntimeError(private_dist_file)

        class MetaHashScalar(metaclass=ExplosiveMeta):
            pass

        with tempfile.TemporaryDirectory() as tmp:
            private_dist_file = str(
                (Path(tmp) / "private" / "employee-only-dists.dss").resolve()
            )
            cases = {
                "generator dss_dist definition": {
                    "1": [
                        {
                            "type": "dss_dist",
                            "dist_file": private_dist_file,
                            "dist_name": "nations",
                        }
                    ],
                },
                "nested string dist_file": {
                    "1": {
                        "parameters": [
                            {"source": {"type": "choice", "dist_file": private_dist_file}}
                        ]
                    },
                },
                "set with hashable mapping": {
                    "1": {
                        HashableDict(
                            type="dss_dist",
                            dist_file=private_dist_file,
                            dist_name="nations",
                        )
                    },
                },
                "frozenset with hashable mapping": {
                    "1": frozenset(
                        {
                            HashableDict(
                                type="dss_dist",
                                dist_file=private_dist_file,
                                dist_name="nations",
                            )
                        }
                    ),
                },
                "custom iterable": {"1": CustomIterable()},
                "mapping subclass": HashableDict(
                    {
                        "1": [
                            {
                                "type": "dss_dist",
                                "dist_file": private_dist_file,
                                "dist_name": "nations",
                            }
                        ]
                    }
                ),
                "sequence subclass": {
                    "1": ListSubclass(
                        [
                            {
                                "type": "dss_dist",
                                "dist_file": private_dist_file,
                                "dist_name": "nations",
                            }
                        ]
                    )
                },
                "path-like scalar": {
                    "1": [{"type": "fixed", "value": Path(private_dist_file)}]
                },
                "custom equality scalar": {
                    "1": [{"type": ExplosiveEquality()}]
                },
                "custom metaclass hash scalar": {
                    "1": [{"type": MetaHashScalar()}]
                },
                "custom equality type key": {
                    "1": [{ExplosiveKey("type"): "dss_dist"}]
                },
                "custom equality dist_file key": {
                    "1": [{ExplosiveKey("dist_file"): private_dist_file}]
                },
            }
            cyclic: list[object] = []
            cyclic.append(cyclic)
            cases["cyclic sequence"] = {"1": cyclic}
            deeply_nested: object = "value"
            for _ in range(80):
                deeply_nested = [deeply_nested]
            cases["excessive nesting"] = {"1": deeply_nested}
            for label, param_specs in cases.items():
                with self.subTest(case=label):
                    backend = FakeRemoteBackend()
                    adapter = TPCHQueries(
                        query_ids=[1],
                        mode="custom",
                        param_specs=param_specs,
                    )
                    with mock.patch.object(
                        adapter,
                        "generate",
                        side_effect=AssertionError("ineligible request generated locally"),
                    ) as generate:
                        with self.assertRaises(CacheConfigurationError) as raised:
                            materialize_artifacts(
                                adapter=adapter,
                                output_dir=tmp,
                                remote_cache=_config(RemoteCacheMode.READ),
                                _backend=backend,
                            )
                    generate.assert_not_called()
                    self.assertEqual(backend.calls, [])
                    self.assertNotIn(private_dist_file, str(raised.exception))

    def test_tpch_custom_semantic_variants_never_cross_hit(self) -> None:
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
                backend = FakeRemoteBackend()
                with (
                    tempfile.TemporaryDirectory() as first_tmp,
                    tempfile.TemporaryDirectory() as second_tmp,
                    tempfile.TemporaryDirectory() as hit_tmp,
                ):
                    first_adapter = TPCHQueries(
                        query_ids=[1],
                        mode="custom",
                        param_specs=first_specs,
                        seed=42,
                        shuffle=False,
                    )
                    second_adapter = TPCHQueries(
                        query_ids=[1],
                        mode="custom",
                        param_specs=second_specs,
                        seed=42,
                        shuffle=False,
                    )
                    first = materialize_artifacts(
                        adapter=first_adapter,
                        output_dir=first_tmp,
                        remote_cache=_config(RemoteCacheMode.READ_WRITE),
                        _backend=backend,
                    )
                    second = materialize_artifacts(
                        adapter=second_adapter,
                        output_dir=second_tmp,
                        remote_cache=_config(RemoteCacheMode.READ_WRITE),
                        _backend=backend,
                    )

                    self.assertEqual(first.cache_outcome, "miss_generated_uploaded")
                    self.assertEqual(second.cache_outcome, "miss_generated_uploaded")
                    self.assertNotEqual(first.remote_path, second.remote_path)
                    first_sql = next(path for path in first.files if path.suffix == ".sql")
                    second_sql = next(path for path in second.files if path.suffix == ".sql")
                    self.assertNotEqual(first_sql.read_bytes(), second_sql.read_bytes())

                    second_manifest = json.loads(
                        second.metadata.read_text(encoding="utf-8")
                    )
                    self.assertEqual(
                        second_manifest["cache"]["parameters"],
                        identity_for(second_adapter).parameters,
                    )

                    exact_hit = materialize_artifacts(
                        adapter=TPCHQueries(
                            query_ids=[1],
                            mode="custom",
                            param_specs=second_specs,
                            seed=42,
                            shuffle=False,
                        ),
                        output_dir=hit_tmp,
                        remote_cache=_config(RemoteCacheMode.READ),
                        _backend=backend,
                    )
                    self.assertEqual(exact_hit.cache_outcome, "hit")
                    hit_sql = next(
                        path for path in exact_hit.files if path.suffix == ".sql"
                    )
                    self.assertEqual(hit_sql.read_bytes(), second_sql.read_bytes())

    def test_every_allowlisted_adapter_has_matching_local_and_remote_identity(self) -> None:
        adapters = (
            TPCHQueries(query_ids=[1], queries_per_template=1, seed=42, shuffle=False),
            TPCDSData(scale_factor=1),
            TPCCData(scale_factor=1),
            TPCCQueries(),
            TPCCSkewData(scale_factor=1, hot_warehouse_fraction=1.0, skew_factor=0.99),
            TPCCSkewQueries(scale_factor=1, hot_warehouse_fraction=1.0, skew_factor=0.99),
            YCSBData(record_count=2),
            DSBData(scale_factor=1),
            DSBQueries(),
            JOBData(scale_factor=1),
            JOBQueries(),
            PgBenchData(scale_factor=1),
            PgBenchQueries(workload="select_only", clients=1, duration=1, rate=0),
        )
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            remote_paths: dict[tuple[str, str], str] = {}
            for index, adapter in enumerate(adapters):
                with self.subTest(adapter=type(adapter).__name__):
                    backend = FakeRemoteBackend()
                    result = materialize_artifacts(
                        adapter=adapter,
                        output_dir=root / str(index),
                        remote_cache=_config(RemoteCacheMode.READ),
                        _backend=backend,
                    )
                    self.assertEqual(result.remote_entry_status, "miss")
                    assert result.remote_path is not None
                    self.assertIn(
                        f"/{result.benchmark}/{result.artifact_type}/",
                        result.remote_path,
                    )
                    remote_paths[(result.benchmark, result.artifact_type)] = result.remote_path

            self.assertIn("/tpcc/data/", remote_paths[("tpcc", "data")])
            self.assertIn("/tpcc/queries/", remote_paths[("tpcc", "queries")])
            self.assertNotEqual(
                remote_paths[("tpcc", "data")],
                remote_paths[("tpcc", "queries")],
            )

    def test_remote_metadata_and_keys_do_not_contain_env_secrets_or_output_path(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            secret = "do-not-leak-this-secret"
            previous = os.environ.get("AZURE_CLIENT_SECRET")
            os.environ["AZURE_CLIENT_SECRET"] = secret
            try:
                backend = FakeRemoteBackend()
                materialize_artifacts(
                    adapter=YCSBData(record_count=2),
                    output_dir=tmp,
                    remote_cache=_config(RemoteCacheMode.READ_WRITE),
                    _backend=backend,
                )
            finally:
                if previous is None:
                    os.environ.pop("AZURE_CLIENT_SECRET", None)
                else:
                    os.environ["AZURE_CLIENT_SECRET"] = previous
            remote_text = "\n".join(backend.files) + "\n" + "\n".join(
                payload.decode("utf-8", errors="ignore") for payload in backend.files.values()
            )
            self.assertNotIn(secret, remote_text)
            self.assertNotIn(str(Path(tmp).resolve()), remote_text)

    def test_local_generator_metadata_symlink_is_rejected_before_upload(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            adapter = YCSBData(record_count=2)
            generation = adapter.generate(output_dir=tmp)
            target = generation.metadata.with_name("manifest-target.json")
            target.write_bytes(generation.metadata.read_bytes())
            generation.metadata.unlink()
            try:
                generation.metadata.symlink_to(target)
            except OSError as exc:
                self.skipTest(f"symlinks unavailable: {exc}")

            backend = FakeRemoteBackend()
            with mock.patch.object(YCSBData, "generate", return_value=generation):
                with self.assertRaises(CacheIntegrityError):
                    materialize_artifacts(
                        adapter=adapter,
                        output_dir=tmp,
                        remote_cache=_config(RemoteCacheMode.READ_WRITE),
                        _backend=backend,
                    )
            self.assertFalse(
                any(operation.startswith("upload") for operation, _ in backend.calls)
            )


if __name__ == "__main__":
    unittest.main()
