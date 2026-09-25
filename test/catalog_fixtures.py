from __future__ import annotations

import hashlib
import json
from typing import Any

from driftbench.cache.models import AzureHNSCacheConfig, RemoteCacheMode


CONFIG = AzureHNSCacheConfig(
    account_url="https://catalogtest.dfs.core.windows.net",
    file_system="benchmark-cache",
    prefix="team",
    mode=RemoteCacheMode.READ,
)
STAMP = "2026-09-23T13:00:00Z"

OFFLINE_READER_SCRIPT = r'''
import sys
catalog_reads_allowed = False
def guard(event, args):
    if event == "import" and (args[0] == "azure" or args[0].startswith("azure.")):
        raise AssertionError("Azure import forbidden")
    if event in {"socket.connect", "subprocess.Popen", "os.system"}:
        raise AssertionError("external action forbidden")
    if event == "open" and not catalog_reads_allowed:
        path = str(args[0])
        if path.endswith(("catalog_snapshot.json", "azure-cache.yaml", ".env")):
            raise AssertionError("configuration/catalog read during import")
sys.addaudithook(guard)
import driftbench
assert not any(name == "azure" or name.startswith("azure.") for name in sys.modules)
catalog_reads_allowed = True
from driftbench import catalog
assert catalog.info()["counts"] == {"data": 10, "queries": 0}
assert len(catalog.list()) == 10
assert catalog.get(catalog.list()[0]["id"])
tpch_by_scale = {entry["parameters"]["scale_factor"]: entry for entry in catalog.list(benchmark="tpch")}
assert set(tpch_by_scale) == {"0.01", "10"}
assert tpch_by_scale["10"]["total_bytes"] == 11_232_136_268
'''


def encode(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=True, allow_nan=False, sort_keys=True,
                      separators=(",", ":")).encode("utf-8")


def sha(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


class Inventory:
    def __init__(self) -> None:
        self.files: dict[str, bytes] = {}
        self.payloads: set[str] = set()
        self.reads: list[tuple[str, int]] = []

    def listing(self) -> list[tuple[str, int]]:
        return [(path, len(value)) for path, value in self.files.items()]

    def read(self, path: str, limit: int) -> bytes:
        if path in self.payloads:
            raise AssertionError("exporter must not download payloads")
        self.reads.append((path, limit))
        return self.files[path][:limit]

    def put(self, root: str, path: str, content: bytes, *, payload: bool = False) -> dict[str, Any]:
        full = root + "/" + path
        self.files[full] = content
        if payload:
            self.payloads.add(full)
        return {"path": path, "bytes": len(content), "sha256": sha(content)}

    def cache(self, *, benchmark: str = "ycsb", kind: str = "data",
              parameters: dict[str, Any] | None = None) -> str:
        canonical = {
            "schema": "driftbench-artifact-cache", "version": "v1",
            "producer": {"distribution": "driftbench-db", "version": "0.1.0b10",
                         "contract_revision": "cache5-20260823", "runtime_family": "windows"},
            "benchmark": benchmark, "artifact_type": kind,
            "generator": f"driftbench.data.{benchmark}.TestAdapter", "generator_revision": "1",
            "parameters": {"scale_factor": 1} if parameters is None else parameters,
        }
        fingerprint = sha(encode(canonical))
        generator_id = f"{benchmark}-test-r1"
        root = f"team/cache/driftbench-artifact-cache/v1/{benchmark}/{kind}/{generator_id}/{fingerprint}"
        suffix = "csv" if kind == "data" else "sql"
        descriptors = []
        for name, content, role in (
            (f"example.{suffix}", b"id,value\n1,hello\n", "artifact"),
            (f"{benchmark}_{kind}_manifest.json",
             encode({"benchmark": benchmark, "artifact_type": kind}), "metadata"),
        ):
            path = f"{benchmark}/{kind}/{name}"
            stored = self.put(root, "artifacts/" + path, content, payload=role == "artifact")
            descriptors.append({**stored, "path": path, "role": role, "executable": False})
        manifest = encode({
            "schema": "driftbench.remote-cache-manifest/v1", "version": 1,
            "identity": {**canonical, "fingerprint": fingerprint, "generator_id": generator_id},
            "output_subtree": f"{benchmark}/{kind}", "files": descriptors,
        })
        self.put(root, "manifest.json", manifest)
        self.put(root, "_COMMITTED.json", encode({
            "schema": "driftbench.remote-cache-commit/v1",
            "fingerprint": fingerprint, "manifest_sha256": sha(manifest),
        }))
        return root

    def dataset(self) -> str:
        fingerprint = sha(b"immutable dataset identity")
        revision = "b" * 40
        root = f"team/datasets/v1/tpch/data/tpchgen-rs/{revision}/sf_0.01/{fingerprint}"
        artifacts = []
        for table in ("region", "nation", "supplier", "customer", "part", "partsupp", "orders", "lineitem"):
            artifacts.append(self.put(root, f"tpch/data/sf_0.01/{table}.tbl",
                                      b"1|example|\n", payload=True))
        control_path = "tpch/data/sf_0.01/tpch_data_manifest.json"
        artifacts.append(self.put(root, control_path, encode({"benchmark": "tpch", "artifact_type": "data"})))
        provenance = encode({
            "schema": "driftbench.dataset-provenance/v1", "benchmark": "tpch",
            "artifact_type": "data", "scale_factor": "0.01", "format": "tbl",
            "bundle_fingerprint": fingerprint, "artifacts": artifacts,
            "driftbench_validation": {"published_manifest_path": control_path},
            "source": {"commit": revision, "private_note": "DO-NOT-PUBLISH"},
            "build": {"command": "SECRET-BUILD-COMMAND"},
        })
        descriptor = self.put(root, "provenance_manifest.json", provenance)
        objects = [descriptor, *artifacts]
        self.put(root, "_COMMITTED.json", encode({
            "schema": "driftbench.immutable-dataset-commit/v1", "state": "COMMITTED",
            "bundle_fingerprint": fingerprint, "object_count": len(objects), "objects": objects,
        }))
        return root

    def change_cache_manifest(self, root: str, change: Any) -> None:
        path = root + "/manifest.json"
        manifest = json.loads(self.files[path])
        change(manifest)
        self.files[path] = encode(manifest)
        marker = root + "/_COMMITTED.json"
        commit = json.loads(self.files[marker])
        commit["manifest_sha256"] = sha(self.files[path])
        self.files[marker] = encode(commit)
