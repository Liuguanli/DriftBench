"""Export reviewed public metadata locally; never modify Azure or read payloads.

Run from the repository root with ``python -m scripts.export_azure_catalog``.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import tempfile
from collections.abc import Callable, Iterable
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any

from driftbench import catalog
from driftbench.cache.credentials import build_azure_credential
from driftbench.cache.errors import AzureDependencyError, CacheConfigurationError
from driftbench.cache.models import AzureHNSCacheConfig, RemoteCacheMode
from driftbench.cache.requests import load_azure_cache_config


_METADATA_LIMIT = 1024 * 1024
_OBJECT_LIMIT = 100_000
_require = catalog._require


def _sha256(encoded: bytes) -> str:
    return hashlib.sha256(encoded).hexdigest()


def _canonical(payload: dict[str, Any]) -> bytes:
    return json.dumps(payload, ensure_ascii=True, allow_nan=False, sort_keys=True,
                      separators=(",", ":")).encode("utf-8")


def build_snapshot(
    config: AzureHNSCacheConfig,
    objects: Iterable[tuple[str, int]],
    read_metadata: Callable[[str, int], bytes],
    *,
    observed_at: str | None = None,
) -> dict[str, Any]:
    """Build a snapshot from a complete listing and bounded metadata-only reads.

    ``read_metadata(path, limit)`` must read at most ``limit`` bytes. Exceptions
    during listing or reading propagate; no partial catalog is returned.
    """
    _require(config.normalized_mode() is RemoteCacheMode.READ,
             "catalog export requires read-only Azure configuration")
    config.validate_enabled()
    prefix = config.prefix + "/" if config.prefix else ""
    listing: dict[str, int] = {}
    folded: set[str] = set()
    for path, size in objects:
        catalog._path(path, "inventory path")
        catalog._integer(size, "inventory object size")
        _require(path.startswith(prefix), "Azure listing escaped the configured prefix")
        _require(path.casefold() not in folded, "Azure inventory paths collide")
        _require(len(listing) < _OBJECT_LIMIT, "Azure inventory exceeds the object limit")
        listing[path] = size
        folded.add(path.casefold())

    def metadata(path: str, expected_hash: str | None = None) -> tuple[dict[str, Any], bytes]:
        _require(path in listing, "committed entry references missing metadata")
        size = listing[path]
        _require(0 < size <= _METADATA_LIMIT, "metadata exceeds the read-size limit or is empty")
        encoded = read_metadata(path, size + 1)
        _require(isinstance(encoded, bytes) and len(encoded) == size,
                 "metadata changed since inventory or the read was incomplete")
        if expected_hash is not None:
            _require(_sha256(encoded) == expected_hash, "committed metadata hash mismatch")
        return catalog._decode(encoded, "remote metadata"), encoded

    def descriptors(values: Any, root: str) -> list[dict[str, Any]]:
        _require(isinstance(values, list) and 0 < len(values) <= catalog._MAX_FILES,
                 "committed entry requires a bounded object list")
        result = []
        seen: set[str] = set()
        for value in values:
            item = catalog._descriptor(value)
            _require(item["path"].casefold() not in seen, "committed object paths collide")
            seen.add(item["path"].casefold())
            path = root + "/" + item["path"]
            _require(path in listing and listing[path] == item["bytes"],
                     "committed object is missing or its listed size differs")
            result.append(dict(item))
        return result

    def local_manifest(root: str, item: dict[str, Any], benchmark: str, kind: str) -> None:
        _require(item["path"].endswith("_manifest.json"), "invalid generator metadata path")
        payload, _ = metadata(root + "/" + item["path"], item["sha256"])
        _require(payload.get("benchmark") == benchmark and payload.get("artifact_type") == kind,
                 "generator metadata disagrees with its committed entry")

    entries: list[dict[str, Any]] = []
    for marker in sorted(path for path in listing if path.endswith("/_COMMITTED.json")):
        relative = marker[len(prefix):]
        layout = next((name for name, base in catalog._LAYOUTS.items()
                       if relative.startswith(base + "/")), None)
        if layout is None:
            continue
        parts = relative[len(catalog._LAYOUTS[layout]) + 1:].split("/")
        _require(len(parts) >= 5, "committed entry has an invalid layout")
        benchmark, kind = parts[:2]
        if kind not in ("data", "queries"):
            continue
        root = marker.rsplit("/", 1)[0]
        fingerprint = catalog._digest(parts[-2], "entry fingerprint")
        commit, commit_bytes = metadata(marker)
        if layout == "artifact-cache-v1":
            _require(len(parts) == 5, "cache entry has an invalid layout")
            catalog._shape(commit, {"schema", "fingerprint", "manifest_sha256"}, "cache commit")
            _require(commit["schema"] == "driftbench.remote-cache-commit/v1"
                     and commit["fingerprint"] == fingerprint, "cache commit identity mismatch")
            manifest_path = "manifest.json"
            manifest, manifest_bytes = metadata(
                root + "/" + manifest_path,
                catalog._digest(commit["manifest_sha256"], "cache manifest hash"),
            )
            catalog._shape(manifest, {"schema", "version", "identity", "output_subtree", "files"},
                           "cache manifest")
            _require(manifest["schema"] == "driftbench.remote-cache-manifest/v1"
                     and type(manifest["version"]) is int and manifest["version"] == 1,
                     "unsupported cache manifest schema/version")
            identity = catalog._shape(manifest["identity"], {
                "schema", "version", "producer", "benchmark", "artifact_type",
                "generator", "generator_revision", "parameters", "fingerprint", "generator_id",
            }, "cache identity")
            _require(identity["schema"] == "driftbench-artifact-cache" and identity["version"] == "v1",
                     "unsupported cache identity schema/version")
            _require(identity["benchmark"] == benchmark and identity["artifact_type"] == kind
                     and identity["fingerprint"] == fingerprint and identity["generator_id"] == parts[2]
                     and manifest["output_subtree"] == f"{benchmark}/{kind}",
                     "cache identity disagrees with its path")
            canonical = {key: value for key, value in identity.items()
                         if key not in {"fingerprint", "generator_id"}}
            _require(_sha256(_canonical(canonical)) == fingerprint, "cache identity fingerprint mismatch")
            raw_files = manifest["files"]
            _require(isinstance(raw_files, list) and 1 < len(raw_files) <= catalog._MAX_FILES,
                     "cache manifest has an invalid file list")
            converted, roles = [], {}
            for raw in raw_files:
                catalog._shape(raw, {"path", "role", "bytes", "sha256", "executable"}, "cache file")
                path = catalog._path(raw["path"], "cache artifact path")
                _require(path.startswith(f"{benchmark}/{kind}/"), "cache artifact escapes its subtree")
                _require(raw["role"] in ("artifact", "metadata") and type(raw["executable"]) is bool,
                         "cache descriptor role or executable flag is invalid")
                converted.append({"path": "artifacts/" + path, "bytes": raw["bytes"], "sha256": raw["sha256"]})
                roles["artifacts/" + path] = raw["role"]
            all_files = descriptors(converted, root)
            control = [item for item in all_files if roles[item["path"]] == "metadata"]
            _require(len(control) == 1, "cache entry must have one generator manifest")
            local_manifest(root, control[0], benchmark, kind)
            payload_files = [item for item in all_files if roles[item["path"]] == "artifact"]
            generator = identity["generator"]
            revision = identity["generator_revision"]
            raw_parameters = identity["parameters"]
            _require(isinstance(raw_parameters, dict), "cache parameters must be an object")
            parameters = {key: value for key, value in raw_parameters.items() if key in catalog._PARAMETERS}
        else:
            catalog._shape(commit, {"schema", "state", "bundle_fingerprint", "object_count", "objects"},
                           "dataset commit")
            _require(commit["schema"] == "driftbench.immutable-dataset-commit/v1"
                     and commit["state"] == "COMMITTED" and commit["bundle_fingerprint"] == fingerprint,
                     "dataset commit identity/state mismatch")
            all_files = descriptors(commit["objects"], root)
            catalog._integer(commit["object_count"], "dataset object_count")
            _require(commit["object_count"] == len(all_files), "dataset object_count mismatch")
            by_path = {item["path"]: item for item in all_files}
            manifest_path = "provenance_manifest.json"
            _require(manifest_path in by_path, "dataset has no committed provenance manifest")
            manifest, manifest_bytes = metadata(root + "/" + manifest_path, by_path[manifest_path]["sha256"])
            _require(manifest.get("schema") == "driftbench.dataset-provenance/v1"
                     and manifest.get("benchmark") == benchmark and manifest.get("artifact_type") == kind
                     and manifest.get("bundle_fingerprint") == fingerprint,
                     "dataset provenance identity mismatch")
            declared = descriptors(manifest.get("artifacts"), root)
            _require({item["path"]: item for item in declared}
                     == {path: item for path, item in by_path.items() if path != manifest_path},
                     "dataset provenance descriptors disagree with its commit")
            validation = manifest.get("driftbench_validation")
            _require(isinstance(validation, dict), "dataset lacks generator metadata reference")
            control_path = catalog._path(validation.get("published_manifest_path"), "dataset generator manifest")
            _require(control_path in by_path, "dataset generator manifest is not committed")
            local_manifest(root, by_path[control_path], benchmark, kind)
            payload_files = [item for item in declared if item["path"] != control_path]
            source = manifest.get("source")
            _require(isinstance(source, dict) and source.get("commit") == parts[3],
                     "dataset generator revision disagrees with its path")
            generator, revision = parts[2], source["commit"]
            parameters = {"scale_factor": manifest["scale_factor"]} if "scale_factor" in manifest else {}
        entries.append({
            "id": f"{benchmark}/{kind}/{layout}/{fingerprint}",
            "benchmark": benchmark,
            "artifact_type": kind,
            "layout": layout,
            "generator": generator,
            "generator_revision": revision,
            "parameters": parameters,
            "path": root,
            "files": sorted(payload_files, key=lambda item: item["path"]),
            "formats": sorted({PurePosixPath(item["path"]).suffix.lstrip(".").lower() or "none"
                               for item in payload_files}),
            "file_count": len(payload_files),
            "total_bytes": sum(item["bytes"] for item in payload_files),
            "manifest_path": manifest_path,
            "manifest_sha256": _sha256(manifest_bytes),
            "commit_sha256": _sha256(commit_bytes),
        })
    snapshot = {
        "schema": catalog._SCHEMA,
        "observed_at": observed_at or datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "source": {"account_url": config.account_url, "file_system": config.file_system, "prefix": config.prefix},
        "entries": sorted(entries, key=lambda entry: entry["id"]),
    }
    catalog._validate(snapshot)
    _require(len(_canonical(snapshot)) <= catalog._MAX_BYTES, "exported catalog exceeds the size limit")
    return snapshot


def write_snapshot(snapshot: dict[str, Any], output: Path) -> None:
    """Replace only the requested local snapshot, after full validation."""
    catalog._validate(snapshot)
    encoded = (json.dumps(snapshot, ensure_ascii=True, allow_nan=False, indent=2) + "\n").encode("utf-8")
    _require(len(encoded) <= catalog._MAX_BYTES, "exported catalog exceeds the size limit")
    output = Path(output)
    _require(not output.is_symlink(), "catalog output must not be a symlink")
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(dir=output.parent, prefix=".catalog-", suffix=".tmp", delete=False) as stream:
            temporary = Path(stream.name)
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())
        temporary.replace(output)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def export_snapshot(config: AzureHNSCacheConfig, output: Path) -> dict[str, Any]:
    """Authenticate explicitly, inventory read-only, then write a local snapshot."""
    _require(config.normalized_mode() is RemoteCacheMode.READ, "catalog export requires read mode")
    config.validate_enabled()
    try:
        from azure.core.exceptions import AzureError
        from azure.storage.filedatalake import DataLakeServiceClient
    except ImportError as exc:
        raise AzureDependencyError("Catalog export requires: pip install 'driftbench-db[azure]'") from exc
    try:
        with build_azure_credential(config.credential_env_file) as credential, DataLakeServiceClient(
            config.account_url, credential=credential, connection_timeout=30, read_timeout=60,
        ) as service:
            file_system = service.get_file_system_client(config.file_system)
            objects = ((item.name, item.content_length)
                       for item in file_system.get_paths(path=config.prefix or None, recursive=True)
                       if not item.is_directory)

            def read_metadata(path: str, limit: int) -> bytes:
                return file_system.get_file_client(path).download_file(offset=0, length=limit).readall()

            snapshot = build_snapshot(config, objects, read_metadata)
    except AzureError as exc:
        raise catalog.CatalogError(
            "Azure catalog inventory failed; check credentials, read permissions, and connectivity"
        ) from exc
    write_snapshot(snapshot, output)
    return snapshot


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--azure-cache-config", required=True)
    parser.add_argument("--credential-env-file")
    parser.add_argument("--output", required=True, type=Path, help="Local metadata JSON destination")
    args = parser.parse_args(argv)
    try:
        config = load_azure_cache_config(args.azure_cache_config, mode=RemoteCacheMode.READ,
                                         credential_env_file=args.credential_env_file)
        snapshot = export_snapshot(config, args.output)
    except (catalog.CatalogError, CacheConfigurationError, AzureDependencyError, OSError) as exc:
        print(f"Catalog export failed: {exc}", file=sys.stderr)
        return 1
    print(json.dumps({"observed_at": snapshot["observed_at"], "entries": len(snapshot["entries"]),
                      "counts": {kind: sum(entry["artifact_type"] == kind for entry in snapshot["entries"])
                                 for kind in ("data", "queries")}}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
