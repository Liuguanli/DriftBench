"""Read-only, catalog-bound dataset downloads with verified offline reuse."""

from __future__ import annotations

import hashlib
import os
from pathlib import Path, PurePosixPath, PureWindowsPath
from typing import Any
from uuid import uuid4

from driftbench import catalog

from .errors import CacheCollisionError, CacheConfigurationError, CacheIntegrityError
from .models import AzureHNSCacheConfig, RemoteCacheMode
from .service import (
    _assert_safe_tree,
    _build_backend,
    _encode_json,
    _ensure_safe_directory,
    _exclusive_lock,
    _is_link_like,
    _lexists,
    _read_remote_bytes,
    _remove_owned_tree,
    _sha256_file,
)


_SCHEMA = "driftbench.catalog-dataset-cache/v1"
_RECORD = "_LOCAL_CACHE.json"
_COMMIT = "_COMMITTED.json"
_METADATA_LIMIT = 1024 * 1024
_STAGING_PREFIX = ".driftbench-dataset-pull-"


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise CacheIntegrityError(message)


def _portable_path(value: str) -> str:
    catalog._path(value, "dataset path")
    if any(
        PureWindowsPath(part).is_reserved() or part.endswith((".", " "))
        or any(character in '<>"|?*' for character in part)
        for part in PurePosixPath(value).parts
    ):
        raise CacheConfigurationError("dataset paths must be portable regular-file paths")
    return value


def _descriptors(values: Any) -> dict[str, dict[str, Any]]:
    _require(isinstance(values, list) and 0 < len(values) <= catalog._MAX_FILES,
             "dataset metadata requires a bounded descriptor list")
    result: dict[str, dict[str, Any]] = {}
    folded: set[str] = set()
    for value in values:
        descriptor = catalog._descriptor(value)
        path = _portable_path(descriptor["path"])
        _require(path.casefold() not in folded, "dataset metadata paths collide")
        result[path] = descriptor
        folded.add(path.casefold())
    return result


def _verify_metadata(
    entry: dict[str, Any], commit_bytes: bytes, provenance_bytes: bytes,
) -> None:
    _require(hashlib.sha256(commit_bytes).hexdigest() == entry["commit_sha256"],
             "dataset commit SHA-256 differs from the catalog")
    _require(hashlib.sha256(provenance_bytes).hexdigest() == entry["manifest_sha256"],
             "dataset provenance SHA-256 differs from the catalog")
    try:
        commit = catalog._decode(commit_bytes, "dataset commit")
        catalog._shape(commit, {"schema", "state", "bundle_fingerprint", "object_count", "objects"},
                       "dataset commit")
        fingerprint = entry["id"].rsplit("/", 1)[-1]
        _require(commit["schema"] == "driftbench.immutable-dataset-commit/v1"
                 and commit["state"] == "COMMITTED"
                 and commit["bundle_fingerprint"] == fingerprint,
                 "dataset commit identity or state differs from the catalog")
        objects = _descriptors(commit["objects"])
        _require(type(commit["object_count"]) is int and commit["object_count"] == len(objects),
                 "dataset commit object count is inconsistent")
        provenance = catalog._decode(provenance_bytes, "dataset provenance")
        _require(provenance.get("schema") == "driftbench.dataset-provenance/v1"
                 and provenance.get("benchmark") == entry["benchmark"]
                 and provenance.get("artifact_type") == entry["artifact_type"]
                 and provenance.get("bundle_fingerprint") == fingerprint,
                 "dataset provenance identity differs from the catalog")
        source = provenance.get("source")
        _require(isinstance(source, dict) and source.get("commit") == entry["generator_revision"],
                 "dataset generator revision differs from the catalog")
        _require(all(provenance.get(key) == value for key, value in entry["parameters"].items()),
                 "dataset provenance parameters differ from the catalog")
        artifacts = _descriptors(provenance.get("artifacts"))
        validation = provenance.get("driftbench_validation")
        _require(isinstance(validation, dict), "dataset lacks a generator manifest reference")
        control_path = _portable_path(validation.get("published_manifest_path"))
        _require(control_path.startswith(f"{entry['benchmark']}/data/")
                 and control_path.endswith("_manifest.json")
                 and control_path in artifacts,
                 "dataset generator manifest reference is invalid")
        expected_payloads = {item["path"]: item for item in entry["files"]}
        _require(control_path not in expected_payloads
                 and {path: item for path, item in artifacts.items() if path != control_path} == expected_payloads,
                 "dataset provenance payload descriptors differ from the catalog")
        expected_provenance = {
            "path": entry["manifest_path"], "bytes": len(provenance_bytes),
            "sha256": entry["manifest_sha256"],
        }
        _require(objects == {**artifacts, entry["manifest_path"]: expected_provenance},
                 "dataset commit and provenance descriptors disagree")
    except (catalog.CatalogError, CacheConfigurationError) as exc:
        raise CacheIntegrityError(f"invalid dataset metadata: {exc}") from exc


def _binding(entry: dict[str, Any], source: dict[str, Any]) -> bytes:
    return _encode_json({"schema": _SCHEMA, "source": source, "entry": entry})


def _same_directory(candidate: Path, reference: Path) -> bool:
    return candidate == reference or (
        _lexists(candidate) and _lexists(reference) and candidate.samefile(reference)
    )


def _local_metadata(path: Path, limit: int) -> bytes:
    _require(0 < path.stat().st_size <= limit, "cached dataset metadata exceeds the size limit")
    with path.open("rb") as stream:
        value = stream.read(limit + 1)
    _require(len(value) <= limit, "cached dataset metadata changed while reading")
    return value


def _verify_local(
    directory: Path, entry: dict[str, Any], binding: bytes, max_bytes: int,
) -> int:
    _assert_safe_tree(directory, directory.parent)
    expected = {item["path"] for item in entry["files"]} | {
        _RECORD, _COMMIT, entry["manifest_path"],
    }
    actual = {
        path.relative_to(directory).as_posix()
        for path in directory.rglob("*") if path.is_file()
    }
    expected_directories = {
        parent.as_posix() for name in expected
        for parent in PurePosixPath(name).parents if parent != PurePosixPath(".")
    }
    actual_directories = {
        path.relative_to(directory).as_posix()
        for path in directory.rglob("*") if path.is_dir()
    }
    _require(actual == expected and actual_directories == expected_directories,
             "cached dataset inventory differs; no automatic repair (use another cache_dir)")
    record = _local_metadata(directory / _RECORD, len(binding))
    _require(record == binding, "cached dataset binding differs; no automatic repair")
    remaining = max_bytes - entry["total_bytes"] - len(binding)
    commit_bytes = _local_metadata(directory / _COMMIT, min(_METADATA_LIMIT, remaining))
    remaining -= len(commit_bytes)
    provenance_bytes = _local_metadata(
        directory / entry["manifest_path"], min(_METADATA_LIMIT, remaining),
    )
    _verify_metadata(entry, commit_bytes, provenance_bytes)
    for item in entry["files"]:
        path = directory.joinpath(*PurePosixPath(item["path"]).parts)
        _require(path.stat().st_size == item["bytes"], "cached dataset payload size differs")
        size, digest = _sha256_file(path)
        _require(size == item["bytes"] and digest == item["sha256"],
                 "cached dataset payload SHA-256 differs; no automatic repair")
    return len(commit_bytes) + len(provenance_bytes) + len(binding)


def materialize_dataset(
    entry: dict[str, Any],
    source: dict[str, Any],
    *,
    cache_dir: str | Path,
    credential_env_file: str | Path | None,
    max_bytes: int,
) -> dict[str, Any]:
    if entry["layout"] != "immutable-dataset-v1" or entry["artifact_type"] != "data":
        raise CacheConfigurationError("catalog.materialize supports immutable-dataset-v1 data only")
    if type(max_bytes) is not int or max_bytes <= 0:
        raise CacheConfigurationError("max_bytes must be a positive integer including payloads and metadata")
    for item in entry["files"]:
        _portable_path(item["path"])
    binding = _binding(entry, source)
    if entry["total_bytes"] + len(binding) >= max_bytes:
        raise CacheConfigurationError("dataset payload and binding exceed max_bytes; leave room for remote metadata")
    key = hashlib.sha256(binding).hexdigest()
    if not isinstance(cache_dir, (str, Path)) or not str(cache_dir).strip():
        raise CacheConfigurationError("cache_dir must name a dedicated local directory")
    root = Path(cache_dir).expanduser().absolute()
    if ".." in root.parts:
        raise CacheConfigurationError("cache_dir must not contain parent traversal")
    for ancestor in reversed((root, *root.parents)):
        if _lexists(ancestor) and (_is_link_like(ancestor) or not ancestor.is_dir()):
            raise CacheIntegrityError("dataset cache ancestors must be non-reparse directories")
    root = root.resolve()
    package_root = Path(__file__).resolve().parents[2]
    if root == Path(root.anchor).resolve() or _same_directory(root, Path.home().resolve()):
        raise CacheConfigurationError("cache_dir must be a dedicated directory, not a filesystem or home root")
    if any(_same_directory(ancestor, package_root) for ancestor in (root, *root.parents)):
        raise CacheConfigurationError("private dataset caches must be outside the checkout or package installation")
    _ensure_safe_directory(root, Path(root.anchor))
    lock_root = root / ".locks"
    _ensure_safe_directory(lock_root, root)
    lock_path = lock_root / f"{key}.lock"
    if _is_link_like(lock_path) or (_lexists(lock_path) and not lock_path.is_file()):
        raise CacheIntegrityError("dataset lock path is not a safe regular file")
    directory = root / f"dataset-{key}"
    outcome = "hit"
    with _exclusive_lock(lock_path):
        if _lexists(directory):
            metadata_bytes = _verify_local(directory, entry, binding, max_bytes)
        else:
            stage = root / f"{_STAGING_PREFIX}{uuid4().hex}"
            stage.mkdir(mode=0o700)
            try:
                config = AzureHNSCacheConfig(
                    **source, mode=RemoteCacheMode.READ, credential_env_file=credential_env_file,
                )
                backend = _build_backend(config)
                remaining = max_bytes - entry["total_bytes"] - len(binding)
                commit_bytes = _read_remote_bytes(
                    backend, f"{entry['path']}/{_COMMIT}", limit=min(_METADATA_LIMIT, remaining),
                )
                _require(hashlib.sha256(commit_bytes).hexdigest() == entry["commit_sha256"],
                         "dataset commit SHA-256 differs from the catalog")
                remaining -= len(commit_bytes)
                _require(remaining > 0, "dataset metadata exceeds max_bytes")
                provenance_bytes = _read_remote_bytes(
                    backend, f"{entry['path']}/{entry['manifest_path']}",
                    limit=min(_METADATA_LIMIT, remaining),
                )
                _verify_metadata(entry, commit_bytes, provenance_bytes)
                for name, content in (
                    (_COMMIT, commit_bytes), (entry["manifest_path"], provenance_bytes), (_RECORD, binding),
                ):
                    with (stage / name).open("xb") as stream:
                        stream.write(content)
                for item in entry["files"]:
                    destination = stage.joinpath(*PurePosixPath(item["path"]).parts)
                    _ensure_safe_directory(destination.parent, stage)
                    digest, size = hashlib.sha256(), 0
                    with destination.open("xb") as stream:
                        for chunk in backend.iter_bytes(f"{entry['path']}/{item['path']}"):
                            _require(isinstance(chunk, (bytes, bytearray, memoryview)),
                                     "dataset backend returned a non-byte payload")
                            size += len(chunk)
                            _require(size <= item["bytes"], "dataset payload exceeds its declared size")
                            digest.update(chunk)
                            stream.write(chunk)
                    _require(size == item["bytes"] and digest.hexdigest() == item["sha256"],
                             "downloaded dataset payload size or SHA-256 differs")
                metadata_bytes = _verify_local(stage, entry, binding, max_bytes)
                _require(not _lexists(directory), "dataset cache destination appeared during population")
                try:
                    stage.rename(directory)
                except FileExistsError as exc:
                    raise CacheCollisionError("dataset cache destination already exists") from exc
                outcome = "downloaded"
            finally:
                _remove_owned_tree(stage, root, _STAGING_PREFIX)
    files = [str(directory.joinpath(*PurePosixPath(item["path"]).parts)) for item in entry["files"]]
    downloaded_metadata = metadata_bytes - len(binding) if outcome == "downloaded" else 0
    return {
        "entry_id": entry["id"], "cache_outcome": outcome,
        "local_path": str(directory), "payload_dir": os.path.commonpath([str(Path(path).parent) for path in files]),
        "files": files, "file_count": entry["file_count"], "payload_bytes": entry["total_bytes"],
        "metadata_bytes": metadata_bytes, "materialized_bytes": entry["total_bytes"] + metadata_bytes,
        "downloaded_payload_bytes": entry["total_bytes"] if outcome == "downloaded" else 0,
        "downloaded_metadata_bytes": downloaded_metadata,
        "source": dict(source), "binding_sha256": key,
        "commit_sha256": entry["commit_sha256"], "manifest_sha256": entry["manifest_sha256"],
        "verification": "complete local inventory, payload hashes, pinned metadata and binding",
        "remote_checked": outcome == "downloaded",
    }
