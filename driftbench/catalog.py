"""Browse the public, packaged snapshot of Azure benchmark artifact metadata.

Importing this module is passive. Explicit calls read a packaged JSON resource;
they do not contact Azure or grant access to the private artifact files.
"""

from __future__ import annotations

import builtins
import hashlib
import json
import math
import re
from datetime import datetime, timedelta
from importlib import resources
from pathlib import PurePosixPath
from typing import Any

from driftbench.cache.errors import CacheConfigurationError
from driftbench.cache.models import (
    AzureHNSCacheConfig,
    RemoteCacheMode,
    validate_posix_relative_path,
)


__all__ = ["CatalogError", "get", "info", "list"]

_SCHEMA = "driftbench.azure-catalog/v1"
_MAX_BYTES = 8 * 1024 * 1024
_MAX_FILES = 10_000
_TOKEN = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,199}")
_BENCHMARK = re.compile(r"[a-z][a-z0-9_]{0,63}")
_HASH = re.compile(r"[0-9a-f]{64}")
_PARAMETERS = frozenset({
    "scale_factor", "record_count", "seed", "field_count", "field_length",
    "hot_warehouse_fraction", "skew_factor", "query_ids",
    "queries_per_template", "shuffle", "mode", "workload", "clients",
    "duration", "rate", "run_seconds", "target_rate",
})
_LAYOUTS = {
    "artifact-cache-v1": "cache/driftbench-artifact-cache/v1",
    "immutable-dataset-v1": "datasets/v1",
}


class CatalogError(ValueError):
    """Catalog metadata is missing, unsupported, or inconsistent."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise CatalogError(message)


def _shape(value: Any, keys: set[str], label: str) -> dict[str, Any]:
    _require(isinstance(value, dict) and set(value) == keys, f"{label} has invalid fields")
    return value


def _path(value: Any, label: str) -> str:
    try:
        return validate_posix_relative_path(value, field=label)
    except CacheConfigurationError as exc:
        raise CatalogError(f"{label} must be a safe relative POSIX path") from exc


def _digest(value: Any, label: str) -> str:
    _require(isinstance(value, str) and _HASH.fullmatch(value) is not None,
             f"{label} must be a lowercase SHA-256 digest")
    return value


def _integer(value: Any, label: str) -> int:
    _require(type(value) is int and value >= 0, f"{label} must be a non-negative integer")
    return value


def _token(value: Any, label: str) -> str:
    _require(isinstance(value, str) and _TOKEN.fullmatch(value) is not None,
             f"{label} must be a non-secret identifier")
    return value


def _parameters(value: Any) -> dict[str, Any]:
    _require(isinstance(value, dict) and set(value) <= _PARAMETERS,
             "catalog parameters contain unsupported fields")
    for key, item in value.items():
        values = item if isinstance(item, builtins.list) else [item]
        _require(len(values) <= 1000, f"catalog parameter {key} is too large")
        for scalar in values:
            valid = scalar is None or type(scalar) in {bool, int}
            valid |= type(scalar) is float and math.isfinite(scalar)
            valid |= isinstance(scalar, str) and _TOKEN.fullmatch(scalar) is not None
            _require(valid, f"catalog parameter {key} is not a public scalar value")
    return value


def _descriptor(value: Any) -> dict[str, Any]:
    item = _shape(value, {"path", "bytes", "sha256"}, "catalog file descriptor")
    _path(item["path"], "catalog file path")
    _integer(item["bytes"], "catalog file size")
    _digest(item["sha256"], "catalog file hash")
    return item


def _object_pairs(pairs: builtins.list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        _require(key not in result, "catalog JSON has duplicate keys")
        result[key] = value
    return result


def _invalid_constant(value: str) -> None:
    raise CatalogError("catalog JSON contains a non-finite number")


def _decode(encoded: bytes, label: str) -> dict[str, Any]:
    _require(len(encoded) <= _MAX_BYTES, f"{label} exceeds the catalog size limit")
    try:
        payload = json.loads(encoded.decode("utf-8"), object_pairs_hook=_object_pairs,
                             parse_constant=_invalid_constant)
    except CatalogError:
        raise
    except (UnicodeError, ValueError, RecursionError) as exc:
        raise CatalogError(f"{label} must be bounded UTF-8 JSON") from exc
    _require(isinstance(payload, dict), f"{label} must be a JSON object")
    return payload


def _validate(payload: Any) -> dict[str, Any]:
    snapshot = _shape(payload, {"schema", "observed_at", "source", "entries"}, "catalog")
    _require(snapshot["schema"] == _SCHEMA, "unsupported catalog schema")
    stamp = snapshot["observed_at"]
    _require(isinstance(stamp, str) and stamp.endswith("Z"),
             "catalog observed_at must be a UTC timestamp ending in Z")
    try:
        parsed = datetime.fromisoformat(stamp[:-1] + "+00:00")
    except ValueError as exc:
        raise CatalogError("catalog observed_at is invalid") from exc
    _require(parsed.utcoffset() == timedelta(0), "catalog observed_at must be UTC")
    source = _shape(snapshot["source"], {"account_url", "file_system", "prefix"},
                    "catalog source")
    try:
        AzureHNSCacheConfig(**source, mode=RemoteCacheMode.READ).validate_enabled()
    except (CacheConfigurationError, ValueError) as exc:
        raise CatalogError("catalog source must be non-secret Azure HNS coordinates") from exc
    entries = snapshot["entries"]
    _require(isinstance(entries, builtins.list) and len(entries) <= _MAX_FILES,
             "catalog entries must be a bounded list")
    seen: set[str] = set()
    locations: set[str] = set()
    for entry in entries:
        _shape(entry, {
            "id", "benchmark", "artifact_type", "layout", "generator",
            "generator_revision", "parameters", "path", "files", "formats",
            "file_count", "total_bytes", "manifest_path", "manifest_sha256",
            "commit_sha256",
        }, "catalog entry")
        benchmark = entry["benchmark"]
        _require(isinstance(benchmark, str) and _BENCHMARK.fullmatch(benchmark) is not None,
                 "catalog benchmark must be a canonical benchmark name")
        kind = entry["artifact_type"]
        _require(kind in ("data", "queries"), "catalog artifact_type must be data or queries")
        layout = entry["layout"]
        _require(isinstance(layout, str) and layout in _LAYOUTS, "unsupported catalog layout")
        _token(entry["generator"], "catalog generator")
        _token(entry["generator_revision"], "catalog generator revision")
        _parameters(entry["parameters"])
        path = _path(entry["path"], "catalog entry path")
        prefix = "/".join(filter(None, (source["prefix"], _LAYOUTS[layout], benchmark, kind)))
        _require(path.startswith(prefix + "/"), "catalog entry escapes its declared scope")
        fingerprint = _digest(path.rsplit("/", 1)[-1], "catalog entry fingerprint")
        expected_id = f"{benchmark}/{kind}/{layout}/{fingerprint}"
        _require(entry["id"] == expected_id and expected_id not in seen,
                 "catalog entry ID is invalid or duplicated")
        _require(path.casefold() not in locations, "catalog entry locations collide")
        seen.add(expected_id)
        locations.add(path.casefold())
        manifest = _path(entry["manifest_path"], "catalog manifest path")
        expected_manifest = ("manifest.json" if layout == "artifact-cache-v1"
                             else "provenance_manifest.json")
        _require(manifest == expected_manifest, "catalog manifest does not match its layout")
        _digest(entry["manifest_sha256"], "catalog manifest hash")
        _digest(entry["commit_sha256"], "catalog commit hash")
        files = entry["files"]
        _require(isinstance(files, builtins.list) and 0 < len(files) <= _MAX_FILES,
                 "catalog entry requires a bounded, non-empty payload list")
        paths: set[str] = set()
        for value in files:
            descriptor = _descriptor(value)
            relative = descriptor["path"]
            subtree = f"{benchmark}/{kind}/"
            if layout == "artifact-cache-v1":
                subtree = "artifacts/" + subtree
            _require(relative.startswith(subtree), "catalog payload escapes its benchmark subtree")
            _require(relative.casefold() not in paths, "catalog payload paths collide")
            _require(not relative.endswith("_manifest.json"), "catalog payload includes metadata")
            paths.add(relative.casefold())
        _integer(entry["file_count"], "catalog file_count")
        _integer(entry["total_bytes"], "catalog total_bytes")
        _require(entry["file_count"] == len(files), "catalog file_count is inconsistent")
        _require(entry["total_bytes"] == sum(item["bytes"] for item in files),
                 "catalog total_bytes is inconsistent")
        formats = sorted({PurePosixPath(item["path"]).suffix.lstrip(".").lower() or "none"
                          for item in files})
        _require(entry["formats"] == formats, "catalog formats are inconsistent")
    return snapshot


def _load() -> dict[str, Any]:
    try:
        with resources.files("driftbench").joinpath("catalog_snapshot.json").open("rb") as stream:
            encoded = stream.read(_MAX_BYTES + 1)
    except OSError as exc:
        raise CatalogError("packaged catalog is missing or unreadable; reinstall driftbench-db") from exc
    return _validate(_decode(encoded, "packaged catalog"))


def list(*, benchmark: str | None = None,
         artifact_type: str | None = None) -> builtins.list[dict[str, Any]]:
    """Return independent JSON-compatible records from the packaged snapshot.

    Filters are combined. Benchmark names are canonical (e.g. ``tpcc_skew``);
    a well-formed name absent from this snapshot returns an empty list.
    """
    if benchmark is not None and (
        not isinstance(benchmark, str) or _BENCHMARK.fullmatch(benchmark) is None
    ):
        raise ValueError("benchmark must be a canonical name such as 'tpch' or 'tpcc_skew'")
    if artifact_type is not None and artifact_type not in ("data", "queries"):
        raise ValueError("artifact_type must be 'data', 'queries', or None")
    return sorted(
        (entry for entry in _load()["entries"]
         if (benchmark is None or entry["benchmark"] == benchmark)
         and (artifact_type is None or entry["artifact_type"] == artifact_type)),
        key=lambda entry: entry["id"],
    )


def get(entry_id: str) -> dict[str, Any]:
    """Return an independent record by its stable ID, or raise ``KeyError``."""
    if not isinstance(entry_id, str) or not entry_id:
        raise ValueError("entry_id must be a non-empty catalog ID from catalog.list()")
    for entry in _load()["entries"]:
        if entry["id"] == entry_id:
            return entry
    raise KeyError("catalog ID is not present in the packaged snapshot")


def info() -> dict[str, Any]:
    """Describe snapshot freshness, scope, access boundaries, and record counts."""
    snapshot = _load()
    entries = snapshot["entries"]
    return {
        "schema": snapshot["schema"],
        "observed_at": snapshot["observed_at"],
        "source": snapshot["source"],
        "freshness": "packaged-snapshot",
        "live": False,
        "data_access": "azure-authorization-required",
        "verification": "commit metadata and listed object sizes; payload hashes not rechecked",
        "included_layouts": sorted(_LAYOUTS),
        "excluded": ["drift-results", "uncommitted-objects", "unknown-namespaces"],
        "entry_count": len(entries),
        "benchmarks": sorted({entry["benchmark"] for entry in entries}),
        "counts": {kind: sum(entry["artifact_type"] == kind for entry in entries)
                   for kind in ("data", "queries")},
        "snapshot_sha256": hashlib.sha256(
            json.dumps(snapshot, ensure_ascii=True, allow_nan=False, sort_keys=True,
                       separators=(",", ":")).encode("utf-8")
        ).hexdigest(),
    }
