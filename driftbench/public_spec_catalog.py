from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any, Callable, Dict


CATALOG_VERSION = 2
CATALOG_KIND = "driftbench.public_specs"


def parse_spec_version(raw: Any, default: int = 1) -> int:
    if isinstance(raw, bool):
        return default
    if isinstance(raw, int):
        return raw if raw > 0 else default
    if isinstance(raw, float):
        return int(raw) if raw > 0 else default
    if isinstance(raw, str):
        value = raw.strip()
        if value.isdigit():
            parsed = int(value)
            return parsed if parsed > 0 else default
    return default


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        while True:
            chunk = fh.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def _normalize_spec_entry(entry: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(entry)
    tags = normalized.get("tags")
    if isinstance(tags, list):
        normalized["tags"] = [str(t).strip() for t in tags if str(t).strip()]
    else:
        normalized["tags"] = []

    normalized["spec_version"] = parse_spec_version(normalized.get("spec_version"), default=1)

    metadata = normalized.get("metadata")
    if not isinstance(metadata, dict):
        metadata = {}
    normalized["metadata"] = metadata
    return normalized


def normalize_catalog_payload(payload: dict[str, Any], now_iso: Callable[[], str]) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise ValueError("catalog format invalid: expected object")

    raw_specs = payload.get("specs")
    if not isinstance(raw_specs, list):
        raise ValueError("catalog format invalid: specs must be a list")

    source_version = payload.get("version")
    if isinstance(source_version, bool):
        source_version = 1
    elif isinstance(source_version, (int, float)):
        source_version = int(source_version)
    else:
        source_version = 1

    specs: list[dict[str, Any]] = []
    for raw_entry in raw_specs:
        if not isinstance(raw_entry, dict):
            raise ValueError("catalog format invalid: each spec entry must be an object")
        specs.append(_normalize_spec_entry(raw_entry))

    metadata = payload.get("metadata")
    if not isinstance(metadata, dict):
        metadata = {}
    metadata = dict(metadata)
    metadata.setdefault("format", CATALOG_KIND)
    metadata.setdefault("catalog_version", CATALOG_VERSION)
    if source_version != CATALOG_VERSION:
        metadata.setdefault("upgraded_from_version", source_version)

    normalized = dict(payload)
    normalized["version"] = CATALOG_VERSION
    normalized["kind"] = CATALOG_KIND
    normalized["updated_at"] = str(payload.get("updated_at") or now_iso())
    normalized["metadata"] = metadata
    normalized["specs"] = specs
    return normalized


def read_catalog(path: Path, now_iso: Callable[[], str]) -> dict[str, Any]:
    if not path.exists():
        return {
            "version": CATALOG_VERSION,
            "kind": CATALOG_KIND,
            "updated_at": now_iso(),
            "metadata": {
                "format": CATALOG_KIND,
                "catalog_version": CATALOG_VERSION,
            },
            "specs": [],
        }
    import json

    payload = json.loads(path.read_text(encoding="utf-8"))
    return normalize_catalog_payload(payload, now_iso)


def write_catalog(path: Path, payload: Dict[str, Any], now_iso: Callable[[], str]) -> None:
    import json

    normalized = normalize_catalog_payload(dict(payload), now_iso)
    normalized["updated_at"] = now_iso()
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(normalized, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)
