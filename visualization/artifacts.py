"""Safe, deterministic artifact and manifest helpers."""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import tempfile
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any, Mapping


_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_WINDOWS_DRIVE_RE = re.compile(r"^[A-Za-z]:[\\/]")


def canonical_json_bytes(payload: Any) -> bytes:
    return json.dumps(
        _json_value(payload),
        allow_nan=False,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def semantic_hash(payload: Any) -> str:
    return hashlib.sha256(canonical_json_bytes(payload)).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def file_descriptor(path: Path, root: Path) -> dict[str, Any]:
    return {
        "path": relative_posix(path, root),
        "bytes": path.stat().st_size,
        "sha256": sha256_file(path),
    }


def ensure_managed_path(root: Path, *parts: str) -> Path:
    root_resolved = root.expanduser().resolve()
    candidate = root_resolved.joinpath(*parts).resolve()
    try:
        candidate.relative_to(root_resolved)
    except ValueError as exc:
        raise ValueError(f"managed path escapes output root: {candidate}") from exc
    return candidate


def relative_posix(path: Path, root: Path) -> str:
    root_resolved = root.expanduser().resolve()
    resolved = path.expanduser().resolve()
    try:
        relative = resolved.relative_to(root_resolved)
    except ValueError as exc:
        raise ValueError(f"path is outside output root: {resolved}") from exc
    value = relative.as_posix()
    validate_relative_posix(value)
    return value


def validate_relative_posix(value: str) -> None:
    if not isinstance(value, str) or not value:
        raise ValueError("artifact path must be a non-empty string")
    if "\\" in value:
        raise ValueError(f"artifact path must use POSIX separators: {value}")
    if value.startswith("/") or value.startswith("//") or _WINDOWS_DRIVE_RE.match(value):
        raise ValueError(f"artifact path must be relative: {value}")
    pure = PurePosixPath(value)
    if pure.is_absolute() or any(part in {"", ".", ".."} for part in pure.parts):
        raise ValueError(f"invalid relative artifact path: {value}")


def utc_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def atomic_write_json(path: Path, payload: Mapping[str, Any]) -> None:
    text = json.dumps(
        _json_value(dict(payload)),
        allow_nan=False,
        ensure_ascii=False,
        indent=2,
        sort_keys=True,
    ) + "\n"
    atomic_write_text(path, text)


def atomic_write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    temp_path = Path(temp_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as stream:
            stream.write(text)
        os.replace(temp_path, path)
    finally:
        if temp_path.exists():
            temp_path.unlink()


def load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"JSON object expected: {path}")
    return payload


def is_sha256(value: Any) -> bool:
    return isinstance(value, str) and bool(_SHA256_RE.fullmatch(value))


def reject_machine_paths(payload: Any, field: str = "manifest") -> None:
    """Reject absolute/UNC/parent-traversal strings anywhere in a manifest."""

    if isinstance(payload, Mapping):
        for key, value in payload.items():
            reject_machine_paths(value, f"{field}.{key}")
        return
    if isinstance(payload, list):
        for index, value in enumerate(payload):
            reject_machine_paths(value, f"{field}[{index}]")
        return
    if not isinstance(payload, str):
        return
    if _WINDOWS_DRIVE_RE.search(payload) or payload.startswith("\\\\"):
        raise ValueError(f"{field} contains an absolute Windows/UNC path")
    if payload.startswith("/"):
        raise ValueError(f"{field} contains an absolute POSIX path")
    path_fields = ("path", "figure", "driftspec")
    if any(token in field.lower() for token in path_fields):
        if "\\" in payload or ".." in PurePosixPath(payload).parts:
            raise ValueError(f"{field} contains an unsafe path")


def _json_value(value: Any) -> Any:
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("JSON payload contains a non-finite float")
        return value
    if isinstance(value, Mapping):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    if hasattr(value, "item"):
        return _json_value(value.item())
    raise TypeError(f"unsupported JSON value: {type(value).__name__}")


__all__ = [
    "atomic_write_json",
    "atomic_write_text",
    "canonical_json_bytes",
    "ensure_managed_path",
    "file_descriptor",
    "is_sha256",
    "load_json",
    "reject_machine_paths",
    "relative_posix",
    "semantic_hash",
    "sha256_file",
    "utc_timestamp",
    "validate_relative_posix",
]
