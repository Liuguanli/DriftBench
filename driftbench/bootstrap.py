from __future__ import annotations

import hashlib
import json
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict
from urllib.parse import urlparse
from urllib.request import urlretrieve

from driftbench.core.schema.factory import get_schema_extractor


class BootstrapError(ValueError):
    pass


@dataclass(frozen=True)
class DatasetBootstrapResult:
    source_kind: str
    source_value: str
    output_dataset: Path
    output_schema: Path
    sha256: str
    sample_size: int
    schema_summary: Dict[str, Any]


_REPO_ROOT = Path(__file__).resolve().parents[1]
_PRESET_DATASETS = {
    "census_original": _REPO_ROOT / "driftbench" / "data" / "census_original.csv",
    "sample": _REPO_ROOT / "driftbench" / "data" / "sample.csv",
    "sample_rich": _REPO_ROOT / "driftbench" / "data" / "sample_rich.csv",
}


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            chunk = f.read(1024 * 1024)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def _normalize_checksum(checksum: str) -> str:
    s = checksum.strip()
    if s.startswith("sha256:"):
        s = s[len("sha256:") :]
    s = s.lower()
    if len(s) != 64 or any(c not in "0123456789abcdef" for c in s):
        raise BootstrapError("Checksum must be a valid SHA-256 hex string (or sha256:<hex>).")
    return s


def _detect_source_kind(source: str) -> str:
    if source in _PRESET_DATASETS:
        return "preset"
    parsed = urlparse(source)
    if parsed.scheme in {"http", "https"}:
        return "url"
    return "local"


def _resolve_dest_file(output_dir: Path, source: str, source_kind: str, filename: str | None) -> Path:
    if filename:
        name = filename.strip()
        if not name:
            raise BootstrapError("--filename cannot be empty.")
        return output_dir / name
    if source_kind == "preset":
        return output_dir / Path(_PRESET_DATASETS[source]).name
    if source_kind == "url":
        parsed = urlparse(source)
        fallback = "dataset.csv"
        tail = Path(parsed.path).name or fallback
        return output_dir / tail
    return output_dir / Path(source).name


def _copy_or_download_dataset(source: str, source_kind: str, dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    if source_kind == "preset":
        src = _PRESET_DATASETS[source]
        if not src.exists():
            raise BootstrapError(f"Preset dataset missing from repository: {src}")
        shutil.copy2(src, dst)
        return
    if source_kind == "local":
        src = Path(source).expanduser().resolve()
        if not src.exists() or not src.is_file():
            raise BootstrapError(f"Local dataset source does not exist: {src}")
        shutil.copy2(src, dst)
        return
    if source_kind == "url":
        try:
            urlretrieve(source, dst)
        except Exception as exc:
            raise BootstrapError(f"Failed to download dataset from URL: {exc}") from exc
        return
    raise BootstrapError(f"Unsupported source kind: {source_kind}")


def bootstrap_dataset(
    *,
    source: str,
    output_dir: str | Path,
    filename: str | None = None,
    checksum: str | None = None,
    sample_size: int = 1000,
    schema_out: str | Path | None = None,
) -> DatasetBootstrapResult:
    source = source.strip()
    if not source:
        raise BootstrapError("--source must be provided.")

    if sample_size < 0:
        raise BootstrapError("--sample-size must be >= 0.")

    out_dir = Path(output_dir).expanduser().resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    source_kind = _detect_source_kind(source)
    dst = _resolve_dest_file(out_dir, source, source_kind, filename)
    _copy_or_download_dataset(source, source_kind, dst)

    actual_sha = _sha256_file(dst)
    if checksum:
        expected = _normalize_checksum(checksum)
        if actual_sha != expected:
            raise BootstrapError(
                f"Checksum mismatch for {dst.name}: expected={expected}, actual={actual_sha}"
            )

    if dst.suffix.lower() != ".csv":
        raise BootstrapError("MVP bootstrap only supports CSV datasets for schema extraction.")

    extractor = get_schema_extractor("csv", csv_path=str(dst), sample_size=int(sample_size))
    schema = extractor.extract_schema()

    if schema_out is not None:
        schema_path = Path(schema_out).expanduser()
        if not schema_path.is_absolute():
            schema_path = (Path.cwd() / schema_path).resolve()
        else:
            schema_path = schema_path.resolve()
    else:
        schema_path = (out_dir / f"{dst.stem}_schema.json").resolve()
    schema_path.parent.mkdir(parents=True, exist_ok=True)
    schema_path.write_text(json.dumps(schema, indent=2, default=str), encoding="utf-8")

    summary = {
        "tables": sorted(list((schema.get("tables") or {}).keys())),
        "num_tables": len(schema.get("tables") or {}),
    }
    return DatasetBootstrapResult(
        source_kind=source_kind,
        source_value=source,
        output_dataset=dst.resolve(),
        output_schema=schema_path,
        sha256=actual_sha,
        sample_size=sample_size,
        schema_summary=summary,
    )

