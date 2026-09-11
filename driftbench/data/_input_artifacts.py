"""Filesystem helpers for importing explicitly supplied benchmark inputs."""
from __future__ import annotations

import hashlib
from pathlib import Path


def source_directory(value: str | Path | None, label: str) -> Path:
    if value is None or not isinstance(value, (str, Path)) or not str(value).strip():
        raise ValueError(f"{label} must name an existing local directory")
    root = Path(value).expanduser().resolve()
    if not root.is_dir():
        raise ValueError(f"{label} is not a directory: {root}")
    return root


def source_file(root: Path, relative: str | Path) -> Path:
    path = root / relative
    resolved = path.resolve()
    if not resolved.is_relative_to(root):
        raise ValueError(f"Input file escapes source directory: {relative}")
    for part in [path, *path.parents]:
        if part == root:
            break
        if part.is_symlink() or (hasattr(part, "is_junction") and part.is_junction()):
            raise ValueError(f"Input symlinks/junctions are not supported: {relative}")
    if not path.is_file() or path.stat().st_size == 0:
        raise ValueError(f"Required input file is missing or empty: {relative}")
    return path


def signature(path: Path) -> dict[str, int | str]:
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as stream:
        while block := stream.read(1024 * 1024):
            digest.update(block)
            size += len(block)
    return {"bytes": size, "sha256": digest.hexdigest()}


def checked_output(adapter, output_dir, relatives: list[str], sources: tuple[Path, ...] = ()) -> Path:
    """Preflight every destination, including the manifest, before creating it."""
    from .base import get_default_data_dir

    root = (get_default_data_dir() if output_dir is None else Path(output_dir).expanduser()).resolve()
    for source in sources:
        if root.is_relative_to(source) or source.is_relative_to(root):
            raise ValueError("Input and output directories must not overlap; choose a separate output_dir")
    for relative in relatives:
        raw = Path(relative)
        if raw.is_absolute() or raw.anchor or ".." in raw.parts:
            raise ValueError(f"Invalid managed output path: {relative}")
        path = root / raw
        if not path.resolve().is_relative_to(root):
            raise ValueError(f"Output path escapes output_dir: {relative}")
        for part in [path, *path.parents]:
            if part == root:
                break
            if part.is_symlink() or (hasattr(part, "is_junction") and part.is_junction()):
                raise ValueError(f"Output symlinks/junctions are not supported: {relative}")
            if part.exists() and part != path and not part.is_dir():
                raise ValueError(f"Output parent is not a directory: {part}")
        if path.exists() and not path.is_file():
            raise ValueError(f"Output file path is not a regular file: {relative}")
        if path.exists() and path.stat().st_nlink > 1:
            raise ValueError(f"Output file must not be a hardlink: {relative}")
    return adapter._require_output_dir(root)
