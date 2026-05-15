from __future__ import annotations

import csv
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any


class OutputDirRequiredError(ValueError):
    """Kept for backwards compatibility. No longer raised by the library."""


def get_default_data_dir() -> Path:
    """Return the default directory for generated benchmark artifacts.

    Checks DRIFTBENCH_DATA_DIR env var first; falls back to ~/.driftbench/data/.
    """
    env = os.environ.get("DRIFTBENCH_DATA_DIR", "").strip()
    if env:
        return Path(env).expanduser().resolve()
    return Path.home() / ".driftbench" / "data"


@dataclass(frozen=True)
class GenerationResult:
    benchmark: str
    artifact_type: str
    output_dir: Path
    files: list[Path]
    metadata: Path

    def as_csv(self) -> "GenerationResult":
        """Convert any pipe-delimited .tbl files to .csv and return a new result.

        Non-.tbl files are carried over unchanged. The original .tbl files are
        kept alongside the new .csv files.
        """
        converted: list[Path] = []
        for f in self.files:
            if f.suffix not in (".tbl", ".dat"):
                converted.append(f)
                continue
            csv_path = f.with_suffix(".csv")
            with f.open(encoding="utf-8") as src, csv_path.open("w", encoding="utf-8", newline="") as dst:
                writer = csv.writer(dst)
                for line in src:
                    row = line.rstrip("\n").rstrip("|").split("|")
                    writer.writerow(row)
            converted.append(csv_path)
        return GenerationResult(
            benchmark=self.benchmark,
            artifact_type=self.artifact_type,
            output_dir=self.output_dir,
            files=converted,
            metadata=self.metadata,
        )


class BenchmarkArtifact:
    """Common utilities for benchmark data/query generators."""

    benchmark: str
    artifact_type: str

    def _require_output_dir(self, output_dir: str | Path | None) -> Path:
        """Resolve output_dir, defaulting to get_default_data_dir() if None."""
        if output_dir is None:
            root = get_default_data_dir()
            print(
                f"[driftbench] No output_dir specified. "
                f"Writing {self.benchmark}/{self.artifact_type} to: {root}"
            )
        else:
            root = Path(output_dir).expanduser().resolve()
        root.mkdir(parents=True, exist_ok=True)
        return root

    def _load_existing(self, manifest_path: Path, root: Path) -> GenerationResult | None:
        """Return a GenerationResult if all previously generated files still exist.

        Returns None when the manifest is missing, unreadable, or any listed
        file has been deleted — signalling that generation must proceed.
        """
        if not manifest_path.exists():
            return None
        try:
            payload = json.loads(manifest_path.read_text(encoding="utf-8"))
            rel_paths: list[str] = payload.get("files", [])
            if not rel_paths:
                return None
            files = [root / p for p in rel_paths]
            if all(f.exists() and f.is_file() for f in files):
                return GenerationResult(
                    benchmark=self.benchmark,
                    artifact_type=self.artifact_type,
                    output_dir=root,
                    files=files,
                    metadata=manifest_path,
                )
        except Exception:
            pass
        return None

    def _write_text(self, path: Path, content: str) -> Path:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
        return path

    def _write_json(self, path: Path, payload: dict[str, Any]) -> Path:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
        return path

    def _paths_relative_to(self, base: Path, paths: list[Path]) -> list[str]:
        base_resolved = base.resolve()
        return [str(path.resolve().relative_to(base_resolved)) for path in paths]

    def _result(
        self,
        output_dir: Path,
        files: list[Path],
        metadata: Path,
    ) -> GenerationResult:
        return GenerationResult(
            benchmark=self.benchmark,
            artifact_type=self.artifact_type,
            output_dir=output_dir,
            files=files,
            metadata=metadata,
        )
