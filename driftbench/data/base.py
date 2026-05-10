from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import json
from typing import Any


class OutputDirRequiredError(ValueError):
    """Raised when the caller does not provide an output directory."""


@dataclass(frozen=True)
class GenerationResult:
    benchmark: str
    artifact_type: str
    output_dir: Path
    files: list[Path]
    metadata: Path


class BenchmarkArtifact:
    """Common utilities for benchmark data/query generators."""

    benchmark: str
    artifact_type: str

    def _require_output_dir(self, output_dir: str | Path | None) -> Path:
        if output_dir is None:
            raise OutputDirRequiredError(
                "output_dir is required. DriftBench will only write artifacts into "
                "the directory you explicitly provide."
            )
        root = Path(output_dir).expanduser().resolve()
        root.mkdir(parents=True, exist_ok=True)
        return root

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
