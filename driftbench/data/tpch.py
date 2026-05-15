from __future__ import annotations

import subprocess
from dataclasses import dataclass
from pathlib import Path
import csv
import shutil
from typing import Any, Iterable, Literal

from driftbench.core.workload.tpch_sql_generator import (
    generate_tpch_queries_indexed,
    generate_tpch_queries_indexed_qgen,
    list_tpch_query_ids,
)

from .base import BenchmarkArtifact, GenerationResult


_REPO_ROOT = Path(__file__).resolve().parents[2]
_PACKAGED_TPCH_DIR = Path(__file__).resolve().parent / "resources" / "tpch"
_PACKAGED_TEMPLATE_DIR = _PACKAGED_TPCH_DIR / "queries"
_PACKAGED_DISTS_FILE = _PACKAGED_TPCH_DIR / "dists.dss"

_REPO_TPCH_DIR = _REPO_ROOT / "existing_benchmarks" / "TPC-H V3.0.1"
_REPO_TEMPLATE_DIR = _REPO_TPCH_DIR / "dbgen" / "queries"
_REPO_DISTS_FILE = _REPO_TPCH_DIR / "dbgen" / "dists.dss"
_REPO_REF_DATA_DIR = _REPO_TPCH_DIR / "ref_data"


@dataclass
class TPCHData(BenchmarkArtifact):
    """Generate or materialize TPC-H data artifacts."""

    scale_factor: str | int | float = 1
    source_dir: str | Path | None = None
    mode: Literal["copy", "plan", "generate"] = "copy"
    dbgen_path: str | Path | None = None  # explicit path to dbgen binary; searches PATH if None

    benchmark: str = "tpch"
    artifact_type: str = "data"

    def generate(self, output_dir: str | Path | None = None, force: bool = False) -> GenerationResult:
        root = self._require_output_dir(output_dir)
        out_dir = root / "tpch" / "data" / f"sf_{self._scale_key()}"
        out_dir.mkdir(parents=True, exist_ok=True)

        if not force:
            existing = self._load_existing(out_dir / "tpch_data_manifest.json", root)
            if existing is not None:
                print(f"[driftbench] TPC-H data (sf={self._scale_key()}) already exists at {out_dir}. Reusing.")
                return existing
        print(f"[driftbench] Generating TPC-H data (sf={self._scale_key()}) → {out_dir}")

        if self.mode == "plan":
            script = self._write_text(
                out_dir / "generate_tpch_data.sh",
                self._plan_script_body(),
            )
            script.chmod(0o755)
            metadata = self._write_json(
                out_dir / "tpch_data_manifest.json",
                {
                    "benchmark": self.benchmark,
                    "artifact_type": self.artifact_type,
                    "mode": self.mode,
                    "scale_factor": self._scale_key(),
                    "files": self._paths_relative_to(root, [script]),
                    "note": (
                        "Plan-only mode: this machine does not generate data. "
                        "Run generate_tpch_data.sh on a server with dbgen."
                    ),
                },
            )
            return self._result(root, [script], metadata)

        if self.mode == "generate":
            tbls = self._run_dbgen(out_dir)
            metadata = self._write_json(
                out_dir / "tpch_data_manifest.json",
                {
                    "benchmark": self.benchmark,
                    "artifact_type": self.artifact_type,
                    "mode": self.mode,
                    "scale_factor": self._scale_key(),
                    "source": "dbgen",
                    "files": self._paths_relative_to(root, tbls),
                },
            )
            return self._result(root, tbls, metadata)

        src = self._resolve_source_dir()
        copied: list[Path] = []
        for path in sorted(src.glob("*.tbl")):
            target = out_dir / path.name
            shutil.copy2(path, target)
            copied.append(target)

        if not copied:
            raise FileNotFoundError(
                f"No .tbl files found in source_dir={src}. "
                "Provide a TPC-H data directory that contains table .tbl files."
            )

        metadata = self._write_json(
            out_dir / "tpch_data_manifest.json",
            {
                "benchmark": self.benchmark,
                "artifact_type": self.artifact_type,
                "mode": self.mode,
                "scale_factor": self._scale_key(),
                "source": "explicit_source_dir" if self.source_dir is not None else "default_ref_data",
                "files": self._paths_relative_to(root, copied),
            },
        )
        return self._result(root, copied, metadata)

    def _plan_script_body(self) -> str:
        return (
            "#!/usr/bin/env bash\n"
            "set -euo pipefail\n\n"
            f"SCALE_FACTOR={self._scale_key()}\n"
            "OUT_DIR=\"$(cd \"$(dirname \"${BASH_SOURCE[0]}\")\" && pwd)/tables\"\n"
            "mkdir -p \"${OUT_DIR}\"\n\n"
            "# Server-side example (TPC-H dbgen):\n"
            "# 1) cd /path/to/tpch-kit/dbgen\n"
            "# 2) make\n"
            "# 3) ./dbgen -s ${SCALE_FACTOR}\n"
            "# 4) mv ./*.tbl \"${OUT_DIR}/\"\n\n"
            "echo \"Generate TPC-H data on server with scale=${SCALE_FACTOR}\"\n"
            "echo \"Output directory: ${OUT_DIR}\"\n"
        )

    def _scale_key(self) -> str:
        value = str(self.scale_factor).strip()
        if value.endswith(".0"):
            value = value[:-2]
        return value

    def _run_dbgen(self, out_dir: Path) -> list[Path]:
        """Run dbgen to generate .tbl files directly into out_dir/tables/."""
        # Resolve dbgen binary: explicit path → PATH → repo-local build
        if self.dbgen_path is not None:
            dbgen = Path(self.dbgen_path).expanduser().resolve()
            if not dbgen.exists():
                raise FileNotFoundError(f"dbgen not found at dbgen_path={dbgen}")
        else:
            found = shutil.which("dbgen")
            if found is None:
                # Also check the repo's own dbgen build
                repo_bin = _REPO_TPCH_DIR / "dbgen" / "dbgen"
                found = str(repo_bin) if repo_bin.exists() else None
            if found is None:
                raise RuntimeError(
                    "dbgen binary not found.\n"
                    "Install TPC-H dbgen and make it available in PATH:\n"
                    "  git clone https://github.com/electrum/tpch-dbgen\n"
                    "  cd tpch-dbgen && make\n"
                    "  export PATH=$PATH:$(pwd)\n"
                    "Or pass dbgen_path='/path/to/dbgen' explicitly.\n"
                    "Or use mode='plan' to generate a shell script instead."
                )
            dbgen = Path(found)

        tables_dir = out_dir / "tables"
        tables_dir.mkdir(parents=True, exist_ok=True)

        sf = self._scale_key()
        print(
            f"[driftbench] Running dbgen -s {sf} in {tables_dir}\n"
            f"[driftbench] This generates all 8 TPC-H tables. "
            f"sf={sf} ≈ {sf} GB — may take a while for large scale factors."
        )
        subprocess.run(
            [str(dbgen), "-vf", "-s", sf],
            cwd=str(tables_dir),
            check=True,
        )

        tbls = sorted(tables_dir.glob("*.tbl"))
        if not tbls:
            raise RuntimeError(
                f"dbgen ran successfully but no .tbl files were found in {tables_dir}. "
                "Check dbgen output above for errors."
            )
        print(f"[driftbench] dbgen complete — {len(tbls)} tables in {tables_dir}")
        return tbls

    def _resolve_source_dir(self) -> Path:
        if self.source_dir is not None:
            path = Path(self.source_dir).expanduser().resolve()
            if not path.exists():
                raise FileNotFoundError(f"TPCH source_dir does not exist: {path}")
            return path

        path = (_REPO_REF_DATA_DIR / self._scale_key()).resolve()
        if not path.exists():
            available = ""
            if _REPO_REF_DATA_DIR.exists():
                available = ", ".join(
                    p.name for p in sorted(_REPO_REF_DATA_DIR.iterdir()) if p.is_dir()
                )
            raise FileNotFoundError(
                "TPC-H data source not found for scale_factor="
                f"{self._scale_key()}. Available scales: [{available}]. "
                "Set source_dir explicitly (directory with .tbl files)."
            )
        return path


@dataclass
class TPCHQueries(BenchmarkArtifact):
    """Generate parameterized TPC-H SQL workloads."""

    query_ids: Iterable[int | str] | None = None
    template_dir: str | Path | None = None
    mode: str = "qgen"
    queries_per_template: int = 1
    seed: int = 42
    shuffle: bool = True
    param_specs: dict[str, Any] | None = None
    qgen_dist_file: str | Path | None = None
    scale: float = 1.0

    benchmark: str = "tpch"
    artifact_type: str = "queries"

    def generate(self, output_dir: str | Path | None = None, force: bool = False) -> GenerationResult:
        root = self._require_output_dir(output_dir)
        out_dir = root / "tpch" / "queries"
        out_dir.mkdir(parents=True, exist_ok=True)

        if not force:
            existing = self._load_existing(out_dir / "tpch_queries_manifest.json", root)
            if existing is not None:
                print(f"[driftbench] TPC-H queries already exist at {out_dir}. Reusing.")
                return existing
        print(f"[driftbench] Generating TPC-H queries → {out_dir}")

        template_dir = self._resolve_template_dir()
        query_ids = self._resolve_query_ids(template_dir)

        if self.mode == "qgen":
            entries = generate_tpch_queries_indexed_qgen(
                template_dir=str(template_dir),
                query_ids=query_ids,
                queries_per_template=self.queries_per_template,
                seed=self.seed,
                shuffle=self.shuffle,
                dist_file=str(self._resolve_dist_file()),
                scale=self.scale,
            )
        elif self.mode == "custom":
            entries = generate_tpch_queries_indexed(
                template_dir=str(template_dir),
                query_ids=query_ids,
                param_specs=self.param_specs or {},
                queries_per_template=self.queries_per_template,
                seed=self.seed,
                shuffle=self.shuffle,
            )
        else:
            raise ValueError("mode must be one of: 'qgen', 'custom'")

        sql_file = self._write_text(out_dir / "tpch_queries.sql", self._render_sql_bundle(entries))
        csv_file = self._write_entries_csv(out_dir / "tpch_queries.csv", entries)
        metadata = self._write_json(
            out_dir / "tpch_queries_manifest.json",
            {
                "benchmark": self.benchmark,
                "artifact_type": self.artifact_type,
                "mode": self.mode,
                "query_ids": [str(qid) for qid in query_ids],
                "queries_per_template": self.queries_per_template,
                "seed": self.seed,
                "shuffle": self.shuffle,
                "template_source": "explicit_template_dir" if self.template_dir is not None else "packaged_default",
                "count": len(entries),
                "files": self._paths_relative_to(root, [sql_file, csv_file]),
            },
        )
        return self._result(root, [sql_file, csv_file], metadata)

    def _resolve_template_dir(self) -> Path:
        if self.template_dir is not None:
            path = Path(self.template_dir).expanduser().resolve()
            if not path.exists():
                raise FileNotFoundError(f"TPC-H template_dir does not exist: {path}")
            return path
        if _PACKAGED_TEMPLATE_DIR.exists():
            return _PACKAGED_TEMPLATE_DIR
        if _REPO_TEMPLATE_DIR.exists():
            return _REPO_TEMPLATE_DIR
        raise FileNotFoundError(
            "Default TPC-H template directory not found. "
            "Set template_dir explicitly."
        )

    def _resolve_query_ids(self, template_dir: Path) -> list[str]:
        if self.query_ids is None:
            return list_tpch_query_ids(str(template_dir))
        return [str(qid) for qid in self.query_ids]

    def _resolve_dist_file(self) -> Path:
        if self.qgen_dist_file is not None:
            path = Path(self.qgen_dist_file).expanduser().resolve()
            if not path.exists():
                raise FileNotFoundError(f"TPC-H qgen_dist_file does not exist: {path}")
            return path
        if _PACKAGED_DISTS_FILE.exists():
            return _PACKAGED_DISTS_FILE
        if _REPO_DISTS_FILE.exists():
            return _REPO_DISTS_FILE
        raise FileNotFoundError(
            "Default dists.dss file not found. Set qgen_dist_file explicitly."
        )

    def _render_sql_bundle(self, entries: list[dict[str, Any]]) -> str:
        chunks: list[str] = []
        for entry in entries:
            chunks.append(
                f"-- TPCH Q{entry['query_id']} instance {entry['index']}\n"
                f"{entry['sql'].strip()}\n"
            )
        return "\n".join(chunks).strip() + "\n"

    def _write_entries_csv(self, path: Path, entries: list[dict[str, Any]]) -> Path:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=["query_id", "index", "sql"])
            writer.writeheader()
            writer.writerows(entries)
        return path


def data(
    scale_factor: str | int | float = 1,
    source_dir: str | Path | None = None,
    mode: Literal["copy", "plan", "generate"] = "copy",
    dbgen_path: str | Path | None = None,
) -> TPCHData:
    """Return a TPCHData adapter.

    Modes:
        "copy"     — copy .tbl files from source_dir (or default ref_data).
        "plan"     — write generate_tpch_data.sh only; no data generated locally.
        "generate" — run dbgen to produce all 8 .tbl files in output_dir/tpch/data/sf_N/tables/.
                     Requires dbgen in PATH or pass dbgen_path explicitly.
    """
    return TPCHData(
        scale_factor=scale_factor,
        source_dir=source_dir,
        mode=mode,
        dbgen_path=dbgen_path,
    )


def queries(
    query_ids: Iterable[int | str] | None = None,
    template_dir: str | Path | None = None,
    mode: str = "qgen",
    queries_per_template: int = 1,
    seed: int = 42,
    shuffle: bool = True,
    param_specs: dict[str, Any] | None = None,
    qgen_dist_file: str | Path | None = None,
    scale: float = 1.0,
) -> TPCHQueries:
    return TPCHQueries(
        query_ids=query_ids,
        template_dir=template_dir,
        mode=mode,
        queries_per_template=queries_per_template,
        seed=seed,
        shuffle=shuffle,
        param_specs=param_specs,
        qgen_dist_file=qgen_dist_file,
        scale=scale,
    )
