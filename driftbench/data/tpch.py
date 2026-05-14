from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import csv
import shutil
from datetime import date, timedelta
from typing import Any, Iterable, Literal
from urllib.error import URLError
from urllib.request import urlopen

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

_DEFAULT_SAMPLE_TPCH_BASE_URL = (
    "https://raw.githubusercontent.com/"
    "SAP-samples/hana-cloud-relational-data-lake-onboarding/main/TPCH"
)
# download mode fetches only these 4 tables from the SAP sample repository.
# This is intentional: the sample source does not provide all 8 TPC-H tables.
# Use mode="synth" to get all 8 tables locally, or mode="plan" for server-side dbgen.
_SAMPLE_TBL_FILENAMES = ("customer.tbl", "nation.tbl", "region.tbl", "supplier.tbl")


@dataclass
class TPCHData(BenchmarkArtifact):
    """Generate or materialize TPC-H data artifacts."""

    scale_factor: str | int | float = 1
    source_dir: str | Path | None = None
    mode: Literal["auto", "copy", "synth", "download", "plan"] = "auto"
    sample_base_url: str = _DEFAULT_SAMPLE_TPCH_BASE_URL

    benchmark: str = "tpch"
    artifact_type: str = "data"

    def generate(self, output_dir: str | Path | None) -> GenerationResult:
        root = self._require_output_dir(output_dir)
        out_dir = root / "tpch" / "data" / f"sf_{self._scale_key()}"
        out_dir.mkdir(parents=True, exist_ok=True)

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

        materialized: list[Path] = []
        source_kind = ""
        if self.mode == "synth":
            materialized = self._generate_synthetic_tbl_data(out_dir)
            source_kind = "synthetic_generator"
        elif self.mode == "copy":
            src = self._resolve_source_dir()
            materialized = self._copy_tbl_files(src, out_dir)
            source_kind = "explicit_source_dir" if self.source_dir is not None else "default_ref_data"
        elif self.mode == "download":
            materialized = self._download_sample_tbl_files(out_dir)
            source_kind = "downloaded_sample_tbl"
        else:
            try:
                src = self._resolve_source_dir()
                materialized = self._copy_tbl_files(src, out_dir)
                source_kind = (
                    "explicit_source_dir" if self.source_dir is not None else "default_ref_data"
                )
            except FileNotFoundError:
                if self._is_unit_scale():
                    try:
                        materialized = self._download_sample_tbl_files(out_dir)
                        source_kind = "downloaded_sample_tbl_auto_fallback"
                    except Exception:
                        materialized = self._generate_synthetic_tbl_data(out_dir)
                        source_kind = "synthetic_auto_fallback"
                else:
                    materialized = self._generate_synthetic_tbl_data(out_dir)
                    source_kind = "synthetic_auto_fallback"

        if not materialized:
            raise FileNotFoundError(
                f"Failed to materialize TPC-H data for mode={self.mode} at output_dir={out_dir}"
            )

        metadata = self._write_json(
            out_dir / "tpch_data_manifest.json",
            {
                "benchmark": self.benchmark,
                "artifact_type": self.artifact_type,
                "mode": self.mode,
                "scale_factor": self._scale_key(),
                "source": source_kind,
                "files": self._paths_relative_to(root, materialized),
                "note": (
                    "Synthetic mode creates lightweight TPC-H-like .tbl files for onboarding and API usage tests; "
                    "it is not a standards-compliant TPC-H benchmark dataset."
                    if source_kind.startswith("synthetic")
                    else (
                        "Download mode fetches 4 of the 8 TPC-H tables (customer, nation, region, supplier) "
                        "from a public sample repository. Use mode='synth' for all 8 tables locally, "
                        "or mode='plan' for full-scale server-side generation."
                        if "downloaded" in source_kind
                        else "Copied .tbl files from local source."
                    )
                ),
            },
        )
        return self._result(root, materialized, metadata)

    def _download_sample_tbl_files(self, out_dir: Path) -> list[Path]:
        downloaded: list[Path] = []
        base = self.sample_base_url.rstrip("/")
        for filename in _SAMPLE_TBL_FILENAMES:
            url = f"{base}/{filename}"
            target = out_dir / filename
            try:
                with urlopen(url, timeout=60) as response:
                    target.write_bytes(response.read())
            except URLError as exc:
                raise FileNotFoundError(
                    f"Failed to download TPC-H sample file: {url}. "
                    "Check network access or set source_dir/mode='synth'."
                ) from exc
            if not target.exists() or target.stat().st_size == 0:
                raise FileNotFoundError(
                    f"Downloaded empty TPC-H sample file from {url}."
                )
            downloaded.append(target)
        return downloaded

    def _copy_tbl_files(self, src: Path, out_dir: Path) -> list[Path]:
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
        return copied

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

    def _generate_synthetic_tbl_data(self, out_dir: Path) -> list[Path]:
        sf = self._scaled_units()
        region_count = 5
        nation_count = 25
        supplier_count = min(20000, max(100, 100 * sf))
        customer_count = min(50000, max(1000, 1000 * sf))
        part_count = min(60000, max(2000, 2000 * sf))
        orders_count = min(150000, max(5000, 5000 * sf))
        lineitems_per_order = 2
        partsupp_per_part = 2

        region_rows = [
            (r, f"REGION#{r}", f"Synthetic region {r}")
            for r in range(region_count)
        ]
        nation_rows = [
            (n, f"NATION#{n}", n % region_count, f"Synthetic nation {n}")
            for n in range(nation_count)
        ]
        supplier_rows = [
            (
                s,
                f"Supplier#{s:09d}",
                f"Address#{s}",
                s % nation_count,
                f"{10 + (s % 90):02d}-{100 + (s % 900):03d}-{1000 + (s % 9000):04d}",
                f"{(s % 10000) / 100:.2f}",
                f"Synthetic supplier comment {s}",
            )
            for s in range(1, supplier_count + 1)
        ]
        customer_rows = [
            (
                c,
                f"Customer#{c:09d}",
                f"Address#{c}",
                c % nation_count,
                f"{10 + (c % 90):02d}-{100 + (c % 900):03d}-{1000 + (c % 9000):04d}",
                f"{(c % 20000) / 100:.2f}",
                f"SEGMENT{c % 5}",
                f"Synthetic customer comment {c}",
            )
            for c in range(1, customer_count + 1)
        ]
        part_rows = [
            (
                p,
                f"Part#{p:09d}",
                f"MFGR#{1 + (p % 5)}",
                f"BRAND#{1 + (p % 25)}",
                f"TYPE#{1 + (p % 10)}",
                1 + (p % 50),
                f"BOX{1 + (p % 8)}",
                f"{10 + (p % 10000) / 10:.2f}",
                f"Synthetic part comment {p}",
            )
            for p in range(1, part_count + 1)
        ]
        partsupp_rows = []
        for p in range(1, part_count + 1):
            for offset in range(partsupp_per_part):
                supp = ((p + offset) % supplier_count) + 1
                partsupp_rows.append(
                    (
                        p,
                        supp,
                        1 + ((p + offset) % 9999),
                        f"{5 + ((p + offset) % 5000) / 10:.2f}",
                        f"Synthetic partsupp comment p{p}s{supp}",
                    )
                )

        start = date(1992, 1, 1)
        orders_rows = []
        lineitem_rows = []
        for o in range(1, orders_count + 1):
            cust = ((o - 1) % customer_count) + 1
            odate = (start + timedelta(days=o % 2555)).isoformat()
            total_price = 0.0
            for lno in range(1, lineitems_per_order + 1):
                part = ((o * 7 + lno) % part_count) + 1
                supp = ((part + lno) % supplier_count) + 1
                quantity = 1 + ((o + lno) % 50)
                ext_price = float(quantity * (5 + (part % 100)))
                discount = (o + lno) % 10 / 100
                tax = (o + lno) % 8 / 100
                ship_date = (start + timedelta(days=(o + lno) % 2555)).isoformat()
                commit_date = (start + timedelta(days=(o + lno + 3) % 2555)).isoformat()
                receipt_date = (start + timedelta(days=(o + lno + 7) % 2555)).isoformat()
                total_price += ext_price * (1 - discount) * (1 + tax)
                lineitem_rows.append(
                    (
                        o,
                        part,
                        supp,
                        lno,
                        quantity,
                        f"{ext_price:.2f}",
                        f"{discount:.2f}",
                        f"{tax:.2f}",
                        "N" if (o + lno) % 2 else "R",
                        "O" if (o + lno) % 2 else "F",
                        ship_date,
                        commit_date,
                        receipt_date,
                        "DELIVER IN PERSON",
                        "AIR",
                        f"Synthetic lineitem comment o{o}l{lno}",
                    )
                )
            orders_rows.append(
                (
                    o,
                    cust,
                    "O",
                    f"{total_price:.2f}",
                    odate,
                    f"{1 + (o % 5)}-URGENT",
                    f"Clerk#{(o % 1000):09d}",
                    0,
                    f"Synthetic order comment {o}",
                )
            )

        files = [
            self._write_tbl(out_dir / "region.tbl", region_rows),
            self._write_tbl(out_dir / "nation.tbl", nation_rows),
            self._write_tbl(out_dir / "supplier.tbl", supplier_rows),
            self._write_tbl(out_dir / "customer.tbl", customer_rows),
            self._write_tbl(out_dir / "part.tbl", part_rows),
            self._write_tbl(out_dir / "partsupp.tbl", partsupp_rows),
            self._write_tbl(out_dir / "orders.tbl", orders_rows),
            self._write_tbl(out_dir / "lineitem.tbl", lineitem_rows),
        ]
        return files

    def _scaled_units(self) -> int:
        try:
            value = float(str(self.scale_factor).strip())
        except ValueError as exc:
            raise ValueError(f"Invalid scale_factor: {self.scale_factor}") from exc
        if value <= 0:
            return 1
        return max(1, int(round(value)))

    def _is_unit_scale(self) -> bool:
        try:
            value = float(str(self.scale_factor).strip())
        except ValueError:
            return False
        return abs(value - 1.0) < 1e-9

    def _write_tbl(self, path: Path, rows: Iterable[tuple[Any, ...]]) -> Path:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8", newline="") as f:
            for row in rows:
                f.write("|".join(str(col) for col in row) + "|\n")
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

    def generate(self, output_dir: str | Path | None) -> GenerationResult:
        root = self._require_output_dir(output_dir)
        out_dir = root / "tpch" / "queries"
        out_dir.mkdir(parents=True, exist_ok=True)

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
    mode: Literal["auto", "copy", "synth", "download", "plan"] = "auto",
    sample_base_url: str = _DEFAULT_SAMPLE_TPCH_BASE_URL,
) -> TPCHData:
    return TPCHData(
        scale_factor=scale_factor,
        source_dir=source_dir,
        mode=mode,
        sample_base_url=sample_base_url,
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
