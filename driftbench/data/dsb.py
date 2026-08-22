from __future__ import annotations

import csv
import random
from dataclasses import dataclass
from pathlib import Path

from driftbench.console import console_print

from .base import BenchmarkArtifact, GenerationResult


_DSB_DATE_DIM_ROWS = 10 * 12 * 28
_DSB_CUSTOMERS_PER_SCALE = 1_000
_DSB_LINEORDERS_PER_SCALE = 5_000


_DSB_SQL_TEMPLATES: dict[str, str] = {
    "q1_revenue_by_year": (
        "SELECT d.year, SUM(lo.revenue) AS total_revenue\n"
        "FROM lineorder lo\n"
        "JOIN date_dim d ON lo.order_date_key = d.date_key\n"
        "GROUP BY d.year\n"
        "ORDER BY d.year;\n"
    ),
    "q2_revenue_by_region": (
        "SELECT c.region, SUM(lo.revenue) AS total_revenue\n"
        "FROM lineorder lo\n"
        "JOIN customer c ON lo.customer_key = c.customer_key\n"
        "GROUP BY c.region\n"
        "ORDER BY total_revenue DESC;\n"
    ),
    "q3_margin_trend": (
        "SELECT d.year, SUM(lo.revenue - lo.supply_cost) AS margin\n"
        "FROM lineorder lo\n"
        "JOIN date_dim d ON lo.order_date_key = d.date_key\n"
        "GROUP BY d.year\n"
        "ORDER BY d.year;\n"
    ),
}


@dataclass
class DSBData(BenchmarkArtifact):
    """Generate DSB-style star-schema data blueprint artifacts."""

    scale_factor: int = 10

    benchmark: str = "dsb"
    artifact_type: str = "data"

    def generate(self, output_dir: str | Path | None = None, force: bool = False) -> GenerationResult:
        root = self._require_output_dir(output_dir)
        out_dir = root / "dsb" / "data"
        out_dir.mkdir(parents=True, exist_ok=True)

        sf = max(1, int(round(float(self.scale_factor))))
        cache_parameters = {"scale_factor": sf}

        if not force:
            existing = self._load_existing(
                out_dir / "dsb_data_manifest.json", root, cache_parameters
            )
            if existing is not None:
                console_print(f"[driftbench] DSB data already exists at {out_dir}. Reusing.")
                return existing
        console_print(f"[driftbench] Generating DSB data (sf={sf}) -> {out_dir}")

        ddl = self._write_text(out_dir / "dsb_schema.sql", self._schema_sql())
        files = self._generate_synth(out_dir)
        files.insert(0, ddl)
        metadata = self._write_manifest(
            out_dir / "dsb_data_manifest.json",
            {
                "benchmark": self.benchmark,
                "artifact_type": self.artifact_type,
                "scale_factor": sf,
                "tables": {
                    "date_dim": _DSB_DATE_DIM_ROWS,
                    "customer": _DSB_CUSTOMERS_PER_SCALE * sf,
                    "lineorder": _DSB_LINEORDERS_PER_SCALE * sf,
                },
                "files": self._paths_relative_to(root, files),
                "note": "Synthetic DSB star-schema data for onboarding and API testing.",
            },
            cache_parameters,
            root=root,
        )
        return self._result(root, files, metadata)

    def _generate_synth(self, out_dir: Path) -> list[Path]:
        sf = max(1, int(round(float(self.scale_factor))))
        rng = random.Random(42)
        regions = ["ASIA", "EUROPE", "AMERICA", "AFRICA", "MIDDLE EAST"]
        nations = ["CHINA", "JAPAN", "GERMANY", "FRANCE", "USA", "BRAZIL", "EGYPT", "IRAN"]
        files: list[Path] = []

        # date_dim: 10 years of daily rows (fixed)
        date_path = out_dir / "date_dim.csv"
        with date_path.open("w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["date_key", "year", "month", "day"])
            key = 1
            for year in range(2015, 2025):
                for month in range(1, 13):
                    for day in range(1, 29):  # 28 days/month keeps it simple
                        writer.writerow([key, year, month, day])
                        key += 1
        files.append(date_path)

        # customer
        cust_path = out_dir / "customer.csv"
        cust_count = _DSB_CUSTOMERS_PER_SCALE * sf
        with cust_path.open("w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["customer_key", "region", "nation"])
            for i in range(1, cust_count + 1):
                writer.writerow([i, rng.choice(regions), rng.choice(nations)])
        files.append(cust_path)

        # lineorder
        lo_path = out_dir / "lineorder.csv"
        lo_count = _DSB_LINEORDERS_PER_SCALE * sf
        date_keys = list(range(1, key))
        with lo_path.open("w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["order_key", "customer_key", "order_date_key", "revenue", "supply_cost"])
            for i in range(1, lo_count + 1):
                rev = round(rng.uniform(100, 100000), 2)
                cost = round(rng.uniform(50, rev), 2)
                writer.writerow([i, rng.randint(1, cust_count), rng.choice(date_keys), rev, cost])
        files.append(lo_path)

        return files

    def _schema_sql(self) -> str:
        return (
            "CREATE TABLE IF NOT EXISTS date_dim (\n"
            "  date_key INTEGER PRIMARY KEY,\n"
            "  year INTEGER NOT NULL,\n"
            "  month INTEGER NOT NULL,\n"
            "  day INTEGER NOT NULL\n"
            ");\n\n"
            "CREATE TABLE IF NOT EXISTS customer (\n"
            "  customer_key INTEGER PRIMARY KEY,\n"
            "  region TEXT NOT NULL,\n"
            "  nation TEXT NOT NULL\n"
            ");\n\n"
            "CREATE TABLE IF NOT EXISTS lineorder (\n"
            "  order_key BIGINT PRIMARY KEY,\n"
            "  customer_key INTEGER NOT NULL,\n"
            "  order_date_key INTEGER NOT NULL,\n"
            "  revenue DOUBLE PRECISION NOT NULL,\n"
            "  supply_cost DOUBLE PRECISION NOT NULL\n"
            ");\n"
        )


@dataclass
class DSBQueries(BenchmarkArtifact):
    """Generate DSB analytical query templates."""

    benchmark: str = "dsb"
    artifact_type: str = "queries"

    def generate(self, output_dir: str | Path | None = None, force: bool = False) -> GenerationResult:
        root = self._require_output_dir(output_dir)
        out_dir = root / "dsb" / "queries"
        out_dir.mkdir(parents=True, exist_ok=True)

        cache_parameters: dict[str, object] = {}

        if not force:
            existing = self._load_existing(
                out_dir / "dsb_queries_manifest.json", root, cache_parameters
            )
            if existing is not None:
                console_print(f"[driftbench] DSB queries already exist at {out_dir}. Reusing.")
                return existing
        console_print(f"[driftbench] Generating DSB queries -> {out_dir}")

        files: list[Path] = []
        for name, sql in _DSB_SQL_TEMPLATES.items():
            files.append(self._write_text(out_dir / f"{name}.sql", sql))

        manifest = self._write_manifest(
            out_dir / "dsb_queries_manifest.json",
            {
                "benchmark": self.benchmark,
                "artifact_type": self.artifact_type,
                "query_count": len(_DSB_SQL_TEMPLATES),
                "files": self._paths_relative_to(root, files),
            },
            cache_parameters,
            root=root,
        )
        return self._result(root, files, manifest)


def data(scale_factor: int = 10) -> DSBData:
    return DSBData(scale_factor=scale_factor)


def queries() -> DSBQueries:
    return DSBQueries()
