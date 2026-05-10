from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from .base import BenchmarkArtifact, GenerationResult


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

    def generate(self, output_dir: str | Path | None) -> GenerationResult:
        root = self._require_output_dir(output_dir)
        out_dir = root / "dsb" / "data"
        out_dir.mkdir(parents=True, exist_ok=True)

        blueprint = self._write_text(out_dir / "schema_blueprint.sql", self._schema_sql())
        seed = self._write_text(out_dir / "seed_plan.yaml", self._seed_plan())

        metadata = self._write_json(
            out_dir / "dsb_data_manifest.json",
            {
                "benchmark": self.benchmark,
                "artifact_type": self.artifact_type,
                "scale_factor": self.scale_factor,
                "files": self._paths_relative_to(root, [blueprint, seed]),
                "note": "DSB artifacts are generated as reusable blueprints to integrate with your preferred data synthesizer.",
            },
        )
        return self._result(root, [blueprint, seed], metadata)

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

    def _seed_plan(self) -> str:
        row_multiplier = max(1, self.scale_factor)
        return (
            "tables:\n"
            "  date_dim:\n"
            "    target_rows: 3650\n"
            "  customer:\n"
            f"    target_rows: {100000 * row_multiplier}\n"
            "  lineorder:\n"
            f"    target_rows: {6000000 * row_multiplier}\n"
            "generator:\n"
            "  strategy: external\n"
            "  note: plug this plan into your data synthesis pipeline\n"
        )


@dataclass
class DSBQueries(BenchmarkArtifact):
    """Generate DSB analytical query templates."""

    benchmark: str = "dsb"
    artifact_type: str = "queries"

    def generate(self, output_dir: str | Path | None) -> GenerationResult:
        root = self._require_output_dir(output_dir)
        out_dir = root / "dsb" / "queries"
        out_dir.mkdir(parents=True, exist_ok=True)

        files: list[Path] = []
        for name, sql in _DSB_SQL_TEMPLATES.items():
            files.append(self._write_text(out_dir / f"{name}.sql", sql))

        manifest = self._write_json(
            out_dir / "dsb_queries_manifest.json",
            {
                "benchmark": self.benchmark,
                "artifact_type": self.artifact_type,
                "query_count": len(_DSB_SQL_TEMPLATES),
                "files": self._paths_relative_to(root, files),
            },
        )
        return self._result(root, files, manifest)


def data(scale_factor: int = 10) -> DSBData:
    return DSBData(scale_factor=scale_factor)


def queries() -> DSBQueries:
    return DSBQueries()
