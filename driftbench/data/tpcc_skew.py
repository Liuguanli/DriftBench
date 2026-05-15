"""TPC-C Skew benchmark adapter.

TPC-C Skew extends the standard TPC-C workload with a configurable Zipf
distribution over warehouse access, simulating "hot warehouse" contention.

This is a common real-world drift pattern: a subset of warehouses receives
disproportionate traffic, causing lock contention and performance degradation.

Quick start:
    from driftbench.data.tpcc_skew import data as tpcc_skew_data, queries as tpcc_skew_queries

    tpcc_skew_data(scale_factor=4, hot_warehouse_fraction=0.25, skew_factor=0.99).generate(output_dir="./artifacts")
    tpcc_skew_queries(scale_factor=4, hot_warehouse_fraction=0.25, skew_factor=0.99).generate(output_dir="./artifacts")

Scale note:
    Same table scaling as TPC-C (scale_factor == number of warehouses).
    The skew affects access probability only; table sizes are identical to TPC-C.
    hot_warehouse_fraction: fraction of warehouses labeled "hot" in the weight manifest.
    skew_factor: Zipf alpha parameter (higher = more skewed). Typical range 0.5–1.5.
"""
from __future__ import annotations

import csv
import math
from dataclasses import dataclass
from pathlib import Path

from .base import BenchmarkArtifact, GenerationResult
from .tpcc import TPCCData, _TPCC_DDL, _TPCC_TRANSACTIONS


@dataclass
class TPCCSkewData(TPCCData):
    """TPC-C data + a Zipf warehouse access weight manifest.

    Generates the same 9-table CSV files as TPCCData, plus
    warehouse_access_weights.csv encoding the Zipf skew distribution.
    The weight manifest is consumed by drivers to bias new-order transactions
    toward hot warehouses.

    Args:
        scale_factor: Number of warehouses (W).
        hot_warehouse_fraction: Fraction of warehouses labeled "hot" (default 0.2).
        skew_factor: Zipf alpha for warehouse selection (default 0.99).
    """

    hot_warehouse_fraction: float = 0.2
    skew_factor: float = 0.99

    benchmark: str = "tpcc_skew"
    artifact_type: str = "data"

    def generate(self, output_dir: str | Path | None = None, force: bool = False) -> GenerationResult:
        root = self._require_output_dir(output_dir)
        out_dir = root / "tpcc_skew" / "data"
        out_dir.mkdir(parents=True, exist_ok=True)

        if not force:
            existing = self._load_existing(out_dir / "tpcc_skew_data_manifest.json", root)
            if existing is not None:
                print(f"[driftbench] TPC-C Skew data already exists at {out_dir}. Reusing.")
                return existing
        print(f"[driftbench] Generating TPC-C Skew data (W={self._w()}, skew={self.skew_factor}) → {out_dir}")

        w = self._w()
        weights = self._zipf_weights(w)
        hot_count = max(1, int(math.ceil(w * self.hot_warehouse_fraction)))

        ddl = self._write_text(out_dir / "tpcc_schema.sql", _TPCC_DDL)

        synth_files = self._generate_synth(out_dir)
        synth_files.insert(0, ddl)
        wts_file = self._write_warehouse_weights(out_dir, weights, hot_count)
        synth_files.append(wts_file)

        metadata = self._write_json(
            out_dir / "tpcc_skew_data_manifest.json",
            {
                "benchmark": self.benchmark,
                "artifact_type": self.artifact_type,
                "scale_factor": w,
                "hot_warehouse_fraction": self.hot_warehouse_fraction,
                "hot_warehouse_count": hot_count,
                "skew_factor": self.skew_factor,
                "files": self._paths_relative_to(root, synth_files),
                "note": (
                    f"Zipf skew (alpha={self.skew_factor}): {hot_count} of {w} warehouses "
                    f"receive ~{self._hot_share(weights, hot_count):.0%} of traffic. "
                    "Use warehouse_access_weights.csv to drive warehouse selection."
                ),
            },
        )
        return self._result(root, synth_files, metadata)

    def _zipf_weights(self, w: int) -> list[float]:
        if w == 1:
            return [1.0]
        raw = [1.0 / (rank ** self.skew_factor) for rank in range(1, w + 1)]
        total = sum(raw)
        return [r / total for r in raw]

    def _hot_share(self, weights: list[float], hot_count: int) -> float:
        return sum(weights[:hot_count])

    def _write_warehouse_weights(
        self, out_dir: Path, weights: list[float], hot_count: int
    ) -> Path:
        path = out_dir / "warehouse_access_weights.csv"
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["warehouse_id", "access_probability", "is_hot"])
            for idx, prob in enumerate(weights):
                writer.writerow([idx + 1, f"{prob:.6f}", int(idx < hot_count)])
        return path


@dataclass
class TPCCSkewQueries(BenchmarkArtifact):
    """TPC-C Skew transaction templates annotated with skew metadata.

    Produces the same 5 SQL transaction templates as TPC-C plus a
    skew header comment block explaining how to bias warehouse selection.

    Args:
        scale_factor: Number of warehouses (used in comment header).
        hot_warehouse_fraction: Fraction of warehouses labeled "hot".
        skew_factor: Zipf alpha for warehouse selection.
    """

    scale_factor: int | float = 1
    hot_warehouse_fraction: float = 0.2
    skew_factor: float = 0.99

    benchmark: str = "tpcc_skew"
    artifact_type: str = "queries"

    def generate(self, output_dir: str | Path | None = None, force: bool = False) -> GenerationResult:
        root = self._require_output_dir(output_dir)
        out_dir = root / "tpcc_skew" / "queries"
        out_dir.mkdir(parents=True, exist_ok=True)

        if not force:
            existing = self._load_existing(out_dir / "tpcc_skew_queries_manifest.json", root)
            if existing is not None:
                print(f"[driftbench] TPC-C Skew queries already exist at {out_dir}. Reusing.")
                return existing
        print(f"[driftbench] Generating TPC-C Skew queries → {out_dir}")

        w = max(1, int(round(float(self.scale_factor))))
        hot_count = max(1, int(math.ceil(w * self.hot_warehouse_fraction)))

        skew_header = (
            f"-- TPC-C Skew: Zipf alpha={self.skew_factor}, "
            f"{hot_count}/{w} warehouses are hot.\n"
            "-- Bias :w_id selection using warehouse_access_weights.csv.\n\n"
        )

        files: list[Path] = []
        for name, sql in _TPCC_TRANSACTIONS.items():
            files.append(self._write_text(out_dir / f"{name}.sql", skew_header + sql))

        bundle = skew_header + "\n".join(
            f"-- === {name.upper()} ===\n{sql}"
            for name, sql in _TPCC_TRANSACTIONS.items()
        )
        files.append(self._write_text(out_dir / "tpcc_skew_all_transactions.sql", bundle))

        metadata = self._write_json(
            out_dir / "tpcc_skew_queries_manifest.json",
            {
                "benchmark": self.benchmark,
                "artifact_type": self.artifact_type,
                "scale_factor": w,
                "hot_warehouse_fraction": self.hot_warehouse_fraction,
                "hot_warehouse_count": hot_count,
                "skew_factor": self.skew_factor,
                "transaction_types": list(_TPCC_TRANSACTIONS.keys()),
                "query_count": len(_TPCC_TRANSACTIONS),
                "files": self._paths_relative_to(root, files),
                "note": (
                    "Use warehouse_access_weights.csv to bias :w_id selection "
                    "in new_order and payment transactions."
                ),
            },
        )
        return self._result(root, files, metadata)


def data(
    scale_factor: int | float = 1,
    hot_warehouse_fraction: float = 0.2,
    skew_factor: float = 0.99,
) -> TPCCSkewData:
    """Return a TPCCSkewData adapter with Zipf warehouse access distribution.

    Args:
        scale_factor: Number of warehouses (W).
        hot_warehouse_fraction: Fraction of warehouses receiving hot traffic (default 0.2).
        skew_factor: Zipf alpha (default 0.99). Higher = more concentrated traffic.
    """
    return TPCCSkewData(
        scale_factor=scale_factor,
        hot_warehouse_fraction=hot_warehouse_fraction,
        skew_factor=skew_factor,
    )


def queries(
    scale_factor: int | float = 1,
    hot_warehouse_fraction: float = 0.2,
    skew_factor: float = 0.99,
) -> TPCCSkewQueries:
    """Return a TPCCSkewQueries adapter annotated with skew parameters."""
    return TPCCSkewQueries(
        scale_factor=scale_factor,
        hot_warehouse_fraction=hot_warehouse_fraction,
        skew_factor=skew_factor,
    )
