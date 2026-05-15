from __future__ import annotations

import csv
import random
from dataclasses import dataclass
from pathlib import Path

from .base import BenchmarkArtifact, GenerationResult


@dataclass
class TPCDSData(BenchmarkArtifact):
    """Generate TPC-DS data artifacts using synthetic pipe-delimited generation."""

    scale_factor: int | float = 10

    benchmark: str = "tpcds"
    artifact_type: str = "data"

    def generate(self, output_dir: str | Path | None = None, force: bool = False) -> GenerationResult:
        root = self._require_output_dir(output_dir)
        out_dir = root / "tpcds" / "data"
        out_dir.mkdir(parents=True, exist_ok=True)

        if not force:
            existing = self._load_existing(out_dir / "tpcds_data_manifest.json", root)
            if existing is not None:
                print(f"[driftbench] TPC-DS data already exists at {out_dir}. Reusing.")
                return existing
        print(f"[driftbench] Generating TPC-DS data (sf={self.scale_factor}) → {out_dir}")

        sf = max(1, int(round(float(self.scale_factor))))
        dats = self._generate_synth(out_dir, sf)
        metadata = self._write_json(
            out_dir / "tpcds_data_manifest.json",
            {
                "benchmark": self.benchmark,
                "artifact_type": self.artifact_type,
                "scale_factor": self.scale_factor,
                "source": "synthetic",
                "tables": {
                    "date_dim": 3360,
                    "store": max(1, sf),
                    "item": 1000 * sf,
                    "customer": 1000 * sf,
                    "store_sales": 10000 * sf,
                },
                "files": self._paths_relative_to(root, dats),
            },
        )
        return self._result(root, dats, metadata)

    def _generate_synth(self, out_dir: Path, sf: int) -> list[Path]:
        """Generate synthetic TPC-DS tables in pipe-delimited .dat format (dsdgen-style)."""
        rng = random.Random(42)
        files: list[Path] = []

        # date_dim: 10 years × 12 months × 28 days = 3360 rows (fixed)
        date_path = out_dir / "date_dim.dat"
        date_sk = 1
        date_keys: list[int] = []
        with date_path.open("w", encoding="utf-8") as f:
            for year in range(2015, 2025):
                for month in range(1, 13):
                    for day in range(1, 29):
                        f.write(
                            f"{date_sk}|{year}{month:02d}{day:02d}|"
                            f"{year}-{month:02d}-{day:02d}|{year}|{month}|{day}|\n"
                        )
                        date_keys.append(date_sk)
                        date_sk += 1
        files.append(date_path)

        # store: 1 row per SF
        store_path = out_dir / "store.dat"
        store_count = max(1, sf)
        with store_path.open("w", encoding="utf-8") as f:
            for i in range(1, store_count + 1):
                f.write(f"{i}|STORE{i:04d}|Store {i}|Open|\n")
        files.append(store_path)

        # item: 1000 per SF
        item_path = out_dir / "item.dat"
        item_count = 1000 * sf
        categories = ["Electronics", "Clothing", "Food", "Books", "Sports"]
        with item_path.open("w", encoding="utf-8") as f:
            for i in range(1, item_count + 1):
                price = round(rng.uniform(1.0, 500.0), 2)
                cat = rng.choice(categories)
                f.write(f"{i}|ITEM{i:08d}|Item {i}|{cat}|{price}|\n")
        files.append(item_path)

        # customer: 1000 per SF
        cust_path = out_dir / "customer.dat"
        cust_count = 1000 * sf
        with cust_path.open("w", encoding="utf-8") as f:
            for i in range(1, cust_count + 1):
                f.write(f"{i}|CUST{i:08d}|First{i}|Last{i}|customer{i}@example.com|\n")
        files.append(cust_path)

        # store_sales (fact): 10000 per SF
        ss_path = out_dir / "store_sales.dat"
        ss_count = 10000 * sf
        with ss_path.open("w", encoding="utf-8") as f:
            for _ in range(ss_count):
                d_sk = rng.choice(date_keys)
                i_sk = rng.randint(1, item_count)
                c_sk = rng.randint(1, cust_count)
                s_sk = rng.randint(1, store_count)
                qty = rng.randint(1, 100)
                net_paid = round(rng.uniform(1.0, 10000.0), 2)
                net_profit = round(net_paid * rng.uniform(-0.1, 0.3), 2)
                f.write(f"{d_sk}|{i_sk}|{c_sk}|{s_sk}|{qty}|{net_paid}|{net_profit}|\n")
        files.append(ss_path)

        return files


@dataclass
class TPCDSQueries(BenchmarkArtifact):
    """Generate TPC-DS query workload artifacts."""

    benchmark: str = "tpcds"
    artifact_type: str = "queries"

    def generate(self, output_dir: str | Path | None = None, force: bool = False) -> GenerationResult:
        root = self._require_output_dir(output_dir)
        out_dir = root / "tpcds" / "queries"
        out_dir.mkdir(parents=True, exist_ok=True)

        if not force:
            existing = self._load_existing(out_dir / "tpcds_queries_manifest.json", root)
            if existing is not None:
                print(f"[driftbench] TPC-DS queries already exist at {out_dir}. Reusing.")
                return existing
        print(f"[driftbench] Generating TPC-DS queries → {out_dir}")

        ids = list(range(1, 100))
        ids_file = self._write_text(
            out_dir / "query_ids.txt",
            "\n".join(f"query{idx:02d}" for idx in ids) + "\n",
        )
        benchbase_cfg = self._write_text(
            out_dir / "sample_tpcds_config.xml",
            self._benchbase_template(ids),
        )

        metadata = self._write_json(
            out_dir / "tpcds_queries_manifest.json",
            {
                "benchmark": self.benchmark,
                "artifact_type": self.artifact_type,
                "query_count": len(ids),
                "files": self._paths_relative_to(root, [ids_file, benchbase_cfg]),
                "note": "Use this as a starter workload profile; attach SQL text from your local TPC-DS kit query templates.",
            },
        )
        return self._result(root, [ids_file, benchbase_cfg], metadata)

    def _benchbase_template(self, ids: list[int]) -> str:
        weights = ",".join("1" for _ in ids)
        txns = "\n".join(
            (
                "        <transactiontype>\n"
                f"            <name>Q{idx}</name>\n"
                f"            <id>{idx}</id>\n"
                "        </transactiontype>"
            )
            for idx in ids
        )
        return (
            "<?xml version=\"1.0\"?>\n"
            "<parameters>\n"
            "    <type>POSTGRES</type>\n"
            "    <driver>org.postgresql.Driver</driver>\n"
            "    <url>jdbc:postgresql://localhost:5432/benchbase?sslmode=disable&amp;ApplicationName=tpcds</url>\n"
            "    <username>admin</username>\n"
            "    <password>password</password>\n"
            "    <terminals>1</terminals>\n"
            "    <works>\n"
            "        <work>\n"
            "            <serial>true</serial>\n"
            "            <rate>unlimited</rate>\n"
            f"            <weights>{weights}</weights>\n"
            "        </work>\n"
            "    </works>\n"
            "    <transactiontypes>\n"
            f"{txns}\n"
            "    </transactiontypes>\n"
            "</parameters>\n"
        )


def data(scale_factor: int | float = 10) -> TPCDSData:
    return TPCDSData(scale_factor=scale_factor)


def queries() -> TPCDSQueries:
    return TPCDSQueries()
