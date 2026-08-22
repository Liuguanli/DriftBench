from __future__ import annotations

import csv
import random
from dataclasses import dataclass
from pathlib import Path

from driftbench.console import console_print

from .base import BenchmarkArtifact, GenerationResult


_YCSB_WORKLOAD_WEIGHTS: dict[str, tuple[int, int, int, int, int, int]] = {
    "A": (50, 0, 0, 50, 0, 0),
    "B": (95, 0, 0, 5, 0, 0),
    "C": (100, 0, 0, 0, 0, 0),
    "D": (95, 5, 0, 0, 0, 0),
    "E": (0, 5, 95, 0, 0, 0),
    "F": (50, 0, 0, 0, 0, 50),
}


@dataclass
class YCSBData(BenchmarkArtifact):
    """Generate YCSB data/load artifacts."""

    scale_factor: int = 1
    record_count: int | None = None

    benchmark: str = "ycsb"
    artifact_type: str = "data"

    def generate(self, output_dir: str | Path | None = None, force: bool = False) -> GenerationResult:
        root = self._require_output_dir(output_dir)
        out_dir = root / "ycsb" / "data"
        out_dir.mkdir(parents=True, exist_ok=True)

        records = self.record_count if self.record_count is not None else self.scale_factor * 1000
        cache_parameters = {
            "scale_factor": self.scale_factor,
            "record_count": records,
        }

        if not force:
            existing = self._load_existing(
                out_dir / "ycsb_data_manifest.json", root, cache_parameters
            )
            if existing is not None:
                console_print(f"[driftbench] YCSB data already exists at {out_dir}. Reusing.")
                return existing
        console_print(f"[driftbench] Generating YCSB data (sf={self.scale_factor}) -> {out_dir}")

        files = self._generate_synth(out_dir, records)
        metadata = self._write_manifest(
            out_dir / "ycsb_data_manifest.json",
            {
                "benchmark": self.benchmark,
                "artifact_type": self.artifact_type,
                "scale_factor": self.scale_factor,
                "record_count": records,
                "tables": {"usertable": records},
                "files": self._paths_relative_to(root, files),
            },
            cache_parameters,
            root=root,
        )
        return self._result(root, files, metadata)

    def _generate_synth(self, out_dir: Path, records: int) -> list[Path]:
        rng = random.Random(42)
        alphabet = "abcdefghijklmnopqrstuvwxyz"
        fields = ["YCSB_KEY"] + [f"FIELD{i}" for i in range(10)]
        path = out_dir / "usertable.csv"
        with path.open("w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(fields)
            for i in range(records):
                key = f"user{i:015d}"
                row = [key] + ["".join(rng.choices(alphabet, k=100)) for _ in range(10)]
                writer.writerow(row)
        return [path]


@dataclass
class YCSBQueries(BenchmarkArtifact):
    """Generate YCSB workload/query-mix artifacts."""

    workload: str = "A"
    run_seconds: int = 60
    target_rate: int = 10000

    benchmark: str = "ycsb"
    artifact_type: str = "queries"

    def generate(self, output_dir: str | Path | None = None, force: bool = False) -> GenerationResult:
        profile = self.workload.upper()
        if profile not in _YCSB_WORKLOAD_WEIGHTS:
            valid = ", ".join(sorted(_YCSB_WORKLOAD_WEIGHTS.keys()))
            raise ValueError(f"Unsupported YCSB workload '{self.workload}'. Use one of: {valid}")

        root = self._require_output_dir(output_dir)
        out_dir = root / "ycsb" / "queries"
        out_dir.mkdir(parents=True, exist_ok=True)

        cache_parameters = {
            "workload": profile,
            "run_seconds": self.run_seconds,
            "target_rate": self.target_rate,
        }

        if not force:
            existing = self._load_existing(
                out_dir / "ycsb_queries_manifest.json", root, cache_parameters
            )
            if existing is not None:
                console_print(
                    f"[driftbench] YCSB queries (workload={profile}) already exists at {out_dir}. Reusing."
                )
                return existing
        console_print(f"[driftbench] Generating YCSB queries (workload={profile}) -> {out_dir}")

        weights = _YCSB_WORKLOAD_WEIGHTS[profile]
        props = self._write_text(out_dir / f"workload_{profile.lower()}.properties", self._properties_body())
        benchbase_cfg = self._write_text(
            out_dir / "sample_ycsb_config.xml",
            self._benchbase_template(weights),
        )

        metadata = self._write_manifest(
            out_dir / "ycsb_queries_manifest.json",
            {
                "benchmark": self.benchmark,
                "artifact_type": self.artifact_type,
                "workload": profile,
                "run_seconds": self.run_seconds,
                "target_rate": self.target_rate,
                "weights": {
                    "ReadRecord": weights[0],
                    "InsertRecord": weights[1],
                    "ScanRecord": weights[2],
                    "UpdateRecord": weights[3],
                    "DeleteRecord": weights[4],
                    "ReadModifyWriteRecord": weights[5],
                },
                "files": self._paths_relative_to(root, [props, benchbase_cfg]),
            },
            cache_parameters,
            root=root,
        )
        return self._result(root, [props, benchbase_cfg], metadata)

    def _properties_body(self) -> str:
        return (
            "operationcount=100000\n"
            "recordcount=1000\n"
            "requestdistribution=zipfian\n"
            f"maxexecutiontime={self.run_seconds}\n"
            f"target={self.target_rate}\n"
        )

    def _benchbase_template(self, weights: tuple[int, int, int, int, int, int]) -> str:
        weight_str = ",".join(str(value) for value in weights)
        return (
            "<?xml version=\"1.0\"?>\n"
            "<parameters>\n"
            "    <type>POSTGRES</type>\n"
            "    <driver>org.postgresql.Driver</driver>\n"
            "    <url>jdbc:postgresql://localhost:5432/benchbase?sslmode=disable&amp;ApplicationName=ycsb</url>\n"
            "    <username>admin</username>\n"
            "    <password>password</password>\n"
            "    <terminals>1</terminals>\n"
            "    <works>\n"
            "        <work>\n"
            f"            <time>{self.run_seconds}</time>\n"
            f"            <rate>{self.target_rate}</rate>\n"
            f"            <weights>{weight_str}</weights>\n"
            "        </work>\n"
            "    </works>\n"
            "    <transactiontypes>\n"
            "        <transactiontype><name>ReadRecord</name></transactiontype>\n"
            "        <transactiontype><name>InsertRecord</name></transactiontype>\n"
            "        <transactiontype><name>ScanRecord</name></transactiontype>\n"
            "        <transactiontype><name>UpdateRecord</name></transactiontype>\n"
            "        <transactiontype><name>DeleteRecord</name></transactiontype>\n"
            "        <transactiontype><name>ReadModifyWriteRecord</name></transactiontype>\n"
            "    </transactiontypes>\n"
            "</parameters>\n"
        )


def data(scale_factor: int = 1, record_count: int | None = None) -> YCSBData:
    return YCSBData(scale_factor=scale_factor, record_count=record_count)


def queries(workload: str = "A", run_seconds: int = 60, target_rate: int = 10000) -> YCSBQueries:
    return YCSBQueries(workload=workload, run_seconds=run_seconds, target_rate=target_rate)
