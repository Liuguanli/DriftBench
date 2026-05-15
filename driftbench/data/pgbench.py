"""pgbench benchmark adapter.

pgbench is PostgreSQL's built-in benchmarking tool implementing a TPC-B-like
workload across 4 tables (branches, tellers, accounts, history).

Quick start:
    from driftbench.data.pgbench import data as pgbench_data, queries as pgbench_queries

    pgbench_data(scale_factor=1).generate(output_dir="./artifacts")
    pgbench_queries(workload="tpcb").generate(output_dir="./artifacts")

Scale note:
    scale_factor (sf) maps directly to pgbench's -s flag:
      branches:  sf rows
      tellers:   10 * sf rows
      accounts:  100_000 * sf rows
      history:   0 initially (grows during execution)
"""
from __future__ import annotations

import csv
import random
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from .base import BenchmarkArtifact, GenerationResult


_PGBENCH_DDL = """\
-- pgbench TPC-B-like schema (PostgreSQL)

CREATE TABLE IF NOT EXISTS pgbench_branches (
    bid      INTEGER PRIMARY KEY,
    bbalance INTEGER NOT NULL,
    filler   CHAR(88)
);

CREATE TABLE IF NOT EXISTS pgbench_tellers (
    tid      INTEGER PRIMARY KEY,
    bid      INTEGER NOT NULL,
    tbalance INTEGER NOT NULL,
    filler   CHAR(84)
);

CREATE TABLE IF NOT EXISTS pgbench_accounts (
    aid      BIGINT PRIMARY KEY,
    bid      INTEGER NOT NULL,
    abalance INTEGER NOT NULL,
    filler   CHAR(84)
);

CREATE TABLE IF NOT EXISTS pgbench_history (
    tid    INTEGER,
    bid    INTEGER,
    aid    BIGINT,
    delta  INTEGER,
    mtime  TIMESTAMP,
    filler CHAR(22)
);
"""

_PGBENCH_TRANSACTIONS: dict[str, str] = {
    "tpcb": """\
-- pgbench default transaction (TPC-B-like)
-- Equivalent to: pgbench -b tpcb-like
\\set aid random(1, 100000 * :scale)
\\set bid random(1, 1 * :scale)
\\set tid random(1, 10 * :scale)
\\set delta random(-5000, 5000)

BEGIN;
UPDATE pgbench_accounts SET abalance = abalance + :delta WHERE aid = :aid;
SELECT abalance FROM pgbench_accounts WHERE aid = :aid;
UPDATE pgbench_tellers SET tbalance = tbalance + :delta WHERE tid = :tid;
UPDATE pgbench_branches SET bbalance = bbalance + :delta WHERE bid = :bid;
INSERT INTO pgbench_history (tid, bid, aid, delta, mtime)
    VALUES (:tid, :bid, :aid, :delta, CURRENT_TIMESTAMP);
END;
""",
    "simple_update": """\
-- pgbench simple-update transaction (-b simple-update)
\\set aid random(1, 100000 * :scale)
\\set bid random(1, 1 * :scale)
\\set tid random(1, 10 * :scale)
\\set delta random(-5000, 5000)

BEGIN;
UPDATE pgbench_accounts SET abalance = abalance + :delta WHERE aid = :aid;
INSERT INTO pgbench_history (tid, bid, aid, delta, mtime)
    VALUES (:tid, :bid, :aid, :delta, CURRENT_TIMESTAMP);
END;
""",
    "select_only": """\
-- pgbench select-only transaction (-b select-only)
\\set aid random(1, 100000 * :scale)

BEGIN;
SELECT abalance FROM pgbench_accounts WHERE aid = :aid;
END;
""",
}


@dataclass
class PgBenchData(BenchmarkArtifact):
    """Generate pgbench TPC-B-like data artifacts.

    Args:
        scale_factor: Passed as pgbench's -s (scale) flag.
                      accounts = 100_000 * sf, tellers = 10 * sf, branches = sf.
        mode: "synth" generates CSV files locally (default).
              "plan" writes a shell script using pgbench -i.
    """

    scale_factor: int | float = 1
    mode: Literal["synth", "plan"] = "synth"

    benchmark: str = "pgbench"
    artifact_type: str = "data"

    def generate(self, output_dir: str | Path | None = None, force: bool = False) -> GenerationResult:
        root = self._require_output_dir(output_dir)
        out_dir = root / "pgbench" / "data"
        out_dir.mkdir(parents=True, exist_ok=True)

        if not force:
            existing = self._load_existing(out_dir / "pgbench_data_manifest.json", root)
            if existing is not None:
                print(f"[driftbench] pgbench data (sf={self._sf()}) already exists at {out_dir}. Reusing.")
                return existing
        print(f"[driftbench] Generating pgbench data (sf={self._sf()}, mode={self.mode}) → {out_dir}")

        ddl = self._write_text(out_dir / "pgbench_schema.sql", _PGBENCH_DDL)

        if self.mode == "plan":
            script = self._write_text(out_dir / "init_pgbench.sh", self._plan_script())
            script.chmod(0o755)
            metadata = self._write_json(
                out_dir / "pgbench_data_manifest.json",
                {
                    "benchmark": self.benchmark,
                    "artifact_type": self.artifact_type,
                    "scale_factor": self._sf(),
                    "mode": self.mode,
                    "files": self._paths_relative_to(root, [ddl, script]),
                    "note": (
                        f"Run init_pgbench.sh on a host with pgbench installed. "
                        f"Equivalent to: pgbench -i -s {self._sf()} <dbname>"
                    ),
                },
            )
            return self._result(root, [ddl, script], metadata)

        files = self._generate_synth(out_dir)
        files.insert(0, ddl)
        sf = self._sf()
        metadata = self._write_json(
            out_dir / "pgbench_data_manifest.json",
            {
                "benchmark": self.benchmark,
                "artifact_type": self.artifact_type,
                "scale_factor": sf,
                "mode": self.mode,
                "tables": {
                    "pgbench_branches": sf,
                    "pgbench_tellers": 10 * sf,
                    "pgbench_accounts": 100_000 * sf,
                    "pgbench_history": 0,
                },
                "files": self._paths_relative_to(root, files),
                "note": "Synthetic TPC-B-like data. pgbench_history starts empty and grows during execution.",
            },
        )
        return self._result(root, files, metadata)

    def _sf(self) -> int:
        return max(1, int(round(float(self.scale_factor))))

    def _generate_synth(self, out_dir: Path) -> list[Path]:
        sf = self._sf()
        rng = random.Random(42)
        files: list[Path] = []

        files.append(self._write_csv(out_dir / "pgbench_branches.csv",
            ["bid", "bbalance", "filler"],
            [[i, 0, ""] for i in range(1, sf + 1)]))

        files.append(self._write_csv(out_dir / "pgbench_tellers.csv",
            ["tid", "bid", "tbalance", "filler"],
            [[i, ((i - 1) // 10) + 1, 0, ""] for i in range(1, 10 * sf + 1)]))

        account_count = 100_000 * sf
        accounts: list[list] = []
        for aid in range(1, account_count + 1):
            bid = ((aid - 1) // 100_000) + 1
            accounts.append([aid, bid, rng.randint(-1000, 1000), ""])
        files.append(self._write_csv(out_dir / "pgbench_accounts.csv",
            ["aid", "bid", "abalance", "filler"],
            accounts))

        # history starts empty
        files.append(self._write_csv(out_dir / "pgbench_history.csv",
            ["tid", "bid", "aid", "delta", "mtime", "filler"],
            []))

        return files

    def _write_csv(self, path: Path, header: list[str], rows: list[list]) -> Path:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(header)
            writer.writerows(rows)
        return path

    def _plan_script(self) -> str:
        return (
            "#!/usr/bin/env bash\n"
            "set -euo pipefail\n\n"
            f"SCALE_FACTOR={self._sf()}\n"
            'DB_NAME="${1:-benchbase}"\n\n'
            "# Initialize pgbench tables and load data:\n"
            "pgbench -i -s ${SCALE_FACTOR} ${DB_NAME}\n\n"
            'echo "pgbench initialized with scale=${SCALE_FACTOR} on DB=${DB_NAME}"\n'
        )


@dataclass
class PgBenchQueries(BenchmarkArtifact):
    """Generate pgbench transaction script templates.

    Args:
        workload: "tpcb" (default, mixed read-write),
                  "simple_update" (UPDATE + INSERT only),
                  "select_only" (read-only).
        clients: Number of concurrent clients for the run script.
        duration: Benchmark run duration in seconds.
        rate: Target transaction rate per second (0 = unlimited).
    """

    workload: Literal["tpcb", "simple_update", "select_only"] = "tpcb"
    clients: int = 1
    duration: int = 60
    rate: int = 0

    benchmark: str = "pgbench"
    artifact_type: str = "queries"

    _VALID_WORKLOADS = {"tpcb", "simple_update", "select_only"}

    def generate(self, output_dir: str | Path | None = None, force: bool = False) -> GenerationResult:
        if self.workload not in self._VALID_WORKLOADS:
            raise ValueError(f"workload must be one of: {sorted(self._VALID_WORKLOADS)}")

        root = self._require_output_dir(output_dir)
        out_dir = root / "pgbench" / "queries"
        out_dir.mkdir(parents=True, exist_ok=True)

        if not force:
            existing = self._load_existing(out_dir / "pgbench_queries_manifest.json", root)
            if existing is not None:
                print(f"[driftbench] pgbench queries ({self.workload}) already exist at {out_dir}. Reusing.")
                return existing
        print(f"[driftbench] Generating pgbench queries (workload={self.workload}) → {out_dir}")

        sql_file = self._write_text(
            out_dir / f"pgbench_{self.workload}.sql",
            _PGBENCH_TRANSACTIONS[self.workload],
        )
        run_script = self._write_text(out_dir / "run_pgbench.sh", self._run_script())
        run_script.chmod(0o755)

        metadata = self._write_json(
            out_dir / "pgbench_queries_manifest.json",
            {
                "benchmark": self.benchmark,
                "artifact_type": self.artifact_type,
                "workload": self.workload,
                "clients": self.clients,
                "duration": self.duration,
                "rate": self.rate if self.rate > 0 else "unlimited",
                "files": self._paths_relative_to(root, [sql_file, run_script]),
                "note": (
                    f"Use run_pgbench.sh to execute, or: "
                    f"pgbench -c {self.clients} -T {self.duration} "
                    f"-f {sql_file.name} <dbname>"
                ),
            },
        )
        return self._result(root, [sql_file, run_script], metadata)

    def _run_script(self) -> str:
        rate_flag = f" -R {self.rate}" if self.rate > 0 else ""
        return (
            "#!/usr/bin/env bash\n"
            "set -euo pipefail\n\n"
            f'SCRIPT_DIR="$(cd "$(dirname "${{BASH_SOURCE[0]}}")" && pwd)"\n'
            'DB_NAME="${1:-benchbase}"\n\n'
            f"pgbench"
            f" -c {self.clients}"
            f" -T {self.duration}"
            f"{rate_flag}"
            f' -f "${{SCRIPT_DIR}}/pgbench_{self.workload}.sql"'
            f" ${{DB_NAME}}\n"
        )


def data(
    scale_factor: int | float = 1,
    mode: Literal["synth", "plan"] = "synth",
) -> PgBenchData:
    """Return a PgBenchData adapter.

    Args:
        scale_factor: Maps to pgbench -s. accounts = 100_000 * sf.
        mode: "synth" generates CSV files locally. "plan" writes a pgbench -i script.
    """
    return PgBenchData(scale_factor=scale_factor, mode=mode)


def queries(
    workload: Literal["tpcb", "simple_update", "select_only"] = "tpcb",
    clients: int = 1,
    duration: int = 60,
    rate: int = 0,
) -> PgBenchQueries:
    """Return a PgBenchQueries adapter.

    Args:
        workload: "tpcb" (default), "simple_update", or "select_only".
        clients: Concurrent pgbench clients (-c flag).
        duration: Run duration in seconds (-T flag).
        rate: Target rate in TPS; 0 = unlimited (-R flag).
    """
    return PgBenchQueries(workload=workload, clients=clients, duration=duration, rate=rate)
