"""BenchBase benchmark adapter.

BenchBase (https://github.com/cmu-db/benchbase) is a Java multi-benchmark
framework supporting TPC-C, TPC-H, YCSB, SEATS, AuctionMark, Smallbank,
Epinions, Wikipedia, Twitter, and Voter.

This adapter generates BenchBase XML configuration files and shell scripts
for loading and executing each benchmark.

Quick start:
    from driftbench.data.benchbase import data as bb_data, queries as bb_queries

    bb_data(benchmark="tpcc", scale_factor=1).generate(output_dir="./artifacts")
    bb_queries(benchmark="tpcc", terminals=4, duration=60).generate(output_dir="./artifacts")

Supported benchmarks:
    tpcc, tpch, ycsb, seats, auctionmark, smallbank, epinions, wikipedia, twitter, voter
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from .base import BenchmarkArtifact, GenerationResult


# ---------------------------------------------------------------------------
# Per-benchmark metadata: transaction names and default weights
# ---------------------------------------------------------------------------

_BENCHBASE_BENCHMARKS: dict[str, dict] = {
    "tpcc": {
        "transactions": [
            ("NewOrder", 45),
            ("Payment", 43),
            ("OrderStatus", 4),
            ("Delivery", 4),
            ("StockLevel", 4),
        ],
        "app_name": "tpcc",
        "description": "TPC-C OLTP benchmark (order-entry)",
    },
    "tpch": {
        "transactions": [(f"Q{i}", 1) for i in range(1, 23)],
        "app_name": "tpch",
        "description": "TPC-H OLAP benchmark (22 analytical queries)",
    },
    "ycsb": {
        "transactions": [
            ("ReadRecord", 50),
            ("InsertRecord", 0),
            ("ScanRecord", 0),
            ("UpdateRecord", 50),
            ("DeleteRecord", 0),
            ("ReadModifyWriteRecord", 0),
        ],
        "app_name": "ycsb",
        "description": "YCSB key-value workload A (50% read, 50% update)",
    },
    "seats": {
        "transactions": [
            ("FindFlights", 10),
            ("FindOpenSeats", 35),
            ("ReserveSeat", 13),
            ("DeleteReservation", 5),
            ("UpdateReservation", 10),
            ("UpdateCustomer", 10),
            ("UpdateFlightReservation", 10),
            ("CheckIn", 7),
        ],
        "app_name": "seats",
        "description": "SEATS airline ticket reservation benchmark",
    },
    "auctionmark": {
        "transactions": [
            ("GetItem", 45),
            ("GetUserInfo", 10),
            ("GetItemBids", 12),
            ("GetItemComments", 2),
            ("GetItemImageInformation", 6),
            ("GetItemsFromCategory", 3),
            ("PostAuction", 1),
            ("PostComment", 2),
            ("PostCommentResponse", 2),
            ("UpdateItem", 1),
            ("NewBid", 12),
            ("NewComment", 2),
            ("NewCommentResponse", 2),
        ],
        "app_name": "auctionmark",
        "description": "AuctionMark online auction benchmark",
    },
    "smallbank": {
        "transactions": [
            ("Amalgamate", 15),
            ("Balance", 25),
            ("DepositChecking", 25),
            ("SendPayment", 25),
            ("TransactSavings", 5),
            ("WriteCheck", 5),
        ],
        "app_name": "smallbank",
        "description": "Smallbank simple banking benchmark",
    },
    "epinions": {
        "transactions": [
            ("GetReviewsByUser", 10),
            ("GetReviewItem", 8),
            ("GetAvgRating", 7),
            ("GetItemReviews", 7),
            ("GetReviewsByTrustedUser", 5),
            ("GetUsersInTrustNetwork", 5),
            ("UpdateUserName", 5),
            ("UpdateItemName", 5),
            ("UpdateTrustRelationship", 3),
            ("UpdateReview", 5),
            ("CreateUser", 5),
            ("CreateItem", 5),
            ("CreateReview", 5),
            ("AddTrustRelationship", 5),
            ("AddDistrust", 5),
        ],
        "app_name": "epinions",
        "description": "Epinions social product-review benchmark",
    },
    "wikipedia": {
        "transactions": [
            ("AddWatchList", 1),
            ("RemoveWatchList", 1),
            ("UpdatePage", 7),
            ("GetPageAnonymous", 19),
            ("GetPageAuthenticated", 19),
        ],
        "app_name": "wikipedia",
        "description": "Wikipedia web-editing benchmark",
    },
    "twitter": {
        "transactions": [
            ("GetTweet", 0),
            ("GetTweetFollowers", 7),
            ("GetFollowers", 7),
            ("GetUserTweets", 0),
            ("InsertTweet", 2),
        ],
        "app_name": "twitter",
        "description": "Twitter social-media benchmark",
    },
    "voter": {
        "transactions": [
            ("Vote", 100),
        ],
        "app_name": "voter",
        "description": "Voter simple phone-voting benchmark",
    },
}

_VALID_BENCHMARKS = set(_BENCHBASE_BENCHMARKS.keys())


def _xml_config(
    benchmark: str,
    scale_factor: int | float,
    terminals: int,
    duration: int,
    rate: int,
    db_url: str,
    username: str,
    password: str,
    create: bool,
    load: bool,
    execute: bool,
) -> str:
    meta = _BENCHBASE_BENCHMARKS[benchmark]
    txn_names = [t[0] for t in meta["transactions"]]
    weights = ",".join(str(t[1]) for t in meta["transactions"])

    txn_types_xml = "\n".join(
        f"        <transactiontype><name>{name}</name></transactiontype>"
        for name in txn_names
    )

    rate_str = str(rate) if rate > 0 else "unlimited"

    return (
        '<?xml version="1.0"?>\n'
        "<parameters>\n"
        "    <type>POSTGRES</type>\n"
        "    <driver>org.postgresql.Driver</driver>\n"
        f"    <url>{db_url}</url>\n"
        f"    <username>{username}</username>\n"
        f"    <password>{password}</password>\n"
        "    <isolation>TRANSACTION_SERIALIZABLE</isolation>\n"
        f"    <scalefactor>{scale_factor}</scalefactor>\n"
        f"    <terminals>{terminals}</terminals>\n"
        f"    <!-- create={str(create).lower()} load={str(load).lower()} execute={str(execute).lower()} -->\n"
        "    <works>\n"
        "        <work>\n"
        f"            <time>{duration}</time>\n"
        f"            <rate>{rate_str}</rate>\n"
        f"            <weights>{weights}</weights>\n"
        "        </work>\n"
        "    </works>\n"
        "    <transactiontypes>\n"
        f"{txn_types_xml}\n"
        "    </transactiontypes>\n"
        "</parameters>\n"
    )


def _run_script(benchmark: str, config_name: str, create: bool, load: bool, execute: bool) -> str:
    flags = (
        f"--create={'true' if create else 'false'} "
        f"--load={'true' if load else 'false'} "
        f"--execute={'true' if execute else 'false'}"
    )
    return (
        "#!/usr/bin/env bash\n"
        "set -euo pipefail\n\n"
        'SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}}")" && pwd)"\n'
        'BENCHBASE_JAR="${BENCHBASE_JAR:-benchbase.jar}"\n\n'
        f"java -jar ${{BENCHBASE_JAR}} \\\n"
        f"    -b {benchmark} \\\n"
        f'    -c "${{SCRIPT_DIR}}/{config_name}" \\\n'
        f"    {flags} \\\n"
        '    --json-histograms results.json\n\n'
        f'echo "BenchBase {benchmark} finished."\n'
    )


@dataclass
class BenchBaseData(BenchmarkArtifact):
    """Generate a BenchBase XML config + load script for a given benchmark.

    The load phase initialises the schema and populates tables.

    Args:
        benchmark: One of the supported BenchBase benchmark names.
        scale_factor: Passed as BenchBase's scalefactor parameter.
        terminals: Number of worker terminals during load (default 1).
        db_url: JDBC URL (default: localhost/benchbase).
        username: Database username (default "admin").
        password: Database password (default "password").
    """

    benchmark_name: str = "tpcc"
    scale_factor: int | float = 1
    terminals: int = 1
    db_url: str = ""
    username: str = "admin"
    password: str = "password"

    benchmark: str = "benchbase"
    artifact_type: str = "data"

    def generate(self, output_dir: str | Path | None = None, force: bool = False) -> GenerationResult:
        self._validate()
        root = self._require_output_dir(output_dir)
        out_dir = root / "benchbase" / self.benchmark_name / "data"
        out_dir.mkdir(parents=True, exist_ok=True)

        if not force:
            existing = self._load_existing(out_dir / "benchbase_data_manifest.json", root)
            if existing is not None:
                print(f"[driftbench] BenchBase {self.benchmark_name} config already exists at {out_dir}. Reusing.")
                return existing
        print(f"[driftbench] Generating BenchBase {self.benchmark_name} load config → {out_dir}")

        url = self.db_url or f"jdbc:postgresql://localhost:5432/benchbase?sslmode=disable&ApplicationName={self.benchmark_name}"
        config_name = f"{self.benchmark_name}_load_config.xml"
        xml = self._write_text(
            out_dir / config_name,
            _xml_config(
                benchmark=self.benchmark_name,
                scale_factor=self.scale_factor,
                terminals=self.terminals,
                duration=0,
                rate=0,
                db_url=url,
                username=self.username,
                password=self.password,
                create=True,
                load=True,
                execute=False,
            ),
        )
        script = self._write_text(
            out_dir / "load.sh",
            _run_script(self.benchmark_name, config_name, create=True, load=True, execute=False),
        )
        script.chmod(0o755)

        meta = _BENCHBASE_BENCHMARKS[self.benchmark_name]
        metadata = self._write_json(
            out_dir / "benchbase_data_manifest.json",
            {
                "benchmark": self.benchmark,
                "benchmark_name": self.benchmark_name,
                "artifact_type": self.artifact_type,
                "scale_factor": self.scale_factor,
                "description": meta["description"],
                "files": self._paths_relative_to(root, [xml, script]),
                "note": (
                    "Set BENCHBASE_JAR=/path/to/benchbase.jar before running load.sh. "
                    "Download BenchBase from https://github.com/cmu-db/benchbase/releases."
                ),
            },
        )
        return self._result(root, [xml, script], metadata)

    def _validate(self) -> None:
        if self.benchmark_name not in _VALID_BENCHMARKS:
            raise ValueError(
                f"benchmark '{self.benchmark_name}' is not supported. "
                f"Choose one of: {sorted(_VALID_BENCHMARKS)}"
            )


@dataclass
class BenchBaseQueries(BenchmarkArtifact):
    """Generate a BenchBase XML config + execute script for a given benchmark.

    The execute phase runs the workload against an already-loaded database.

    Args:
        benchmark_name: One of the supported BenchBase benchmark names.
        scale_factor: Scalefactor in the config (should match load config).
        terminals: Number of concurrent worker terminals.
        duration: Workload run time in seconds.
        rate: Target rate in TPS; 0 = unlimited.
        db_url: JDBC URL (default: localhost/benchbase).
        username: Database username (default "admin").
        password: Database password (default "password").
    """

    benchmark_name: str = "tpcc"
    scale_factor: int | float = 1
    terminals: int = 4
    duration: int = 60
    rate: int = 0
    db_url: str = ""
    username: str = "admin"
    password: str = "password"

    benchmark: str = "benchbase"
    artifact_type: str = "queries"

    def generate(self, output_dir: str | Path | None = None, force: bool = False) -> GenerationResult:
        self._validate()
        root = self._require_output_dir(output_dir)
        out_dir = root / "benchbase" / self.benchmark_name / "queries"
        out_dir.mkdir(parents=True, exist_ok=True)

        if not force:
            existing = self._load_existing(out_dir / "benchbase_queries_manifest.json", root)
            if existing is not None:
                print(f"[driftbench] BenchBase {self.benchmark_name} execute config already exists at {out_dir}. Reusing.")
                return existing
        print(f"[driftbench] Generating BenchBase {self.benchmark_name} execute config → {out_dir}")

        url = self.db_url or f"jdbc:postgresql://localhost:5432/benchbase?sslmode=disable&ApplicationName={self.benchmark_name}"
        config_name = f"{self.benchmark_name}_execute_config.xml"
        xml = self._write_text(
            out_dir / config_name,
            _xml_config(
                benchmark=self.benchmark_name,
                scale_factor=self.scale_factor,
                terminals=self.terminals,
                duration=self.duration,
                rate=self.rate,
                db_url=url,
                username=self.username,
                password=self.password,
                create=False,
                load=False,
                execute=True,
            ),
        )
        script = self._write_text(
            out_dir / "execute.sh",
            _run_script(self.benchmark_name, config_name, create=False, load=False, execute=True),
        )
        script.chmod(0o755)

        meta = _BENCHBASE_BENCHMARKS[self.benchmark_name]
        metadata = self._write_json(
            out_dir / "benchbase_queries_manifest.json",
            {
                "benchmark": self.benchmark,
                "benchmark_name": self.benchmark_name,
                "artifact_type": self.artifact_type,
                "scale_factor": self.scale_factor,
                "terminals": self.terminals,
                "duration_seconds": self.duration,
                "rate": self.rate if self.rate > 0 else "unlimited",
                "transaction_types": [t[0] for t in meta["transactions"]],
                "transaction_weights": {t[0]: t[1] for t in meta["transactions"]},
                "description": meta["description"],
                "files": self._paths_relative_to(root, [xml, script]),
                "note": (
                    "Set BENCHBASE_JAR=/path/to/benchbase.jar before running execute.sh. "
                    "Database must already be loaded via the BenchBase load config."
                ),
            },
        )
        return self._result(root, [xml, script], metadata)

    def _validate(self) -> None:
        if self.benchmark_name not in _VALID_BENCHMARKS:
            raise ValueError(
                f"benchmark '{self.benchmark_name}' is not supported. "
                f"Choose one of: {sorted(_VALID_BENCHMARKS)}"
            )


def data(
    benchmark: str = "tpcc",
    scale_factor: int | float = 1,
    terminals: int = 1,
    db_url: str = "",
    username: str = "admin",
    password: str = "password",
) -> BenchBaseData:
    """Return a BenchBaseData adapter (generates load config + script).

    Args:
        benchmark: BenchBase benchmark name (e.g. "tpcc", "ycsb", "seats").
        scale_factor: Scalefactor passed to BenchBase.
        terminals: Worker terminals during load phase.
        db_url: JDBC connection URL. Defaults to localhost/benchbase.
        username: DB username (default "admin").
        password: DB password (default "password").
    """
    return BenchBaseData(
        benchmark_name=benchmark,
        scale_factor=scale_factor,
        terminals=terminals,
        db_url=db_url,
        username=username,
        password=password,
    )


def queries(
    benchmark: str = "tpcc",
    scale_factor: int | float = 1,
    terminals: int = 4,
    duration: int = 60,
    rate: int = 0,
    db_url: str = "",
    username: str = "admin",
    password: str = "password",
) -> BenchBaseQueries:
    """Return a BenchBaseQueries adapter (generates execute config + script).

    Args:
        benchmark: BenchBase benchmark name.
        scale_factor: Should match the load config's scalefactor.
        terminals: Concurrent worker terminals (-T equivalent).
        duration: Run time in seconds.
        rate: Target TPS; 0 = unlimited.
        db_url: JDBC connection URL. Defaults to localhost/benchbase.
        username: DB username (default "admin").
        password: DB password (default "password").
    """
    return BenchBaseQueries(
        benchmark_name=benchmark,
        scale_factor=scale_factor,
        terminals=terminals,
        duration=duration,
        rate=rate,
        db_url=db_url,
        username=username,
        password=password,
    )
