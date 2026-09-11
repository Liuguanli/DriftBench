"""Export native sysbench 1.0 preparation and workload plans.

These adapters write configuration, a structured command plan, and provenance.
They do not create database rows, install sysbench, or execute a workload.
``data()`` describes the native ``prepare`` phase; ``queries()`` describes ``run``.
The user supplies a native sysbench installation and database connection locally.
"""
from __future__ import annotations

import stat
from dataclasses import dataclass
from pathlib import Path
from typing import Any, ClassVar

from .base import BenchmarkArtifact, GenerationResult, get_default_data_dir


_UPSTREAM = "https://github.com/akopytov/sysbench"
_NATIVE_REVISION = "3ceba0b1e115f8c50d1d045a4574d8ed643bd497"
_WORKLOADS = (
    "oltp_read_only", "oltp_read_write", "oltp_write_only", "oltp_point_select",
)
_DISTRIBUTIONS = ("uniform", "gaussian", "pareto", "zipfian")
_INT_MAX = 2**31 - 1


def _integer(name: str, value: Any, minimum: int = 1, maximum: int = _INT_MAX) -> int:
    if type(value) is not int or not minimum <= value <= maximum:
        raise ValueError(f"sysbench {name} must be an integer in {minimum}..{maximum}")
    return value


def _choice(name: str, value: Any, choices: tuple[str, ...]) -> str:
    if not isinstance(value, str) or value not in choices:
        raise ValueError(f"sysbench {name} must be one of: {', '.join(choices)}")
    return value


def _common_parameters(tables: int, table_size: int, db_driver: str, seed: int) -> dict[str, Any]:
    return {
        "tables": _integer("tables", tables),
        "table_size": _integer("table_size", table_size),
        "db_driver": _choice("db_driver", db_driver, ("mysql", "pgsql")),
        # Native rand-seed is a signed INT; zero selects the current time.
        "seed": _integer("seed", seed),
    }


def _check_output_path(path: Path, *, directory: bool) -> None:
    """Reject managed links and file/directory collisions before any writes."""
    try:
        info = path.lstat()
    except FileNotFoundError:
        return
    if stat.S_ISLNK(info.st_mode) or (
        getattr(info, "st_file_attributes", 0)
        & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0)
    ):
        raise ValueError(f"sysbench output path must not be a link or reparse point: {path}")
    if directory:
        if not stat.S_ISDIR(info.st_mode):
            raise ValueError(f"sysbench output directory conflicts with an existing file: {path}")
    elif not stat.S_ISREG(info.st_mode) or info.st_nlink > 1:
        raise ValueError(f"sysbench output file must be a regular, unshared file: {path}")


class _SysbenchPlan(BenchmarkArtifact):
    benchmark: ClassVar[str] = "sysbench"

    def _generate_plan(
        self,
        output_dir: str | Path | None,
        force: bool,
        parameters: dict[str, Any],
        phase: str,
        workload: str,
        options: dict[str, str | int],
    ) -> GenerationResult:
        if type(force) is not bool:
            raise ValueError("sysbench force must be a boolean")
        root = (
            get_default_data_dir() if output_dir is None else Path(output_dir)
        ).expanduser().resolve()
        out_dir = root / self.benchmark / self.artifact_type
        files = [out_dir / name for name in ("sysbench.cnf", "command.json", "README.md")]
        manifest = out_dir / f"sysbench_{self.artifact_type}_manifest.json"

        # Check every managed target, including the manifest, before creating
        # directories or consulting the cache. Unknown files are left alone.
        for directory in (root, root / self.benchmark, out_dir):
            _check_output_path(directory, directory=True)
        for path in [*files, manifest]:
            _check_output_path(path, directory=False)

        cache_parameters = {
            **parameters,
            "phase": phase,
            "workload": workload,
            "adapter_revision": 1,
            "native_contract_revision": _NATIVE_REVISION,
        }
        if not force:
            existing = self._load_existing(manifest, root, cache_parameters)
            if existing is not None:
                return existing

        out_dir.mkdir(parents=True, exist_ok=True)
        self._write_text(files[0], "".join(f"{key}={value}\n" for key, value in options.items()))
        self._write_json(
            files[1],
            {
                "schema": "driftbench.sysbench-command",
                "version": 1,
                "phase": phase,
                "cwd": ".",
                "cwd_relative_to": "command.json directory",
                "argv": ["sysbench", "--config-file=sysbench.cnf", workload, phase],
                "requires_local_connection_options": True,
                "executed": False,
            },
        )
        self._write_text(files[2], self._instructions(phase, workload))
        self._write_manifest(
            manifest,
            {
                "benchmark": self.benchmark,
                "artifact_type": self.artifact_type,
                "artifact_kind": f"{phase}_plan",
                "parameters": parameters,
                "phase": phase,
                "workload": workload,
                "database_executed": False,
                "data_rows_generated": 0,
                "provenance": {
                    "upstream_repository": _UPSTREAM,
                    "native_contract": "sysbench 1.0",
                    "native_contract_revision": _NATIVE_REVISION,
                    "reference_files": [
                        "README.md", "src/sysbench.c", "src/sb_rand.c",
                        "src/lua/oltp_common.lua", f"src/lua/{workload}.lua",
                    ],
                    "mode": "configuration_only",
                    "native_installation_verified": False,
                    "native_execution_reproducibility": "not guaranteed",
                },
                "files": self._paths_relative_to(root, files),
            },
            cache_parameters,
            root=root,
        )
        return self._result(root, files, manifest)

    @staticmethod
    def _instructions(phase: str, workload: str) -> str:
        action = (
            "This is a preparation plan, not a dataset. Native prepare creates and populates sbtest tables."
            if phase == "prepare" else
            "This is a workload plan, not a SQL trace or measurement. Native run sends queries to a database."
        )
        return (
            f"# sysbench {phase} plan\n\n{action}\n\n"
            "DriftBench has only written files. It has not installed sysbench, connected to a database, "
            "or executed this command. No host, username, password, or database name is included.\n\n"
            "1. Install sysbench 1.0 with the selected MySQL or PostgreSQL driver and its stock Lua scripts.\n"
            "2. Supply your database connection options locally. The native defaults are not an explicit "
            "target; select your test database before executing anything.\n"
            "3. Read command.json as an argv list, with its directory as the working directory. "
            "Add driver connection options before the workload name, and invoke the native tool only "
            "when you intend to use that database. The plan is not a shell script.\n"
            "4. For run, first prepare the same tables and table-size using data() and the same driver. "
            "prepare creates data, and write workloads change data. No cleanup command is generated.\n\n"
            f"Workload: {workload}. Phase: {phase}.\n"
            "The run configuration uses time as the duration in seconds and events=0 (no event-count "
            "limit). rate=0 means no rate cap. Duration is a configured run window, not a timeout for "
            "a blocked database request.\n\n"
            "The positive rand-seed makes the configuration explicit. Native timing, thread scheduling, "
            "database state, platform, and installed sysbench version can still change execution results. "
            "These files do not certify benchmark conformance or promise a reproducible native query trace.\n\n"
            f"Native option and stock-script contract: {_UPSTREAM}/tree/{_NATIVE_REVISION}\n"
        )


@dataclass
class SysbenchData(_SysbenchPlan):
    """Export a native prepare plan; no dataset is generated by DriftBench.

    Counts and seed must be integers in 1..2147483647. ``db_driver`` is
    ``mysql`` or ``pgsql``. Native prepare uses one thread and oltp_read_write.
    """

    tables: int = 1
    table_size: int = 10000
    db_driver: str = "mysql"
    seed: int = 42
    artifact_type: ClassVar[str] = "data"

    def generate(self, output_dir: str | Path | None = None, force: bool = False) -> GenerationResult:
        parameters = _common_parameters(self.tables, self.table_size, self.db_driver, self.seed)
        return self._generate_plan(
            output_dir, force, parameters, "prepare", "oltp_read_write",
            {
                "db-driver": parameters["db_driver"],
                "tables": parameters["tables"],
                "table-size": parameters["table_size"],
                "threads": 1,
                "rand-seed": parameters["seed"],
            },
        )


@dataclass
class SysbenchQueries(_SysbenchPlan):
    """Export a native run plan for four stock OLTP workloads.

    Supported workloads: oltp_read_only, oltp_read_write, oltp_write_only,
    oltp_point_select. Distributions: uniform, gaussian, pareto, zipfian.
    Counts and positive seed are integers up to 2147483647. Threads are 1..1024;
    duration is 1..86400 seconds; rate is 0..2147483647 events/second (0 uncapped).
    The generated plan requires matching prepared tables and local connection
    options. It does not contain generated SQL, execute queries, or measure time.
    """

    workload: str = "oltp_read_write"
    tables: int = 1
    table_size: int = 10000
    threads: int = 4
    duration: int = 60
    rate: int = 0
    db_driver: str = "mysql"
    distribution: str = "uniform"
    seed: int = 42
    artifact_type: ClassVar[str] = "queries"

    def generate(self, output_dir: str | Path | None = None, force: bool = False) -> GenerationResult:
        parameters = _common_parameters(self.tables, self.table_size, self.db_driver, self.seed)
        parameters.update({
            "workload": _choice("workload", self.workload, _WORKLOADS),
            "threads": _integer("threads", self.threads, maximum=1024),
            "duration": _integer("duration", self.duration, maximum=86400),
            "rate": _integer("rate", self.rate, minimum=0),
            "distribution": _choice("distribution", self.distribution, _DISTRIBUTIONS),
        })
        return self._generate_plan(
            output_dir, force, parameters, "run", parameters["workload"],
            {
                "db-driver": parameters["db_driver"],
                "tables": parameters["tables"],
                "table-size": parameters["table_size"],
                "threads": parameters["threads"],
                "time": parameters["duration"],
                "rate": parameters["rate"],
                "events": 0,
                "rand-type": parameters["distribution"],
                "rand-seed": parameters["seed"],
            },
        )


def data(
    tables: int = 1,
    table_size: int = 10000,
    db_driver: str = "mysql",
    seed: int = 42,
) -> SysbenchData:
    """Return a prepare-plan adapter; call generate() to export local files."""
    return SysbenchData(tables=tables, table_size=table_size, db_driver=db_driver, seed=seed)


def queries(
    workload: str = "oltp_read_write",
    tables: int = 1,
    table_size: int = 10000,
    threads: int = 4,
    duration: int = 60,
    rate: int = 0,
    db_driver: str = "mysql",
    distribution: str = "uniform",
    seed: int = 42,
) -> SysbenchQueries:
    """Return a run-plan adapter; call generate() to export local files."""
    return SysbenchQueries(
        workload=workload, tables=tables, table_size=table_size, threads=threads,
        duration=duration, rate=rate, db_driver=db_driver,
        distribution=distribution, seed=seed,
    )
