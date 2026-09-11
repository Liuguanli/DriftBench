"""Run a bounded artifact-to-database teaching demo, with an explicit engine.

This source-checkout helper uses a hand-authored miniature TPC-H-shaped fixture.
It is not an official TPC-H dataset, performance test, or benchmark runner API.
PostgreSQL runs only in a new task-owned Docker container; SQLite runs only in a
new output-local database. No existing database or arbitrary DSN is accepted.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
import csv
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import shutil
import sqlite3
import subprocess
import sys
import time
import uuid


REPO_ROOT = Path(__file__).resolve().parents[1]
GUIDE_DIR = REPO_ROOT / "docs" / "examples" / "run-guide"
IMAGE = "postgres:16"
LABEL = "org.driftbench.learning-demo"
SCALE = "0.001"  # adapter output-directory tag, not a fixture scale claim
_DOCKER_CONTEXT: str | None = None


class DemoError(RuntimeError):
    pass


class CommandFailure(DemoError):
    def __init__(self, message: str, record: dict):
        super().__init__(message)
        self.record = record


def _write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("x", encoding="utf-8") as stream:
        json.dump(value, stream, indent=2, default=str, allow_nan=False)
        stream.write("\n")


def _write_yaml(path: Path, value: dict) -> None:
    import yaml

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("x", encoding="utf-8") as stream:
        yaml.safe_dump(value, stream, sort_keys=False)


def _command(args: list[str], *, timeout: int = 60, env: dict | None = None):
    try:
        return subprocess.run(
            args, cwd=REPO_ROOT, env=env, capture_output=True,
            text=True, encoding="utf-8", errors="replace", timeout=timeout,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        def captured(value):
            return value.decode("utf-8", errors="replace") if isinstance(value, bytes) else value or ""

        raise CommandFailure(f"Command failed or timed out: {args[0]}", {
            "argv": args, "returncode": None,
            "error_kind": "timeout" if isinstance(exc, subprocess.TimeoutExpired) else "os_error",
            "timeout_seconds": timeout,
            "stdout": captured(getattr(exc, "stdout", None)),
            "stderr": captured(getattr(exc, "stderr", None)),
            "error": str(exc),
        }) from exc


def _recorded_command(args: list[str], commands: list[dict]):
    try:
        result = _command(args)
    except CommandFailure as exc:
        commands.append(exc.record)
        raise
    commands.append({
        "argv": args, "returncode": result.returncode,
        "stdout": result.stdout, "stderr": result.stderr,
    })
    return result


def _cli(args: list[str], commands: list[dict]) -> None:
    full = [sys.executable, "-m", "driftbench.cli", *args]
    result = _recorded_command(full, commands)
    if result.returncode:
        raise DemoError(f"CLI step failed: {' '.join(args)}\n{result.stderr or result.stdout}")


def _make_fixture(root: Path) -> Path:
    source = root / "source"
    source.mkdir()
    rows = {
        "region": [[0, "AFRICA", "Teaching fixture"]],
        "nation": [[0, "ALGERIA", 0, "Teaching fixture"]],
        "supplier": [[1, "Supplier#1", "Address", 0, "00-000-000-0000", 0, "Teaching fixture"]],
        "customer": [[1, "Customer#1", "Address", 0, "00-000-000-0000", 0, "BUILDING", "Teaching fixture"]],
        "part": [[1, "Example part", "Manufacturer#1", "Brand#11", "STANDARD TIN", 1, "SM BOX", 100, "Teaching fixture"]],
        "partsupp": [[1, 1, 100, 10, "Teaching fixture"]],
        "orders": [[1, 1, "F", 7800, "1994-01-01", "1-URGENT", "Clerk#1", 0, "Teaching fixture"]],
        "lineitem": [[
            1, 1, 1, index, index + 4, index * 100, "0.06", "0.02",
            "N", "F", f"1994-{index:02d}-10", f"1994-{index:02d}-11",
            f"1994-{index:02d}-12", "DELIVER IN PERSON", "AIR", "Teaching fixture",
        ] for index in range(1, 13)],
    }
    for name, records in rows.items():
        with (source / f"{name}.tbl").open("x", encoding="utf-8", newline="") as stream:
            writer = csv.writer(stream, delimiter="|", lineterminator="\n")
            writer.writerows([*record, ""] for record in records)
    _write_json(source / "fixture-provenance.json", {
        "kind": "hand-authored-miniature-teaching-fixture",
        "official_tpch_data": False,
        "scale_factor_claim": None,
        "adapter_directory_tag": SCALE,
        "row_counts": {name: len(records) for name, records in rows.items()},
        "purpose": "Exercise artifact preparation, price drift, and Q6-shaped SQL execution.",
    })
    return source


def _prepare(root: Path, commands: list[dict]) -> dict:
    import yaml

    source = _make_fixture(root)
    templates = root / "templates"
    templates.mkdir()
    shutil.copyfile(GUIDE_DIR / "templates" / "6.sql", templates / "6.sql")
    data_request = root / "requests" / "tpch-data-copy.yaml"
    _write_yaml(data_request, {
        "schema": "driftbench.artifact-request/v1", "benchmark": "tpch", "artifact_type": "data",
        "parameters": {"mode": "copy", "scale_factor": SCALE, "source_dir": str(source)},
    })
    _cli(["cache", "materialize", "--request", str(data_request), "--output-dir", str(root / "data"), "--cache-mode", "off", "--json"], commands)

    query_request = root / "requests" / "tpch-queries.yaml"
    _write_yaml(query_request, {
        "schema": "driftbench.artifact-request/v1", "benchmark": "tpch", "artifact_type": "queries",
        "parameters": {
            "mode": "custom", "template_dir": str(templates), "query_ids": [6],
            "queries_per_template": 1, "seed": 42, "shuffle": False,
            "param_specs": {"6": ["1994-01-01", "1995-01-01", 0.06, 24]},
        },
    })
    _cli(["cache", "materialize", "--request", str(query_request), "--output-dir", str(root / "queries"), "--cache-mode", "off", "--json"], commands)
    lineitem = root / "lineitem.csv"
    convert_args = [
        sys.executable, str(GUIDE_DIR / "prepare_lineitem.py"),
        "--input", str(root / "data" / "tpch" / "data" / f"sf_{SCALE}" / "lineitem.tbl"),
        "--output", str(lineitem),
    ]
    converted = _recorded_command(convert_args, commands)
    if converted.returncode:
        raise DemoError(f"lineitem conversion failed: {converted.stderr}")

    drift_spec = yaml.safe_load((GUIDE_DIR / "tpch-data-drift.yaml").read_text(encoding="utf-8"))
    drift_spec["data_source"]["path"] = str(lineitem)
    drift_spec["data_source"]["schema_extractor"]["schema_output_path"] = str(root / "lineitem-schema.json")
    drifted = root / "lineitem-drifted.csv"
    drift_spec["variables"]["drifts"][0]["output_path"] = str(drifted)
    drift_path = root / "specs" / "tpch-data-drift.yaml"
    _write_yaml(drift_path, drift_spec)
    _cli(["validate-spec", str(drift_path), "--deep", "--json"], commands)
    _cli(["run-yaml", str(drift_path)], commands)

    trace = json.loads((GUIDE_DIR / "workload-trace.json").read_text(encoding="utf-8"))
    trace["variables"]["template_dir"] = str(templates)
    temporal = root / "trace-workload.csv"
    trace["variables"]["query_runs"][0]["outputs"][0]["path"] = str(temporal)
    trace_path = root / "trace" / "workload-trace.json"
    _write_json(trace_path, trace)
    trace_spec_path = root / "specs" / "trace-workload.yaml"
    _cli(["trace-to-spec", str(trace_path), "--output", str(trace_spec_path), "--trace-type", "workload"], commands)
    _cli(["validate-spec", str(trace_spec_path), "--deep", "--json"], commands)
    _cli(["run-yaml", str(trace_spec_path)], commands)

    query_csv = root / "queries" / "tpch" / "queries" / "tpch_queries.csv"
    with query_csv.open(encoding="utf-8", newline="") as stream:
        queries = [{"source": "fixed-baseline", "sql": row["sql"]} for row in csv.DictReader(stream)]
    with temporal.open(encoding="utf-8", newline="") as stream:
        queries.extend({"source": "trace-summary", "scheduled_timestamp": row["timestamp"], "sql": row["sql"]} for row in csv.DictReader(stream))
    return {"baseline": lineitem, "drifted": drifted, "queries": queries}


def _docker(args: list[str], *, timeout: int = 20, env: dict | None = None):
    context = ["--context", _DOCKER_CONTEXT] if _DOCKER_CONTEXT else []
    return _command(["docker", *context, *args], timeout=timeout, env=env)


def _select_local_docker_context() -> str:
    # Explicit --context below also prevents an inherited DOCKER_HOST from
    # redirecting the teaching container to an unrelated remote daemon.
    current = _command(["docker", "context", "show"])
    context = current.stdout.strip()
    if current.returncode or not context:
        raise DemoError("Could not select a Docker context")
    endpoint = _command(["docker", "context", "inspect", context, "--format", "{{.Endpoints.docker.Host}}"])
    if endpoint.returncode or not endpoint.stdout.strip().startswith(("npipe://", "unix://")):
        raise DemoError("The teaching demo requires a local Docker context using a named pipe or Unix socket")
    return context


def _remove_owned_container(name: str, token: str, expected_id: str | None) -> None:
    info = _docker(["inspect", "--format", '{{.Id}}|{{index .Config.Labels "' + LABEL + '"}}', name])
    if info.returncode:
        # A stopped --rm container has already been removed. Distinguish that
        # case from an unreachable engine, for which cleanup is unverified.
        inventory = _docker(["ps", "--all", "--filter", f"name=^/{name}$", "--format", "{{.ID}}"])
        if inventory.returncode or inventory.stdout.strip():
            raise DemoError(f"Could not verify cleanup of task container {name}")
        return
    container_id, separator, label_value = info.stdout.strip().partition("|")
    if not separator or label_value != token or not re.fullmatch(r"[a-f0-9]{64}", container_id):
        raise DemoError(f"Refusing cleanup: container ownership mismatch for {name}")
    if expected_id is not None and container_id != expected_id:
        raise DemoError(f"Refusing cleanup: container identity changed for {name}")
    removed = _docker(["rm", "--force", container_id])
    if removed.returncode:
        raise DemoError(f"Cleanup failed for task container {name}")
    remaining = _docker(["ps", "--all", "--filter", f"id={container_id}", "--format", "{{.ID}}"])
    if remaining.returncode or remaining.stdout.strip():
        raise DemoError(f"Could not verify removal of task container {name}")


@contextmanager
def _postgres():
    global _DOCKER_CONTEXT
    import psycopg2

    if not shutil.which("docker"):
        raise DemoError("Docker CLI is unavailable. Start Docker Desktop or choose --engine sqlite.")
    _DOCKER_CONTEXT = _select_local_docker_context()
    server = _docker(["info", "--format", "{{.ServerVersion}}"])
    if server.returncode:
        raise DemoError("Docker engine is unavailable. Start Docker Desktop, then use a new output directory; or choose --engine sqlite.")
    image = _docker(["image", "inspect", IMAGE, "--format", "{{.Id}}"])
    if image.returncode:
        print(f"Pulling {IMAGE} for the isolated teaching container...", flush=True)
        pulled = _docker(["pull", IMAGE], timeout=240)
        if pulled.returncode:
            raise DemoError(f"Could not pull {IMAGE}: {pulled.stderr.strip()}")
        image = _docker(["image", "inspect", IMAGE, "--format", "{{.Id}}"])
        if image.returncode:
            raise DemoError("Pulled PostgreSQL image could not be inspected")
    token = uuid.uuid4().hex
    name = f"driftbench-learning-{token}"
    secret = secrets.token_urlsafe(32)
    environment = dict(os.environ)
    environment["POSTGRES_PASSWORD"] = secret
    container_id = None
    connection = None
    environment_info = {
        "engine": "postgres", "image": IMAGE, "image_id": image.stdout.strip(),
        "docker_server_version": server.stdout.strip(), "docker_context": _DOCKER_CONTEXT,
        "container_name": name,
        "storage": "task-owned tmpfs", "bind_address": "127.0.0.1",
        "container_removed": False,
    }
    try:
        started = _docker([
            "run", "--detach", "--rm", "--name", name, "--label", f"{LABEL}={token}",
            "--memory", "512m", "--cpus", "1", "--pids-limit", "128",
            "--mount", "type=tmpfs,destination=/var/lib/postgresql/data,tmpfs-size=268435456",
            "--publish", "127.0.0.1::5432", "--env", "POSTGRES_PASSWORD",
            "--env", "POSTGRES_USER=driftbench", "--env", "POSTGRES_DB=driftbench_learning", image.stdout.strip(),
        ], timeout=60, env=environment)
        if started.returncode or not re.fullmatch(r"[a-f0-9]{64}", started.stdout.strip()):
            raise DemoError(f"Could not create the teaching container: {started.stderr.strip()}")
        container_id = started.stdout.strip()
        environment_info["container_id"] = container_id
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            port_info = _docker(["port", container_id, "5432/tcp"])
            matched = re.fullmatch(r"127\.0\.0\.1:(\d+)", port_info.stdout.strip())
            if port_info.returncode == 0 and matched:
                try:
                    connection = psycopg2.connect(
                        host="127.0.0.1", port=int(matched.group(1)),
                        dbname="driftbench_learning", user="driftbench", password=secret,
                        connect_timeout=2, options="-c statement_timeout=5000 -c lock_timeout=2000",
                    )
                    environment_info["port"] = int(matched.group(1))
                    break
                except psycopg2.OperationalError:
                    pass
            time.sleep(1)
        if connection is None:
            raise DemoError("Teaching PostgreSQL container did not become ready within 60 seconds")
        with connection.cursor() as cursor:
            cursor.execute("SELECT version()")
            environment_info["database_version"] = cursor.fetchone()[0]
        connection.commit()
        yield connection, environment_info
    finally:
        try:
            if connection is not None:
                connection.close()
        finally:
            # Connection shutdown can itself fail; it must never bypass the
            # separately verified removal of our ephemeral container.
            _remove_owned_container(name, token, container_id)
            environment_info["container_removed"] = True


def _execute(connection, engine: str, artifacts: dict) -> list[dict]:
    integer_columns = {"l_orderkey", "l_partkey", "l_suppkey", "l_linenumber"}
    real_columns = {"l_quantity", "l_extendedprice", "l_discount", "l_tax"}
    phases = []
    columns = None
    for phase in ("baseline", "drifted"):
        with artifacts[phase].open(encoding="utf-8", newline="") as stream:
            reader = csv.DictReader(stream)
            current_columns = reader.fieldnames
            records = list(reader)
        if not current_columns or len(current_columns) != 16 or any(not re.fullmatch(r"l_[a-z]+", column) for column in current_columns):
            raise DemoError("Unexpected teaching lineitem CSV headers")
        if columns is not None and current_columns != columns:
            raise DemoError("Baseline and drifted CSV column order differs")
        columns = current_columns
        cursor = connection.cursor()
        try:
            if phase == "baseline":
                ddl = ", ".join(
                    f'"{column}" ' + ("INTEGER" if column in integer_columns else "REAL" if column in real_columns else "TEXT")
                    for column in columns
                )
                cursor.execute(f"CREATE TABLE lineitem ({ddl})")
            else:
                cursor.execute("DELETE FROM lineitem")
            values = [tuple(
                int(record[column]) if column in integer_columns else float(record[column]) if column in real_columns else record[column]
                for column in columns
            ) for record in records]
            marker = "%s" if engine == "postgres" else "?"
            cursor.executemany("INSERT INTO lineitem VALUES (" + ",".join([marker] * len(columns)) + ")", values)
            connection.commit()
            if engine == "postgres":
                connection.set_session(readonly=True)
            else:
                connection.execute("PRAGMA query_only=ON")
            executions = []
            for index, query in enumerate(artifacts["queries"], 1):
                # Inputs are generated from the helper-owned, fixed SELECT template.
                if engine == "sqlite":
                    deadline = time.monotonic() + 5
                    connection.set_progress_handler(lambda: int(time.monotonic() > deadline), 1000)
                started = time.perf_counter()
                cursor.execute(query["sql"])
                result_rows = cursor.fetchall()
                elapsed_ms = (time.perf_counter() - started) * 1000
                executions.append({
                    "index": index, **query, "elapsed_ms": round(elapsed_ms, 6),
                    "columns": [description[0] for description in cursor.description],
                    "rows": result_rows,
                })
            phases.append({"phase": phase, "loaded_table": "lineitem", "loaded_rows": len(records), "executions": executions})
        finally:
            cursor.close()
            connection.rollback()
            if engine == "postgres":
                connection.set_session(readonly=False)
            else:
                connection.set_progress_handler(None, 0)
                connection.execute("PRAGMA query_only=OFF")
    return phases


def run_demo(engine: str, output_dir: Path) -> dict:
    if engine not in {"postgres", "sqlite"}:
        raise DemoError("Select postgres or sqlite explicitly")
    output_dir = output_dir.expanduser()
    if output_dir.is_symlink():
        raise DemoError("Output directory must not be a symlink")
    root = output_dir.resolve()
    if root.exists() and (not root.is_dir() or any(root.iterdir())):
        raise DemoError("Output directory must be new or empty; existing files are preserved")
    root.mkdir(parents=True, exist_ok=True)
    commands: list[dict] = []
    try:
        artifacts = _prepare(root, commands)
        if engine == "postgres":
            with _postgres() as (connection, environment):
                phases = _execute(connection, engine, artifacts)
        else:
            database_path = root / "teaching.sqlite"
            connection = sqlite3.connect(database_path)
            try:
                phases = _execute(connection, engine, artifacts)
            finally:
                connection.close()
            environment = {"engine": "sqlite", "database_version": sqlite3.sqlite_version, "database_file": database_path.name}
        hash_paths = [*sorted((root / "source").glob("*.tbl")), artifacts["baseline"], artifacts["drifted"], root / "templates" / "6.sql", root / "queries" / "tpch" / "queries" / "tpch_queries.csv", root / "trace-workload.csv"]
        result = {
            "schema": "driftbench.learning-demo/v1", "status": "ok", "seed": 42,
            "fixture": "hand-authored-miniature-tpch-shaped", "official_tpch": False,
            "environment": environment, "phases": phases,
            "execution_policy": "sequential SELECTs; timestamps are annotations and are not replayed",
            "measurement_scope": "teaching-only elapsed time; no performance or regression-gate claim",
            "artifact_sha256": {path.relative_to(root).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest() for path in hash_paths},
        }
        _write_json(root / "results.json", result)
        return result
    except Exception as exc:
        _write_json(root / "failure.json", {"engine": engine, "status": "error", "error": str(exc)})
        raise
    finally:
        _write_json(root / "commands.json", commands)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--engine", choices=("postgres", "sqlite"), required=True, help="Explicitly select real isolated PostgreSQL or an actual SQLite teaching simulation")
    parser.add_argument("--output-dir", type=Path, required=True, help="New or empty directory for artifacts and results")
    args = parser.parse_args(argv)
    try:
        result = run_demo(args.engine, args.output_dir)
    except Exception as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(json.dumps({"status": result["status"], "engine": args.engine, "results": str(args.output_dir / "results.json")}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
