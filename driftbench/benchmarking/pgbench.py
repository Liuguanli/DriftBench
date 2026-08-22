"""Real PostgreSQL/pgbench producer for BenchmarkRunResult v1."""

from __future__ import annotations

import hashlib
import os
import platform
import re
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from driftbench import __version__ as DRIFTBENCH_VERSION

from .metrics import (
    BENCHMARK_RESULT_SCHEMA_VERSION,
    BenchmarkResultError,
    aggregate_repetitions,
    build_repetition_metrics,
    validate_benchmark_result,
    write_json_strict,
)
from .policy import (
    PgBenchRegressionPolicy,
    evaluate_regression,
    validate_pgbench_policy_operational_limits,
)
from .provenance import (
    DriftBenchSourceIdentity,
    SourceProvenanceError,
    inspect_driftbench_source,
    serialize_pgbench_policy,
)


class PgBenchExecutionError(RuntimeError):
    """Raised after pgbench evidence files have been preserved on failure."""


@dataclass(frozen=True)
class PgBenchConnection:
    database: str
    host: str = "localhost"
    port: int = 5432
    username: str = "postgres"


@dataclass(frozen=True)
class PgBenchStdoutMetrics:
    transactions_successful: int
    transactions_failed: int
    transactions_total: int
    reported_latency_mean_ms: float
    reported_tps: float
    scale_factor: int
    clients: int


@dataclass(frozen=True)
class PgBenchTransactionLogSummary:
    latencies_us: tuple[int, ...]
    transactions_successful: int
    transactions_failed: int
    failure_types: Mapping[str, int]

    @property
    def transactions_total(self) -> int:
        return self.transactions_successful + self.transactions_failed


@dataclass(frozen=True)
class PairedPgBenchOutcome:
    ok: bool
    baseline_path: Path
    candidate_path: Path
    decision_path: Path
    execution_order_path: Path


_PROCESSED_RE = re.compile(
    r"^number of transactions actually processed:\s*(\d+)(?:\s*/\s*(\d+))?\s*$",
    re.MULTILINE,
)
_FAILED_RE = re.compile(
    r"^number of failed transactions:\s*(\d+)(?:\s+\([^\n]*\))?\s*$",
    re.MULTILINE,
)
_LATENCY_RE = re.compile(
    r"^latency average\s*=\s*([^\s]+)\s+ms\s*$", re.MULTILINE
)
_TPS_RE = re.compile(r"^tps\s*=\s*([^\s]+)(?:\s+.*)?$", re.MULTILINE)
_SCALE_RE = re.compile(r"^scaling factor:\s*(\d+)\s*$", re.MULTILINE)
_CLIENTS_RE = re.compile(r"^number of clients:\s*(\d+)\s*$", re.MULTILINE)
_VERSION_MAJOR_RE = re.compile(r"\b(\d+)(?:\.\d+)?\b")
_POSTGRESQL_SETTINGS = (
    "effective_cache_size",
    "fsync",
    "full_page_writes",
    "jit",
    "max_connections",
    "max_parallel_workers",
    "max_parallel_workers_per_gather",
    "random_page_cost",
    "shared_buffers",
    "synchronous_commit",
    "work_mem",
)


def build_paired_execution_plan(
    policy: PgBenchRegressionPolicy,
) -> list[dict[str, Any]]:
    """Return the deterministic AB/BA alternating execution plan."""

    validate_pgbench_policy_operational_limits(policy)
    return [
        {
            "index": index,
            "order": (
                ["baseline", "candidate"]
                if index % 2 == 1
                else ["candidate", "baseline"]
            ),
            "phases_per_role": ["warmup", "measurement"],
        }
        for index in range(1, policy.repetitions + 1)
    ]


def _parse_finite(raw: str, field: str) -> float:
    try:
        value = float(raw)
    except ValueError as exc:
        raise BenchmarkResultError(f"pgbench {field} is not numeric") from exc
    if value != value or value in (float("inf"), float("-inf")):
        raise BenchmarkResultError(f"pgbench {field} must be finite")
    return value


def parse_pgbench_stdout(stdout: str) -> PgBenchStdoutMetrics:
    """Parse required C-locale pgbench summary fields, failing on omissions."""

    processed = _PROCESSED_RE.search(stdout)
    failed_match = _FAILED_RE.search(stdout)
    latency_match = _LATENCY_RE.search(stdout)
    tps_match = _TPS_RE.search(stdout)
    scale_match = _SCALE_RE.search(stdout)
    clients_match = _CLIENTS_RE.search(stdout)
    missing = [
        name
        for name, match in (
            ("transactions processed", processed),
            ("latency average", latency_match),
            ("tps", tps_match),
            ("scaling factor", scale_match),
            ("number of clients", clients_match),
        )
        if match is None
    ]
    if missing:
        raise BenchmarkResultError(
            "pgbench stdout missing required field(s): " + ", ".join(missing)
        )
    assert processed is not None
    assert latency_match is not None
    assert tps_match is not None
    assert scale_match is not None
    assert clients_match is not None
    successful = int(processed.group(1))
    attempted_from_ratio = (
        int(processed.group(2)) if processed.group(2) is not None else None
    )
    if failed_match is None and attempted_from_ratio is None:
        raise BenchmarkResultError(
            "pgbench stdout does not provide a failed transaction count"
        )
    failed = (
        int(failed_match.group(1))
        if failed_match is not None
        else int(attempted_from_ratio) - successful
    )
    if failed < 0:
        raise BenchmarkResultError("pgbench failed transaction count is invalid")
    total = successful + failed
    if attempted_from_ratio is not None and attempted_from_ratio != total:
        raise BenchmarkResultError(
            "pgbench processed/attempted counts conflict with failed transactions"
        )
    latency = _parse_finite(latency_match.group(1), "latency average")
    tps = _parse_finite(tps_match.group(1), "TPS")
    scale_factor = int(scale_match.group(1))
    clients = int(clients_match.group(1))
    if successful <= 0 or total <= 0:
        raise BenchmarkResultError("pgbench processed zero transactions")
    if latency < 0 or tps <= 0 or scale_factor <= 0 or clients <= 0:
        raise BenchmarkResultError("pgbench stdout contains invalid metrics")
    return PgBenchStdoutMetrics(
        transactions_successful=successful,
        transactions_failed=failed,
        transactions_total=total,
        reported_latency_mean_ms=latency,
        reported_tps=tps,
        scale_factor=scale_factor,
        clients=clients,
    )


def parse_pgbench_transaction_log_texts(
    sources: Iterable[tuple[str, str]],
) -> PgBenchTransactionLogSummary:
    """Parse successful latencies and typed failures from named log text.

    Non-aggregate pgbench log records have at least six whitespace-separated
    fields.  For a successful transaction, the third field is its latency in
    microseconds.  PostgreSQL 16 writes ``failed`` there for an undifferentiated
    failure, or ``serialization``/``deadlock`` with ``--failures-detailed``.
    Empty, partial, unknown, or malformed records are rejected rather than
    silently skipped.
    """

    logs = sorted((str(name), text) for name, text in sources)
    if not logs:
        raise BenchmarkResultError("pgbench transaction log is missing")
    latencies: list[int] = []
    failure_types: dict[str, int] = {}
    for name, text in logs:
        if not isinstance(text, str):
            raise BenchmarkResultError(f"pgbench transaction log is not text: {name}")
        if not text:
            raise BenchmarkResultError(f"pgbench transaction log is empty: {name}")
        lines = text.splitlines()
        for line_number, line in enumerate(lines, start=1):
            if not line.strip():
                raise BenchmarkResultError(
                    f"blank pgbench log record at {name}:{line_number}"
                )
            fields = line.split()
            if len(fields) < 6:
                raise BenchmarkResultError(
                    f"partial pgbench log record at {name}:{line_number}"
                )
            raw_latency = fields[2]
            if raw_latency in {"failed", "serialization", "deadlock"}:
                failure_types[raw_latency] = failure_types.get(raw_latency, 0) + 1
                continue
            try:
                latency_us = int(raw_latency)
            except ValueError as exc:
                raise BenchmarkResultError(
                    f"invalid pgbench latency/failure status at {name}:{line_number}"
                ) from exc
            if latency_us < 0:
                raise BenchmarkResultError(
                    f"negative pgbench latency at {name}:{line_number}"
                )
            latencies.append(latency_us)
    failed = sum(failure_types.values())
    if not latencies and not failed:
        raise BenchmarkResultError("pgbench transaction logs contain no records")
    return PgBenchTransactionLogSummary(
        latencies_us=tuple(latencies),
        transactions_successful=len(latencies),
        transactions_failed=failed,
        failure_types=dict(sorted(failure_types.items())),
    )


def parse_pgbench_transaction_log_summary(
    paths: Iterable[str | Path],
) -> PgBenchTransactionLogSummary:
    """Read transaction logs once and parse their strict PostgreSQL 16 records."""

    sources: list[tuple[str, str]] = []
    for raw_path in paths:
        path = Path(raw_path)
        if not path.exists() or not path.is_file():
            raise BenchmarkResultError(f"pgbench transaction log is missing: {path}")
        try:
            text = path.read_text(encoding="utf-8")
        except Exception as exc:
            raise BenchmarkResultError(f"failed to read pgbench log {path}: {exc}") from exc
        sources.append((str(path), text))
    return parse_pgbench_transaction_log_texts(sources)


def parse_pgbench_transaction_logs(paths: Iterable[str | Path]) -> list[int]:
    """Return successful transaction latencies from strict pgbench logs."""

    return list(parse_pgbench_transaction_log_summary(paths).latencies_us)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _artifact(path: Path, root: Path) -> dict[str, Any]:
    return {
        "path": path.resolve().relative_to(root.resolve()).as_posix(),
        "sha256": _sha256(path),
        "bytes": path.stat().st_size,
    }


def _connection_args(connection: PgBenchConnection) -> list[str]:
    args: list[str] = []
    if connection.host:
        args.extend(["--host", connection.host])
    if connection.port:
        args.extend(["--port", str(connection.port)])
    if connection.username:
        args.extend(["--username", connection.username])
    return args


def _run_process(
    command: Sequence[str], *, timeout: float, env: Mapping[str, str]
) -> tuple[subprocess.CompletedProcess[str], float]:
    started = time.perf_counter()
    try:
        process = subprocess.run(
            list(command),
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            check=False,
            timeout=timeout,
            env=dict(env),
        )
    except FileNotFoundError as exc:
        raise PgBenchExecutionError(f"executable not found: {command[0]}") from exc
    except subprocess.TimeoutExpired as exc:
        raise PgBenchExecutionError(f"pgbench timed out: {' '.join(command)}") from exc
    return process, time.perf_counter() - started


def _run_phase(
    *,
    output_root: Path,
    phase_dir: Path,
    role: str,
    duration_seconds: int,
    candidate_script: Path,
    policy: PgBenchRegressionPolicy,
    connection: PgBenchConnection,
    pgbench_binary: str,
) -> dict[str, Any]:
    phase_dir.mkdir(parents=True, exist_ok=True)
    stdout_path = phase_dir / "pgbench.stdout.log"
    stderr_path = phase_dir / "pgbench.stderr.log"
    log_prefix = (phase_dir / "transactions").resolve()
    command = [
        pgbench_binary,
        *_connection_args(connection),
        "--client",
        str(policy.clients),
        "--jobs",
        str(policy.jobs),
        "--time",
        str(duration_seconds),
        "--no-vacuum",
        "--log",
        "--failures-detailed",
        f"--log-prefix={log_prefix}",
    ]
    if role == "baseline":
        command.extend(["--builtin", "select-only"])
    else:
        command.extend(["--file", str(candidate_script.resolve())])
    command.append(connection.database)
    environment = dict(os.environ)
    environment["LC_ALL"] = "C"
    process, elapsed = _run_process(
        command,
        timeout=float(duration_seconds + 120),
        env=environment,
    )
    stdout_path.write_bytes(process.stdout.encode("utf-8"))
    stderr_path.write_bytes(process.stderr.encode("utf-8"))
    logs = sorted(
        path
        for path in phase_dir.glob(f"{log_prefix.name}*")
        if path.is_file()
    )
    base_artifacts: dict[str, Any] = {
        "stdout": _artifact(stdout_path, output_root),
        "stderr": _artifact(stderr_path, output_root),
        "transaction_logs": [_artifact(path, output_root) for path in logs],
    }
    if process.returncode != 0:
        raise PgBenchExecutionError(
            f"pgbench {role} phase failed with exit code {process.returncode}; "
            f"see {stderr_path}"
        )
    # Parse the exact stdout bytes covered by the descriptor rather than the
    # in-memory subprocess string.
    parsed = parse_pgbench_stdout(stdout_path.read_text(encoding="utf-8"))
    if parsed.scale_factor != policy.scale_factor:
        raise BenchmarkResultError(
            f"pgbench scale factor {parsed.scale_factor} does not match policy {policy.scale_factor}"
        )
    if parsed.clients != policy.clients:
        raise BenchmarkResultError(
            f"pgbench clients {parsed.clients} does not match policy {policy.clients}"
        )
    log_summary = parse_pgbench_transaction_log_summary(logs)
    if log_summary.transactions_successful != parsed.transactions_successful:
        raise BenchmarkResultError(
            "pgbench successful transaction log count does not match stdout"
        )
    if log_summary.transactions_failed != parsed.transactions_failed:
        raise BenchmarkResultError(
            "pgbench failed transaction log count does not match stdout"
        )
    return {
        "actual_seconds": elapsed,
        "parsed": parsed,
        "latencies_us": list(log_summary.latencies_us),
        "failure_types": dict(log_summary.failure_types),
        "artifacts": base_artifacts,
        "returncode": process.returncode,
    }


def _pgbench_version(pgbench_binary: str) -> dict[str, Any]:
    environment = dict(os.environ)
    environment["LC_ALL"] = "C"
    process, _ = _run_process(
        [pgbench_binary, "--version"], timeout=30.0, env=environment
    )
    if process.returncode != 0:
        raise PgBenchExecutionError("pgbench --version failed")
    full = (process.stdout or process.stderr).strip()
    match = _VERSION_MAJOR_RE.search(full)
    if not full or match is None:
        raise PgBenchExecutionError("could not parse pgbench version")
    return {"full": full, "major": int(match.group(1))}


def _safe_error_message(exc: BaseException) -> str:
    message = str(exc) or exc.__class__.__name__
    for variable in ("PGPASSWORD",):
        secret = os.environ.get(variable)
        if secret:
            message = message.replace(secret, "<redacted>")
    return message


def _postgresql_environment(
    connection: PgBenchConnection,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Capture PostgreSQL version, settings, and pgbench initialization evidence."""

    try:
        import psycopg2
    except ImportError as exc:
        raise PgBenchExecutionError("psycopg2 is required for PostgreSQL provenance") from exc
    kwargs: dict[str, Any] = {
        "dbname": connection.database,
        "connect_timeout": 10,
    }
    if connection.host:
        kwargs["host"] = connection.host
    if connection.port:
        kwargs["port"] = connection.port
    if connection.username:
        kwargs["user"] = connection.username
    try:
        with psycopg2.connect(**kwargs) as database:
            with database.cursor() as cursor:
                cursor.execute(
                    "SELECT version(), current_setting('server_version_num'), current_database()"
                )
                version_row = cursor.fetchone()
                cursor.execute(
                    "SELECT name, setting, unit, source "
                    "FROM pg_catalog.pg_settings "
                    "WHERE name = ANY(%s) ORDER BY name",
                    (list(_POSTGRESQL_SETTINGS),),
                )
                setting_rows = cursor.fetchall()
                cursor.execute(
                    "SELECT "
                    "(SELECT count(*) FROM public.pgbench_branches), "
                    "(SELECT count(*) FROM public.pgbench_tellers), "
                    "(SELECT count(*) FROM public.pgbench_accounts), "
                    "(SELECT count(*) FROM public.pgbench_history)"
                )
                count_row = cursor.fetchone()
    except Exception as exc:
        raise PgBenchExecutionError(
            f"failed to capture PostgreSQL environment: {_safe_error_message(exc)}"
        ) from exc
    if not version_row or len(version_row) != 3:
        raise PgBenchExecutionError("PostgreSQL provenance query returned no version data")
    full = str(version_row[0]).strip()
    try:
        version_number = int(str(version_row[1]))
    except (TypeError, ValueError) as exc:
        raise PgBenchExecutionError("PostgreSQL server_version_num is invalid") from exc
    major = version_number // 10000
    if not full or major <= 0:
        raise PgBenchExecutionError("PostgreSQL version is invalid")

    settings = {
        str(name): {
            "setting": str(setting),
            "unit": None if unit is None else str(unit),
            "source": str(source),
        }
        for name, setting, unit, source in setting_rows
    }
    missing_settings = sorted(set(_POSTGRESQL_SETTINGS) - set(settings))
    if missing_settings:
        raise PgBenchExecutionError(
            "PostgreSQL provenance is missing setting(s): "
            + ", ".join(missing_settings)
        )
    if not count_row or len(count_row) != 4:
        raise PgBenchExecutionError("pgbench initialization counts are unavailable")
    try:
        branches = int(count_row[0])
        tellers = int(count_row[1])
        accounts = int(count_row[2])
        history = int(count_row[3])
    except (TypeError, ValueError) as exc:
        raise PgBenchExecutionError("pgbench initialization counts are invalid") from exc
    if (
        branches <= 0
        or tellers != branches * 10
        or accounts != branches * 100_000
        or history != 0
    ):
        raise PgBenchExecutionError(
            "pgbench tables do not identify a fresh, valid initialization scale"
        )
    current_database = str(version_row[2]).strip()
    if not current_database:
        raise PgBenchExecutionError("PostgreSQL current database is unavailable")
    return (
        {"full": full, "major": major},
        {
            "current_database": current_database,
            "settings": settings,
            "initialization": {
                "pgbench_branches": branches,
                "pgbench_tellers": tellers,
                "pgbench_accounts": accounts,
                "pgbench_history": history,
                "scale_factor_inferred": branches,
            },
        },
    )


def _base_environment(
    connection: PgBenchConnection,
    *,
    source: DriftBenchSourceIdentity | SourceProvenanceError,
) -> dict[str, Any]:
    processor = platform.processor().strip() or platform.machine().strip() or "unavailable"
    detected_cpu_count = os.cpu_count()
    logical_cpu_count = (
        detected_cpu_count
        if isinstance(detected_cpu_count, int) and detected_cpu_count > 0
        else None
    )
    return {
        "schema_version": "1.0",
        "status": "in_progress",
        "driftbench": {
            "version": DRIFTBENCH_VERSION,
            **source.environment_fields(),
        },
        "python": {
            "version": sys.version,
            "implementation": platform.python_implementation(),
        },
        "platform": {
            "os": platform.platform(),
            "system": platform.system(),
            "release": platform.release(),
            "machine": platform.machine() or "unavailable",
            "cpu": processor,
            "logical_cpu_count": logical_cpu_count,
        },
        "connection": {
            "database": connection.database,
            "host": connection.host,
            "port": connection.port,
            "username": connection.username,
        },
    }


def _source_identity() -> DriftBenchSourceIdentity:
    return inspect_driftbench_source()


def _git_sha() -> DriftBenchSourceIdentity:
    """Backward-compatible test seam for source identity resolution."""

    return _source_identity()


def _coerce_source_identity(
    value: DriftBenchSourceIdentity | str,
) -> DriftBenchSourceIdentity:
    if isinstance(value, DriftBenchSourceIdentity):
        return value
    if (
        isinstance(value, str)
        and len(value) == 40
        and all(ch in "0123456789abcdef" for ch in value)
    ):
        return DriftBenchSourceIdentity(
            source_sha=value,
            source_state="clean",
            source_sha_source="git_head",
            repository_root=Path(__file__).resolve().parents[2],
        )
    raise SourceProvenanceError(
        "DriftBench source identity must contain a full lowercase 40-character SHA"
    )


def _result(
    *,
    role: str,
    policy: PgBenchRegressionPolicy,
    versions: Mapping[str, Any],
    git_sha: str,
    script_sha256: str,
    inputs: Mapping[str, Any],
    environment: Mapping[str, Any],
    warmups: list[dict[str, Any]],
    repetitions: list[dict[str, Any]],
) -> dict[str, Any]:
    workload: dict[str, Any] = {
        "name": policy.workload,
        "source": "builtin" if role == "baseline" else "driftbench_script",
    }
    if role == "candidate":
        workload["script_sha256"] = script_sha256
    result = {
        "schema_version": BENCHMARK_RESULT_SCHEMA_VERSION,
        "benchmark": "pgbench",
        "role": role,
        "workload": workload,
        "config": policy.run_config(),
        "git_sha": git_sha,
        "versions": dict(versions),
        "inputs": dict(inputs),
        "environment": dict(environment),
        "warmups": warmups,
        "repetitions": repetitions,
        "metrics": aggregate_repetitions(repetitions),
        "valid": True,
    }
    validate_benchmark_result(result)
    return result


def _failure_result(
    *,
    role: str,
    policy: PgBenchRegressionPolicy,
    message: str,
    script_sha256: str | None,
    git_sha: str,
    inputs: Mapping[str, Any],
    environment: Mapping[str, Any] | None,
) -> dict[str, Any]:
    workload: dict[str, Any] = {
        "name": policy.workload,
        "source": "builtin" if role == "baseline" else "driftbench_script",
    }
    if role == "candidate" and script_sha256:
        workload["script_sha256"] = script_sha256
    result = {
        "schema_version": BENCHMARK_RESULT_SCHEMA_VERSION,
        "benchmark": "pgbench",
        "role": role,
        "workload": workload,
        "config": policy.run_config(),
        "git_sha": git_sha,
        "versions": {
            "postgresql": {"full": "unavailable", "major": policy.postgresql_major},
            "pgbench": {"full": "unavailable", "major": policy.pgbench_major},
        },
        "warmups": [],
        "repetitions": [],
        "metrics": None,
        "valid": False,
        "error": message,
    }
    if inputs:
        result["inputs"] = dict(inputs)
    if environment is not None:
        result["environment"] = dict(environment)
    return result


def _write_source_provenance_failure(
    *,
    output_root: Path,
    policy: PgBenchRegressionPolicy,
    connection: PgBenchConnection,
    error: SourceProvenanceError,
) -> None:
    """Persist a minimal failure bundle without running any benchmark phase."""

    output_root.mkdir(parents=True, exist_ok=True)
    inputs_dir = output_root / "inputs"
    inputs_dir.mkdir(parents=True, exist_ok=True)
    policy_path = inputs_dir / "policy.json"
    policy_path.write_bytes(serialize_pgbench_policy(policy).encode("utf-8"))
    inputs = {"policy": _artifact(policy_path, output_root)}

    message = _safe_error_message(error)
    environment_payload = _base_environment(connection, source=error)
    environment_payload["status"] = "failed"
    environment_payload["error"] = message
    environment_path = output_root / "environment.json"
    write_json_strict(environment_path, environment_payload)
    environment = _artifact(environment_path, output_root)

    baseline_path = output_root / "baseline.json"
    candidate_path = output_root / "candidate.json"
    for role, path in (("baseline", baseline_path), ("candidate", candidate_path)):
        write_json_strict(
            path,
            _failure_result(
                role=role,
                policy=policy,
                message=message,
                script_sha256=None,
                git_sha=error.source_sha,
                inputs=inputs,
                environment=environment,
            ),
        )

    pairs = build_paired_execution_plan(policy)
    write_json_strict(
        output_root / "execution_order.json",
        {
            "schema_version": "1.0",
            "policy_version": policy.policy_version,
            "strategy": policy.execution_order,
            "status": "failed",
            "pairs": pairs,
            "completed": [],
            "error": message,
            "inputs": inputs,
            "environment": environment,
        },
    )
    write_json_strict(
        output_root / "decision.json",
        {
            "schema_version": "1.0",
            "policy_version": policy.policy_version,
            "benchmark": "pgbench",
            "ok": False,
            "baseline_result": baseline_path.name,
            "candidate_result": candidate_path.name,
            "compatibility": {"ok": False, "reasons": [message]},
            "checks": [],
            "reasons": [message],
            "inputs": inputs,
            "environment": environment,
        },
    )


def run_paired_pgbench(
    *,
    policy: PgBenchRegressionPolicy,
    candidate_script: str | Path,
    output_dir: str | Path,
    connection: PgBenchConnection,
    pgbench_binary: str = "pgbench",
) -> PairedPgBenchOutcome:
    """Run AB/BA/AB paired pgbench rounds and persist all gate evidence."""

    output_root = Path(output_dir).expanduser().resolve()
    if output_root.exists():
        if not output_root.is_dir():
            raise PgBenchExecutionError(
                f"benchmark output path is not a directory: {output_root}"
            )
        if any(output_root.iterdir()):
            raise PgBenchExecutionError(
                "benchmark output directory must be new or empty to prevent mixed-run evidence: "
                f"{output_root}"
            )
    candidate_path_input = Path(candidate_script).expanduser().resolve()
    inputs_dir = output_root / "inputs"
    copied_candidate_path = inputs_dir / "candidate.sql"
    policy_snapshot_path = inputs_dir / "policy.json"
    environment_path = output_root / "environment.json"
    baseline_path = output_root / "baseline.json"
    candidate_path = output_root / "candidate.json"
    decision_path = output_root / "decision.json"
    order_path = output_root / "execution_order.json"
    script_digest: str | None = None
    try:
        initial_source = _coerce_source_identity(_git_sha())
    except SourceProvenanceError as exc:
        try:
            _write_source_provenance_failure(
                output_root=output_root,
                policy=policy,
                connection=connection,
                error=exc,
            )
        except Exception as evidence_exc:
            raise PgBenchExecutionError(
                f"{_safe_error_message(exc)}; failed to write provenance evidence: "
                f"{_safe_error_message(evidence_exc)}"
            ) from exc
        raise PgBenchExecutionError(_safe_error_message(exc)) from exc

    # The initial clean-source check intentionally precedes creation of any
    # output, database access, or pgbench phase.
    if not output_root.exists():
        output_root.mkdir(parents=True, exist_ok=False)
    git_sha = initial_source.source_sha
    input_descriptors: dict[str, Any] = {}
    environment_payload: dict[str, Any] | None = None
    environment_descriptor: dict[str, Any] | None = None
    pairs = build_paired_execution_plan(policy)
    execution_order: dict[str, Any] = {
        "schema_version": "1.0",
        "policy_version": policy.policy_version,
        "strategy": policy.execution_order,
        "status": "in_progress",
        "pairs": pairs,
        "completed": [],
    }
    write_json_strict(order_path, execution_order)
    try:
        inputs_dir.mkdir(parents=True, exist_ok=False)
        # Persist the exact canonical bytes on every platform.  Path.write_text
        # applies Windows newline translation, which would make the evidence
        # digest platform-dependent.
        policy_snapshot_path.write_bytes(
            serialize_pgbench_policy(policy).encode("utf-8")
        )
        input_descriptors["policy"] = _artifact(policy_snapshot_path, output_root)

        environment_payload = _base_environment(connection, source=initial_source)
        write_json_strict(environment_path, environment_payload)
        environment_descriptor = _artifact(environment_path, output_root)

        if not candidate_path_input.exists() or not candidate_path_input.is_file():
            raise PgBenchExecutionError(
                f"candidate workload script not found: {candidate_path_input}"
            )
        candidate_bytes = candidate_path_input.read_bytes()
        if not candidate_bytes:
            raise PgBenchExecutionError("candidate workload script is empty")
        copied_candidate_path.write_bytes(candidate_bytes)
        input_descriptors["candidate_script"] = _artifact(
            copied_candidate_path, output_root
        )
        script_digest = input_descriptors["candidate_script"]["sha256"]

        pgbench_version = _pgbench_version(pgbench_binary)
        postgresql_version, postgresql_environment = _postgresql_environment(connection)
        versions = {
            "postgresql": postgresql_version,
            "pgbench": pgbench_version,
        }
        if versions["postgresql"]["major"] != policy.postgresql_major:
            raise PgBenchExecutionError(
                "PostgreSQL major version does not match the regression policy"
            )
        if versions["pgbench"]["major"] != policy.pgbench_major:
            raise PgBenchExecutionError(
                "pgbench major version does not match the regression policy"
            )
        initialization = postgresql_environment["initialization"]
        if initialization != {
            "pgbench_branches": policy.scale_factor,
            "pgbench_tellers": policy.scale_factor * 10,
            "pgbench_accounts": policy.scale_factor * 100_000,
            "pgbench_history": 0,
            "scale_factor_inferred": policy.scale_factor,
        }:
            raise PgBenchExecutionError(
                "pgbench initialization does not match the regression policy"
            )

        environment_payload.update(
            {
                "status": "complete",
                "postgresql": {
                    **postgresql_version,
                    **postgresql_environment,
                },
                "pgbench": dict(pgbench_version),
            }
        )
        write_json_strict(environment_path, environment_payload)
        environment_descriptor = _artifact(environment_path, output_root)
        execution_order["inputs"] = dict(input_descriptors)
        execution_order["environment"] = dict(environment_descriptor)
        write_json_strict(order_path, execution_order)

        warmups: dict[str, list[dict[str, Any]]] = {
            "baseline": [],
            "candidate": [],
        }
        measurements: dict[str, list[dict[str, Any]]] = {
            "baseline": [],
            "candidate": [],
        }
        for pair in pairs:
            index = int(pair["index"])
            for role in pair["order"]:
                role_dir = output_root / "raw" / f"rep-{index:02d}" / role
                warmup_phase = _run_phase(
                    output_root=output_root,
                    phase_dir=role_dir / "warmup",
                    role=role,
                    duration_seconds=policy.warmup_seconds,
                    candidate_script=copied_candidate_path,
                    policy=policy,
                    connection=connection,
                    pgbench_binary=pgbench_binary,
                )
                warmup_parsed: PgBenchStdoutMetrics = warmup_phase["parsed"]
                if warmup_parsed.transactions_failed != 0:
                    raise BenchmarkResultError(f"{role} warmup had failed transactions")
                warmups[role].append(
                    {
                        "index": index,
                        "actual_seconds": warmup_phase["actual_seconds"],
                        "transactions_successful": warmup_parsed.transactions_successful,
                        "transactions_failed": warmup_parsed.transactions_failed,
                        "artifacts": warmup_phase["artifacts"],
                    }
                )
                execution_order["completed"].append(
                    {"index": index, "role": role, "phase": "warmup"}
                )
                write_json_strict(order_path, execution_order)

                measurement_phase = _run_phase(
                    output_root=output_root,
                    phase_dir=role_dir / "measurement",
                    role=role,
                    duration_seconds=policy.measurement_seconds,
                    candidate_script=copied_candidate_path,
                    policy=policy,
                    connection=connection,
                    pgbench_binary=pgbench_binary,
                )
                parsed: PgBenchStdoutMetrics = measurement_phase["parsed"]
                measurement = build_repetition_metrics(
                    index=index,
                    latencies_us=measurement_phase["latencies_us"],
                    transactions_successful=parsed.transactions_successful,
                    transactions_failed=parsed.transactions_failed,
                    actual_measurement_seconds=measurement_phase["actual_seconds"],
                    artifacts=measurement_phase["artifacts"],
                    reported_tps=parsed.reported_tps,
                    reported_latency_mean_ms=parsed.reported_latency_mean_ms,
                    max_tps_relative_delta=policy.max_tps_relative_delta,
                    failure_types=measurement_phase["failure_types"],
                )
                measurements[role].append(measurement)
                execution_order["completed"].append(
                    {"index": index, "role": role, "phase": "measurement"}
                )
                write_json_strict(order_path, execution_order)

        # Re-check after the final measurement and before writing any valid
        # result or decision.  A clean but different HEAD is also invalid.
        final_source = _coerce_source_identity(_git_sha())
        if final_source.source_sha != initial_source.source_sha:
            raise SourceProvenanceError(
                "DriftBench source HEAD changed while the benchmark was running",
                source_sha=final_source.source_sha,
                source_state="head_changed",
                source_sha_source=final_source.source_sha_source,
            )

        assert script_digest is not None
        assert environment_descriptor is not None
        baseline = _result(
            role="baseline",
            policy=policy,
            versions=versions,
            git_sha=git_sha,
            script_sha256=script_digest,
            inputs=input_descriptors,
            environment=environment_descriptor,
            warmups=warmups["baseline"],
            repetitions=measurements["baseline"],
        )
        candidate = _result(
            role="candidate",
            policy=policy,
            versions=versions,
            git_sha=git_sha,
            script_sha256=script_digest,
            inputs=input_descriptors,
            environment=environment_descriptor,
            warmups=warmups["candidate"],
            repetitions=measurements["candidate"],
        )
        write_json_strict(baseline_path, baseline)
        write_json_strict(candidate_path, candidate)
        decision = evaluate_regression(
            baseline,
            candidate,
            policy,
            baseline_path=baseline_path.name,
            candidate_path=candidate_path.name,
        )
        write_json_strict(decision_path, decision)
        execution_order["status"] = "completed"
        write_json_strict(order_path, execution_order)
        return PairedPgBenchOutcome(
            ok=bool(decision["ok"]),
            baseline_path=baseline_path,
            candidate_path=candidate_path,
            decision_path=decision_path,
            execution_order_path=order_path,
        )
    except Exception as exc:
        message = _safe_error_message(exc)
        if environment_payload is not None:
            environment_payload["status"] = "failed"
            environment_payload["error"] = message
            if isinstance(exc, SourceProvenanceError):
                environment_payload["driftbench"].update(exc.environment_fields())
            write_json_strict(environment_path, environment_payload)
            environment_descriptor = _artifact(environment_path, output_root)
        execution_order["status"] = "failed"
        execution_order["error"] = message
        if input_descriptors:
            execution_order["inputs"] = dict(input_descriptors)
        if environment_descriptor is not None:
            execution_order["environment"] = dict(environment_descriptor)
        write_json_strict(order_path, execution_order)
        if not baseline_path.exists():
            write_json_strict(
                baseline_path,
                _failure_result(
                    role="baseline",
                    policy=policy,
                    message=message,
                    script_sha256=script_digest,
                    git_sha=git_sha,
                    inputs=input_descriptors,
                    environment=environment_descriptor,
                ),
            )
        if not candidate_path.exists():
            write_json_strict(
                candidate_path,
                _failure_result(
                    role="candidate",
                    policy=policy,
                    message=message,
                    script_sha256=script_digest,
                    git_sha=git_sha,
                    inputs=input_descriptors,
                    environment=environment_descriptor,
                ),
            )
        write_json_strict(
            decision_path,
            {
                "schema_version": "1.0",
                "policy_version": policy.policy_version,
                "benchmark": "pgbench",
                "ok": False,
                "baseline_result": baseline_path.name,
                "candidate_result": candidate_path.name,
                "compatibility": {"ok": False, "reasons": [message]},
                "checks": [],
                "reasons": [message],
                "inputs": dict(input_descriptors),
                "environment": (
                    dict(environment_descriptor)
                    if environment_descriptor is not None
                    else None
                ),
            },
        )
        raise PgBenchExecutionError(message) from exc
