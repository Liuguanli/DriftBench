"""Offline integrity and reproducibility verification for pgbench bundles."""

from __future__ import annotations

import hashlib
import json
import math
import os
import stat
import unicodedata
from dataclasses import dataclass
from pathlib import Path, PurePosixPath, PureWindowsPath
from typing import Any, Iterable, Mapping, Sequence

from .metrics import BenchmarkResultError, build_repetition_metrics
from .pgbench import (
    _POSTGRESQL_SETTINGS,
    _result,
    build_paired_execution_plan,
    parse_pgbench_stdout,
    parse_pgbench_transaction_log_texts,
)
from .policy import (
    BenchmarkPolicyError,
    evaluate_regression,
    parse_pgbench_policy_payload,
)
from .provenance import serialize_pgbench_policy


class BenchmarkBundleError(ValueError):
    """Raised when an existing bundle is incomplete, unsafe, or inconsistent."""


@dataclass(frozen=True)
class BenchmarkBundleVerification:
    bundle: Path
    decision: Mapping[str, Any]

    @property
    def ok(self) -> bool:
        return bool(self.decision["ok"])

    def payload(self) -> dict[str, Any]:
        return {
            "verified": True,
            "ok": self.ok,
            "outcome": "passed" if self.ok else "threshold_failed",
            "command": "benchmark verify",
            "bundle": str(self.bundle),
            "decision": str(self.bundle / "decision.json"),
            "reasons": list(self.decision.get("reasons", [])),
        }


def _canonical_json_bytes(value: Any) -> bytes:
    try:
        return (
            json.dumps(
                value,
                indent=2,
                sort_keys=True,
                ensure_ascii=False,
                allow_nan=False,
            )
            + "\n"
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise BenchmarkBundleError(f"JSON value is not canonicalizable: {exc}") from exc


def _strict_json_bytes(data: bytes, label: str) -> Any:
    def reject_constant(value: str) -> None:
        raise ValueError(f"invalid JSON number {value}")

    def reject_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError(f"duplicate JSON key {key!r}")
            result[key] = value
        return result

    try:
        text = data.decode("utf-8", errors="strict")
        return json.loads(
            text,
            parse_constant=reject_constant,
            object_pairs_hook=reject_duplicates,
        )
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise BenchmarkBundleError(f"{label} is not strict UTF-8 JSON: {exc}") from exc


def _mapping(value: Any, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise BenchmarkBundleError(f"{label} must be an object")
    return value


def _sequence(value: Any, label: str) -> Sequence[Any]:
    if isinstance(value, (str, bytes)) or not isinstance(value, Sequence):
        raise BenchmarkBundleError(f"{label} must be an array")
    return value


def _required(value: Mapping[str, Any], key: str, label: str) -> Any:
    if key not in value:
        raise BenchmarkBundleError(f"missing required field: {label}.{key}")
    return value[key]


def _fixed_object(
    value: Any,
    label: str,
    required: set[str],
    *,
    optional: set[str] | None = None,
) -> Mapping[str, Any]:
    payload = _mapping(value, label)
    optional_fields = optional or set()
    missing = required - set(payload)
    if missing:
        raise BenchmarkBundleError(
            f"missing required field: {label}.{sorted(missing)[0]}"
        )
    unknown = set(payload) - required - optional_fields
    if unknown:
        raise BenchmarkBundleError(
            f"unknown field in {label}: {sorted(unknown)[0]}"
        )
    return payload


def _string(value: Any, label: str, *, non_empty: bool = False) -> str:
    if not isinstance(value, str):
        raise BenchmarkBundleError(f"{label} must be a string")
    if non_empty and not value.strip():
        raise BenchmarkBundleError(f"{label} must be a non-empty string")
    return value


def _integer(
    value: Any,
    label: str,
    *,
    minimum: int | None = None,
    maximum: int | None = None,
) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise BenchmarkBundleError(f"{label} must be an integer")
    if minimum is not None and value < minimum:
        raise BenchmarkBundleError(f"{label} must be >= {minimum}")
    if maximum is not None and value > maximum:
        raise BenchmarkBundleError(f"{label} must be <= {maximum}")
    return value


def _canonical_relative_path(raw: Any, label: str) -> str:
    if not isinstance(raw, str) or not raw or "\x00" in raw:
        raise BenchmarkBundleError(f"{label} must be a non-empty path without NUL")
    if "\\" in raw:
        raise BenchmarkBundleError(f"{label} must use canonical POSIX separators")
    posix = PurePosixPath(raw)
    windows = PureWindowsPath(raw)
    if posix.is_absolute() or windows.is_absolute() or windows.drive:
        raise BenchmarkBundleError(f"{label} must be relative and have no drive or UNC root")
    if posix.as_posix() != raw or any(part in {"", ".", ".."} for part in posix.parts):
        raise BenchmarkBundleError(f"{label} must be a canonical relative path")
    return raw


class _BundleReader:
    def __init__(self, root: Path):
        supplied = root.expanduser()
        if not supplied.exists() or not supplied.is_dir():
            raise BenchmarkBundleError(f"bundle directory does not exist: {supplied}")
        if supplied.is_symlink():
            raise BenchmarkBundleError("bundle directory must not be a symlink")
        self.root = supplied.resolve(strict=True)
        self._reject_reparse(self.root, "bundle directory")
        self._case_paths: dict[str, str] = {}
        self._descriptors: dict[str, dict[str, Any]] = {}
        self._bytes: dict[str, bytes] = {}

    @staticmethod
    def _reject_reparse(path: Path, label: str) -> None:
        try:
            metadata = os.lstat(path)
        except OSError as exc:
            raise BenchmarkBundleError(f"cannot inspect {label}: {exc}") from exc
        reparse_flag = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0)
        attributes = getattr(metadata, "st_file_attributes", 0)
        if stat.S_ISLNK(metadata.st_mode) or (reparse_flag and attributes & reparse_flag):
            raise BenchmarkBundleError(f"{label} must not be a symlink or reparse point")

    def _register_path(self, relative: str, label: str) -> Path:
        relative = _canonical_relative_path(relative, label)
        collision_key = unicodedata.normalize("NFC", relative).casefold()
        previous = self._case_paths.get(collision_key)
        if previous is not None and previous != relative:
            raise BenchmarkBundleError(
                f"case-normalized path collision between {previous!r} and {relative!r}"
            )
        self._case_paths[collision_key] = relative

        candidate = self.root
        for part in PurePosixPath(relative).parts:
            candidate = candidate / part
            self._reject_reparse(candidate, f"bundle path {relative!r}")
        try:
            resolved = candidate.resolve(strict=True)
            resolved.relative_to(self.root)
        except (OSError, ValueError) as exc:
            raise BenchmarkBundleError(
                f"bundle path escapes the bundle or is missing: {relative!r}"
            ) from exc
        try:
            metadata = os.lstat(resolved)
        except OSError as exc:
            raise BenchmarkBundleError(f"cannot inspect bundle file {relative!r}: {exc}") from exc
        if not stat.S_ISREG(metadata.st_mode):
            raise BenchmarkBundleError(f"bundle path is not a regular file: {relative!r}")
        return resolved

    def read_core(self, relative: str) -> bytes:
        path = self._register_path(relative, relative)
        try:
            return path.read_bytes()
        except OSError as exc:
            raise BenchmarkBundleError(f"failed to read {relative}: {exc}") from exc

    def verify_descriptor(self, raw: Any, label: str) -> bytes:
        descriptor = _mapping(raw, label)
        if set(descriptor) != {"path", "sha256", "bytes"}:
            raise BenchmarkBundleError(
                f"{label} must contain only path, sha256, and bytes"
            )
        relative = _canonical_relative_path(descriptor["path"], f"{label}.path")
        digest = descriptor["sha256"]
        byte_count = descriptor["bytes"]
        if (
            not isinstance(digest, str)
            or len(digest) != 64
            or any(character not in "0123456789abcdef" for character in digest)
        ):
            raise BenchmarkBundleError(f"{label}.sha256 must be lowercase SHA-256 hex")
        if isinstance(byte_count, bool) or not isinstance(byte_count, int) or byte_count < 0:
            raise BenchmarkBundleError(f"{label}.bytes must be a non-negative integer")
        normalized = {"path": relative, "sha256": digest, "bytes": byte_count}
        previous = self._descriptors.get(relative)
        if previous is not None:
            if previous != normalized:
                raise BenchmarkBundleError(
                    f"conflicting descriptors reference bundle path {relative!r}"
                )
            return self._bytes[relative]

        path = self._register_path(relative, f"{label}.path")
        try:
            data = path.read_bytes()
        except OSError as exc:
            raise BenchmarkBundleError(f"failed to read descriptor path {relative!r}: {exc}") from exc
        actual_digest = hashlib.sha256(data).hexdigest()
        if len(data) != byte_count:
            raise BenchmarkBundleError(f"descriptor byte count mismatch for {relative!r}")
        if actual_digest != digest:
            raise BenchmarkBundleError(f"descriptor SHA-256 mismatch for {relative!r}")
        self._descriptors[relative] = normalized
        self._bytes[relative] = data
        return data

    def descriptor_bytes(self, descriptor: Mapping[str, Any]) -> bytes:
        relative = str(descriptor["path"])
        if relative not in self._bytes:
            raise BenchmarkBundleError(f"descriptor was not prevalidated: {relative!r}")
        return self._bytes[relative]


def _descriptor_objects(value: Any, label: str) -> Iterable[tuple[Mapping[str, Any], str]]:
    if isinstance(value, Mapping):
        if {"path", "sha256", "bytes"}.issubset(value):
            yield value, label
            return
        for key, nested in value.items():
            yield from _descriptor_objects(nested, f"{label}.{key}")
    elif isinstance(value, list):
        for index, nested in enumerate(value):
            yield from _descriptor_objects(nested, f"{label}[{index}]")


def _decode_text(data: bytes, label: str) -> str:
    try:
        return data.decode("utf-8", errors="strict")
    except UnicodeDecodeError as exc:
        raise BenchmarkBundleError(f"{label} is not UTF-8 text") from exc


def _verify_phase(
    *,
    reader: _BundleReader,
    role: str,
    phase_name: str,
    phase: Mapping[str, Any],
    index: int,
    policy: Any,
) -> dict[str, Any]:
    artifacts = _mapping(_required(phase, "artifacts", "phase"), "phase.artifacts")
    expected_dir = f"raw/rep-{index:02d}/{role}/{phase_name}"
    stdout_descriptor = _mapping(
        _required(artifacts, "stdout", "phase.artifacts"), "phase.artifacts.stdout"
    )
    stderr_descriptor = _mapping(
        _required(artifacts, "stderr", "phase.artifacts"), "phase.artifacts.stderr"
    )
    if stdout_descriptor.get("path") != f"{expected_dir}/pgbench.stdout.log":
        raise BenchmarkBundleError(f"unexpected {role} {phase_name} stdout path")
    if stderr_descriptor.get("path") != f"{expected_dir}/pgbench.stderr.log":
        raise BenchmarkBundleError(f"unexpected {role} {phase_name} stderr path")
    log_descriptors = _sequence(
        _required(artifacts, "transaction_logs", "phase.artifacts"),
        "phase.artifacts.transaction_logs",
    )
    if not log_descriptors:
        raise BenchmarkBundleError(f"{role} {phase_name} has no transaction logs")
    log_sources: list[tuple[str, str]] = []
    seen_log_paths: set[str] = set()
    for raw_descriptor in log_descriptors:
        descriptor = _mapping(raw_descriptor, "transaction log descriptor")
        raw_path = descriptor.get("path")
        if not isinstance(raw_path, str):
            raise BenchmarkBundleError("transaction log descriptor path is invalid")
        parsed_path = PurePosixPath(raw_path)
        if parsed_path.parent.as_posix() != expected_dir or not parsed_path.name.startswith(
            "transactions"
        ):
            raise BenchmarkBundleError(f"unexpected {role} {phase_name} transaction log path")
        if raw_path in seen_log_paths:
            raise BenchmarkBundleError(
                f"duplicate {role} {phase_name} transaction log descriptor path"
            )
        seen_log_paths.add(raw_path)
        log_sources.append(
            (
                raw_path,
                _decode_text(reader.descriptor_bytes(descriptor), raw_path),
            )
        )

    stdout = _decode_text(
        reader.descriptor_bytes(stdout_descriptor), str(stdout_descriptor["path"])
    )
    parsed_stdout = parse_pgbench_stdout(stdout)
    summary = parse_pgbench_transaction_log_texts(log_sources)
    if parsed_stdout.scale_factor != policy.scale_factor:
        raise BenchmarkBundleError(f"{role} {phase_name} scale does not match policy")
    if parsed_stdout.clients != policy.clients:
        raise BenchmarkBundleError(f"{role} {phase_name} clients do not match policy")
    if summary.transactions_successful != parsed_stdout.transactions_successful:
        raise BenchmarkBundleError(f"{role} {phase_name} successful counts disagree")
    if summary.transactions_failed != parsed_stdout.transactions_failed:
        raise BenchmarkBundleError(f"{role} {phase_name} failed counts disagree")

    elapsed_key = "actual_seconds" if phase_name == "warmup" else "actual_measurement_seconds"
    elapsed_raw = _required(phase, elapsed_key, f"{role}.{phase_name}")
    if isinstance(elapsed_raw, bool) or not isinstance(elapsed_raw, (int, float)):
        raise BenchmarkBundleError(f"{role} {phase_name} {elapsed_key} is invalid")
    elapsed = float(elapsed_raw)
    if not math.isfinite(elapsed) or elapsed <= 0:
        raise BenchmarkBundleError(f"{role} {phase_name} {elapsed_key} must be positive")

    if phase_name == "warmup":
        expected = {
            "index": index,
            "actual_seconds": elapsed_raw,
            "transactions_successful": summary.transactions_successful,
            "transactions_failed": summary.transactions_failed,
            "artifacts": dict(artifacts),
        }
        if dict(phase) != expected:
            raise BenchmarkBundleError(f"{role} warmup {index} does not match raw evidence")
        return expected

    try:
        expected = build_repetition_metrics(
            index=index,
            latencies_us=summary.latencies_us,
            transactions_successful=summary.transactions_successful,
            transactions_failed=summary.transactions_failed,
            actual_measurement_seconds=elapsed,
            artifacts=artifacts,
            reported_tps=parsed_stdout.reported_tps,
            reported_latency_mean_ms=parsed_stdout.reported_latency_mean_ms,
            max_tps_relative_delta=policy.max_tps_relative_delta,
            failure_types=summary.failure_types,
        )
    except BenchmarkResultError as exc:
        raise BenchmarkBundleError(
            f"{role} measurement {index} is invalid: {exc}"
        ) from exc
    if dict(phase) != expected:
        raise BenchmarkBundleError(
            f"{role} measurement {index} does not match raw evidence"
        )
    return expected


def _verify_environment(
    environment: Mapping[str, Any],
    baseline: Mapping[str, Any],
    candidate: Mapping[str, Any],
    policy: Any,
) -> tuple[dict[str, Any], str]:
    environment = _fixed_object(
        environment,
        "environment",
        {
            "schema_version",
            "status",
            "driftbench",
            "python",
            "platform",
            "connection",
            "postgresql",
            "pgbench",
        },
    )
    if environment["schema_version"] != "1.0":
        raise BenchmarkBundleError("environment schema_version must be 1.0")
    if environment["status"] != "complete":
        raise BenchmarkBundleError("environment status must be complete")
    driftbench = _fixed_object(
        environment["driftbench"],
        "environment.driftbench",
        {"version", "source_sha", "source_state", "source_sha_source"},
        optional={"source_sha_assertion"},
    )
    _string(
        driftbench["version"],
        "environment.driftbench.version",
        non_empty=True,
    )
    source_sha = driftbench["source_sha"]
    if (
        not isinstance(source_sha, str)
        or len(source_sha) != 40
        or any(ch not in "0123456789abcdef" for ch in source_sha)
    ):
        raise BenchmarkBundleError(
            "environment DriftBench source SHA must be full lowercase 40-character hex"
        )
    if driftbench.get("source_state") != "clean":
        raise BenchmarkBundleError("environment DriftBench source_state must be clean")
    if driftbench.get("source_sha_source") != "git_head":
        raise BenchmarkBundleError(
            "environment DriftBench source_sha_source must be git_head"
        )
    if "source_sha_assertion" in driftbench and driftbench.get(
        "source_sha_assertion"
    ) != "DRIFTBENCH_GIT_SHA":
        raise BenchmarkBundleError(
            "environment DriftBench source SHA assertion is invalid"
        )
    if baseline.get("git_sha") != source_sha or candidate.get("git_sha") != source_sha:
        raise BenchmarkBundleError("result source SHA does not match environment")

    python = _fixed_object(
        environment["python"],
        "environment.python",
        {"version", "implementation"},
    )
    _string(python["version"], "environment.python.version", non_empty=True)
    _string(
        python["implementation"],
        "environment.python.implementation",
        non_empty=True,
    )

    platform = _fixed_object(
        environment["platform"],
        "environment.platform",
        {"os", "system", "release", "machine", "cpu", "logical_cpu_count"},
    )
    for field in ("os", "system", "release", "machine", "cpu"):
        _string(platform[field], f"environment.platform.{field}")
    logical_cpu_count = platform["logical_cpu_count"]
    if logical_cpu_count is not None:
        _integer(
            logical_cpu_count,
            "environment.platform.logical_cpu_count",
            minimum=1,
        )

    connection = _fixed_object(
        environment["connection"],
        "environment.connection",
        {"database", "host", "port", "username"},
    )
    _string(
        connection["database"],
        "environment.connection.database",
        non_empty=True,
    )
    _string(connection["host"], "environment.connection.host")
    _integer(connection["port"], "environment.connection.port", minimum=1, maximum=65535)
    _string(connection["username"], "environment.connection.username")

    postgresql = _fixed_object(
        environment["postgresql"],
        "environment.postgresql",
        {"full", "major", "current_database", "settings", "initialization"},
    )
    _string(postgresql["full"], "environment.postgresql.full", non_empty=True)
    postgresql_major = _integer(
        postgresql["major"], "environment.postgresql.major", minimum=1
    )
    if postgresql_major != policy.postgresql_major:
        raise BenchmarkBundleError(
            "environment PostgreSQL major version does not match policy"
        )
    _string(
        postgresql["current_database"],
        "environment.postgresql.current_database",
        non_empty=True,
    )
    settings = _fixed_object(
        postgresql["settings"],
        "environment.postgresql.settings",
        set(_POSTGRESQL_SETTINGS),
    )
    for name in _POSTGRESQL_SETTINGS:
        setting = _fixed_object(
            settings[name],
            f"environment.postgresql.settings.{name}",
            {"setting", "unit", "source"},
        )
        _string(
            setting["setting"],
            f"environment.postgresql.settings.{name}.setting",
        )
        unit = setting["unit"]
        if unit is not None:
            _string(unit, f"environment.postgresql.settings.{name}.unit")
        _string(
            setting["source"],
            f"environment.postgresql.settings.{name}.source",
        )

    initialization = _fixed_object(
        postgresql["initialization"],
        "environment.postgresql.initialization",
        {
            "pgbench_branches",
            "pgbench_tellers",
            "pgbench_accounts",
            "pgbench_history",
            "scale_factor_inferred",
        },
    )
    for field in initialization:
        _integer(
            initialization[field],
            f"environment.postgresql.initialization.{field}",
            minimum=0,
        )

    pgbench = _fixed_object(
        environment["pgbench"],
        "environment.pgbench",
        {"full", "major"},
    )
    _string(pgbench["full"], "environment.pgbench.full", non_empty=True)
    pgbench_major = _integer(pgbench["major"], "environment.pgbench.major", minimum=1)
    if pgbench_major != policy.pgbench_major:
        raise BenchmarkBundleError(
            "environment pgbench major version does not match policy"
        )
    versions = {
        "postgresql": {
            "full": postgresql["full"],
            "major": postgresql_major,
        },
        "pgbench": {"full": pgbench["full"], "major": pgbench_major},
    }
    if baseline.get("versions") != versions or candidate.get("versions") != versions:
        raise BenchmarkBundleError("result versions do not match environment")
    expected_initialization = {
        "pgbench_branches": policy.scale_factor,
        "pgbench_tellers": policy.scale_factor * 10,
        "pgbench_accounts": policy.scale_factor * 100_000,
        "pgbench_history": 0,
        "scale_factor_inferred": policy.scale_factor,
    }
    if dict(initialization) != expected_initialization:
        raise BenchmarkBundleError("environment pgbench initialization does not match policy")
    if connection["database"] != postgresql["current_database"]:
        raise BenchmarkBundleError("environment connection database is inconsistent")

    def reject_password(value: Any, label: str) -> None:
        if isinstance(value, Mapping):
            for key, nested in value.items():
                if "password" in str(key).casefold():
                    raise BenchmarkBundleError(f"password material found at {label}.{key}")
                reject_password(nested, f"{label}.{key}")
        elif isinstance(value, list):
            for index, nested in enumerate(value):
                reject_password(nested, f"{label}[{index}]")

    reject_password(environment, "environment")
    return versions, source_sha


def verify_pgbench_bundle(bundle: str | Path) -> BenchmarkBundleVerification:
    """Verify a pgbench result bundle without database or network access."""

    reader = _BundleReader(Path(bundle))
    core_bytes = {
        name: reader.read_core(name)
        for name in (
            "baseline.json",
            "candidate.json",
            "decision.json",
            "execution_order.json",
        )
    }
    core = {
        name: _strict_json_bytes(data, name) for name, data in core_bytes.items()
    }
    baseline = _mapping(core["baseline.json"], "baseline.json")
    candidate = _mapping(core["candidate.json"], "candidate.json")
    decision = _mapping(core["decision.json"], "decision.json")
    execution_order = _mapping(core["execution_order.json"], "execution_order.json")

    for name, payload in core.items():
        for descriptor, label in _descriptor_objects(payload, name):
            reader.verify_descriptor(descriptor, label)

    try:
        inputs = _mapping(baseline.get("inputs"), "baseline.inputs")
        policy_descriptor = _mapping(inputs.get("policy"), "baseline.inputs.policy")
        candidate_descriptor = _mapping(
            inputs.get("candidate_script"), "baseline.inputs.candidate_script"
        )
        environment_descriptor = _mapping(
            baseline.get("environment"), "baseline.environment"
        )
        policy_bytes = reader.descriptor_bytes(policy_descriptor)
        candidate_bytes = reader.descriptor_bytes(candidate_descriptor)
        environment_bytes = reader.descriptor_bytes(environment_descriptor)
    except (KeyError, TypeError) as exc:
        raise BenchmarkBundleError(f"bundle input descriptors are invalid: {exc}") from exc
    if not candidate_bytes:
        raise BenchmarkBundleError("candidate input snapshot is empty")

    policy_payload = _strict_json_bytes(policy_bytes, "inputs/policy.json")
    try:
        policy = parse_pgbench_policy_payload(policy_payload)
    except BenchmarkPolicyError as exc:
        raise BenchmarkBundleError(f"bundle policy is invalid: {exc}") from exc
    if policy_bytes != serialize_pgbench_policy(policy).encode("utf-8"):
        raise BenchmarkBundleError("bundle policy is not the canonical policy snapshot")
    environment_payload = _mapping(
        _strict_json_bytes(environment_bytes, "environment.json"),
        "environment.json",
    )
    if environment_bytes != _canonical_json_bytes(environment_payload):
        raise BenchmarkBundleError("environment.json is not canonical JSON")

    try:
        versions, source_sha = _verify_environment(
            environment_payload, baseline, candidate, policy
        )
        expected_inputs = dict(inputs)
        if candidate.get("inputs") != expected_inputs:
            raise BenchmarkBundleError("baseline/candidate input descriptors differ")
        if candidate.get("environment") != dict(environment_descriptor):
            raise BenchmarkBundleError("baseline/candidate environment descriptors differ")

        expected_results: dict[str, dict[str, Any]] = {}
        for role, result in (("baseline", baseline), ("candidate", candidate)):
            warmups_raw = _sequence(result.get("warmups"), f"{role}.warmups")
            repetitions_raw = _sequence(
                result.get("repetitions"), f"{role}.repetitions"
            )
            if len(warmups_raw) != policy.repetitions or len(repetitions_raw) != policy.repetitions:
                raise BenchmarkBundleError(f"{role} phase count does not match policy")
            warmups = [
                _verify_phase(
                    reader=reader,
                    role=role,
                    phase_name="warmup",
                    phase=_mapping(raw, f"{role}.warmups[{index - 1}]"),
                    index=index,
                    policy=policy,
                )
                for index, raw in enumerate(warmups_raw, start=1)
            ]
            repetitions = [
                _verify_phase(
                    reader=reader,
                    role=role,
                    phase_name="measurement",
                    phase=_mapping(raw, f"{role}.repetitions[{index - 1}]"),
                    index=index,
                    policy=policy,
                )
                for index, raw in enumerate(repetitions_raw, start=1)
            ]
            expected_results[role] = _result(
                role=role,
                policy=policy,
                versions=versions,
                git_sha=source_sha,
                script_sha256=str(candidate_descriptor["sha256"]),
                inputs=expected_inputs,
                environment=environment_descriptor,
                warmups=warmups,
                repetitions=repetitions,
            )
            if dict(result) != expected_results[role]:
                raise BenchmarkBundleError(
                    f"{role}.json does not match the result reconstructed from raw evidence"
                )
            if core_bytes[f"{role}.json"] != _canonical_json_bytes(expected_results[role]):
                raise BenchmarkBundleError(f"{role}.json is not canonical reconstructed JSON")
    except BenchmarkResultError as exc:
        raise BenchmarkBundleError(f"benchmark result reconstruction failed: {exc}") from exc

    expected_decision = evaluate_regression(
        expected_results["baseline"],
        expected_results["candidate"],
        policy,
        baseline_path="baseline.json",
        candidate_path="candidate.json",
    )
    if dict(decision) != expected_decision:
        raise BenchmarkBundleError("decision.json does not match the verified policy decision")
    if core_bytes["decision.json"] != _canonical_json_bytes(expected_decision):
        raise BenchmarkBundleError("decision.json is not canonical verified JSON")

    pairs = build_paired_execution_plan(policy)
    completed: list[dict[str, Any]] = []
    for pair in pairs:
        for role in pair["order"]:
            completed.append({"index": pair["index"], "role": role, "phase": "warmup"})
            completed.append(
                {"index": pair["index"], "role": role, "phase": "measurement"}
            )
    expected_order = {
        "schema_version": "1.0",
        "policy_version": policy.policy_version,
        "strategy": policy.execution_order,
        "status": "completed",
        "pairs": pairs,
        "completed": completed,
        "inputs": expected_inputs,
        "environment": dict(environment_descriptor),
    }
    if dict(execution_order) != expected_order:
        raise BenchmarkBundleError("execution_order.json does not match the policy plan")
    if core_bytes["execution_order.json"] != _canonical_json_bytes(expected_order):
        raise BenchmarkBundleError("execution_order.json is not canonical verified JSON")

    return BenchmarkBundleVerification(bundle=reader.root, decision=expected_decision)
