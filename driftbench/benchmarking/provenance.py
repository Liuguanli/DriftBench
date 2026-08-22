"""Canonical benchmark inputs and DriftBench source provenance."""

from __future__ import annotations

import hashlib
import json
import os
import re
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any


SOURCE_SHA_UNAVAILABLE = "unavailable"
SOURCE_STATE_CLEAN = "clean"
SOURCE_SHA_SOURCE_GIT_HEAD = "git_head"
DRIFTBENCH_SOURCE_PATHS = (
    "driftbench",
    "driftbench_service",
    "driftbench_mcp",
    "pyproject.toml",
)
_FULL_GIT_SHA_RE = re.compile(r"^[0-9a-fA-F]{40}$")


@dataclass(frozen=True)
class DriftBenchSourceIdentity:
    """Auditable identity for a clean DriftBench source checkout."""

    source_sha: str
    source_state: str
    source_sha_source: str
    repository_root: Path
    override_asserted: bool = False

    def environment_fields(self) -> dict[str, Any]:
        fields: dict[str, Any] = {
            "source_sha": self.source_sha,
            "source_state": self.source_state,
            "source_sha_source": self.source_sha_source,
        }
        if self.override_asserted:
            fields["source_sha_assertion"] = "DRIFTBENCH_GIT_SHA"
        return fields


class SourceProvenanceError(RuntimeError):
    """Raised when a clean, immutable DriftBench checkout cannot be proven."""

    def __init__(
        self,
        message: str,
        *,
        source_sha: str = SOURCE_SHA_UNAVAILABLE,
        source_state: str = "unavailable",
        source_sha_source: str = SOURCE_SHA_UNAVAILABLE,
    ) -> None:
        super().__init__(message)
        self.source_sha = source_sha
        self.source_state = source_state
        self.source_sha_source = source_sha_source

    def environment_fields(self) -> dict[str, str]:
        return {
            "source_sha": self.source_sha,
            "source_state": self.source_state,
            "source_sha_source": self.source_sha_source,
        }


def canonical_pgbench_policy_payload(policy: Any) -> dict[str, Any]:
    """Return the normalized JSON representation of a loaded pgbench policy."""

    return {
        "schema_version": policy.schema_version,
        "policy_version": policy.policy_version,
        "benchmark": "pgbench",
        "workload": policy.workload,
        "execution_order": policy.execution_order,
        "versions": {
            "postgresql_major": policy.postgresql_major,
            "pgbench_major": policy.pgbench_major,
        },
        "config": policy.run_config(),
        "thresholds": {
            "min_tps_ratio": policy.min_tps_ratio,
            "max_p95_latency_ratio": policy.max_p95_latency_ratio,
            "max_error_rate": policy.max_error_rate,
            "max_tps_relative_delta": policy.max_tps_relative_delta,
        },
    }


def serialize_pgbench_policy(policy: Any) -> str:
    """Serialize a loaded policy deterministically for an evidence snapshot."""

    return (
        json.dumps(
            canonical_pgbench_policy_payload(policy),
            indent=2,
            sort_keys=True,
            ensure_ascii=False,
            allow_nan=False,
        )
        + "\n"
    )


def pgbench_policy_sha256(policy: Any) -> str:
    """Return the digest of the canonical policy snapshot bytes."""

    return hashlib.sha256(serialize_pgbench_policy(policy).encode("utf-8")).hexdigest()


def inspect_driftbench_source(
    *, package_root: str | Path | None = None
) -> DriftBenchSourceIdentity:
    """Require a clean DriftBench checkout and return its immutable identity.

    Only the Git root containing this DriftBench package is inspected.  This
    deliberately refuses to borrow a consumer repository SHA for an installed
    wheel.  ``DRIFTBENCH_GIT_SHA`` is an assertion against the real checkout
    HEAD, never a replacement for Git or the clean-source check.
    """

    root = (
        Path(package_root).expanduser().resolve()
        if package_root is not None
        else Path(__file__).resolve().parents[2]
    )
    if not (root / ".git").exists():
        raise SourceProvenanceError(
            "DriftBench source Git metadata is unavailable; run the benchmark from "
            "a clean DriftBench checkout"
        )

    def run_git(arguments: list[str]) -> subprocess.CompletedProcess[str]:
        try:
            completed = subprocess.run(
                ["git", *arguments],
                cwd=root,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                check=False,
                timeout=15,
            )
        except Exception as exc:
            raise SourceProvenanceError(
                f"failed to inspect DriftBench source provenance: {exc}"
            ) from exc
        if completed.returncode != 0:
            detail = completed.stderr.strip() or completed.stdout.strip()
            suffix = f": {detail}" if detail else ""
            raise SourceProvenanceError(
                f"failed to inspect DriftBench source provenance{suffix}"
            )
        return completed

    top_level = run_git(["rev-parse", "--show-toplevel"])
    reported_root = top_level.stdout.strip()
    if not reported_root or Path(reported_root).resolve() != root:
        raise SourceProvenanceError(
            "DriftBench package root is not the root of its own Git checkout"
        )

    revision = run_git(["rev-parse", "--verify", "HEAD^{commit}"])
    source_sha = revision.stdout.strip().lower()
    if not _FULL_GIT_SHA_RE.fullmatch(source_sha):
        raise SourceProvenanceError(
            "DriftBench Git HEAD is not a full 40-character commit SHA",
            source_sha=source_sha or SOURCE_SHA_UNAVAILABLE,
            source_state="invalid_head",
            source_sha_source=SOURCE_SHA_SOURCE_GIT_HEAD,
        )

    # GITHUB_SHA is intentionally ignored: in a wheel consumer's workflow it
    # identifies the caller repository, not the installed DriftBench source.
    explicit = os.environ.get("DRIFTBENCH_GIT_SHA", "").strip()
    if explicit:
        if not _FULL_GIT_SHA_RE.fullmatch(explicit):
            raise SourceProvenanceError(
                "DRIFTBENCH_GIT_SHA must be a full 40-character hexadecimal SHA",
                source_sha=source_sha,
                source_state="invalid_override",
                source_sha_source=SOURCE_SHA_SOURCE_GIT_HEAD,
            )
        if explicit.lower() != source_sha:
            raise SourceProvenanceError(
                "DRIFTBENCH_GIT_SHA does not match the DriftBench checkout HEAD",
                source_sha=source_sha,
                source_state="override_mismatch",
                source_sha_source=SOURCE_SHA_SOURCE_GIT_HEAD,
            )

    status = run_git(
        [
            "status",
            "--porcelain=v1",
            "--untracked-files=all",
            "--",
            *DRIFTBENCH_SOURCE_PATHS,
        ]
    )
    dirty = status.stdout.strip()
    if dirty:
        details = "; ".join(line.strip() for line in dirty.splitlines()[:10])
        raise SourceProvenanceError(
            f"DriftBench runtime source is dirty: {details}",
            source_sha=source_sha,
            source_state="dirty",
            source_sha_source=SOURCE_SHA_SOURCE_GIT_HEAD,
        )

    return DriftBenchSourceIdentity(
        source_sha=source_sha,
        source_state=SOURCE_STATE_CLEAN,
        source_sha_source=SOURCE_SHA_SOURCE_GIT_HEAD,
        repository_root=root,
        override_asserted=bool(explicit),
    )


def driftbench_source_sha(*, package_root: str | Path | None = None) -> str:
    """Return the full SHA of a clean DriftBench source checkout."""

    return inspect_driftbench_source(package_root=package_root).source_sha
