#!/usr/bin/env python3
"""Fail-closed validators shared by release preparation and tag publishing."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import re
import sys
from pathlib import Path
from typing import Any, Mapping

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - exercised by the Python 3.10 job
    import tomli as tomllib


REQUIRED_WORKFLOWS = (
    "CI",
    "Benchmark Regression",
    "CLI Contract",
    "Schema and Spec Validation",
    "Content Safety Check",
)
_FULL_SHA_RE = re.compile(r"^[0-9a-f]{40}$")
_TAG_RE = re.compile(r"^v(?P<version>[0-9A-Za-z][0-9A-Za-z.!+_-]*)$")


class ReleaseGateError(ValueError):
    """Raised when release evidence does not satisfy the frozen gate."""


def validate_release_metadata(
    *, expected_tag: str, pyproject_text: str, changelog_text: str
) -> dict[str, str]:
    tag_match = _TAG_RE.fullmatch(expected_tag)
    if tag_match is None:
        raise ReleaseGateError(
            "release branch must derive a version tag such as v0.1.0b10"
        )
    expected_version = tag_match.group("version")
    try:
        project = tomllib.loads(pyproject_text)
    except (tomllib.TOMLDecodeError, ValueError) as exc:
        raise ReleaseGateError(f"pyproject.toml is invalid: {exc}") from exc
    actual_version = project.get("project", {}).get("version")
    if actual_version != expected_version:
        raise ReleaseGateError(
            f"pyproject.toml version must be {expected_version}, got {actual_version!r}"
        )

    heading_pattern = re.compile(
        rf"^## \[{re.escape(expected_tag)}\] - (?P<date>\d{{4}}-\d{{2}}-\d{{2}})$",
        re.MULTILINE,
    )
    heading = heading_pattern.search(changelog_text)
    if heading is None:
        raise ReleaseGateError(
            f"CHANGELOG.md must contain exact dated heading for {expected_tag}"
        )
    try:
        dt.date.fromisoformat(heading.group("date"))
    except ValueError as exc:
        raise ReleaseGateError(
            f"CHANGELOG.md heading for {expected_tag} has an invalid date"
        ) from exc

    section_start = heading.end()
    next_heading = re.search(r"^## \[", changelog_text[section_start:], re.MULTILINE)
    section_end = (
        len(changelog_text)
        if next_heading is None
        else section_start + next_heading.start()
    )
    section = changelog_text[section_start:section_end]
    if re.search(r"^### Services\s*$", section, re.MULTILINE) is None:
        raise ReleaseGateError(
            f"CHANGELOG.md section for {expected_tag} must include ### Services"
        )
    if re.search(r"^### (?:Added|Changed|Fixed)\s*$", section, re.MULTILINE) is None:
        raise ReleaseGateError(
            f"CHANGELOG.md section for {expected_tag} must include Added, Changed, or Fixed"
        )
    return {
        "expected_tag": expected_tag,
        "expected_version": expected_version,
        "release_date": heading.group("date"),
    }


def validate_workflow_runs(
    *, payload: Mapping[str, Any], source_sha: str
) -> dict[str, Mapping[str, Any]]:
    if _FULL_SHA_RE.fullmatch(source_sha) is None:
        raise ReleaseGateError("SOURCE_SHA must be full lowercase 40-character hex")
    runs = payload.get("workflow_runs")
    if not isinstance(runs, list):
        raise ReleaseGateError("GitHub Actions response must contain workflow_runs array")

    selected: dict[str, Mapping[str, Any]] = {}
    for raw in runs:
        if not isinstance(raw, Mapping) or raw.get("head_sha") != source_sha:
            continue
        name = raw.get("name")
        if name in REQUIRED_WORKFLOWS and name not in selected:
            selected[str(name)] = raw

    missing = [name for name in REQUIRED_WORKFLOWS if name not in selected]
    bad = [
        name
        for name, run in selected.items()
        if run.get("status") != "completed" or run.get("conclusion") != "success"
    ]
    if missing or bad:
        details: list[str] = []
        if missing:
            details.append("missing exact-SHA workflows: " + ", ".join(missing))
        if bad:
            details.append("non-success exact-SHA workflows: " + ", ".join(bad))
        raise ReleaseGateError("; ".join(details))
    return selected


def validate_source_unchanged(*, source_sha: str, latest_source_sha: str) -> str:
    if _FULL_SHA_RE.fullmatch(source_sha) is None or _FULL_SHA_RE.fullmatch(
        latest_source_sha
    ) is None:
        raise ReleaseGateError("source ref checks require full lowercase commit SHAs")
    if latest_source_sha != source_sha:
        raise ReleaseGateError(
            f"source branch advanced after validation: {source_sha} -> {latest_source_sha}"
        )
    return source_sha


def _metadata_command(args: argparse.Namespace) -> None:
    result = validate_release_metadata(
        expected_tag=args.expected_tag,
        pyproject_text=Path(args.pyproject).read_text(encoding="utf-8"),
        changelog_text=Path(args.changelog).read_text(encoding="utf-8"),
    )
    print(json.dumps({"ok": True, **result}, sort_keys=True))


def _runs_command(args: argparse.Namespace) -> None:
    with Path(args.runs_json).open("r", encoding="utf-8") as source:
        payload = json.load(source)
    selected = validate_workflow_runs(payload=payload, source_sha=args.source_sha)
    print(
        json.dumps(
            {"ok": True, "source_sha": args.source_sha, "workflows": sorted(selected)},
            sort_keys=True,
        )
    )


def _source_command(args: argparse.Namespace) -> None:
    source_sha = validate_source_unchanged(
        source_sha=args.source_sha,
        latest_source_sha=args.latest_source_sha,
    )
    print(json.dumps({"ok": True, "source_sha": source_sha}, sort_keys=True))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    commands = parser.add_subparsers(dest="command", required=True)
    metadata = commands.add_parser("metadata")
    metadata.add_argument("--expected-tag", required=True)
    metadata.add_argument("--pyproject", required=True)
    metadata.add_argument("--changelog", required=True)
    metadata.set_defaults(handler=_metadata_command)
    runs = commands.add_parser("runs")
    runs.add_argument("--source-sha", required=True)
    runs.add_argument("--runs-json", required=True)
    runs.set_defaults(handler=_runs_command)
    source = commands.add_parser("source")
    source.add_argument("--source-sha", required=True)
    source.add_argument("--latest-source-sha", required=True)
    source.set_defaults(handler=_source_command)
    args = parser.parse_args(argv)
    try:
        args.handler(args)
    except (OSError, json.JSONDecodeError, ReleaseGateError) as exc:
        print(f"release gate failed: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
