"""Guarded entry point for the review-only DriftBench experiment lab."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

import yaml

LAB_ROOT = Path(__file__).resolve().parents[1]
REPOSITORY_ROOT = LAB_ROOT.parents[1]
sys.path.insert(0, str(LAB_ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from audit import audit  # noqa: E402
from dashboard.server import discover_runs, serve  # noqa: E402


def _catalog() -> dict[str, Any]:
    return yaml.safe_load((LAB_ROOT / "catalog.yaml").read_text(encoding="utf-8"))


def known_experiment_ids() -> set[str]:
    return {entry["id"] for entry in _catalog()["experiments"]}


def validate_experiment_selection(value: str, known: set[str] | None = None) -> str:
    known = known or known_experiment_ids()
    if not value or value.strip() != value or "," in value or " " in value:
        raise ValueError("select exactly one experiment ID; lists and whitespace are not accepted")
    if value not in known:
        raise ValueError(f"unknown experiment ID: {value}")
    return value


def _manifest(experiment_id: str) -> dict[str, Any]:
    experiment_id = validate_experiment_selection(experiment_id)
    entry = next(item for item in _catalog()["experiments"] if item["id"] == experiment_id)
    return yaml.safe_load((LAB_ROOT / entry["manifest"]).read_text(encoding="utf-8"))


def _outside_repository(path: Path) -> Path:
    resolved = path.expanduser().resolve()
    try:
        resolved.relative_to(REPOSITORY_ROOT.resolve())
    except ValueError:
        return resolved
    raise ValueError("state root must be outside the DriftBench repository")


def command_audit(_: argparse.Namespace) -> int:
    errors, digest, manifests = audit()
    if errors:
        print(f"BLOCKED: {len(errors)} policy violation(s)", file=sys.stderr)
        for error in errors:
            print(f"- {error}", file=sys.stderr)
        return 1
    print(f"PASS: {len(manifests)} review-only experiment plans")
    print(f"Configuration SHA-256: {digest}")
    print("Audit was local and parser-only; no Docker or benchmark command ran.")
    return 0


def command_plan(args: argparse.Namespace) -> int:
    try:
        manifest = _manifest(args.experiment_id)
    except ValueError as exc:
        print(f"BLOCKED: {exc}", file=sys.stderr)
        return 2
    print(f"{manifest['id']} — {manifest['family']} / {manifest['component']}")
    print(f"Tracker status: {manifest['tracker']['repository_status']}")
    print(f"Readiness: {manifest['readiness']} | review_only={str(manifest['review_only']).lower()}")
    print(f"Target stack: {manifest['target_stack']['kind']} ({manifest['target_stack']['planned_containers']} planned container(s))")
    print(f"Dataset: {manifest['dataset']['name']}")
    print(f"Seed: {manifest['workload'].get('seed')} | resource tier: {manifest['resources']['tier']}")
    print(f"Run method: {manifest['execution']['run']}")
    print("Evidence paths:")
    for name, path in manifest["artifacts"].items():
        print(f"  {name}: {path}")
    print("Blockers:")
    for blocker in manifest["blockers"]:
        print(f"  - {blocker}")
    print("DEPLOYMENT DISABLED: a later reviewed implementation slice is required.")
    return 0


def command_status(args: argparse.Namespace) -> int:
    try:
        state_root = _outside_repository(args.state_root)
    except ValueError as exc:
        print(f"BLOCKED: {exc}", file=sys.stderr)
        return 2
    rows = discover_runs(state_root)
    if not rows:
        print("No runs found.")
        return 0
    for row in rows:
        progress = "?" if row["progress"] is None else f"{row['progress'] * 100:.0f}%"
        print(f"{row['experiment_id']}/{row['run_id']} {row['state']} {progress} {row['phase']}")
    return 0


def command_dashboard(args: argparse.Namespace) -> int:
    try:
        state_root = _outside_repository(args.state_root)
    except ValueError as exc:
        print(f"BLOCKED: {exc}", file=sys.stderr)
        return 2
    serve(state_root, "127.0.0.1", args.port)
    return 0


def command_review_only(args: argparse.Namespace) -> int:
    try:
        experiment_id = validate_experiment_selection(args.experiment_id)
    except ValueError as exc:
        print(f"BLOCKED: {exc}", file=sys.stderr)
        return 2
    print(
        f"REVIEW_ONLY: '{args.command}' is disabled for {experiment_id}. "
        "No Docker command was executed. Read REVIEW_CHECKLIST.md and open a new implementation slice.",
        file=sys.stderr,
    )
    return 78


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    audit_parser = sub.add_parser("audit", help="run the local parser/policy audit")
    audit_parser.set_defaults(func=command_audit)
    plan_parser = sub.add_parser("plan", help="show one human-readable experiment plan")
    plan_parser.add_argument("experiment_id")
    plan_parser.set_defaults(func=command_plan)
    status_parser = sub.add_parser("status", help="read saved run status without Docker")
    status_parser.add_argument("--state-root", type=Path, required=True)
    status_parser.set_defaults(func=command_status)
    dashboard_parser = sub.add_parser("dashboard", help="serve the read-only local progress page")
    dashboard_parser.add_argument("--state-root", type=Path, required=True)
    dashboard_parser.add_argument("--port", type=int, default=8765)
    dashboard_parser.set_defaults(func=command_dashboard)
    for name in ("prepare", "run", "stop", "destroy", "grafana-export"):
        blocked = sub.add_parser(name, help="disabled until a later approved implementation slice")
        blocked.add_argument("experiment_id")
        blocked.set_defaults(func=command_review_only)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
