"""Offline-only planner for the review-gated TPC-H cardinality study."""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any

import yaml


ROOT = Path(__file__).resolve().parent
PLAN_PATH = ROOT / "tpch_plan.yaml"
REGISTRY_PATH = ROOT / "baselines.yaml"
REVIEW_ONLY_EXIT = 78
REVISION = re.compile(r"^[0-9a-f]{40}$")


class PlanError(ValueError):
    """Raised when the offline plan or registry violates its contract."""


def _load_yaml(path: Path) -> dict[str, Any]:
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise PlanError(f"{path.name} must contain a YAML mapping")
    return data


def load_and_validate() -> tuple[dict[str, Any], dict[str, Any]]:
    plan = _load_yaml(PLAN_PATH)
    registry = _load_yaml(REGISTRY_PATH)
    if plan.get("experiment_id") != "rel-ce-tpch" or plan.get("review_only") is not True:
        raise PlanError("TPC-H CE plan must be review-only and use experiment_id rel-ce-tpch")
    if plan.get("dataset", {}).get("scale_factor") != 1.0:
        raise PlanError("active TPC-H CE plan must use SF1")
    probes = plan.get("workload", {}).get("probes")
    if not isinstance(probes, list) or not probes:
        raise PlanError("TPC-H CE plan must declare at least one provenance-bearing SPJ probe")
    probe_ids = [item.get("id") for item in probes if isinstance(item, dict)]
    if len(probe_ids) != len(probes) or len(probe_ids) != len(set(probe_ids)):
        raise PlanError("TPC-H CE probe IDs must be present and unique")
    if any(not item.get("provenance") or not item.get("subquery_id") for item in probes):
        raise PlanError("every TPC-H CE probe needs template/subquery provenance")
    estimators = registry.get("estimators")
    if not isinstance(estimators, list) or not estimators:
        raise PlanError("estimator registry must contain entries")
    estimator_ids = [item.get("id") for item in estimators if isinstance(item, dict)]
    if len(estimator_ids) != len(estimators) or len(estimator_ids) != len(set(estimator_ids)):
        raise PlanError("estimator IDs must be present and unique")
    groups = registry.get("groups")
    if not isinstance(groups, dict) or not groups:
        raise PlanError("estimator registry must define baseline groups")
    group_ids = list(groups)
    plan_registry = plan.get("baseline_registry", {})
    plan_group_ids = plan_registry.get("group_ids")
    if plan_registry.get("path") != REGISTRY_PATH.name:
        raise PlanError("TPC-H CE plan must reference baselines.yaml")
    if not isinstance(plan_group_ids, list) or len(plan_group_ids) != len(set(plan_group_ids)):
        raise PlanError("TPC-H CE plan baseline group IDs must be present and unique")
    if plan_group_ids != group_ids:
        raise PlanError("TPC-H CE plan baseline groups must match the registry")
    anchors = registry.get("selection_policy", {}).get("active_plan_anchors", [])
    if not anchors or not set(anchors).issubset(estimator_ids):
        raise PlanError("all active plan anchors must exist in the estimator registry")
    for estimator in estimators:
        required = {
            "paper", "venue", "year", "source", "native_datasets",
            "query_shape_support", "training_or_update_regime",
            "hardware_expectation", "tpch_compatibility", "adapter_status", "blocker",
        }
        missing = sorted(required - set(estimator))
        if missing:
            raise PlanError(f"{estimator['id']}: missing registry fields {missing}")
        if estimator.get("group") not in groups:
            raise PlanError(f"{estimator['id']}: baseline group is not defined by the registry")
        source = estimator["source"]
        if not isinstance(source, dict):
            raise PlanError(f"{estimator['id']}: source must be a mapping")
        repository = source.get("repository")
        revision = source.get("revision")
        if repository is not None and not REVISION.fullmatch(str(revision)):
            raise PlanError(f"{estimator['id']}: public code needs an immutable 40-character revision")
        if repository is None and revision is not None:
            raise PlanError(f"{estimator['id']}: a revision without a repository is invalid")
        if estimator["adapter_status"] == "verified":
            raise PlanError(f"{estimator['id']}: this review-only registry cannot claim a verified adapter")
    return plan, registry


def normalized_plan() -> dict[str, Any]:
    plan, registry = load_and_validate()
    return {
        "baseline_registry": {
            "as_of": registry["as_of"],
            "claim_boundary": registry["claim_boundary"],
            "estimators": registry["estimators"],
            "excluded_from_active_multi_table_set": registry["excluded_from_active_multi_table_set"],
            "groups": registry["groups"],
            "selection_policy": registry["selection_policy"],
        },
        "experiment": plan,
    }


def command_plan(_: argparse.Namespace) -> int:
    print(json.dumps(normalized_plan(), ensure_ascii=False, sort_keys=True, indent=2))
    return 0


def command_validate(_: argparse.Namespace) -> int:
    plan, registry = load_and_validate()
    print(
        f"PASS: {plan['experiment_id']} offline plan; "
        f"{len(plan['workload']['probes'])} probes; {len(registry['estimators'])} estimator entries"
    )
    print("No Docker command, database, generator, query, or estimator ran.")
    return 0


def command_run(args: argparse.Namespace) -> int:
    _, registry = load_and_validate()
    known = {entry["id"]: entry for entry in registry["estimators"]}
    selected = [part.strip() for part in args.estimators.split(",") if part.strip()]
    unknown = sorted(set(selected) - set(known))
    if unknown:
        print(f"BLOCKED: unknown estimator(s): {', '.join(unknown)}", file=sys.stderr)
        return 2
    unready = [item for item in selected if known[item]["adapter_status"] != "planned-anchor"]
    if unready:
        details = "; ".join(f"{item}: {known[item]['blocker']}" for item in unready)
        print(f"ESTIMATOR_NOT_RUNNABLE: {details}", file=sys.stderr)
        print("No side effect occurred; use 'plan' to inspect the research candidate.", file=sys.stderr)
        return REVIEW_ONLY_EXIT
    print(
        "REVIEW_ONLY: TPC-H preparation, PostgreSQL, and estimator execution are disabled. "
        "No side effect occurred.",
        file=sys.stderr,
    )
    return REVIEW_ONLY_EXIT


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    plan = sub.add_parser("plan", help="emit the normalized review-only plan as deterministic JSON")
    plan.set_defaults(func=command_plan)
    validate = sub.add_parser("validate", help="validate the local plan and estimator registry")
    validate.set_defaults(func=command_validate)
    run = sub.add_parser("run", help="always blocked; validates estimator readiness before returning 78")
    run.add_argument("--estimators", default="postgres-default,postgres-extended-statistics")
    run.set_defaults(func=command_run)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return int(args.func(args))
    except (OSError, PlanError, yaml.YAMLError) as exc:
        print(f"BLOCKED: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
