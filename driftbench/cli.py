from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

import driftbench.spec.types  # ensure handlers registered
from driftbench.spec.core import (
    get_type_triple,
    load_spec,
    migrate_spec,
    run_all as run_yaml_all,
    seed_everything,
    validate_spec,
)
from driftbench.spec.registry import get_handler
from driftbench.spec.trace_spec import trace_to_spec


EXIT_OK = 0
EXIT_VALIDATION_ERROR = 3
EXIT_RUNTIME_ERROR = 4


class CLIError(Exception):
    def __init__(self, message: str, exit_code: int = EXIT_RUNTIME_ERROR):
        super().__init__(message)
        self.exit_code = exit_code


def _emit(data: Dict[str, Any], as_json: bool) -> None:
    if as_json:
        print(json.dumps(data, ensure_ascii=False, indent=2))
        return
    for key, value in data.items():
        print(f"{key}: {value}")


def _collect_declared_outputs(obj: Any) -> List[str]:
    found: List[str] = []

    def walk(x: Any) -> None:
        if isinstance(x, dict):
            for k, v in x.items():
                if k == "output_path" and isinstance(v, str):
                    found.append(v)
                else:
                    walk(v)
        elif isinstance(x, list):
            for item in x:
                walk(item)

    walk(obj)
    return sorted(set(found))


def _validate_and_resolve(spec_path: str) -> Tuple[Dict[str, Any], Tuple[str, str, str]]:
    spec = load_spec(spec_path)
    spec = migrate_spec(spec)
    validate_spec(spec)
    triple = get_type_triple(spec)
    # Ensure a runnable handler exists for the declared type.
    get_handler(triple)
    return spec, triple


def _cmd_run_yaml(args: argparse.Namespace) -> int:
    run_yaml_all(args.spec)
    print(f"[OK] run-yaml completed: {args.spec}")
    return EXIT_OK


def _cmd_trace_to_spec(args: argparse.Namespace) -> int:
    spec = trace_to_spec(
        args.trace,
        args.output,
        trace_type=args.trace_type,
        mapping_path=args.mapping,
    )
    pattern_id = spec.get("pattern_id", "")
    print(f"[OK] trace-to-spec generated: {args.output} (pattern_id={pattern_id})")
    return EXIT_OK


def _cmd_validate_spec(args: argparse.Namespace) -> int:
    spec, triple = _validate_and_resolve(args.spec)
    seed_everything(spec.get("seed"))
    _emit(
        {
            "ok": True,
            "command": "validate-spec",
            "spec_path": args.spec,
            "pattern_id": spec.get("pattern_id", ""),
            "type": ".".join(triple),
            "declared_outputs": len(_collect_declared_outputs(spec)),
        },
        as_json=args.json,
    )
    return EXIT_OK


def _cmd_dry_run(args: argparse.Namespace) -> int:
    spec, triple = _validate_and_resolve(args.spec)
    outputs = _collect_declared_outputs(spec)
    summary = {
        "ok": True,
        "command": "dry-run",
        "spec_path": args.spec,
        "pattern_id": spec.get("pattern_id", ""),
        "type": {
            "family": triple[0],
            "category": triple[1],
            "subtype": triple[2],
        },
        "seed": spec.get("seed"),
        "declared_outputs": outputs,
        "variables_keys": sorted((spec.get("variables") or {}).keys()),
        "would_execute": True,
    }
    _emit(summary, as_json=args.json)
    return EXIT_OK


def _iter_paths(root: Path, pattern: str) -> Iterable[Path]:
    # Path.glob already supports recursive patterns like **/*.
    yield from root.glob(pattern)


def _cmd_list_outputs(args: argparse.Namespace) -> int:
    root = Path(args.root).expanduser()
    if not root.is_absolute():
        root = Path.cwd() / root
    root = root.resolve()

    if not root.exists():
        _emit(
            {
                "ok": True,
                "command": "list-outputs",
                "root": str(root),
                "count": 0,
                "paths": [],
                "note": "root path does not exist",
            },
            as_json=args.json,
        )
        return EXIT_OK

    if not root.is_dir():
        raise CLIError(f"Root path is not a directory: {root}")

    paths: List[str] = []
    for path in _iter_paths(root, args.glob):
        if not path.exists():
            continue
        if not args.include_dirs and not path.is_file():
            continue
        rel = os.path.relpath(path, Path.cwd())
        paths.append(rel)

    paths = sorted(set(paths))
    if args.limit is not None and args.limit >= 0:
        paths = paths[: args.limit]

    _emit(
        {
            "ok": True,
            "command": "list-outputs",
            "root": str(root),
            "count": len(paths),
            "paths": paths,
        },
        as_json=args.json,
    )
    return EXIT_OK


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser("driftbench")
    sub = parser.add_subparsers(dest="cmd", required=True)

    y = sub.add_parser("run-yaml", help="Run a DriftSpec YAML")
    y.add_argument("spec", help="Path to YAML spec")
    y.set_defaults(func=_cmd_run_yaml)

    t = sub.add_parser("trace-to-spec", help="Generate a DriftSpec YAML from a trace summary")
    t.add_argument("trace", help="Path to trace summary (CSV or JSON)")
    t.add_argument("output", help="Path to output DriftSpec YAML")
    t.add_argument("--trace-type", choices=["data", "workload"], help="Override trace_type inference")
    t.add_argument("--mapping", help="Optional mapping JSON for trace column selection")
    t.set_defaults(func=_cmd_trace_to_spec)

    v = sub.add_parser(
        "validate-spec",
        help="Validate a DriftSpec and ensure a runnable handler is registered",
    )
    v.add_argument("spec", help="Path to YAML spec")
    v.add_argument("--json", action="store_true", help="Emit machine-readable JSON output")
    v.set_defaults(func=_cmd_validate_spec)

    d = sub.add_parser(
        "dry-run",
        help="Validate and summarize a spec without executing handlers",
    )
    d.add_argument("spec", help="Path to YAML spec")
    d.add_argument("--json", action="store_true", help="Emit machine-readable JSON output")
    d.set_defaults(func=_cmd_dry_run)

    lo = sub.add_parser(
        "list-outputs",
        help="List generated output files for inspection/automation",
    )
    lo.add_argument("--root", default="output", help="Root directory to scan (default: output)")
    lo.add_argument("--glob", default="**/*", help="Glob pattern relative to root (default: **/*)")
    lo.add_argument("--limit", type=int, default=200, help="Max number of paths to return")
    lo.add_argument("--include-dirs", action="store_true", help="Include directories in results")
    lo.add_argument("--json", action="store_true", help="Emit machine-readable JSON output")
    lo.set_defaults(func=_cmd_list_outputs)

    return parser


def main(argv: List[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return int(args.func(args))
    except (ValueError, FileNotFoundError) as exc:
        print(f"[VALIDATION ERROR] {exc}", file=sys.stderr)
        return EXIT_VALIDATION_ERROR
    except CLIError as exc:
        print(f"[ERROR] {exc}", file=sys.stderr)
        return exc.exit_code
    except Exception as exc:
        print(f"[ERROR] {exc}", file=sys.stderr)
        return EXIT_RUNTIME_ERROR


if __name__ == "__main__":
    raise SystemExit(main())
