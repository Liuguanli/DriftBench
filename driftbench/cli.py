from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

from driftbench.agent_init import init_agent_directory
from driftbench.bootstrap import BootstrapError, bootstrap_dataset
from driftbench.orchestrate import TargetConfigError, orchestrate_targets
from driftbench.spec.core import (
    ensure_handlers_loaded,
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
    ensure_handlers_loaded()
    triple = get_type_triple(spec)
    # Ensure a runnable handler exists for the declared type.
    get_handler(triple)
    return spec, triple


def _cmd_run_yaml(args: argparse.Namespace) -> int:
    run_yaml_all(args.spec)
    print(f"[OK] run-yaml completed: {args.spec}")
    return EXIT_OK


def _cmd_trace_to_spec(args: argparse.Namespace) -> int:
    output = args.output_opt or args.output
    if not output:
        raise CLIError("trace-to-spec requires an output path (positional OUTPUT or --output).")
    spec = trace_to_spec(
        args.trace,
        output,
        trace_type=args.trace_type,
        mapping_path=args.mapping,
    )
    pattern_id = spec.get("pattern_id", "")
    print(f"[OK] trace-to-spec generated: {output} (pattern_id={pattern_id})")
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


def _cmd_init_agent(args: argparse.Namespace) -> int:
    result = init_agent_directory(
        output_dir=args.output,
        force=bool(args.force),
        dry_run=bool(args.dry_run),
    )

    if result.dry_run:
        print(f"[DRY-RUN] would initialize DriftBench agent files under: {result.output_dir}")
    else:
        print(f"[OK] initialized DriftBench agent files under: {result.output_dir}")

    for path in result.created_files:
        rel = path.relative_to(result.output_dir).as_posix()
        print(f"- {rel}")
    return EXIT_OK


def _cmd_orchestrate(args: argparse.Namespace) -> int:
    _validate_and_resolve(args.spec)
    manifest = orchestrate_targets(
        spec_path=args.spec,
        targets_file=args.targets,
        manifest_path=args.manifest_out,
        execute=bool(args.execute),
    )
    _emit(
        {
            "ok": True,
            "command": "orchestrate",
            "spec_path": manifest["spec_path"],
            "targets_file": manifest["targets_file"],
            "manifest_path": str(Path(args.manifest_out).expanduser().resolve()),
            "execute": manifest["execute"],
            "summary": manifest["summary"],
        },
        as_json=args.json,
    )
    return EXIT_OK


def _cmd_bootstrap_dataset(args: argparse.Namespace) -> int:
    result = bootstrap_dataset(
        source=args.source,
        output_dir=args.output_dir,
        filename=args.filename,
        checksum=args.checksum,
        sample_size=args.sample_size,
        schema_out=args.schema_out,
    )
    _emit(
        {
            "ok": True,
            "command": "bootstrap dataset",
            "source_kind": result.source_kind,
            "source": result.source_value,
            "dataset_path": str(result.output_dataset),
            "schema_path": str(result.output_schema),
            "sha256": result.sha256,
            "sample_size": result.sample_size,
            "schema_summary": result.schema_summary,
        },
        as_json=args.json,
    )
    return EXIT_OK


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser("driftbench-db")
    sub = parser.add_subparsers(dest="cmd", required=True)

    y = sub.add_parser("run-yaml", help="Run a DriftSpec YAML")
    y.add_argument("spec", help="Path to YAML spec")
    y.set_defaults(func=_cmd_run_yaml)

    t = sub.add_parser("trace-to-spec", help="Generate a DriftSpec YAML from a trace summary")
    t.add_argument("trace", help="Path to trace summary (CSV or JSON)")
    t.add_argument("output", nargs="?", help="Path to output DriftSpec YAML")
    t.add_argument("--output", dest="output_opt", help="Path to output DriftSpec YAML")
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

    ia = sub.add_parser(
        "init-agent",
        help="Generate DriftBench agent support files in the current project",
    )
    ia.add_argument(
        "--output",
        default="./driftbench-agent",
        help="Output directory (default: ./driftbench-agent)",
    )
    ia.add_argument(
        "--force",
        action="store_true",
        help="Overwrite managed generated files if the output directory is not empty",
    )
    ia.add_argument(
        "--dry-run",
        action="store_true",
        help="Print files that would be generated without writing",
    )
    ia.set_defaults(func=_cmd_init_agent)

    orch = sub.add_parser(
        "orchestrate",
        help="Run one DriftSpec suite across multiple benchmark targets (MVP)",
    )
    orch.add_argument("--spec", required=True, help="Path to DriftSpec YAML")
    orch.add_argument("--targets", required=True, help="Path to benchmark_target YAML config")
    orch.add_argument(
        "--manifest-out",
        default="output/orchestrate_manifest.json",
        help="Path to output manifest JSON",
    )
    orch.add_argument(
        "--execute",
        action="store_true",
        help="Execute setup/run commands (default: plan-only dry orchestration)",
    )
    orch.add_argument("--json", action="store_true", help="Emit machine-readable JSON output")
    orch.set_defaults(func=_cmd_orchestrate)

    boot = sub.add_parser(
        "bootstrap",
        help="Bootstrap datasets/workloads for faster onboarding",
    )
    boot_sub = boot.add_subparsers(dest="bootstrap_cmd", required=True)

    boot_data = boot_sub.add_parser(
        "dataset",
        help="Bootstrap dataset from preset/local path/URL and extract schema",
    )
    boot_data.add_argument(
        "--source",
        required=True,
        help="Dataset source (preset name, local path, or http/https URL)",
    )
    boot_data.add_argument(
        "--output-dir",
        default="output/bootstrap/datasets",
        help="Output directory for downloaded/copied dataset",
    )
    boot_data.add_argument("--filename", help="Optional output dataset filename override")
    boot_data.add_argument(
        "--checksum",
        help="Optional SHA-256 checksum (hex or sha256:<hex>) for integrity verification",
    )
    boot_data.add_argument(
        "--sample-size",
        type=int,
        default=1000,
        help="CSV sample size for schema extraction (0 = full file)",
    )
    boot_data.add_argument(
        "--schema-out",
        help="Optional explicit schema output path (default: <output-dir>/<stem>_schema.json)",
    )
    boot_data.add_argument("--json", action="store_true", help="Emit machine-readable JSON output")
    boot_data.set_defaults(func=_cmd_bootstrap_dataset)

    return parser


def main(argv: List[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return int(args.func(args))
    except (ValueError, FileNotFoundError, TargetConfigError, BootstrapError) as exc:
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
