from __future__ import annotations

import argparse
import contextlib
import io
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

from driftbench.agent_init import init_agent_directory
from driftbench.benchmarking.pgbench import (
    PgBenchConnection,
    PgBenchExecutionError,
    run_paired_pgbench,
)
from driftbench.benchmarking.policy import (
    BenchmarkPolicyError,
    load_pgbench_policy,
)
from driftbench.benchmarking.verify import BenchmarkBundleError, verify_pgbench_bundle
from driftbench.bootstrap import BootstrapError, bootstrap_dataset
from driftbench.console import console_print
from driftbench.orchestrate import TargetConfigError, orchestrate_targets
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
from driftbench.spec.preflight import deep_validate_spec_file
from driftbench.spec.trace_spec import trace_to_spec


EXIT_OK = 0
EXIT_VALIDATION_ERROR = 3
EXIT_RUNTIME_ERROR = 4
EXIT_REGRESSION_FAILURE = 5


class CLIError(Exception):
    def __init__(self, message: str, exit_code: int = EXIT_RUNTIME_ERROR):
        super().__init__(message)
        self.exit_code = exit_code


def _emit(data: Dict[str, Any], as_json: bool) -> None:
    if as_json:
        # ASCII JSON remains valid on legacy Windows consoles (for example
        # CP1252), including non-BMP characters that Python would otherwise
        # backslash-escape as the non-JSON ``\Uxxxxxxxx`` form.
        console_print(json.dumps(data, ensure_ascii=True, allow_nan=False, indent=2))
        return
    for key, value in data.items():
        console_print(f"{key}: {value}")


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
    console_print(f"[OK] run-yaml completed: {args.spec}")
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
    console_print(f"[OK] trace-to-spec generated: {output} (pattern_id={pattern_id})")
    return EXIT_OK


def _cmd_validate_spec(args: argparse.Namespace) -> int:
    if bool(getattr(args, "deep", False)):
        try:
            report = deep_validate_spec_file(args.spec)
            payload = report.as_dict()
        except Exception:
            # Deep JSON validation is an automation boundary: unexpected
            # validator defects are reported without traceback or exception
            # text, which could contain secrets from a spec or environment.
            payload = {
                "ok": False,
                "outcome": "validator_error",
                "command": "validate-spec",
                "spec_path": args.spec,
                "pattern_id": "",
                "type": "",
                "declared_outputs": 0,
                "mode": "deep",
                "valid": False,
                "locally_ready": False,
                "checks": [],
                "issues": [
                    {
                        "severity": "error",
                        "code": "validator_internal_error",
                        "field": "$",
                        "message": "Deep validation could not be completed.",
                        "hint": "Report this validator defect without attaching secrets.",
                    }
                ],
                "summary": {
                    "status": "validator_error",
                    "errors": 1,
                    "warnings": 0,
                    "checks_passed": 0,
                    "checks_failed": 1,
                },
            }
            _emit(payload, as_json=args.json)
            return EXIT_RUNTIME_ERROR
        _emit(payload, as_json=args.json)
        return EXIT_OK if report.valid else EXIT_VALIDATION_ERROR

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
        console_print(f"[DRY-RUN] would initialize DriftBench agent files under: {result.output_dir}")
    else:
        console_print(f"[OK] initialized DriftBench agent files under: {result.output_dir}")

    for path in result.created_files:
        rel = path.relative_to(result.output_dir).as_posix()
        console_print(f"- {rel}")
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
            "ok": manifest["ok"],
            "outcome": manifest["outcome"],
            "command": "orchestrate",
            "spec_path": manifest["spec_path"],
            "targets_file": manifest["targets_file"],
            "manifest_path": str(Path(args.manifest_out).expanduser().resolve()),
            "execute": manifest["execute"],
            "summary": manifest["summary"],
        },
        as_json=args.json,
    )
    return EXIT_OK if manifest["ok"] else EXIT_RUNTIME_ERROR


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


def _benchmark_result_paths(output_dir: str | Path) -> Dict[str, str]:
    root = Path(output_dir).expanduser().resolve()
    return {
        "output_dir": str(root),
        "baseline_result": str(root / "baseline.json"),
        "candidate_result": str(root / "candidate.json"),
        "decision": str(root / "decision.json"),
        "execution_order": str(root / "execution_order.json"),
    }


def _emit_benchmark_error(
    args: argparse.Namespace,
    *,
    outcome: str,
    message: str,
    exit_code: int,
) -> int:
    payload: Dict[str, Any] = {
        "ok": False,
        "outcome": outcome,
        "command": "benchmark pgbench",
        "error": message,
        **_benchmark_result_paths(args.output_dir),
    }
    if args.json:
        _emit(payload, as_json=True)
    else:
        console_print(f"[ERROR] {message}", file=sys.stderr)
        _emit({key: value for key, value in payload.items() if key != "error"}, as_json=False)
    return exit_code


def _cmd_benchmark_pgbench(args: argparse.Namespace) -> int:
    if not str(args.database).strip():
        return _emit_benchmark_error(
            args,
            outcome="configuration_error",
            message="database must be non-empty",
            exit_code=EXIT_VALIDATION_ERROR,
        )
    if args.port < 1 or args.port > 65535:
        return _emit_benchmark_error(
            args,
            outcome="configuration_error",
            message="port must be between 1 and 65535",
            exit_code=EXIT_VALIDATION_ERROR,
        )
    try:
        policy = load_pgbench_policy(args.policy)
    except BenchmarkPolicyError as exc:
        return _emit_benchmark_error(
            args,
            outcome="configuration_error",
            message=str(exc),
            exit_code=EXIT_VALIDATION_ERROR,
        )

    try:
        result = run_paired_pgbench(
            policy=policy,
            candidate_script=args.candidate_script,
            output_dir=args.output_dir,
            connection=PgBenchConnection(
                database=str(args.database).strip(),
                host=args.host,
                port=args.port,
                username=args.username,
            ),
            pgbench_binary=args.pgbench_binary,
        )
    except PgBenchExecutionError as exc:
        return _emit_benchmark_error(
            args,
            outcome="execution_error",
            message=str(exc),
            exit_code=EXIT_RUNTIME_ERROR,
        )

    payload = {
        "ok": result.ok,
        "outcome": "passed" if result.ok else "threshold_failed",
        "command": "benchmark pgbench",
        **_benchmark_result_paths(args.output_dir),
    }
    _emit(payload, as_json=args.json)
    return EXIT_OK if result.ok else EXIT_REGRESSION_FAILURE


def _cmd_benchmark_verify(args: argparse.Namespace) -> int:
    supplied = Path(args.bundle).expanduser()
    if not supplied.exists() or not supplied.is_dir():
        payload = {
            "verified": False,
            "ok": False,
            "outcome": "configuration_error",
            "command": "benchmark verify",
            "bundle": str(supplied.resolve()),
            "error": f"bundle directory does not exist: {supplied}",
        }
        _emit(payload, as_json=args.json)
        return EXIT_VALIDATION_ERROR
    try:
        verification = verify_pgbench_bundle(supplied)
    except BenchmarkBundleError as exc:
        payload = {
            "verified": False,
            "ok": False,
            "outcome": "verification_error",
            "command": "benchmark verify",
            "bundle": str(supplied.resolve()),
            "error": str(exc),
        }
        if args.json:
            _emit(payload, as_json=True)
        else:
            console_print(f"[ERROR] {exc}", file=sys.stderr)
            _emit({key: value for key, value in payload.items() if key != "error"}, False)
        return EXIT_RUNTIME_ERROR

    payload = verification.payload()
    _emit(payload, as_json=args.json)
    return EXIT_OK if verification.ok else EXIT_REGRESSION_FAILURE


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
    v.add_argument(
        "--deep",
        action="store_true",
        help="Run a read-only local readiness preflight (files, outputs, and adapters)",
    )
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

    benchmark = sub.add_parser(
        "benchmark",
        help="Run a reproducible benchmark regression gate",
    )
    benchmark_sub = benchmark.add_subparsers(dest="benchmark_cmd", required=True)
    pgbench = benchmark_sub.add_parser(
        "pgbench",
        help="Compare a native pgbench baseline with a DriftBench SQL candidate",
    )
    pgbench.add_argument(
        "--policy",
        default=str(
            Path(__file__).resolve().parent
            / "benchmarking"
            / "policies"
            / "pgbench_ci_v1.json"
        ),
        help="Path to the version-controlled pgbench regression policy",
    )
    pgbench.add_argument(
        "--candidate-script",
        required=True,
        help="Path to the DriftBench-generated pgbench SQL script",
    )
    pgbench.add_argument(
        "--output-dir",
        default="benchmark-artifacts/results",
        help="Directory for metrics, decisions, execution order, and raw logs",
    )
    pgbench.add_argument("--database", required=True, help="PostgreSQL database name")
    pgbench.add_argument("--host", default="localhost", help="PostgreSQL host")
    pgbench.add_argument("--port", type=int, default=5432, help="PostgreSQL port")
    pgbench.add_argument("--username", default="postgres", help="PostgreSQL user")
    pgbench.add_argument(
        "--pgbench-binary",
        default="pgbench",
        help="pgbench executable name or path",
    )
    pgbench.add_argument("--json", action="store_true", help="Emit one JSON document")
    pgbench.set_defaults(func=_cmd_benchmark_pgbench)

    verify = benchmark_sub.add_parser(
        "verify",
        help="Verify a pgbench evidence bundle without database or network access",
    )
    verify.add_argument("--bundle", required=True, help="Path to a pgbench result bundle")
    verify.add_argument("--json", action="store_true", help="Emit one JSON document")
    verify.set_defaults(func=_cmd_benchmark_verify)

    return parser


def _benchmark_json_error_payload(
    argv: List[str] | None,
    *,
    message: str,
    outcome: str,
) -> Dict[str, Any] | None:
    arguments = list(sys.argv[1:] if argv is None else argv)
    if "--json" not in arguments or arguments[:1] != ["benchmark"]:
        return None
    subcommand = arguments[1] if len(arguments) > 1 else "unknown"
    command = f"benchmark {subcommand}"
    payload: Dict[str, Any] = {
        "ok": False,
        "outcome": outcome,
        "command": command,
        "error": message,
    }
    if subcommand == "verify":
        payload["verified"] = False
        if "--bundle" in arguments:
            index = arguments.index("--bundle") + 1
            if index < len(arguments):
                payload["bundle"] = str(Path(arguments[index]).expanduser().resolve())
    return payload


def _deep_validate_json_error_payload(
    argv: List[str] | None,
    *,
    internal: bool,
) -> Dict[str, Any] | None:
    """Return a redacted JSON error for failures before the deep command runs."""

    arguments = list(sys.argv[1:] if argv is None else argv)
    if not _deep_json_requested(arguments):
        return None
    issue = {
        "severity": "error",
        "code": "validator_internal_error" if internal else "cli_argument_invalid",
        "field": "$" if internal else "spec_path",
        "message": (
            "Deep validation could not be completed."
            if internal
            else "The deep-validation command arguments are invalid."
        ),
        "hint": (
            "Report this validator defect without attaching secrets."
            if internal
            else "Pass one DriftSpec path followed by --deep --json."
        ),
    }
    status = "validator_error" if internal else "not_ready"
    return {
        "ok": False,
        "outcome": status,
        "command": "validate-spec",
        # Parsing did not necessarily establish which token is the operand.
        # Keep it redacted instead of guessing and echoing an option value.
        "spec_path": "",
        "pattern_id": "",
        "type": "",
        "declared_outputs": 0,
        "mode": "deep",
        "valid": False,
        "locally_ready": False,
        "checks": [],
        "issues": [issue],
        "summary": {
            "status": status,
            "errors": 1,
            "warnings": 0,
            "checks_passed": 0,
            "checks_failed": 1,
        },
    }


def _option_requested(arguments: List[str], canonical: str) -> bool:
    """Match argparse's unambiguous long-option abbreviation behavior."""

    for token in arguments:
        if token == "--":
            break
        option = token.split("=", 1)[0]
        if option.startswith("--") and len(option) > 2 and canonical.startswith(option):
            return True
    return False


def _deep_json_requested(arguments: List[str]) -> bool:
    return (
        arguments[:1] == ["validate-spec"]
        and _option_requested(arguments[1:], "--deep")
        and _option_requested(arguments[1:], "--json")
    )


def _benchmark_json_requested(arguments: List[str]) -> bool:
    return arguments[:1] == ["benchmark"] and _option_requested(
        arguments[1:], "--json"
    )


def main(argv: List[str] | None = None) -> int:
    parser = build_parser()
    arguments = list(sys.argv[1:] if argv is None else argv)
    try:
        structured_parse_error = _deep_json_requested(
            arguments
        ) or _benchmark_json_requested(arguments)
        if structured_parse_error:
            # Stock argparse writes usage to stderr before raising SystemExit.
            # Suppress that only for commands that explicitly request a
            # single machine-readable JSON document.
            with contextlib.redirect_stderr(io.StringIO()):
                try:
                    args = parser.parse_args(arguments)
                except SystemExit as exc:
                    if exc.code == 0:
                        raise
                    payload = _deep_validate_json_error_payload(
                        arguments, internal=False
                    )
                    if payload is None:
                        payload = _benchmark_json_error_payload(
                            arguments,
                            message="The command arguments are invalid.",
                            outcome="configuration_error",
                        )
                    if payload is not None:
                        _emit(payload, as_json=True)
                    return EXIT_VALIDATION_ERROR
        else:
            try:
                args = parser.parse_args(arguments)
            except SystemExit as exc:
                # Benchmark commands were added with an explicit exit-code
                # contract.  Existing commands retain stock argparse
                # SystemExit/usage behavior.
                if arguments[:1] == ["benchmark"] and exc.code != 0:
                    return EXIT_VALIDATION_ERROR
                raise
        return int(args.func(args))
    except (ValueError, FileNotFoundError, TargetConfigError, BootstrapError) as exc:
        payload = _deep_validate_json_error_payload(argv, internal=False)
        if payload is None:
            payload = _benchmark_json_error_payload(
                argv, message=str(exc), outcome="configuration_error"
            )
        if payload is not None:
            _emit(payload, as_json=True)
            return EXIT_VALIDATION_ERROR
        console_print(f"[VALIDATION ERROR] {exc}", file=sys.stderr)
        return EXIT_VALIDATION_ERROR
    except CLIError as exc:
        payload = _deep_validate_json_error_payload(
            argv, internal=exc.exit_code != EXIT_VALIDATION_ERROR
        )
        if payload is None:
            payload = _benchmark_json_error_payload(
                argv,
                message=str(exc),
                outcome=(
                    "configuration_error"
                    if exc.exit_code == EXIT_VALIDATION_ERROR
                    else "execution_error"
                ),
            )
        if payload is not None:
            _emit(payload, as_json=True)
            return exc.exit_code
        console_print(f"[ERROR] {exc}", file=sys.stderr)
        return exc.exit_code
    except Exception as exc:
        payload = _deep_validate_json_error_payload(argv, internal=True)
        if payload is None:
            payload = _benchmark_json_error_payload(
                argv, message=str(exc), outcome="execution_error"
            )
        if payload is not None:
            _emit(payload, as_json=True)
            return EXIT_RUNTIME_ERROR
        console_print(f"[ERROR] {exc}", file=sys.stderr)
        return EXIT_RUNTIME_ERROR


if __name__ == "__main__":
    raise SystemExit(main())
