from __future__ import annotations

import json
import os
import platform
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List

import yaml


@dataclass(frozen=True)
class BenchmarkTarget:
    name: str
    workdir: Path
    repo_url: str | None
    ref: str | None
    setup_command: str | None
    run_command: str
    output_globs: List[str]
    env: Dict[str, str]


class TargetConfigError(ValueError):
    pass


def _load_yaml(path: Path) -> Dict[str, Any]:
    try:
        obj = yaml.safe_load(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        raise TargetConfigError(f"Targets file not found: {path}") from None
    except Exception as exc:
        raise TargetConfigError(f"Failed to read targets YAML: {exc}") from exc
    if not isinstance(obj, dict):
        raise TargetConfigError("Targets file must be a YAML mapping.")
    return obj


def _resolve_dir(base_dir: Path, value: str) -> Path:
    p = Path(value).expanduser()
    if not p.is_absolute():
        p = (base_dir / p).resolve()
    return p


def load_benchmark_targets(path: str | Path) -> list[BenchmarkTarget]:
    cfg_path = Path(path).expanduser().resolve()
    obj = _load_yaml(cfg_path)

    targets_obj = obj.get("targets")
    if not isinstance(targets_obj, list) or not targets_obj:
        raise TargetConfigError("Targets file must define a non-empty 'targets' list.")

    base_dir = cfg_path.parent
    seen_names: set[str] = set()
    targets: list[BenchmarkTarget] = []
    for idx, raw in enumerate(targets_obj):
        if not isinstance(raw, dict):
            raise TargetConfigError(f"targets[{idx}] must be a mapping.")

        name = raw.get("name")
        if not isinstance(name, str) or not name.strip():
            raise TargetConfigError(f"targets[{idx}].name must be a non-empty string.")
        name = name.strip()
        if name in seen_names:
            raise TargetConfigError(f"Duplicate target name: {name}")
        seen_names.add(name)

        workdir_raw = raw.get("workdir")
        if not isinstance(workdir_raw, str) or not workdir_raw.strip():
            raise TargetConfigError(f"targets[{idx}].workdir must be a non-empty string.")
        workdir = _resolve_dir(base_dir, workdir_raw)

        run_command = raw.get("run_command")
        if not isinstance(run_command, str) or not run_command.strip():
            raise TargetConfigError(f"targets[{idx}].run_command must be a non-empty string.")

        setup_command = raw.get("setup_command")
        if setup_command is not None:
            if not isinstance(setup_command, str) or not setup_command.strip():
                raise TargetConfigError(
                    f"targets[{idx}].setup_command must be a non-empty string when provided."
                )
            setup_command = setup_command.strip()

        repo_url = raw.get("repo_url")
        if repo_url is not None and (not isinstance(repo_url, str) or not repo_url.strip()):
            raise TargetConfigError(f"targets[{idx}].repo_url must be a non-empty string when provided.")
        repo_url = repo_url.strip() if isinstance(repo_url, str) else None

        ref = raw.get("ref")
        if ref is not None and (not isinstance(ref, str) or not ref.strip()):
            raise TargetConfigError(f"targets[{idx}].ref must be a non-empty string when provided.")
        ref = ref.strip() if isinstance(ref, str) else None

        output_globs_raw = raw.get("output_globs", [])
        if output_globs_raw is None:
            output_globs_raw = []
        if not isinstance(output_globs_raw, list) or any(
            (not isinstance(x, str)) or (not x.strip()) for x in output_globs_raw
        ):
            raise TargetConfigError(f"targets[{idx}].output_globs must be a list of non-empty strings.")
        output_globs = [x.strip() for x in output_globs_raw]

        env_raw = raw.get("env", {})
        if env_raw is None:
            env_raw = {}
        if not isinstance(env_raw, dict):
            raise TargetConfigError(f"targets[{idx}].env must be a mapping when provided.")
        env: Dict[str, str] = {}
        for k, v in env_raw.items():
            if not isinstance(k, str) or not k:
                raise TargetConfigError(f"targets[{idx}].env contains invalid key.")
            env[k] = str(v)

        targets.append(
            BenchmarkTarget(
                name=name,
                workdir=workdir,
                repo_url=repo_url,
                ref=ref,
                setup_command=setup_command,
                run_command=run_command.strip(),
                output_globs=output_globs,
                env=env,
            )
        )

    return targets


def _git_head_sha(workdir: Path) -> str | None:
    git_dir = workdir / ".git"
    if not git_dir.exists():
        return None
    try:
        proc = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=str(workdir),
            capture_output=True,
            text=True,
            check=False,
        )
    except Exception:
        return None
    if proc.returncode != 0:
        return None
    sha = proc.stdout.strip()
    return sha or None


def _collect_outputs(workdir: Path, globs: list[str]) -> list[str]:
    found: list[str] = []
    for pattern in globs:
        for p in workdir.glob(pattern):
            if p.exists():
                found.append(str(p.relative_to(workdir)))
    return sorted(set(found))


def _render_command(template: str, spec_path: Path, target_name: str, manifest_path: Path) -> str:
    return template.format(
        spec_path=str(spec_path),
        target_name=target_name,
        manifest_path=str(manifest_path),
        manifest_dir=str(manifest_path.parent),
    )


def _run_shell(command: str, workdir: Path, env_patch: Dict[str, str]) -> subprocess.CompletedProcess[str]:
    env = dict(env_patch)
    merged_env = dict(**os.environ)
    merged_env.update(env)
    return subprocess.run(
        command,
        cwd=str(workdir),
        shell=True,
        capture_output=True,
        text=True,
        check=False,
        env=merged_env,
    )


def orchestrate_targets(
    *,
    spec_path: str | Path,
    targets_file: str | Path,
    manifest_path: str | Path,
    execute: bool = False,
) -> Dict[str, Any]:
    spec = Path(spec_path).expanduser().resolve()
    targets = load_benchmark_targets(targets_file)
    out_manifest = Path(manifest_path).expanduser().resolve()
    out_manifest.parent.mkdir(parents=True, exist_ok=True)

    started_at = time.time()
    run_items: list[Dict[str, Any]] = []

    for target in targets:
        item: Dict[str, Any] = {
            "target": target.name,
            "repo_url": target.repo_url,
            "ref": target.ref,
            "workdir": str(target.workdir),
            "commit_sha": _git_head_sha(target.workdir),
            "status": "planned",
            "setup": None,
            "run": None,
            "artifacts": [],
        }

        if not target.workdir.exists() or not target.workdir.is_dir():
            item["status"] = "invalid_target_workdir"
            run_items.append(item)
            continue

        if not execute:
            item["status"] = "planned"
            run_items.append(item)
            continue

        setup_ok = True
        if target.setup_command:
            setup_cmd = _render_command(target.setup_command, spec, target.name, out_manifest)
            proc = _run_shell(setup_cmd, target.workdir, target.env)
            item["setup"] = {
                "command": setup_cmd,
                "returncode": proc.returncode,
                "stdout": proc.stdout,
                "stderr": proc.stderr,
            }
            setup_ok = proc.returncode == 0

        run_ok = False
        if setup_ok:
            run_cmd = _render_command(target.run_command, spec, target.name, out_manifest)
            proc = _run_shell(run_cmd, target.workdir, target.env)
            item["run"] = {
                "command": run_cmd,
                "returncode": proc.returncode,
                "stdout": proc.stdout,
                "stderr": proc.stderr,
            }
            run_ok = proc.returncode == 0

        item["artifacts"] = _collect_outputs(target.workdir, target.output_globs)
        if setup_ok and run_ok:
            item["status"] = "completed"
        elif not setup_ok:
            item["status"] = "setup_failed"
        else:
            item["status"] = "run_failed"
        run_items.append(item)

    completed = sum(1 for x in run_items if x["status"] == "completed")
    failed = sum(1 for x in run_items if x["status"] in {"setup_failed", "run_failed", "invalid_target_workdir"})
    planned = sum(1 for x in run_items if x["status"] == "planned")

    manifest: Dict[str, Any] = {
        "schema_version": "0.1",
        "release_context": "dev-orchestrate-mvp",
        "spec_path": str(spec),
        "targets_file": str(Path(targets_file).expanduser().resolve()),
        "execute": bool(execute),
        "environment_fingerprint": {
            "python": sys.version.split()[0],
            "platform": platform.platform(),
        },
        "summary": {
            "total_targets": len(run_items),
            "completed": completed,
            "failed": failed,
            "planned": planned,
            "duration_seconds": round(time.time() - started_at, 6),
        },
        "targets": run_items,
    }
    out_manifest.write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8")
    return manifest
