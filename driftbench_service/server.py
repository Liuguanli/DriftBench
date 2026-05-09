#!/usr/bin/env python3
from __future__ import annotations

import argparse
import base64
import json
import mimetypes
import os
import re
import shutil
import signal
import subprocess
import threading
import time
import sys
import yaml
from datetime import date, datetime, time as dt_time
from decimal import Decimal
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse, parse_qs

ROOT_DIR = Path(__file__).resolve().parents[1]
STATIC_DIR = Path(__file__).resolve().parent / "static"
UPLOAD_DIR = Path(__file__).resolve().parent / "uploads"
SCHEMA_DIR = Path(__file__).resolve().parent / "schemas"
STATE_DIR = Path(__file__).resolve().parent / "state"
JOBS_STATE_PATH = STATE_DIR / "jobs.json"
PUBLIC_SPECS_CATALOG_PATH = Path(
    os.environ.get(
        "DRIFTBENCH_MCP_CATALOG_PATH",
        str((Path(__file__).resolve().parent / "state" / "public_specs_catalog.json")),
    )
)
if not PUBLIC_SPECS_CATALOG_PATH.is_absolute():
    PUBLIC_SPECS_CATALOG_PATH = (ROOT_DIR / PUBLIC_SPECS_CATALOG_PATH).resolve()
SHARED_SPECS_DIR = Path(
    os.environ.get(
        "DRIFTBENCH_MCP_SHARED_SPECS_DIR",
        str((ROOT_DIR / "driftspec" / "shared")),
    )
)
if not SHARED_SPECS_DIR.is_absolute():
    SHARED_SPECS_DIR = (ROOT_DIR / SHARED_SPECS_DIR).resolve()
MAX_UPLOAD_BYTES = 5 * 1024 * 1024

# Allow in-process imports like `import driftbench...` when running
# `python driftbench_service/server.py`.
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

MAX_LOG_LINES = 2000
MAX_PERSISTED_JOBS = 200
JOBS: dict[int, dict] = {}
JOB_LOCK = threading.Lock()
JOB_COUNTER = 1


def now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime())


def is_within_root(path: Path) -> bool:
    try:
        path.resolve().relative_to(ROOT_DIR)
        return True
    except ValueError:
        return False


def is_within_static(path: Path) -> bool:
    try:
        path.resolve().relative_to(STATIC_DIR)
        return True
    except ValueError:
        return False


def resolve_repo_path(path_str: str) -> Path | None:
    raw = Path(path_str)
    if raw.is_absolute():
        resolved = raw.resolve()
    else:
        resolved = (ROOT_DIR / raw).resolve()
    if not is_within_root(resolved):
        return None
    return resolved


def list_specs() -> list[dict]:
    specs = []
    for folder in [ROOT_DIR / "driftspec" / "examples", ROOT_DIR / "driftspec" / "generated"]:
        if not folder.exists():
            continue
        for ext in ("*.yaml", "*.yml"):
            for path in sorted(folder.glob(ext)):
                label = path.name
                rel = path.relative_to(ROOT_DIR).as_posix()
                specs.append({"label": label, "path": rel})
    return specs


def list_uploads(exts: list[str] | None = None, prefix: str | None = None) -> list[str]:
    if not UPLOAD_DIR.exists():
        return []
    files = []
    for path in sorted(UPLOAD_DIR.glob("*")):
        if not path.is_file():
            continue
        name = path.name
        if prefix and not name.startswith(prefix):
            continue
        if exts:
            if not any(name.lower().endswith(ext.lower()) for ext in exts):
                continue
        files.append(path.relative_to(ROOT_DIR).as_posix())
    return files


def list_schema_files() -> list[str]:
    if not SCHEMA_DIR.exists():
        return []
    files = []
    for path in sorted(SCHEMA_DIR.glob("*.json"), key=lambda p: p.stat().st_mtime, reverse=True):
        if path.is_file():
            files.append(path.relative_to(ROOT_DIR).as_posix())
    return files


def _slugify(raw: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9]+", "-", raw.strip().lower()).strip("-")
    return slug or "spec"


def _read_public_specs_catalog() -> dict:
    if not PUBLIC_SPECS_CATALOG_PATH.exists():
        return {"version": 1, "updated_at": now_iso(), "specs": []}
    try:
        payload = json.loads(PUBLIC_SPECS_CATALOG_PATH.read_text(encoding="utf-8"))
    except Exception as exc:
        raise ValueError(f"failed to read catalog: {exc}") from exc
    if not isinstance(payload, dict):
        raise ValueError("catalog format invalid: expected object")
    specs = payload.get("specs")
    if not isinstance(specs, list):
        raise ValueError("catalog format invalid: specs must be a list")
    payload.setdefault("version", 1)
    payload.setdefault("updated_at", now_iso())
    return payload


def list_public_specs(tag: str | None = None, query: str | None = None, limit: int = 100) -> list[dict]:
    payload = _read_public_specs_catalog()
    specs = list(payload.get("specs") or [])
    if tag:
        wanted = tag.strip().lower()
        specs = [s for s in specs if wanted in [str(t).lower() for t in (s.get("tags") or [])]]
    if query:
        q = query.strip().lower()
        if q:
            specs = [
                s
                for s in specs
                if q in str(s.get("id", "")).lower()
                or q in str(s.get("title", "")).lower()
                or q in str(s.get("description", "")).lower()
            ]
    specs = sorted(specs, key=lambda s: str(s.get("updated_at", "")), reverse=True)
    limit = max(0, min(limit, 5000))
    if limit == 0:
        return []
    return specs[:limit]


def _find_catalog_entry(spec_id: str) -> dict | None:
    payload = _read_public_specs_catalog()
    for entry in payload.get("specs") or []:
        if entry.get("id") == spec_id:
            return entry
    return None


def _resolve_source_spec_from_public(spec_id: str | None, spec_path: str | None) -> tuple[Path, str]:
    if not spec_id and not spec_path:
        raise ValueError("one of spec_id or spec_path required")
    if spec_id and spec_path:
        raise ValueError("provide only one of spec_id or spec_path")

    if spec_id:
        if not isinstance(spec_id, str) or not spec_id.strip():
            raise ValueError("spec_id must be a non-empty string")
        entry = _find_catalog_entry(spec_id.strip())
        if not entry:
            raise ValueError(f"spec_id not found in catalog: {spec_id}")
        shared_path = entry.get("shared_path")
        if not isinstance(shared_path, str) or not shared_path.strip():
            raise ValueError(f"invalid shared_path for spec_id: {spec_id}")
        resolved = resolve_repo_path(shared_path)
        if not resolved or not resolved.exists():
            raise ValueError(f"shared spec path missing: {shared_path}")
        return resolved, spec_id.strip()

    assert spec_path is not None
    resolved = resolve_repo_path(str(spec_path))
    if not resolved or not resolved.exists():
        raise ValueError("spec_path not found or outside repo")
    return resolved, _slugify(Path(spec_path).stem)


def import_public_spec(
    *,
    spec_id: str | None = None,
    spec_path: str | None = None,
    target_path: str | None = None,
    overwrite: bool = False,
) -> dict:
    source_spec, source_id = _resolve_source_spec_from_public(spec_id, spec_path)
    if target_path:
        target_resolved = resolve_repo_path(str(target_path))
        if not target_resolved:
            raise ValueError("target_path outside repo")
    else:
        ts = int(time.time() * 1000)
        target_resolved = resolve_repo_path(f"driftspec/generated/imported_{_slugify(source_id)}_{ts}.yaml")
        if not target_resolved:
            raise ValueError("failed to resolve default target path")
    if target_resolved.exists() and not overwrite:
        raise ValueError(f"target spec already exists: {target_resolved.relative_to(ROOT_DIR).as_posix()}")
    target_resolved.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source_spec, target_resolved)
    return {
        "source_spec_path": source_spec.relative_to(ROOT_DIR).as_posix(),
        "imported_spec_path": target_resolved.relative_to(ROOT_DIR).as_posix(),
    }


def _append_log(job: dict, line: str) -> None:
    if "logs" not in job:
        job["logs"] = []
    job["logs"].append(line)
    if len(job["logs"]) > MAX_LOG_LINES:
        drop = len(job["logs"]) - MAX_LOG_LINES
        job["logs"] = job["logs"][drop:]
        job["log_dropped"] = job.get("log_dropped", 0) + drop


def _persist_jobs_state_locked() -> None:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    jobs = sorted(JOBS.values(), key=lambda j: j.get("created_ts", 0), reverse=True)[:MAX_PERSISTED_JOBS]
    payload = {
        "saved_at": now_iso(),
        "next_job_counter": JOB_COUNTER,
        "jobs": jobs,
    }
    temp_path = JOBS_STATE_PATH.with_suffix(".json.tmp")
    temp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    temp_path.replace(JOBS_STATE_PATH)


def _load_jobs_state() -> None:
    global JOB_COUNTER
    if not JOBS_STATE_PATH.exists():
        return
    try:
        raw = JOBS_STATE_PATH.read_text(encoding="utf-8")
        payload = json.loads(raw)
        loaded_jobs = payload.get("jobs") or []
        loaded_jobs = loaded_jobs[:MAX_PERSISTED_JOBS]
    except Exception:
        return

    interrupted_at = now_iso()
    with JOB_LOCK:
        JOBS.clear()
        max_id = 0
        for rec in loaded_jobs:
            try:
                job_id = int(rec.get("id"))
            except Exception:
                continue
            max_id = max(max_id, job_id)
            job = {
                "id": job_id,
                "kind": rec.get("kind"),
                "cmd": rec.get("cmd"),
                "meta": rec.get("meta") or {},
                "status": rec.get("status") or "failed",
                "created_at": rec.get("created_at"),
                "created_ts": rec.get("created_ts", 0),
                "started_at": rec.get("started_at"),
                "ended_at": rec.get("ended_at"),
                "exit_code": rec.get("exit_code"),
                "pid": None,
                "logs": rec.get("logs") or [],
                "log_dropped": rec.get("log_dropped", 0),
            }

            # Jobs cannot resume after a process restart.
            if job["status"] in {"queued", "running"}:
                _append_log(job, "[recovered] service restarted before this job finished.")
                job["status"] = "interrupted"
                job["exit_code"] = -1
                if not job.get("ended_at"):
                    job["ended_at"] = interrupted_at

            JOBS[job_id] = job

        next_counter = payload.get("next_job_counter")
        if isinstance(next_counter, int) and next_counter > max_id:
            JOB_COUNTER = next_counter
        else:
            JOB_COUNTER = max_id + 1


def _run_job(job_id: int, cmd: list[str]) -> None:
    with JOB_LOCK:
        job = JOBS[job_id]
        job["status"] = "running"
        job["started_at"] = now_iso()
        job["pid"] = None
        _persist_jobs_state_locked()
    try:
        proc = subprocess.Popen(
            cmd,
            cwd=str(ROOT_DIR),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        with JOB_LOCK:
            job["pid"] = proc.pid
            _persist_jobs_state_locked()
        if proc.stdout:
            for line in proc.stdout:
                _append_log(job, line.rstrip("\n"))
        exit_code = proc.wait()
        with JOB_LOCK:
            job["exit_code"] = exit_code
            job["status"] = "completed" if exit_code == 0 else "failed"
            job["ended_at"] = now_iso()
            _persist_jobs_state_locked()
    except Exception as exc:
        with JOB_LOCK:
            _append_log(job, f"[server error] {exc}")
            job["status"] = "failed"
            job["ended_at"] = now_iso()
            _persist_jobs_state_locked()


def _run_func_job(job_id: int, func, *args, **kwargs) -> None:
    with JOB_LOCK:
        job = JOBS[job_id]
        job["status"] = "running"
        job["started_at"] = now_iso()
        _persist_jobs_state_locked()
    try:
        result = func(job, *args, **kwargs)
        with JOB_LOCK:
            job["result"] = result
            job["exit_code"] = 0
            job["status"] = "completed"
            job["ended_at"] = now_iso()
            _persist_jobs_state_locked()
    except Exception as exc:
        with JOB_LOCK:
            _append_log(job, f"[server error] {exc}")
            job["status"] = "failed"
            job["ended_at"] = now_iso()
            _persist_jobs_state_locked()


def create_job(kind: str, cmd: list[str], meta: dict | None = None) -> dict:
    global JOB_COUNTER
    with JOB_LOCK:
        job_id = JOB_COUNTER
        JOB_COUNTER += 1
        job = {
            "id": job_id,
            "kind": kind,
            "cmd": cmd,
            "meta": meta or {},
            "status": "queued",
            "created_at": now_iso(),
            "created_ts": time.time(),
            "started_at": None,
            "ended_at": None,
            "exit_code": None,
            "pid": None,
            "logs": [],
            "log_dropped": 0,
        }
        JOBS[job_id] = job
        _persist_jobs_state_locked()

    thread = threading.Thread(target=_run_job, args=(job_id, cmd), daemon=True)
    thread.start()
    return job


def create_func_job(kind: str, func, args: tuple, meta: dict | None = None) -> dict:
    global JOB_COUNTER
    with JOB_LOCK:
        job_id = JOB_COUNTER
        JOB_COUNTER += 1
        job = {
            "id": job_id,
            "kind": kind,
            "cmd": None,
            "meta": meta or {},
            "status": "queued",
            "created_at": now_iso(),
            "created_ts": time.time(),
            "started_at": None,
            "ended_at": None,
            "exit_code": None,
            "pid": None,
            "logs": [],
            "log_dropped": 0,
        }
        JOBS[job_id] = job
        _persist_jobs_state_locked()

    thread = threading.Thread(target=_run_func_job, args=(job_id, func, *args), daemon=True)
    thread.start()
    return job


def _ensure_parent_dir(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def _safe_filename(name: str) -> str:
    base = Path(name).name
    if not base:
        raise ValueError("filename is required.")
    return base


def _write_upload_bytes(filename: str, data: bytes) -> Path:
    if len(data) > MAX_UPLOAD_BYTES:
        raise ValueError("upload too large (max 5MB).")
    _ensure_parent_dir(UPLOAD_DIR / "dummy")
    safe = _safe_filename(filename)
    target = (UPLOAD_DIR / safe).resolve()
    if not is_within_root(target):
        raise ValueError("invalid upload path.")
    with open(target, "wb") as f:
        f.write(data)
    return target


def _extract_schema_job(job: dict, payload: dict) -> dict:
    try:
        from driftbench.core.schema.factory import get_schema_extractor
    except ModuleNotFoundError:
        # Extra guard for environments where sys.path is altered after startup.
        root = str(ROOT_DIR.resolve())
        if root not in sys.path:
            sys.path.insert(0, root)
        _append_log(job, f"[import retry] inserted ROOT_DIR into sys.path: {root}")
        try:
            from driftbench.core.schema.factory import get_schema_extractor
        except ModuleNotFoundError as exc:
            _append_log(job, f"[import failed] sys.executable={sys.executable}")
            _append_log(job, f"[import failed] cwd={os.getcwd()}")
            _append_log(job, f"[import failed] ROOT_DIR={ROOT_DIR}")
            _append_log(job, f"[import failed] sys.path[0:5]={sys.path[:5]}")
            raise exc

    source_type = (payload.get("source_type") or "").lower()
    sample_size = int(payload.get("sample_size") or 0)

    if source_type == "csv":
        data_path = payload.get("path")
        if not data_path:
            raise ValueError("path is required for csv.")
        resolved = resolve_repo_path(data_path)
        if not resolved or not resolved.exists():
            raise ValueError("data path not found or outside repo.")
        output_path = payload.get("output_path")
        if output_path:
            out_resolved = resolve_repo_path(output_path)
        else:
            stem = resolved.stem
            out_resolved = resolve_repo_path(f"driftbench_service/schemas/{stem}_schema.json")
        if not out_resolved:
            raise ValueError("output_path outside repo.")
        _ensure_parent_dir(out_resolved)

        _append_log(job, f"[schema] source_type={source_type} path={resolved}")
        kwargs = {f"{source_type}_path": str(resolved)}
        extractor = get_schema_extractor(source_type=source_type, **kwargs, sample_size=sample_size)
        schema = extractor.extract_schema()
        rel_source = resolved.relative_to(ROOT_DIR).as_posix() if is_within_root(resolved) else str(resolved)
        schema["_meta"] = {
            "source_type": "csv",
            "path": rel_source,
            "generated_at": now_iso(),
        }
        with open(out_resolved, "w", encoding="utf-8") as f:
            json.dump(schema, f, indent=2, default=str)
        _append_log(job, f"[SCHEMA OK] -> {out_resolved}")
        return {"schema_path": str(out_resolved)}

    if source_type == "postgres":
        schema_name = payload.get("schema_name") or "public"
        db_config = payload.get("db_config")
        db_config_path = payload.get("db_config_path")
        if not db_config and db_config_path:
            cfg_resolved = resolve_repo_path(db_config_path)
            if not cfg_resolved or not cfg_resolved.exists():
                raise ValueError("db_config_path not found or outside repo.")
            with open(cfg_resolved, "r", encoding="utf-8") as f:
                db_config = json.load(f)
        if not db_config:
            raise ValueError("db_config or db_config_path required for postgres.")
        output_path = payload.get("output_path")
        if output_path:
            out_resolved = resolve_repo_path(output_path)
        else:
            out_resolved = resolve_repo_path(f"driftbench_service/schemas/{schema_name}_schema.json")
        if not out_resolved:
            raise ValueError("output_path outside repo.")
        _ensure_parent_dir(out_resolved)

        _append_log(job, f"[schema] source_type=postgres schema={schema_name}")
        extractor = get_schema_extractor(
            source_type="postgres",
            db_config=db_config,
            schema_name=schema_name,
            sample_size=sample_size or 1000,
        )
        schema = extractor.extract_schema()
        schema["_meta"] = {
            "source_type": "postgres",
            "schema_name": schema_name,
            "db_config_path": db_config_path,
            "generated_at": now_iso(),
        }
        with open(out_resolved, "w", encoding="utf-8") as f:
            json.dump(schema, f, indent=2, default=str)
        _append_log(job, f"[SCHEMA OK] -> {out_resolved}")
        return {"schema_path": str(out_resolved)}

    raise ValueError("Unsupported source_type. Use csv or postgres.")


def get_job(job_id: int) -> dict | None:
    with JOB_LOCK:
        job = JOBS.get(job_id)
        if not job:
            return None
        return dict(job)


def list_jobs(limit: int = 20) -> list[dict]:
    with JOB_LOCK:
        jobs = sorted(JOBS.values(), key=lambda j: j.get("created_ts", 0), reverse=True)
        return [
            {
                "id": j["id"],
                "kind": j["kind"],
                "status": j["status"],
                "created_at": j["created_at"],
                "started_at": j["started_at"],
                "ended_at": j["ended_at"],
                "exit_code": j["exit_code"],
            }
            for j in jobs[:limit]
        ]


def delete_job(job_id: int) -> tuple[bool, str]:
    with JOB_LOCK:
        job = JOBS.get(job_id)
        if not job:
            return False, "job not found"
        pid = job.get("pid")
        if pid:
            try:
                os.kill(int(pid), signal.SIGTERM)
                _append_log(job, f"[job deleted] sent SIGTERM to pid={pid}")
            except Exception:
                # Process may already have exited; safe to ignore.
                pass
        del JOBS[job_id]
        _persist_jobs_state_locked()
    return True, "deleted"


def _json_safe_scalar(value):
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, (datetime, date, dt_time)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    return str(value)


def _resolve_table_key(schema_obj: dict, table_name: str) -> str | None:
    tables = schema_obj.get("tables") or {}
    if table_name in tables:
        return table_name
    bare = table_name.split(".")[-1]
    for key in tables.keys():
        if key.split(".")[-1] == bare:
            return key
    return None


def _build_table_preview(schema_obj: dict, table_name: str, limit: int, payload: dict) -> dict:
    table_key = _resolve_table_key(schema_obj, table_name)
    if not table_key:
        raise ValueError(f"table not found in schema: {table_name}")
    table_schema = (schema_obj.get("tables") or {}).get(table_key) or {}
    meta = schema_obj.get("_meta") or {}

    source_type = (payload.get("source_type") or meta.get("source_type") or "").lower()
    if not source_type:
        source_tag = str(schema_obj.get("source") or "").lower()
        source_type = "csv" if source_tag == "csv" else "postgres"

    if source_type == "csv":
        import pandas as pd

        csv_path = payload.get("path") or meta.get("path")
        if not csv_path:
            raise ValueError("csv path missing; regenerate schema from csv source or provide path.")
        csv_resolved = resolve_repo_path(str(csv_path))
        if not csv_resolved or not csv_resolved.exists():
            raise ValueError("csv path not found or outside repo.")
        df = pd.read_csv(csv_resolved, nrows=limit)
        rows = json.loads(df.to_json(orient="records", date_format="iso"))
        return {
            "table_name": table_key,
            "columns": list(df.columns),
            "rows": rows,
            "schema": table_schema,
            "source_type": "csv",
            "row_count": len(rows),
        }

    if source_type == "postgres":
        import psycopg2
        from psycopg2 import sql

        db_config = payload.get("db_config")
        db_config_path = payload.get("db_config_path") or meta.get("db_config_path")
        if not db_config and db_config_path:
            cfg_resolved = resolve_repo_path(str(db_config_path))
            if not cfg_resolved or not cfg_resolved.exists():
                raise ValueError("db_config_path not found or outside repo.")
            with open(cfg_resolved, "r", encoding="utf-8") as f:
                db_config = json.load(f)
        if not db_config:
            raise ValueError("db_config missing; select a saved DB config and regenerate schema.")

        schema_name = payload.get("schema_name") or meta.get("schema_name")
        table_part = table_key
        if "." in table_part:
            schema_from_key, table_part = table_part.split(".", 1)
            if not schema_name:
                schema_name = schema_from_key
        if not schema_name:
            schema_name = "public"

        conn = psycopg2.connect(**db_config)
        try:
            cursor = conn.cursor()
            q = sql.SQL("SELECT * FROM {}.{} LIMIT %s").format(
                sql.Identifier(schema_name),
                sql.Identifier(table_part),
            )
            cursor.execute(q, (limit,))
            col_names = [d[0] for d in cursor.description] if cursor.description else []
            raw_rows = cursor.fetchall()
            rows = [{col_names[i]: _json_safe_scalar(v) for i, v in enumerate(rec)} for rec in raw_rows]
            return {
                "table_name": table_key,
                "columns": col_names,
                "rows": rows,
                "schema": table_schema,
                "source_type": "postgres",
                "row_count": len(rows),
            }
        finally:
            conn.close()

    raise ValueError(f"unsupported source_type: {source_type}")


class Handler(BaseHTTPRequestHandler):
    server_version = "DriftBenchService/0.1"

    def _send_json(self, data: dict, status: int = 200) -> None:
        payload = json.dumps(data, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def _send_text(self, text: str, status: int = 200) -> None:
        payload = text.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def _read_json(self) -> dict:
        length = int(self.headers.get("Content-Length", 0))
        if length == 0:
            return {}
        raw = self.rfile.read(length)
        try:
            return json.loads(raw.decode("utf-8"))
        except json.JSONDecodeError:
            return {}

    def _serve_static(self, path: str) -> None:
        if path == "/":
            path = "/index.html"
        rel = path.lstrip("/")
        file_path = (STATIC_DIR / rel).resolve()
        if not file_path.exists() or not file_path.is_file() or not is_within_static(file_path):
            self._send_text("Not found", status=404)
            return
        mime, _ = mimetypes.guess_type(str(file_path))
        if not mime:
            mime = "application/octet-stream"
        content = file_path.read_bytes()
        self.send_response(200)
        self.send_header("Content-Type", mime)
        self.send_header("Content-Length", str(len(content)))
        self.end_headers()
        self.wfile.write(content)

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path.startswith("/api/"):
            if parsed.path == "/api/health":
                self._send_json({"ok": True, "time": now_iso()})
                return
            if parsed.path == "/api/specs":
                self._send_json({"specs": list_specs()})
                return
            if parsed.path == "/api/public-specs":
                qs = parse_qs(parsed.query or "")
                tag = (qs.get("tag") or [None])[0]
                query = (qs.get("query") or [None])[0]
                limit_raw = (qs.get("limit") or [None])[0]
                try:
                    limit = int(limit_raw) if limit_raw is not None else 100
                except Exception:
                    self._send_json({"error": "invalid limit"}, status=400)
                    return
                try:
                    specs = list_public_specs(tag=tag, query=query, limit=limit)
                except Exception as exc:
                    self._send_json({"error": str(exc)}, status=400)
                    return
                self._send_json({"specs": specs, "count": len(specs)})
                return
            if parsed.path == "/api/uploads":
                qs = parse_qs(parsed.query or "")
                exts = qs.get("ext") or []
                prefix = (qs.get("prefix") or [None])[0]
                self._send_json({"files": list_uploads(exts=exts, prefix=prefix)})
                return
            if parsed.path == "/api/schemas":
                self._send_json({"files": list_schema_files()})
                return
            if parsed.path == "/api/jobs":
                self._send_json({"jobs": list_jobs()})
                return
            if parsed.path == "/api/schema/read":
                qs = parse_qs(parsed.query or "")
                path = (qs.get("path") or [None])[0]
                if not path:
                    self._send_json({"error": "path required"}, status=400)
                    return
                resolved = resolve_repo_path(path)
                if not resolved or not resolved.exists():
                    self._send_json({"error": "path not found or outside repo"}, status=400)
                    return
                try:
                    data = json.loads(resolved.read_text(encoding="utf-8"))
                except Exception as exc:
                    self._send_json({"error": f"failed to read schema: {exc}"}, status=400)
                    return
                self._send_json({"schema": data, "path": resolved.relative_to(ROOT_DIR).as_posix()})
                return
            if parsed.path.startswith("/api/jobs/"):
                try:
                    job_id = int(parsed.path.split("/")[-1])
                except ValueError:
                    self._send_json({"error": "invalid job id"}, status=400)
                    return
                job = get_job(job_id)
                if not job:
                    self._send_json({"error": "job not found"}, status=404)
                    return
                self._send_json({"job": job})
                return
            self._send_json({"error": "unknown endpoint"}, status=404)
            return

        self._serve_static(parsed.path)

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        if not parsed.path.startswith("/api/"):
            self._send_json({"error": "unknown endpoint"}, status=404)
            return
        payload = self._read_json()

        if parsed.path == "/api/spec/build":
            spec = payload.get("spec")
            output_path = payload.get("output_path")
            if not isinstance(spec, dict):
                self._send_json({"error": "spec (object) required"}, status=400)
                return
            if not output_path:
                output_path = f"driftspec/generated/service_spec_{int(time.time() * 1000)}.yaml"
            resolved = resolve_repo_path(str(output_path))
            if not resolved:
                self._send_json({"error": "output_path outside repo"}, status=400)
                return
            try:
                _ensure_parent_dir(resolved)
                with open(resolved, "w", encoding="utf-8") as f:
                    yaml.safe_dump(spec, f, sort_keys=False, allow_unicode=True)
            except Exception as exc:
                self._send_json({"error": f"failed to write spec: {exc}"}, status=400)
                return
            rel = resolved.relative_to(ROOT_DIR).as_posix()
            self._send_json({"ok": True, "path": rel})
            return

        if parsed.path == "/api/run":
            spec_path = payload.get("spec_path")
            if not spec_path:
                self._send_json({"error": "spec_path required"}, status=400)
                return
            resolved = resolve_repo_path(spec_path)
            if not resolved or not resolved.exists():
                self._send_json({"error": "spec_path not found or outside repo"}, status=400)
                return
            cmd = [
                os.environ.get("PYTHON", sys.executable),
                "-m",
                "driftbench.cli",
                "run-yaml",
                str(resolved),
            ]
            job = create_job("run-yaml", cmd, meta={"spec_path": str(resolved)})
            self._send_json({"job": job})
            return

        if parsed.path == "/api/trace-to-spec":
            trace_path = payload.get("trace_path")
            output_path = payload.get("output_path")
            trace_type = payload.get("trace_type")
            mapping_path = payload.get("mapping_path")
            if not trace_path or not output_path:
                self._send_json({"error": "trace_path and output_path required"}, status=400)
                return
            trace_resolved = resolve_repo_path(trace_path)
            output_resolved = resolve_repo_path(output_path)
            if not trace_resolved or not trace_resolved.exists():
                self._send_json({"error": "trace_path not found or outside repo"}, status=400)
                return
            if not output_resolved:
                self._send_json({"error": "output_path outside repo"}, status=400)
                return
            cmd = [
                os.environ.get("PYTHON", sys.executable),
                "-m",
                "driftbench.cli",
                "trace-to-spec",
                str(trace_resolved),
                str(output_resolved),
            ]
            if trace_type:
                cmd.extend(["--trace-type", str(trace_type)])
            if mapping_path:
                mapping_resolved = resolve_repo_path(mapping_path)
                if not mapping_resolved or not mapping_resolved.exists():
                    self._send_json({"error": "mapping_path not found or outside repo"}, status=400)
                    return
                cmd.extend(["--mapping", str(mapping_resolved)])
            job = create_job(
                "trace-to-spec",
                cmd,
                meta={
                    "trace_path": str(trace_resolved),
                    "output_path": str(output_resolved),
                    "trace_type": trace_type,
                    "mapping_path": mapping_path,
                },
            )
            self._send_json({"job": job})
            return

        if parsed.path == "/api/schema/extract":
            source_type = payload.get("source_type")
            if not source_type:
                self._send_json({"error": "source_type required"}, status=400)
                return
            job = create_func_job(
                "schema-extract",
                _extract_schema_job,
                (payload,),
                meta={
                    "source_type": source_type,
                    "path": payload.get("path"),
                    "db_config_path": payload.get("db_config_path"),
                    "schema_name": payload.get("schema_name"),
                    "output_path": payload.get("output_path"),
                },
            )
            self._send_json({"job": job})
            return

        if parsed.path == "/api/schema/table-preview":
            schema_path = payload.get("schema_path")
            table_name = payload.get("table_name")
            if not schema_path or not table_name:
                self._send_json({"error": "schema_path and table_name required"}, status=400)
                return
            try:
                limit = int(payload.get("limit") or 5)
            except Exception:
                self._send_json({"error": "invalid limit"}, status=400)
                return
            limit = max(1, min(limit, 5))
            resolved = resolve_repo_path(str(schema_path))
            if not resolved or not resolved.exists():
                self._send_json({"error": "schema_path not found or outside repo"}, status=400)
                return
            try:
                schema_obj = json.loads(resolved.read_text(encoding="utf-8"))
                data = _build_table_preview(schema_obj, str(table_name), limit, payload)
            except Exception as exc:
                self._send_json({"error": str(exc)}, status=400)
                return
            self._send_json(data)
            return

        if parsed.path == "/api/jobs/delete":
            job_id = payload.get("job_id")
            try:
                job_id = int(job_id)
            except Exception:
                self._send_json({"error": "valid job_id required"}, status=400)
                return
            ok, message = delete_job(job_id)
            if not ok:
                status = 404 if message == "job not found" else 400
                self._send_json({"error": message}, status=status)
                return
            self._send_json({"ok": True, "message": message, "job_id": job_id})
            return

        if parsed.path == "/api/files/upload":
            filename = payload.get("filename")
            content_b64 = payload.get("content_b64")
            if not filename or not content_b64:
                self._send_json({"error": "filename and content_b64 required"}, status=400)
                return
            try:
                data = base64.b64decode(content_b64, validate=True)
            except Exception:
                self._send_json({"error": "invalid base64 content"}, status=400)
                return
            try:
                path = _write_upload_bytes(filename, data)
            except Exception as exc:
                self._send_json({"error": str(exc)}, status=400)
                return
            rel = path.relative_to(ROOT_DIR).as_posix()
            self._send_json({"path": rel})
            return

        if parsed.path == "/api/files/save-text":
            filename = payload.get("filename")
            content = payload.get("content")
            if not filename or content is None:
                self._send_json({"error": "filename and content required"}, status=400)
                return
            data = content.encode("utf-8")
            try:
                path = _write_upload_bytes(filename, data)
            except Exception as exc:
                self._send_json({"error": str(exc)}, status=400)
                return
            rel = path.relative_to(ROOT_DIR).as_posix()
            self._send_json({"path": rel})
            return

        if parsed.path == "/api/public-specs/import-run":
            spec_id = payload.get("spec_id")
            spec_path = payload.get("spec_path")
            target_path = payload.get("target_path")
            overwrite = bool(payload.get("overwrite", False))
            execute = bool(payload.get("execute", True))

            try:
                imported = import_public_spec(
                    spec_id=spec_id,
                    spec_path=spec_path,
                    target_path=target_path,
                    overwrite=overwrite,
                )
            except Exception as exc:
                self._send_json({"error": str(exc)}, status=400)
                return

            if not execute:
                self._send_json({"ok": True, **imported, "executed": False})
                return

            resolved = resolve_repo_path(imported["imported_spec_path"])
            if not resolved or not resolved.exists():
                self._send_json({"error": "imported spec missing after copy"}, status=400)
                return
            cmd = [
                os.environ.get("PYTHON", sys.executable),
                "-m",
                "driftbench.cli",
                "run-yaml",
                str(resolved),
            ]
            job = create_job(
                "run-yaml",
                cmd,
                meta={
                    "spec_path": str(resolved),
                    "source_spec_path": imported["source_spec_path"],
                    "imported_spec_path": imported["imported_spec_path"],
                    "from_public_specs": True,
                },
            )
            self._send_json({"ok": True, **imported, "executed": True, "job": job})
            return

        self._send_json({"error": "unknown endpoint"}, status=404)


def main() -> None:
    parser = argparse.ArgumentParser(description="DriftBench local service")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=int(os.environ.get("DRIFTBENCH_PORT", "8000")))
    args = parser.parse_args()
    _load_jobs_state()

    server = ThreadingHTTPServer((args.host, args.port), Handler)
    print(f"DriftBench service running on http://{args.host}:{args.port}")
    print(f"Repo root: {ROOT_DIR}")
    server.serve_forever()


if __name__ == "__main__":
    main()
