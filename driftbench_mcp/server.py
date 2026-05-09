from __future__ import annotations

import json
import os
import re
import shutil
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Tuple

import yaml

from driftbench import load_and_validate_spec, run_spec_and_return_summary, trace_to_spec

JSONRPC_VERSION = "2.0"
MCP_PROTOCOL_VERSION = "2024-11-05"
SERVER_NAME = "driftbench-mcp"
SERVER_VERSION = "0.1.0"
EXIT_RUNTIME_ERROR = 4


class ToolExecutionError(Exception):
    pass


def _now_iso() -> str:
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")


def _repo_root() -> Path:
    configured = os.environ.get("DRIFTBENCH_ROOT")
    if configured:
        return Path(configured).expanduser().resolve()
    return Path.cwd().resolve()


def _resolve_repo_path(raw_path: str, *, must_exist: bool) -> Path:
    if not isinstance(raw_path, str) or not raw_path.strip():
        raise ToolExecutionError("path must be a non-empty string")

    candidate = Path(raw_path).expanduser()
    if not candidate.is_absolute():
        candidate = _repo_root() / candidate

    candidate = candidate.resolve()
    root = _repo_root()
    try:
        candidate.relative_to(root)
    except ValueError as exc:
        raise ToolExecutionError(f"path escapes repository root: {raw_path}") from exc

    if must_exist and not candidate.exists():
        raise ToolExecutionError(f"path does not exist: {raw_path}")
    return candidate


def _as_rel(path: Path) -> str:
    return os.path.relpath(path, _repo_root())


def _shared_spec_dir() -> Path:
    configured = os.environ.get("DRIFTBENCH_MCP_SHARED_SPECS_DIR")
    if configured:
        return _resolve_repo_path(configured, must_exist=False)
    return (_repo_root() / "driftspec" / "shared").resolve()


def _catalog_path() -> Path:
    configured = os.environ.get("DRIFTBENCH_MCP_CATALOG_PATH")
    if configured:
        return _resolve_repo_path(configured, must_exist=False)
    return (_repo_root() / "driftbench_service" / "state" / "public_specs_catalog.json").resolve()


def _slugify(raw: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9]+", "-", raw.strip().lower()).strip("-")
    return slug or "spec"


def _read_catalog() -> Dict[str, Any]:
    path = _catalog_path()
    if not path.exists():
        return {"version": 1, "updated_at": _now_iso(), "specs": []}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise ToolExecutionError(f"failed to read catalog: {path}") from exc
    if not isinstance(payload, dict):
        raise ToolExecutionError("catalog format invalid: expected object")
    specs = payload.get("specs")
    if not isinstance(specs, list):
        raise ToolExecutionError("catalog format invalid: 'specs' must be a list")
    payload.setdefault("version", 1)
    payload.setdefault("updated_at", _now_iso())
    return payload


def _write_catalog(payload: Dict[str, Any]) -> None:
    path = _catalog_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    payload["updated_at"] = _now_iso()
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def _find_catalog_entry(catalog: Dict[str, Any], spec_id: str) -> Dict[str, Any] | None:
    for entry in catalog.get("specs", []):
        if entry.get("id") == spec_id:
            return entry
    return None


def _ensure_str_list(value: Any, key: str) -> List[str]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise ToolExecutionError(f"argument '{key}' must be an array of strings")
    out: List[str] = []
    for item in value:
        if not isinstance(item, str) or not item.strip():
            raise ToolExecutionError(f"argument '{key}' must contain non-empty strings")
        out.append(item.strip())
    return out


def _load_yaml_file(path: Path) -> Dict[str, Any]:
    try:
        payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise ToolExecutionError(f"invalid YAML file: {_as_rel(path)}") from exc
    if not isinstance(payload, dict):
        raise ToolExecutionError("spec YAML must be a mapping object")
    return payload


def _pick_source_spec(arguments: Dict[str, Any]) -> Tuple[Path, str]:
    spec_id = arguments.get("spec_id")
    spec_path_raw = arguments.get("spec_path")
    if spec_id is None and spec_path_raw is None:
        raise ToolExecutionError("one of 'spec_id' or 'spec_path' is required")
    if spec_id is not None and spec_path_raw is not None:
        raise ToolExecutionError("provide only one of 'spec_id' or 'spec_path'")

    if spec_id is not None:
        if not isinstance(spec_id, str) or not spec_id.strip():
            raise ToolExecutionError("argument 'spec_id' must be a non-empty string")
        catalog = _read_catalog()
        entry = _find_catalog_entry(catalog, spec_id.strip())
        if not entry:
            raise ToolExecutionError(f"spec_id not found in catalog: {spec_id}")
        shared_path = entry.get("shared_path")
        if not isinstance(shared_path, str):
            raise ToolExecutionError(f"catalog entry missing shared_path for id: {spec_id}")
        source = _resolve_repo_path(shared_path, must_exist=True)
        return source, spec_id.strip()

    if not isinstance(spec_path_raw, str) or not spec_path_raw.strip():
        raise ToolExecutionError("argument 'spec_path' must be a non-empty string")
    source = _resolve_repo_path(spec_path_raw, must_exist=True)
    return source, source.stem


def _collect_declared_outputs(obj: Any) -> List[str]:
    found: List[str] = []

    def walk(x: Any) -> None:
        if isinstance(x, dict):
            for key, value in x.items():
                if key == "output_path" and isinstance(value, str):
                    found.append(value)
                else:
                    walk(value)
            return
        if isinstance(x, list):
            for item in x:
                walk(item)

    walk(obj)
    return sorted(set(found))


def _require_str(arguments: Dict[str, Any], key: str) -> str:
    value = arguments.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ToolExecutionError(f"argument '{key}' is required and must be a non-empty string")
    return value


def _require_bool(arguments: Dict[str, Any], key: str, default: bool) -> bool:
    value = arguments.get(key, default)
    if isinstance(value, bool):
        return value
    raise ToolExecutionError(f"argument '{key}' must be a boolean")


def _require_int(arguments: Dict[str, Any], key: str, default: int) -> int:
    value = arguments.get(key, default)
    if isinstance(value, int):
        return value
    raise ToolExecutionError(f"argument '{key}' must be an integer")


def _tool_health(_: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "ok": True,
        "server": SERVER_NAME,
        "version": SERVER_VERSION,
        "repo_root": str(_repo_root()),
    }


def _tool_trace_to_spec(arguments: Dict[str, Any]) -> Dict[str, Any]:
    trace_path = _resolve_repo_path(_require_str(arguments, "trace_path"), must_exist=True)
    output_path = _resolve_repo_path(_require_str(arguments, "output_path"), must_exist=False)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    trace_type = arguments.get("trace_type")
    if trace_type is not None and trace_type not in {"data", "workload"}:
        raise ToolExecutionError("argument 'trace_type' must be 'data' or 'workload' when provided")

    mapping_path_raw = arguments.get("mapping_path")
    mapping_path = None
    if mapping_path_raw is not None:
        if not isinstance(mapping_path_raw, str) or not mapping_path_raw.strip():
            raise ToolExecutionError("argument 'mapping_path' must be a non-empty string when provided")
        mapping_path = str(_resolve_repo_path(mapping_path_raw, must_exist=True))

    spec = trace_to_spec(
        str(trace_path),
        str(output_path),
        trace_type=trace_type,
        mapping_path=mapping_path,
    )
    return {
        "ok": True,
        "spec_path": _as_rel(output_path),
        "pattern_id": spec.get("pattern_id", ""),
        "trace_type": spec.get("trace_type", ""),
    }


def _load_spec_for_summary(spec_path: Path) -> Tuple[Dict[str, Any], Dict[str, str]]:
    spec, type_info = load_and_validate_spec(str(spec_path))
    return spec, type_info


def _tool_validate_spec(arguments: Dict[str, Any]) -> Dict[str, Any]:
    spec_path = _resolve_repo_path(_require_str(arguments, "spec_path"), must_exist=True)
    spec, type_info = _load_spec_for_summary(spec_path)
    return {
        "ok": True,
        "spec_path": _as_rel(spec_path),
        "pattern_id": spec.get("pattern_id", ""),
        "type": type_info,
        "declared_outputs": _collect_declared_outputs(spec),
    }


def _tool_dry_run_spec(arguments: Dict[str, Any]) -> Dict[str, Any]:
    spec_path = _resolve_repo_path(_require_str(arguments, "spec_path"), must_exist=True)
    spec, type_info = _load_spec_for_summary(spec_path)
    return {
        "ok": True,
        "spec_path": _as_rel(spec_path),
        "pattern_id": spec.get("pattern_id", ""),
        "type": type_info,
        "seed": spec.get("seed"),
        "declared_outputs": _collect_declared_outputs(spec),
        "variables_keys": sorted((spec.get("variables") or {}).keys()),
        "would_execute": True,
    }


def _tool_run_spec(arguments: Dict[str, Any]) -> Dict[str, Any]:
    spec_path = _resolve_repo_path(_require_str(arguments, "spec_path"), must_exist=True)
    spec, type_info = _load_spec_for_summary(spec_path)
    run_summary = run_spec_and_return_summary(str(spec_path))
    return {
        "ok": bool(run_summary.get("ok", True)),
        "spec_path": _as_rel(spec_path),
        "pattern_id": spec.get("pattern_id", ""),
        "type": type_info,
        "declared_outputs": _collect_declared_outputs(spec),
    }


def _tool_list_outputs(arguments: Dict[str, Any]) -> Dict[str, Any]:
    root_raw = arguments.get("root", "output")
    if not isinstance(root_raw, str) or not root_raw.strip():
        raise ToolExecutionError("argument 'root' must be a non-empty string")
    root = _resolve_repo_path(root_raw, must_exist=False)
    include_dirs = _require_bool(arguments, "include_dirs", False)
    glob_pattern = arguments.get("glob", "**/*")
    if not isinstance(glob_pattern, str) or not glob_pattern.strip():
        raise ToolExecutionError("argument 'glob' must be a non-empty string")
    limit = _require_int(arguments, "limit", 200)
    if limit < 0 or limit > 5000:
        raise ToolExecutionError("argument 'limit' must be between 0 and 5000")

    if not root.exists():
        return {
            "ok": True,
            "root": _as_rel(root),
            "count": 0,
            "paths": [],
            "note": "root path does not exist",
        }

    if not root.is_dir():
        raise ToolExecutionError("argument 'root' must point to a directory")

    paths: List[str] = []
    for path in root.glob(glob_pattern):
        if not path.exists():
            continue
        if not include_dirs and not path.is_file():
            continue
        paths.append(_as_rel(path.resolve()))

    deduped = sorted(set(paths))
    if limit:
        deduped = deduped[:limit]
    else:
        deduped = []

    return {
        "ok": True,
        "root": _as_rel(root),
        "count": len(deduped),
        "paths": deduped,
    }


def _tool_save_spec(arguments: Dict[str, Any]) -> Dict[str, Any]:
    spec_path = _resolve_repo_path(_require_str(arguments, "spec_path"), must_exist=True)
    _load_yaml_file(spec_path)
    spec, type_info = _load_spec_for_summary(spec_path)

    spec_id_raw = arguments.get("spec_id")
    if spec_id_raw is not None:
        if not isinstance(spec_id_raw, str) or not spec_id_raw.strip():
            raise ToolExecutionError("argument 'spec_id' must be a non-empty string")
        spec_id = _slugify(spec_id_raw)
    else:
        stem = spec.get("pattern_id") or spec_path.stem
        spec_id = _slugify(str(stem))

    shared_dir = _shared_spec_dir()
    shared_dir.mkdir(parents=True, exist_ok=True)
    target_path = (shared_dir / f"{spec_id}.yaml").resolve()
    _resolve_repo_path(str(target_path), must_exist=False)

    overwrite = _require_bool(arguments, "overwrite", False)
    if target_path.exists() and not overwrite:
        raise ToolExecutionError(f"shared spec already exists: {_as_rel(target_path)}")

    title = arguments.get("title")
    if title is None:
        title = spec.get("pattern_id") or spec_path.stem
    if not isinstance(title, str) or not title.strip():
        raise ToolExecutionError("argument 'title' must be a non-empty string when provided")
    title = title.strip()

    description = arguments.get("description", "")
    if not isinstance(description, str):
        raise ToolExecutionError("argument 'description' must be a string")
    description = description.strip()

    tags = _ensure_str_list(arguments.get("tags"), "tags")
    owner = arguments.get("owner", "anonymous")
    if not isinstance(owner, str) or not owner.strip():
        raise ToolExecutionError("argument 'owner' must be a non-empty string when provided")
    owner = owner.strip()

    shutil.copy2(spec_path, target_path)

    catalog = _read_catalog()
    existing = _find_catalog_entry(catalog, spec_id)
    created_at = _now_iso()
    if existing:
        if not overwrite:
            raise ToolExecutionError(f"catalog entry already exists for id: {spec_id}")
        created_at = str(existing.get("created_at") or created_at)

    entry = {
        "id": spec_id,
        "title": title,
        "description": description,
        "tags": tags,
        "owner": owner,
        "pattern_id": str(spec.get("pattern_id", "")),
        "type": type_info,
        "source_path": _as_rel(spec_path),
        "shared_path": _as_rel(target_path),
        "created_at": created_at,
        "updated_at": _now_iso(),
    }

    if existing:
        catalog["specs"] = [entry if s.get("id") == spec_id else s for s in catalog.get("specs", [])]
    else:
        catalog.setdefault("specs", []).append(entry)

    _write_catalog(catalog)
    return {"ok": True, "spec": entry}


def _tool_list_public_specs(arguments: Dict[str, Any]) -> Dict[str, Any]:
    catalog = _read_catalog()
    specs = list(catalog.get("specs", []))

    tag = arguments.get("tag")
    if tag is not None:
        if not isinstance(tag, str) or not tag.strip():
            raise ToolExecutionError("argument 'tag' must be a non-empty string when provided")
        wanted = tag.strip().lower()
        specs = [s for s in specs if wanted in [str(t).lower() for t in (s.get("tags") or [])]]

    query = arguments.get("query")
    if query is not None:
        if not isinstance(query, str):
            raise ToolExecutionError("argument 'query' must be a string when provided")
        q = query.strip().lower()
        if q:
            specs = [
                s
                for s in specs
                if q in str(s.get("id", "")).lower()
                or q in str(s.get("title", "")).lower()
                or q in str(s.get("description", "")).lower()
            ]

    limit = _require_int(arguments, "limit", 100)
    if limit < 0 or limit > 5000:
        raise ToolExecutionError("argument 'limit' must be between 0 and 5000")

    specs = sorted(specs, key=lambda s: str(s.get("updated_at", "")), reverse=True)
    if limit:
        specs = specs[:limit]
    else:
        specs = []
    return {"ok": True, "count": len(specs), "specs": specs}


def _tool_import_spec_and_run(arguments: Dict[str, Any]) -> Dict[str, Any]:
    source_path, source_id = _pick_source_spec(arguments)
    _load_yaml_file(source_path)
    spec, type_info = _load_spec_for_summary(source_path)

    target_raw = arguments.get("target_path")
    if target_raw is None:
        ts = datetime.utcnow().strftime("%Y%m%d%H%M%S")
        target = _resolve_repo_path(f"driftspec/generated/imported_{_slugify(source_id)}_{ts}.yaml", must_exist=False)
    else:
        if not isinstance(target_raw, str) or not target_raw.strip():
            raise ToolExecutionError("argument 'target_path' must be a non-empty string when provided")
        target = _resolve_repo_path(target_raw, must_exist=False)

    overwrite = _require_bool(arguments, "overwrite", False)
    if target.exists() and not overwrite:
        raise ToolExecutionError(f"target spec already exists: {_as_rel(target)}")
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source_path, target)

    execute = _require_bool(arguments, "execute", True)
    run_result: Dict[str, Any] = {"executed": False}
    if execute:
        summary = run_spec_and_return_summary(str(target))
        run_result = {"executed": True, "ok": bool(summary.get("ok", True))}

    return {
        "ok": True,
        "source_spec_path": _as_rel(source_path),
        "imported_spec_path": _as_rel(target),
        "pattern_id": spec.get("pattern_id", ""),
        "type": type_info,
        "declared_outputs": _collect_declared_outputs(spec),
        "run": run_result,
    }


@dataclass(frozen=True)
class ToolDef:
    name: str
    description: str
    input_schema: Dict[str, Any]
    handler: Callable[[Dict[str, Any]], Dict[str, Any]]


TOOLS: List[ToolDef] = [
    ToolDef(
        name="driftbench_health",
        description="Return DriftBench MCP server health and active repo root.",
        input_schema={
            "type": "object",
            "properties": {},
            "additionalProperties": False,
        },
        handler=_tool_health,
    ),
    ToolDef(
        name="trace_to_spec",
        description="Generate a DriftSpec YAML from a trace summary input.",
        input_schema={
            "type": "object",
            "properties": {
                "trace_path": {"type": "string"},
                "output_path": {"type": "string"},
                "trace_type": {"type": "string", "enum": ["data", "workload"]},
                "mapping_path": {"type": "string"},
            },
            "required": ["trace_path", "output_path"],
            "additionalProperties": False,
        },
        handler=_tool_trace_to_spec,
    ),
    ToolDef(
        name="validate_spec",
        description="Validate a DriftSpec and return type metadata plus declared outputs.",
        input_schema={
            "type": "object",
            "properties": {
                "spec_path": {"type": "string"},
            },
            "required": ["spec_path"],
            "additionalProperties": False,
        },
        handler=_tool_validate_spec,
    ),
    ToolDef(
        name="dry_run_spec",
        description="Preview a DriftSpec execution without running generators.",
        input_schema={
            "type": "object",
            "properties": {
                "spec_path": {"type": "string"},
            },
            "required": ["spec_path"],
            "additionalProperties": False,
        },
        handler=_tool_dry_run_spec,
    ),
    ToolDef(
        name="run_spec",
        description="Execute a DriftSpec end-to-end and return summary metadata.",
        input_schema={
            "type": "object",
            "properties": {
                "spec_path": {"type": "string"},
            },
            "required": ["spec_path"],
            "additionalProperties": False,
        },
        handler=_tool_run_spec,
    ),
    ToolDef(
        name="list_outputs",
        description="List output files from a root directory for automation.",
        input_schema={
            "type": "object",
            "properties": {
                "root": {"type": "string"},
                "glob": {"type": "string"},
                "limit": {"type": "integer"},
                "include_dirs": {"type": "boolean"},
            },
            "additionalProperties": False,
        },
        handler=_tool_list_outputs,
    ),
    ToolDef(
        name="save_spec",
        description="Save a validated DriftSpec into shared storage and register it in the public spec catalog.",
        input_schema={
            "type": "object",
            "properties": {
                "spec_path": {"type": "string"},
                "spec_id": {"type": "string"},
                "title": {"type": "string"},
                "description": {"type": "string"},
                "tags": {"type": "array", "items": {"type": "string"}},
                "owner": {"type": "string"},
                "overwrite": {"type": "boolean"},
            },
            "required": ["spec_path"],
            "additionalProperties": False,
        },
        handler=_tool_save_spec,
    ),
    ToolDef(
        name="list_public_specs",
        description="List shared/public specs from the MCP catalog with optional filters.",
        input_schema={
            "type": "object",
            "properties": {
                "tag": {"type": "string"},
                "query": {"type": "string"},
                "limit": {"type": "integer"},
            },
            "additionalProperties": False,
        },
        handler=_tool_list_public_specs,
    ),
    ToolDef(
        name="import_spec_and_run",
        description="Import a shared spec (by id or path) into local generated specs and optionally execute it.",
        input_schema={
            "type": "object",
            "properties": {
                "spec_id": {"type": "string"},
                "spec_path": {"type": "string"},
                "target_path": {"type": "string"},
                "overwrite": {"type": "boolean"},
                "execute": {"type": "boolean"},
            },
            "additionalProperties": False,
        },
        handler=_tool_import_spec_and_run,
    ),
]


TOOL_MAP: Dict[str, ToolDef] = {tool.name: tool for tool in TOOLS}


def call_tool(name: str, arguments: Dict[str, Any]) -> Dict[str, Any]:
    tool = TOOL_MAP.get(name)
    if tool is None:
        raise ToolExecutionError(f"unknown tool: {name}")
    if not isinstance(arguments, dict):
        raise ToolExecutionError("tool arguments must be an object")
    return tool.handler(arguments)


def _jsonrpc_result(request_id: Any, result: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "jsonrpc": JSONRPC_VERSION,
        "id": request_id,
        "result": result,
    }


def _jsonrpc_error(request_id: Any, code: int, message: str, data: Any = None) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "jsonrpc": JSONRPC_VERSION,
        "id": request_id,
        "error": {"code": code, "message": message},
    }
    if data is not None:
        payload["error"]["data"] = data
    return payload


def _write_message(message: Dict[str, Any]) -> None:
    body = json.dumps(message, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    header = f"Content-Length: {len(body)}\r\n\r\n".encode("ascii")
    sys.stdout.buffer.write(header)
    sys.stdout.buffer.write(body)
    sys.stdout.buffer.flush()


def _read_message() -> Dict[str, Any] | None:
    headers: Dict[str, str] = {}
    while True:
        line = sys.stdin.buffer.readline()
        if not line:
            return None
        if line in (b"\r\n", b"\n"):
            break
        decoded = line.decode("ascii", errors="replace").strip()
        if ":" not in decoded:
            continue
        key, value = decoded.split(":", 1)
        headers[key.strip().lower()] = value.strip()

    length_raw = headers.get("content-length")
    if length_raw is None:
        return None
    try:
        length = int(length_raw)
    except ValueError:
        return None
    if length <= 0:
        return None

    body = sys.stdin.buffer.read(length)
    if not body:
        return None
    return json.loads(body.decode("utf-8"))


def _handle_request(request: Dict[str, Any]) -> Dict[str, Any] | None:
    request_id = request.get("id")
    method = request.get("method")
    params = request.get("params", {})

    if not isinstance(method, str):
        return _jsonrpc_error(request_id, -32600, "Invalid Request")

    if method == "initialize":
        result = {
            "protocolVersion": MCP_PROTOCOL_VERSION,
            "capabilities": {
                "tools": {"listChanged": False},
                "resources": {"subscribe": False, "listChanged": False},
                "prompts": {"listChanged": False},
            },
            "serverInfo": {
                "name": SERVER_NAME,
                "version": SERVER_VERSION,
            },
        }
        return _jsonrpc_result(request_id, result)

    if method == "ping":
        return _jsonrpc_result(request_id, {"ok": True})

    if method == "tools/list":
        tools_payload = [
            {
                "name": tool.name,
                "description": tool.description,
                "inputSchema": tool.input_schema,
            }
            for tool in TOOLS
        ]
        return _jsonrpc_result(request_id, {"tools": tools_payload})

    if method == "tools/call":
        if not isinstance(params, dict):
            return _jsonrpc_error(request_id, -32602, "Invalid params")
        name = params.get("name")
        arguments = params.get("arguments", {})
        if not isinstance(name, str) or not name:
            return _jsonrpc_error(request_id, -32602, "Invalid params: missing tool name")
        if not isinstance(arguments, dict):
            return _jsonrpc_error(request_id, -32602, "Invalid params: arguments must be object")
        try:
            structured = call_tool(name, arguments)
            return _jsonrpc_result(
                request_id,
                {
                    "content": [{"type": "text", "text": json.dumps(structured, ensure_ascii=False)}],
                    "structuredContent": structured,
                    "isError": False,
                },
            )
        except ToolExecutionError as exc:
            return _jsonrpc_result(
                request_id,
                {
                    "content": [{"type": "text", "text": str(exc)}],
                    "isError": True,
                },
            )
        except Exception as exc:
            return _jsonrpc_result(
                request_id,
                {
                    "content": [{"type": "text", "text": f"internal tool error: {exc}"}],
                    "isError": True,
                },
            )

    if method == "resources/list":
        return _jsonrpc_result(request_id, {"resources": []})

    if method == "prompts/list":
        return _jsonrpc_result(request_id, {"prompts": []})

    if method == "shutdown":
        return _jsonrpc_result(request_id, {})

    if method in {"initialized", "notifications/initialized", "exit"}:
        # Notification methods do not expect responses.
        return None

    return _jsonrpc_error(request_id, -32601, f"Method not found: {method}")


def serve() -> int:
    try:
        while True:
            message = _read_message()
            if message is None:
                return 0
            if not isinstance(message, dict):
                _write_message(_jsonrpc_error(None, -32600, "Invalid Request"))
                continue
            response = _handle_request(message)
            if response is not None and "id" in message:
                _write_message(response)
    except KeyboardInterrupt:
        return 0
    except Exception as exc:
        sys.stderr.write(f"[{SERVER_NAME}] fatal server error: {exc}\n")
        return EXIT_RUNTIME_ERROR


def main() -> int:
    return serve()


if __name__ == "__main__":
    raise SystemExit(main())
