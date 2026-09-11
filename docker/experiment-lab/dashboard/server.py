"""Small, dependency-free, read-only dashboard for DriftBench run artifacts."""

from __future__ import annotations

import argparse
import html
import json
import math
import os
import re
import secrets
import stat
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import unquote_to_bytes, urlsplit

SAFE_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,79}$")
VISIBLE_DIRS = {"logs", "results", "metrics", "figures"}
HIDDEN_PARTS = {"secrets", "postgres-data", "grafana", "prometheus", "pushgateway"}
MAX_EVENTS = 50
MAX_ARTIFACTS = 200
MAX_EVENT_BYTES = 256 * 1024
MAX_JSON_BYTES = 256 * 1024
MAX_JSON_NESTING = 100
MAX_RUNS = 200
MAX_DIRECTORY_ENTRIES = 2_000
MAX_ARTIFACT_DEPTH = 12
INVALID_PERCENT_ESCAPE = re.compile(r"%(?![0-9A-Fa-f]{2})")
PORTABLE_SEGMENT = re.compile(r"^[A-Za-z0-9_][A-Za-z0-9._-]*$")
MAX_INTEGER_DIGITS = 128
WINDOWS_RESERVED_NAMES = {
    "con",
    "prn",
    "aux",
    "nul",
    *(f"com{number}" for number in range(1, 10)),
    *(f"lpt{number}" for number in range(1, 10)),
}


def _inside(child: Path, parent: Path) -> bool:
    try:
        child.relative_to(parent)
        return True
    except ValueError:
        return False


def _portable_windows_segment(value: str) -> bool:
    if not value or value[-1] in {".", " "} or "\\" in value or ":" in value:
        return False
    if any(ord(character) < 32 or ord(character) == 127 for character in value):
        return False
    if PORTABLE_SEGMENT.fullmatch(value) is None:
        return False
    return value.split(".", 1)[0].casefold() not in WINDOWS_RESERVED_NAMES


def _safe_id(value: str) -> bool:
    return SAFE_ID.fullmatch(value) is not None and _portable_windows_segment(value)


def _artifact_parts(relative_path: str) -> tuple[str, ...] | None:
    if not isinstance(relative_path, str) or relative_path.startswith("/"):
        return None
    parts = tuple(relative_path.split("/"))
    if len(parts) < 2 or parts[0] not in VISIBLE_DIRS:
        return None
    if any(not _portable_windows_segment(part) for part in parts):
        return None
    if HIDDEN_PARTS.intersection(part.casefold() for part in parts):
        return None
    return parts


def _strict_json_loads(value: str | bytes) -> Any:
    quote = 34 if isinstance(value, bytes) else '"'
    backslash = 92 if isinstance(value, bytes) else "\\"
    openers = {91, 123} if isinstance(value, bytes) else {"[", "{"}
    closers = {93, 125} if isinstance(value, bytes) else {"]", "}"}
    depth = 0
    in_string = False
    escaped = False
    for token in value:
        if in_string:
            if escaped:
                escaped = False
            elif token == backslash:
                escaped = True
            elif token == quote:
                in_string = False
            continue
        if token == quote:
            in_string = True
        elif token in openers:
            depth += 1
            if depth > MAX_JSON_NESTING:
                raise ValueError("JSON nesting exceeds dashboard limit")
        elif token in closers:
            depth = max(0, depth - 1)

    def parse_int(token: str) -> int:
        if len(token.lstrip("-")) > MAX_INTEGER_DIGITS:
            raise ValueError("integer exceeds dashboard limit")
        return int(token)

    def parse_float(token: str) -> float:
        parsed = float(token)
        if not math.isfinite(parsed):
            raise ValueError("non-finite number")
        return parsed

    def reject_constant(token: str) -> None:
        raise ValueError(f"non-finite constant: {token}")

    try:
        return json.loads(value, parse_int=parse_int, parse_float=parse_float, parse_constant=reject_constant)
    except RecursionError as error:
        raise ValueError("JSON nesting exceeds dashboard limit") from error


def _finite_number(value: Any) -> bool:
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        return False
    try:
        return math.isfinite(float(value))
    except (OverflowError, ValueError):
        return False


def _is_link_like(path: Path) -> bool:
    """Return true for symlinks and Windows junction/reparse points."""
    try:
        if path.is_symlink():
            return True
        is_junction = getattr(path, "is_junction", None)
        if is_junction is not None and is_junction():
            return True
        attributes = getattr(path.lstat(), "st_file_attributes", 0)
    except (OSError, ValueError):
        return False
    return bool(attributes & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0))


def _safe_path(path: Path, root: Path, *, directory: bool) -> Path | None:
    """Resolve an existing child while rejecting links in every path component."""
    try:
        resolved_root = root.resolve(strict=True)
        relative = path.relative_to(resolved_root)
    except (OSError, ValueError):
        return None
    current = resolved_root
    for part in relative.parts:
        current = current / part
        if _is_link_like(current):
            return None
    try:
        resolved = path.resolve(strict=True)
    except (OSError, ValueError):
        return None
    if not _inside(resolved, resolved_root):
        return None
    try:
        expected_type = resolved.is_dir() if directory else resolved.is_file()
    except (OSError, ValueError):
        return None
    return resolved if expected_type else None


def _safe_directory(path: Path, root: Path) -> Path | None:
    return _safe_path(path, root, directory=True)


def _safe_file(path: Path, root: Path) -> Path | None:
    return _safe_path(path, root, directory=False)


def _runs_root(state_root: Path) -> Path | None:
    try:
        state = state_root.resolve(strict=True)
    except (OSError, ValueError):
        return None
    if not state.is_dir():
        return None
    unresolved = state / "runs"
    if _is_link_like(unresolved):
        return None
    return _safe_directory(unresolved, state)


def _run_directory(state_root: Path, experiment_id: str, run_id: str) -> Path | None:
    if not _safe_id(experiment_id) or not _safe_id(run_id):
        return None
    runs_root = _runs_root(state_root)
    if runs_root is None:
        return None
    experiment_dir = _safe_directory(runs_root / experiment_id, runs_root)
    if experiment_dir is None:
        return None
    return _safe_directory(experiment_dir / run_id, experiment_dir)


def _bounded_children(directory: Path, limit: int) -> list[Path]:
    children: list[Path] = []
    try:
        with os.scandir(directory) as entries:
            for entry in entries:
                if len(children) >= limit:
                    break
                children.append(Path(entry.path))
    except (OSError, ValueError):
        return []
    return sorted(children, key=lambda path: path.name)


def _read_json(path: Path, root: Path) -> dict[str, Any] | None:
    safe = _safe_file(path, root)
    if safe is None:
        return None
    try:
        if safe.stat().st_size > MAX_JSON_BYTES:
            return None
        raw = safe.read_bytes()
        if len(raw) > MAX_JSON_BYTES:
            return None
        value = _strict_json_loads(raw.decode("utf-8"))
    except (OSError, UnicodeError, ValueError):
        return None
    return value if isinstance(value, dict) else None


def _read_events(path: Path, root: Path) -> list[dict[str, Any]]:
    safe = _safe_file(path, root)
    if safe is None:
        return []
    try:
        with safe.open("rb") as stream:
            stream.seek(0, 2)
            size = stream.tell()
            stream.seek(max(0, size - MAX_EVENT_BYTES))
            raw = stream.read(MAX_EVENT_BYTES)
    except (OSError, UnicodeError):
        return []
    try:
        lines = raw.decode("utf-8", errors="strict").splitlines()[-MAX_EVENTS:]
    except UnicodeError:
        return []
    events: list[dict[str, Any]] = []
    for line in lines:
        try:
            event = _strict_json_loads(line)
        except ValueError:
            return []
        if not isinstance(event, dict):
            return []
        events.append(event)
    return events


def _artifacts(run_dir: Path) -> list[dict[str, Any]]:
    try:
        root = run_dir.resolve(strict=True)
    except (OSError, ValueError):
        return []
    output: list[dict[str, Any]] = []
    visited = 0
    for category in sorted(VISIBLE_DIRS):
        base = _safe_directory(root / category, root)
        if base is None:
            continue
        pending: list[tuple[Path, int]] = [(base, 0)]
        while pending and visited < MAX_DIRECTORY_ENTRIES and len(output) < MAX_ARTIFACTS:
            current, depth = pending.pop()
            remaining = MAX_DIRECTORY_ENTRIES - visited
            children = _bounded_children(current, remaining)
            visited += len(children)
            for path in children:
                try:
                    relative = path.relative_to(root)
                except ValueError:
                    continue
                if _artifact_parts(relative.as_posix()) is None or _is_link_like(path):
                    continue
                directory = _safe_directory(path, root)
                if directory is not None:
                    if depth < MAX_ARTIFACT_DEPTH:
                        pending.append((directory, depth + 1))
                    continue
                resolved = _safe_file(path, root)
                if resolved is None:
                    continue
                try:
                    canonical_relative = resolved.relative_to(root)
                except ValueError:
                    continue
                if _artifact_parts(canonical_relative.as_posix()) is None:
                    continue
                try:
                    size = resolved.stat().st_size
                except OSError:
                    continue
                output.append({"path": canonical_relative.as_posix(), "bytes": size, "category": category})
                if len(output) >= MAX_ARTIFACTS:
                    break
    return sorted(output, key=lambda item: str(item["path"]))


def discover_runs(state_root: Path) -> list[dict[str, Any]]:
    """Return safe, newest-first summaries without changing the state tree."""
    runs_root = _runs_root(state_root)
    if runs_root is None:
        return []
    runs: list[dict[str, Any]] = []
    for experiment_path in _bounded_children(runs_root, MAX_RUNS):
        if not _safe_id(experiment_path.name):
            continue
        experiment_dir = _safe_directory(experiment_path, runs_root)
        if experiment_dir is None:
            continue
        for run_path in _bounded_children(experiment_dir, MAX_RUNS - len(runs)):
            if len(runs) >= MAX_RUNS or not _safe_id(run_path.name):
                continue
            run_dir = _safe_directory(run_path, experiment_dir)
            if run_dir is None or not _inside(run_dir, runs_root):
                continue
            status = _read_json(run_dir / "status.json", run_dir) or {}
            manifest = _read_json(run_dir / "run-manifest.json", run_dir) or {}
            completed = status.get("completed")
            total = status.get("total")
            progress = None
            if _finite_number(completed) and _finite_number(total) and total > 0:
                progress = max(0.0, min(1.0, float(completed) / float(total)))
            updated = status.get("updated_at") or manifest.get("finished_at") or manifest.get("started_at") or ""
            runs.append(
                {
                    "experiment_id": experiment_dir.name,
                    "run_id": run_dir.name,
                    "state": status.get("state") or manifest.get("exit_status") or "unknown",
                    "phase": status.get("phase") or "",
                    "message": status.get("message") or "",
                    "completed": completed,
                    "total": total,
                    "progress": progress,
                    "updated_at": updated,
                    "events": _read_events(run_dir / "events.ndjson", run_dir),
                    "artifacts": _artifacts(run_dir),
                }
            )
    return sorted(runs, key=lambda item: str(item["updated_at"]), reverse=True)


def resolve_artifact(state_root: Path, experiment_id: str, run_id: str, relative_path: str) -> Path | None:
    """Resolve a public artifact while rejecting traversal, symlinks, and private state."""
    parts = _artifact_parts(relative_path)
    if parts is None:
        return None
    run_dir = _run_directory(state_root, experiment_id, run_id)
    if run_dir is None:
        return None
    resolved = _safe_file(run_dir.joinpath(*parts), run_dir)
    if resolved is None:
        return None
    try:
        canonical = resolved.relative_to(run_dir).as_posix()
    except ValueError:
        return None
    return resolved if _artifact_parts(canonical) is not None else None


def _decode_request_path(request_target: str) -> str | None:
    if "\x00" in request_target or INVALID_PERCENT_ESCAPE.search(request_target):
        return None
    try:
        raw_path = urlsplit(request_target).path
        decoded = unquote_to_bytes(raw_path).decode("utf-8", errors="strict")
    except (UnicodeError, ValueError):
        return None
    if any(ord(character) < 32 or ord(character) == 127 for character in decoded):
        return None
    return decoded


def _download_name(name: str) -> str:
    safe = re.sub(r"[^A-Za-z0-9._-]", "_", name)[:120]
    return safe or "artifact"


HTML_PAGE = """<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>DriftBench Experiment Lab</title>
<style nonce="__CSP_NONCE__">
:root{color-scheme:dark;--bg:#0b1020;--card:#141b2f;--ink:#ecf2ff;--muted:#9aa7bd;--cyan:#51d6ca;--blue:#668cff;--red:#ff6b7a;--line:#26324e}
*{box-sizing:border-box}body{margin:0;background:radial-gradient(circle at 15% 0,#18284a 0,transparent 36%),var(--bg);color:var(--ink);font:14px/1.5 Inter,Segoe UI,sans-serif}
main{max-width:1180px;margin:auto;padding:32px 24px 72px}h1{font-size:30px;margin:0}.lede{color:var(--muted);margin:6px 0 28px}.summary{display:flex;gap:12px;margin-bottom:18px;flex-wrap:wrap}.pill{background:#10182b;border:1px solid var(--line);padding:8px 12px;border-radius:999px}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(340px,1fr));gap:16px}.card{background:linear-gradient(145deg,#17213a,#11182b);border:1px solid var(--line);border-radius:16px;padding:18px;box-shadow:0 18px 50px #0005}.top{display:flex;justify-content:space-between;gap:12px}.id{font:600 13px ui-monospace,Consolas,monospace;color:var(--cyan)}.state{font-weight:700}.failed{color:var(--red)}.succeeded{color:var(--cyan)}
.phase{font-size:18px;font-weight:650;margin:8px 0 2px}.muted{color:var(--muted)}progress{width:100%;height:8px;appearance:none;margin:14px 0;border:0;border-radius:99px;overflow:hidden;background:#0b1120}progress::-webkit-progress-bar{background:#0b1120}progress::-webkit-progress-value{background:linear-gradient(90deg,var(--blue),var(--cyan))}progress::-moz-progress-bar{background:linear-gradient(90deg,var(--blue),var(--cyan))}.artifacts{display:flex;gap:7px;flex-wrap:wrap;margin-top:13px}.artifacts a{color:var(--ink);text-decoration:none;background:#202c48;border:1px solid #334363;border-radius:8px;padding:5px 8px}.event{border-left:2px solid #344769;padding-left:9px;margin-top:9px;color:var(--muted)}
.empty{padding:28px;border:1px dashed var(--line);border-radius:14px;color:var(--muted)}code{font-family:ui-monospace,Consolas,monospace}button{background:#233150;color:var(--ink);border:1px solid #3a4c72;border-radius:8px;padding:7px 10px;cursor:pointer}
</style></head><body><main><div class="top"><div><h1>DriftBench Experiment Lab</h1><p class="lede">Read-only local progress and saved evidence</p></div><button id="refresh" type="button">Refresh</button></div><div id="summary" class="summary"></div><div id="runs" class="grid"></div></main>
<script nonce="__CSP_NONCE__">
const esc=s=>String(s??'').replace(/[&<>\"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','\"':'&quot;',"'":'&#39;'}[c]));
async function loadRuns(){const root=document.getElementById('runs');try{const rows=await(await fetch('/api/runs',{cache:'no-store'})).json();document.getElementById('summary').innerHTML=`<span class="pill">${rows.length} runs</span><span class="pill">${rows.filter(x=>x.state==='running').length} active</span>`;root.innerHTML=rows.length?rows.map(card).join(''):'<div class="empty">No run artifacts found under the selected state root.</div>';}catch(e){root.innerHTML='<div class="empty">Unable to read run state.</div>';}}
function card(r){const p=r.progress==null?0:Math.round(r.progress*100);const links=r.artifacts.slice(0,10).map(a=>`<a href="/artifacts/${encodeURIComponent(r.experiment_id)}/${encodeURIComponent(r.run_id)}/${a.path.split('/').map(encodeURIComponent).join('/')}">${esc(a.path)}</a>`).join('');const events=r.events.slice(-2).map(e=>`<div class="event">${esc(e.timestamp||'')} · ${esc(e.message||'')}</div>`).join('');return `<article class="card"><div class="top"><span class="id">${esc(r.experiment_id)} / ${esc(r.run_id)}</span><span class="state ${esc(r.state)}">${esc(r.state)}</span></div><div class="phase">${esc(r.phase||'No phase reported')}</div><div class="muted">${esc(r.message||r.updated_at||'No status message')}</div><progress max="100" value="${p}">${p}%</progress><div class="muted">${r.progress==null?'Progress unavailable':p+'%'}</div>${events}<div class="artifacts">${links}</div></article>`}
document.getElementById('refresh').addEventListener('click',loadRuns);loadRuns();setInterval(loadRuns,5000);
</script></body></html>"""

LOCKED_CSP = (
    "default-src 'none'; base-uri 'none'; form-action 'none'; "
    "frame-ancestors 'none'; object-src 'none'; sandbox"
)


def _page_csp(nonce: str) -> str:
    return (
        "default-src 'none'; "
        f"script-src 'nonce-{nonce}'; style-src 'nonce-{nonce}'; "
        "connect-src 'self'; base-uri 'none'; form-action 'none'; "
        "frame-ancestors 'none'; object-src 'none'"
    )


def make_handler(state_root: Path) -> type[BaseHTTPRequestHandler]:
    root = state_root.resolve()

    class Handler(BaseHTTPRequestHandler):
        server_version = "DriftBenchLab/1"

        def _headers(
            self,
            status: HTTPStatus,
            content_type: str,
            length: int | None = None,
            *,
            csp: str = LOCKED_CSP,
            download_name: str | None = None,
        ) -> None:
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.send_header("X-Frame-Options", "DENY")
            self.send_header("Content-Security-Policy", csp)
            self.send_header("Cross-Origin-Resource-Policy", "same-origin")
            self.send_header("Referrer-Policy", "no-referrer")
            self.send_header("Permissions-Policy", "camera=(), microphone=(), geolocation=()")
            if download_name is not None:
                self.send_header("Content-Disposition", f'attachment; filename="{_download_name(download_name)}"')
                self.send_header("X-Download-Options", "noopen")
            if length is not None:
                self.send_header("Content-Length", str(length))
            self.end_headers()

        def _json(self, value: Any, status: HTTPStatus = HTTPStatus.OK) -> None:
            body = json.dumps(value, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode("utf-8")
            self._headers(status, "application/json; charset=utf-8", len(body))
            self.wfile.write(body)

        def do_GET(self) -> None:  # noqa: N802 - stdlib API
            path = _decode_request_path(self.path)
            if path is None:
                self._json({"error": "not_found"}, HTTPStatus.NOT_FOUND)
                return
            if path == "/":
                nonce = secrets.token_urlsafe(18)
                body = HTML_PAGE.replace("__CSP_NONCE__", nonce).encode("utf-8")
                self._headers(HTTPStatus.OK, "text/html; charset=utf-8", len(body), csp=_page_csp(nonce))
                self.wfile.write(body)
                return
            if path == "/api/runs":
                self._json(discover_runs(root))
                return
            if path.startswith("/artifacts/"):
                parts = path.split("/")[2:]
                if len(parts) < 3:
                    self._json({"error": "not_found"}, HTTPStatus.NOT_FOUND)
                    return
                relative_path = "/".join(parts[2:])
                artifact = resolve_artifact(root, parts[0], parts[1], relative_path)
                if artifact is None:
                    self._json({"error": "not_found"}, HTTPStatus.NOT_FOUND)
                    return
                try:
                    stream = artifact.open("rb")
                    opened = os.fstat(stream.fileno())
                    confirmed = resolve_artifact(root, parts[0], parts[1], relative_path)
                    current = artifact.stat(follow_symlinks=False)
                except OSError:
                    self._json({"error": "unavailable"}, HTTPStatus.NOT_FOUND)
                    return
                with stream:
                    if (
                        confirmed != artifact
                        or _is_link_like(artifact)
                        or not stat.S_ISREG(opened.st_mode)
                        or (opened.st_dev, opened.st_ino) != (current.st_dev, current.st_ino)
                    ):
                        self._json({"error": "unavailable"}, HTTPStatus.NOT_FOUND)
                        return
                    self._headers(
                        HTTPStatus.OK,
                        "application/octet-stream",
                        opened.st_size,
                        download_name=artifact.name,
                    )
                    while chunk := stream.read(64 * 1024):
                        self.wfile.write(chunk)
                return
            self._json({"error": "not_found"}, HTTPStatus.NOT_FOUND)

        def log_message(self, fmt: str, *args: object) -> None:
            print(f"dashboard {self.address_string()} {fmt % args}")

    return Handler


def serve(state_root: Path, host: str = "127.0.0.1", port: int = 8765) -> None:
    if host != "127.0.0.1":
        raise ValueError("The review dashboard may bind only to 127.0.0.1")
    server = ThreadingHTTPServer((host, port), make_handler(state_root))
    print(f"DriftBench dashboard: http://{host}:{port}")
    print(f"Read-only state root: {html.escape(str(state_root.resolve()))}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-root", type=Path, required=True)
    parser.add_argument("--host", default="127.0.0.1", choices=["127.0.0.1"])
    parser.add_argument("--port", type=int, default=8765)
    args = parser.parse_args()
    serve(args.state_root, args.host, args.port)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
