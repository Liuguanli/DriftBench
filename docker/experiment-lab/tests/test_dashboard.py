from __future__ import annotations

import json
import os
import re
import sys
import threading
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import urlopen

import pytest

LAB_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(LAB_ROOT))

import dashboard.server as dashboard_server  # noqa: E402
from dashboard.server import ThreadingHTTPServer, discover_runs, make_handler, resolve_artifact  # noqa: E402


def _write_run(state_root: Path) -> Path:
    run = state_root / "runs" / "rel-ce-tpch" / "run-42"
    (run / "logs").mkdir(parents=True)
    (run / "results" / "secrets").mkdir(parents=True)
    (run / "figures").mkdir(parents=True)
    (run / "status.json").write_text(
        json.dumps(
            {
                "state": "running",
                "phase": "skew-4",
                "completed": 3,
                "total": 6,
                "updated_at": "2026-08-23T12:04:00Z",
                "message": "Applying skew",
            }
        ),
        encoding="utf-8",
    )
    (run / "events.ndjson").write_text(
        '{"timestamp":"2026-08-23T12:04:00Z","message":"phase started"}\n',
        encoding="utf-8",
    )
    (run / "logs" / "runner.log").write_text("safe log\n", encoding="utf-8")
    (run / "figures" / "overview.png").write_bytes(b"not-a-real-png")
    (run / "results" / "secrets" / "password.txt").write_text("hidden", encoding="utf-8")
    return run


def test_dashboard_discovers_progress_and_hides_secrets(tmp_path: Path) -> None:
    _write_run(tmp_path)
    rows = discover_runs(tmp_path)
    assert len(rows) == 1
    assert rows[0]["experiment_id"] == "rel-ce-tpch"
    assert rows[0]["state"] == "running"
    assert rows[0]["progress"] == 0.5
    paths = {item["path"] for item in rows[0]["artifacts"]}
    assert "logs/runner.log" in paths
    assert "figures/overview.png" in paths
    assert not any("secrets" in path for path in paths)
    assert len(rows[0]["events"]) == 1


def test_artifact_resolution_rejects_escape_and_private_paths(tmp_path: Path) -> None:
    run = _write_run(tmp_path)
    assert resolve_artifact(tmp_path, "rel-ce-tpch", "run-42", "logs/runner.log") == (run / "logs/runner.log").resolve()
    assert resolve_artifact(tmp_path, "rel-ce-tpch", "run-42", "../outside.txt") is None
    assert resolve_artifact(tmp_path, "rel-ce-tpch", "run-42", "results/secrets/password.txt") is None
    assert resolve_artifact(tmp_path, "rel-ce-tpch,other", "run-42", "logs/runner.log") is None


@pytest.mark.parametrize(
    "relative_path",
    [
        "/etc/passwd",
        "C:/Windows/win.ini",
        "logs\\..\\outside.txt",
        "logs//runner.log",
        "logs/runner.log\x00.html",
        "logs/NUL.txt",
        "logs/report.",
        "logs/report ",
    ],
)
def test_artifact_resolution_rejects_absolute_and_malformed_paths(tmp_path: Path, relative_path: str) -> None:
    _write_run(tmp_path)
    assert resolve_artifact(tmp_path, "rel-ce-tpch", "run-42", relative_path) is None


def test_artifact_resolution_rejects_symlink(tmp_path: Path) -> None:
    run = _write_run(tmp_path)
    outside = tmp_path / "outside.txt"
    outside.write_text("outside", encoding="utf-8")
    link = run / "logs" / "escape.txt"
    try:
        os.symlink(outside, link)
    except (OSError, NotImplementedError):
        pytest.skip("symlinks unavailable for this user")
    assert resolve_artifact(tmp_path, "rel-ce-tpch", "run-42", "logs/escape.txt") is None


def test_link_like_run_directory_is_rejected_without_os_symlink_support(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_run(tmp_path)
    original = dashboard_server._is_link_like
    monkeypatch.setattr(
        dashboard_server,
        "_is_link_like",
        lambda path: path.name == "run-42" or original(path),
    )
    assert discover_runs(tmp_path) == []
    assert resolve_artifact(tmp_path, "rel-ce-tpch", "run-42", "logs/runner.log") is None


def test_link_like_metadata_and_artifact_files_are_never_read(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    run = _write_run(tmp_path)
    (run / "run-manifest.json").write_text(
        json.dumps({"exit_status": "succeeded", "started_at": "outside"}),
        encoding="utf-8",
    )
    original = dashboard_server._is_link_like
    blocked = {"status.json", "events.ndjson", "run-manifest.json", "runner.log"}
    monkeypatch.setattr(
        dashboard_server,
        "_is_link_like",
        lambda path: path.name in blocked or original(path),
    )
    rows = discover_runs(tmp_path)
    assert len(rows) == 1
    assert rows[0]["state"] == "unknown"
    assert rows[0]["updated_at"] == ""
    assert rows[0]["events"] == []
    assert "logs/runner.log" not in {artifact["path"] for artifact in rows[0]["artifacts"]}
    assert resolve_artifact(tmp_path, "rel-ce-tpch", "run-42", "logs/runner.log") is None


def test_link_like_runs_root_is_rejected_without_junction_privileges(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_run(tmp_path)
    original = dashboard_server._is_link_like
    runs_root = (tmp_path / "runs").resolve()
    monkeypatch.setattr(
        dashboard_server,
        "_is_link_like",
        lambda path: path == runs_root or original(path),
    )
    assert discover_runs(tmp_path) == []
    assert resolve_artifact(tmp_path, "rel-ce-tpch", "run-42", "logs/runner.log") is None


def test_post_resolve_canonical_path_is_rechecked(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    run = _write_run(tmp_path)
    hidden = (run / "results" / "secrets" / "password.txt").resolve()
    monkeypatch.setattr(dashboard_server, "_safe_file", lambda _path, _root: hidden)
    assert resolve_artifact(tmp_path, "rel-ce-tpch", "run-42", "logs/runner.log") is None


@pytest.mark.parametrize("identifier", ["CON", "nul.txt", "run-42.", "run-42 ", "run\x01"])
def test_windows_noncanonical_ids_are_rejected(identifier: str) -> None:
    assert dashboard_server._safe_id(identifier) is False


def test_oversized_status_is_treated_as_unavailable(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _write_run(tmp_path)
    monkeypatch.setattr(dashboard_server, "MAX_JSON_BYTES", 16)
    rows = discover_runs(tmp_path)
    assert rows[0]["state"] == "unknown"
    assert rows[0]["progress"] is None


@pytest.mark.parametrize(
    "payload",
    [
        '{"value":NaN}',
        '{"value":Infinity}',
        '{"value":1e999}',
        '{"value":' + "9" * 129 + "}",
    ],
)
def test_strict_json_rejects_nonfinite_and_huge_numbers(payload: str) -> None:
    with pytest.raises(ValueError):
        dashboard_server._strict_json_loads(payload)


def test_malformed_status_manifest_and_event_streams_are_unavailable(tmp_path: Path) -> None:
    run = _write_run(tmp_path)
    (run / "status.json").write_text('{"completed":NaN}', encoding="utf-8")
    (run / "run-manifest.json").write_text('{"exit_status":1e999}', encoding="utf-8")
    (run / "events.ndjson").write_text(
        '{"timestamp":"ok","message":"first"}\n{"value":' + "9" * 129 + "}\n",
        encoding="utf-8",
    )
    rows = discover_runs(tmp_path)
    assert rows[0]["state"] == "unknown"
    assert rows[0]["progress"] is None
    assert rows[0]["events"] == []


def test_deeply_nested_status_is_treated_as_unavailable(tmp_path: Path) -> None:
    run = _write_run(tmp_path)
    nested = "[" * 5_000 + "0" + "]" * 5_000
    payload = '{"state":' + nested + "}"
    assert len(payload.encode("utf-8")) < dashboard_server.MAX_JSON_BYTES
    (run / "status.json").write_text(payload, encoding="utf-8")

    rows = discover_runs(tmp_path)
    assert rows[0]["state"] == "unknown"
    assert rows[0]["progress"] is None


def test_deeply_nested_event_stream_is_treated_as_unavailable(tmp_path: Path) -> None:
    run = _write_run(tmp_path)
    nested = "[" * 5_000 + "0" + "]" * 5_000
    payload = '{"timestamp":"nested","message":' + nested + "}\n"
    assert len(payload.encode("utf-8")) < dashboard_server.MAX_EVENT_BYTES
    (run / "events.ndjson").write_text(payload, encoding="utf-8")

    rows = discover_runs(tmp_path)
    assert rows[0]["state"] == "running"
    assert rows[0]["events"] == []


def test_localhost_http_smoke(tmp_path: Path) -> None:
    _write_run(tmp_path)
    server = ThreadingHTTPServer(("127.0.0.1", 0), make_handler(tmp_path))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        host, port = server.server_address
        with urlopen(f"http://{host}:{port}/api/runs", timeout=2) as response:  # noqa: S310 - loopback-only test
            rows = json.loads(response.read().decode("utf-8"))
        assert rows[0]["progress"] == 0.5
        with urlopen(f"http://{host}:{port}/", timeout=2) as response:  # noqa: S310 - loopback-only test
            page = response.read().decode("utf-8")
        assert "DriftBench Experiment Lab" in page
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def test_http_artifacts_are_forced_downloads_and_malformed_paths_are_404(tmp_path: Path) -> None:
    run = _write_run(tmp_path)
    active = run / "results" / "active.html"
    active.write_text("<script>document.body.textContent='executed'</script>", encoding="utf-8")
    server = ThreadingHTTPServer(("127.0.0.1", 0), make_handler(tmp_path))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        host, port = server.server_address
        base = f"http://{host}:{port}"
        with urlopen(f"{base}/", timeout=2) as response:  # noqa: S310 - loopback-only test
            page = response.read().decode("utf-8")
            page_csp = response.headers["Content-Security-Policy"]
        assert "unsafe-inline" not in page_csp
        assert "script-src 'self'" not in page_csp
        nonce_match = re.search(r"script-src 'nonce-([^']+)'", page_csp)
        assert nonce_match is not None
        assert page.count(f'nonce="{nonce_match.group(1)}"') == 2
        assert "onclick=" not in page

        with urlopen(  # noqa: S310 - loopback-only test
            f"{base}/artifacts/rel-ce-tpch/run-42/results/active.html",
            timeout=2,
        ) as response:
            assert response.read() == active.read_bytes()
            assert response.headers["Content-Type"] == "application/octet-stream"
            assert response.headers["Content-Disposition"] == 'attachment; filename="active.html"'
            assert response.headers["X-Content-Type-Options"] == "nosniff"
            artifact_csp = response.headers["Content-Security-Policy"]
        assert "default-src 'none'" in artifact_csp
        assert "sandbox" in artifact_csp

        for path in (
            "/%00",
            "/%FF",
            "/%ZZ",
            "/artifacts/rel-ce-tpch/run-42/logs%2F..%2Foutside.txt",
        ):
            with pytest.raises(HTTPError) as caught:
                urlopen(f"{base}{path}", timeout=2)  # noqa: S310 - loopback-only test
            assert caught.value.code == 404
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)
