from __future__ import annotations

from pathlib import Path
from typing import Callable, Iterable

from driftbench.cache.errors import RemoteObjectConflict, RemoteObjectNotFound


class FakeRemoteBackend:
    def __init__(self) -> None:
        self.files: dict[str, bytes] = {}
        self.calls: list[tuple[str, str]] = []
        self.bytes_yielded: dict[str, int] = {}
        self.read_error: Exception | None = None
        self.upload_error: Exception | None = None
        self.rename_error: Exception | None = None
        self.before_rename: Callable[[str, str], None] | None = None

    def iter_bytes(self, path: str) -> Iterable[bytes]:
        self.calls.append(("read", path))
        if self.read_error is not None:
            raise self.read_error
        if path not in self.files:
            raise RemoteObjectNotFound("fake exact 404")
        payload = self.files[path]
        midpoint = len(payload) // 2
        if midpoint:
            self.bytes_yielded[path] = self.bytes_yielded.get(path, 0) + midpoint
            yield payload[:midpoint]
            self.bytes_yielded[path] = self.bytes_yielded.get(path, 0) + len(payload) - midpoint
            yield payload[midpoint:]
        else:
            self.bytes_yielded[path] = self.bytes_yielded.get(path, 0) + len(payload)
            yield payload

    def upload_file(self, source: Path, path: str) -> None:
        self.calls.append(("upload_file", path))
        if self.upload_error is not None:
            raise self.upload_error
        if path in self.files:
            raise RemoteObjectConflict("fake create conflict")
        self.files[path] = Path(source).read_bytes()

    def upload_bytes(self, payload: bytes, path: str) -> None:
        self.calls.append(("upload_bytes", path))
        if self.upload_error is not None:
            raise self.upload_error
        if path in self.files:
            raise RemoteObjectConflict("fake create conflict")
        self.files[path] = bytes(payload)

    def rename_directory(self, source: str, destination: str) -> None:
        self.calls.append(("rename", destination))
        if self.before_rename is not None:
            self.before_rename(source, destination)
        if self.rename_error is not None:
            raise self.rename_error
        source_prefix = source.rstrip("/") + "/"
        destination_prefix = destination.rstrip("/") + "/"
        if any(path.startswith(destination_prefix) for path in self.files):
            raise RemoteObjectConflict("fake rename conflict")
        matched = {
            path: payload
            for path, payload in self.files.items()
            if path.startswith(source_prefix)
        }
        if not matched:
            raise RemoteObjectNotFound("fake staging directory not found")
        for path, payload in matched.items():
            relative = path[len(source_prefix):]
            self.files[destination_prefix + relative] = payload
            del self.files[path]

    def delete_directory(self, path: str) -> None:
        self.calls.append(("delete", path))
        prefix = path.rstrip("/") + "/"
        matched = [key for key in self.files if key.startswith(prefix)]
        if not matched:
            raise RemoteObjectNotFound("fake directory not found")
        for key in matched:
            del self.files[key]
