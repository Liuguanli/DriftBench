from __future__ import annotations

from pathlib import Path
from typing import Iterable, Protocol


class RemoteCacheBackend(Protocol):
    """Exact-path storage port; implementations must never list the namespace."""

    def iter_bytes(self, path: str) -> Iterable[bytes]: ...

    def upload_file(self, source: Path, path: str) -> None: ...

    def upload_bytes(self, payload: bytes, path: str) -> None: ...

    def rename_directory(self, source: str, destination: str) -> None: ...

    def delete_directory(self, path: str) -> None: ...
