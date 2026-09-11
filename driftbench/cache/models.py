from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from driftbench.data.base import GenerationResult

from .errors import CacheConfigurationError


class RemoteCacheMode(str, Enum):
    OFF = "off"
    READ = "read"
    READ_WRITE = "read-write"

    @classmethod
    def parse(cls, value: "RemoteCacheMode | str") -> "RemoteCacheMode":
        if isinstance(value, cls):
            return value
        try:
            return cls(str(value).strip().lower())
        except ValueError as exc:
            raise CacheConfigurationError(
                "cache mode must be one of: off, read, read-write"
            ) from exc


_PUBLIC_AZURE_DFS_HOST_RE = re.compile(
    r"^[a-z0-9]{3,24}\.dfs\.core\.windows\.net$"
)
_FILESYSTEM_RE = re.compile(r"^[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$")


def validate_posix_relative_path(value: str, *, field: str, allow_empty: bool = False) -> str:
    if not isinstance(value, str):
        raise CacheConfigurationError(f"{field} must be a string")
    if value == "" and allow_empty:
        return ""
    if not value or value.startswith("/") or "\\" in value or "%" in value:
        raise CacheConfigurationError(f"{field} must be a safe relative POSIX path")
    if any(ord(char) < 32 or ord(char) == 127 for char in value):
        raise CacheConfigurationError(f"{field} contains control characters")
    parts = value.split("/")
    if any(part in {"", ".", ".."} for part in parts):
        raise CacheConfigurationError(f"{field} must not contain empty, '.' or '..' segments")
    if any(":" in part for part in parts):
        raise CacheConfigurationError(f"{field} contains an invalid path segment")
    return "/".join(parts)


@dataclass(frozen=True)
class AzureHNSCacheConfig:
    """Non-secret ADLS Gen2 cache configuration.

    Constructing this object is offline.  Azure packages and credentials are
    touched only when a non-off materialization starts.
    """

    account_url: str = ""
    file_system: str = ""
    prefix: str = ""
    mode: RemoteCacheMode | str = RemoteCacheMode.OFF
    credential_env_file: str | Path | None = None

    def normalized_mode(self) -> RemoteCacheMode:
        return RemoteCacheMode.parse(self.mode)

    def validate_enabled(self) -> None:
        if self.normalized_mode() is RemoteCacheMode.OFF:
            return
        if not isinstance(self.account_url, str) or not isinstance(
            self.file_system, str
        ):
            raise CacheConfigurationError(
                "account_url and file_system must be strings"
            )
        parsed = urlsplit(self.account_url)
        try:
            port = parsed.port
        except ValueError as exc:
            raise CacheConfigurationError(
                "account_url must be a public Azure DFS endpoint"
            ) from exc
        hostname = parsed.hostname or ""
        if (
            parsed.scheme != "https"
            or not _PUBLIC_AZURE_DFS_HOST_RE.fullmatch(hostname)
            or parsed.netloc != hostname
            or parsed.username is not None
            or parsed.password is not None
            or port is not None
            or parsed.query
            or parsed.fragment
            or parsed.path != ""
        ):
            raise CacheConfigurationError(
                "account_url must be exactly a public Azure HTTPS DFS endpoint "
                "(https://<storage-account>.dfs.core.windows.net)"
            )
        if (
            not _FILESYSTEM_RE.fullmatch(self.file_system)
            or "--" in self.file_system
        ):
            raise CacheConfigurationError(
                "file_system must be a 3-63 character lowercase ADLS filesystem "
                "name without leading, trailing, or consecutive hyphens"
            )
        validate_posix_relative_path(
            self.prefix, field="prefix", allow_empty=True
        )


@dataclass(frozen=True)
class MaterializationResult:
    generation: GenerationResult
    cache_mode: RemoteCacheMode
    cache_outcome: str
    materialization_source: str
    remote_entry_status: str
    force: bool
    remote_path: str | None = None
    warnings: tuple[str, ...] = ()

    @property
    def benchmark(self) -> str:
        return self.generation.benchmark

    @property
    def artifact_type(self) -> str:
        return self.generation.artifact_type

    @property
    def output_dir(self) -> Path:
        return self.generation.output_dir

    @property
    def files(self) -> list[Path]:
        return self.generation.files

    @property
    def metadata(self) -> Path:
        return self.generation.metadata

    def summary(self) -> dict[str, Any]:
        payload = self.generation.summary()
        payload.update(
            {
                "cache_mode": self.cache_mode.value,
                "cache_outcome": self.cache_outcome,
                "materialization_source": self.materialization_source,
                "remote_entry_status": self.remote_entry_status,
                "force": self.force,
                "remote_path": self.remote_path,
                "warnings": list(self.warnings),
            }
        )
        return payload
