from __future__ import annotations

from pathlib import Path
from typing import Any

from .errors import AzureDependencyError, CacheConfigurationError


_ENV_LIMIT_BYTES = 16 * 1024
_REQUIRED_ENV_KEYS = frozenset(
    {"AZURE_TENANT_ID", "AZURE_CLIENT_ID", "AZURE_CLIENT_SECRET"}
)


def _parse_explicit_env(path_value: str | Path) -> dict[str, str]:
    candidate = Path(path_value).expanduser()
    try:
        if candidate.is_symlink():
            raise CacheConfigurationError(
                "credential env file must be a non-symlink regular file"
            )
        path = candidate.resolve(strict=True)
        if not path.is_file():
            raise CacheConfigurationError(
                "credential env file must be a non-symlink regular file"
            )
        size = path.stat().st_size
        if size < 1 or size > _ENV_LIMIT_BYTES:
            raise CacheConfigurationError(
                "credential env file must be between 1 byte and 16 KiB"
            )
        text = path.read_text(encoding="utf-8")
    except CacheConfigurationError:
        raise
    except (OSError, UnicodeError) as exc:
        raise CacheConfigurationError("credential env file could not be read") from exc

    values: dict[str, str] = {}
    for line_number, raw_line in enumerate(text.splitlines(), start=1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export ") or "=" not in line:
            raise CacheConfigurationError(
                f"credential env file has invalid syntax on line {line_number}"
            )
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if key not in _REQUIRED_ENV_KEYS:
            raise CacheConfigurationError(
                f"credential env file contains unsupported key on line {line_number}"
            )
        if key in values:
            raise CacheConfigurationError(
                f"credential env file repeats a key on line {line_number}"
            )
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        if not value or "\x00" in value or "\n" in value or "\r" in value:
            raise CacheConfigurationError(
                f"credential env file has an empty or invalid value on line {line_number}"
            )
        values[key] = value
    missing = sorted(_REQUIRED_ENV_KEYS - values.keys())
    if missing:
        raise CacheConfigurationError(
            "credential env file is missing required Azure service-principal keys"
        )
    return values


def build_azure_credential(
    credential_env_file: str | Path | None,
) -> Any:
    """Build one explicitly selected Azure credential mode.

    With no file, DriftBench uses non-interactive ``DefaultAzureCredential``.
    Supplying a file explicitly selects ``ClientSecretCredential`` instead.
    The file is parsed into a private mapping and never merged into
    ``os.environ``; authentication/RBAC failures never switch identities.
    """

    try:
        from azure.identity import ClientSecretCredential, DefaultAzureCredential
    except ImportError as exc:
        raise AzureDependencyError(
            "Azure cache support requires: pip install 'driftbench-db[azure]'"
        ) from exc

    if credential_env_file is None:
        return DefaultAzureCredential(
            exclude_interactive_browser_credential=True,
            exclude_broker_credential=True,
        )

    values = _parse_explicit_env(credential_env_file)
    return ClientSecretCredential(
        tenant_id=values["AZURE_TENANT_ID"],
        client_id=values["AZURE_CLIENT_ID"],
        client_secret=values["AZURE_CLIENT_SECRET"],
    )
