from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable, NoReturn

from .credentials import build_azure_credential
from .errors import (
    AzureDependencyError,
    CacheAuthenticationError,
    CacheAuthorizationError,
    CacheCredentialError,
    CacheTransportError,
    RemoteObjectConflict,
    RemoteObjectNotFound,
)
from .models import AzureHNSCacheConfig, validate_posix_relative_path


class AzureDataLakeBackend:
    """Exact-path ADLS Gen2 backend with lazy Azure imports."""

    def __init__(self, config: AzureHNSCacheConfig) -> None:
        config.validate_enabled()
        try:
            from azure.core import MatchConditions
            from azure.core.exceptions import (
                ClientAuthenticationError,
                HttpResponseError,
                ResourceExistsError,
                ResourceNotFoundError,
                ServiceRequestError,
                ServiceResponseError,
            )
            from azure.identity import CredentialUnavailableError
            from azure.storage.filedatalake import DataLakeServiceClient
        except ImportError as exc:
            raise AzureDependencyError(
                "Azure cache support requires: pip install 'driftbench-db[azure]'"
            ) from exc

        self._exceptions = {
            "ClientAuthenticationError": ClientAuthenticationError,
            "CredentialUnavailableError": CredentialUnavailableError,
            "HttpResponseError": HttpResponseError,
            "ResourceExistsError": ResourceExistsError,
            "ResourceNotFoundError": ResourceNotFoundError,
            "ServiceRequestError": ServiceRequestError,
            "ServiceResponseError": ServiceResponseError,
        }
        self._if_missing = MatchConditions.IfMissing
        self._if_not_modified = MatchConditions.IfNotModified
        self._file_system_name = config.file_system
        credential = build_azure_credential(config.credential_env_file)
        try:
            service = DataLakeServiceClient(
                account_url=config.account_url,
                credential=credential,
            )
            self._file_system = service.get_file_system_client(config.file_system)
        except Exception as exc:
            self._raise_mapped(exc, "client initialization")

    def _raise_mapped(self, exc: Exception, operation: str) -> NoReturn:
        unavailable = self._exceptions["CredentialUnavailableError"]
        auth = self._exceptions["ClientAuthenticationError"]
        not_found = self._exceptions["ResourceNotFoundError"]
        exists = self._exceptions["ResourceExistsError"]
        request_errors = (
            self._exceptions["ServiceRequestError"],
            self._exceptions["ServiceResponseError"],
        )
        status = getattr(exc, "status_code", None)
        if isinstance(exc, not_found) and status == 404:
            raise RemoteObjectNotFound(f"ADLS {operation} returned not found") from exc
        if isinstance(exc, unavailable):
            raise CacheCredentialError("no Azure credential is available") from exc
        if isinstance(exc, auth) or status == 401:
            raise CacheAuthenticationError("Azure authentication was rejected") from exc
        if status == 403:
            raise CacheAuthorizationError(
                "Azure authorization failed; verify data-plane RBAC and HNS ACLs"
            ) from exc
        if isinstance(exc, exists) or status in {409, 412}:
            raise RemoteObjectConflict(f"ADLS {operation} found an existing path") from exc
        if isinstance(exc, request_errors):
            raise CacheTransportError(f"ADLS {operation} failed at the transport layer") from exc
        raise CacheTransportError(f"ADLS {operation} failed") from exc

    def _validate_path(self, path: str) -> str:
        return validate_posix_relative_path(path, field="ADLS path")

    def _ensure_parent(self, path: str) -> None:
        parent_parts = path.split("/")[:-1]
        current: list[str] = []
        for part in parent_parts:
            current.append(part)
            directory = "/".join(current)
            try:
                self._file_system.get_directory_client(directory).create_directory()
            except self._exceptions["ResourceExistsError"]:
                continue
            except Exception as exc:
                self._raise_mapped(exc, "directory creation")

    def iter_bytes(self, path: str) -> Iterable[bytes]:
        safe_path = self._validate_path(path)
        try:
            downloader = self._file_system.get_file_client(safe_path).download_file()
            for chunk in downloader.chunks():
                yield bytes(chunk)
        except Exception as exc:
            self._raise_mapped(exc, "exact-path read")

    def upload_file(self, source: Path, path: str) -> None:
        safe_path = self._validate_path(path)
        self._ensure_parent(safe_path)
        try:
            file_client = self._file_system.get_file_client(safe_path)
            created = file_client.create_file(match_condition=self._if_missing)
            created_etag = created.get("etag") if isinstance(created, dict) else None
            if not isinstance(created_etag, str) or not created_etag:
                raise ValueError("conditional file creation returned no ETag")
            with Path(source).open("rb") as stream:
                file_client.upload_data(
                    stream,
                    length=Path(source).stat().st_size,
                    overwrite=False,
                    etag=created_etag,
                    match_condition=self._if_not_modified,
                )
        except Exception as exc:
            self._raise_mapped(exc, "non-overwriting file upload")

    def upload_bytes(self, payload: bytes, path: str) -> None:
        safe_path = self._validate_path(path)
        self._ensure_parent(safe_path)
        try:
            file_client = self._file_system.get_file_client(safe_path)
            created = file_client.create_file(match_condition=self._if_missing)
            created_etag = created.get("etag") if isinstance(created, dict) else None
            if not isinstance(created_etag, str) or not created_etag:
                raise ValueError("conditional file creation returned no ETag")
            file_client.upload_data(
                payload,
                length=len(payload),
                overwrite=False,
                etag=created_etag,
                match_condition=self._if_not_modified,
            )
        except Exception as exc:
            self._raise_mapped(exc, "non-overwriting metadata upload")

    def rename_directory(self, source: str, destination: str) -> None:
        safe_source = self._validate_path(source)
        safe_destination = self._validate_path(destination)
        try:
            self._file_system.get_directory_client(safe_source).rename_directory(
                f"{self._file_system_name}/{safe_destination}",
                mode="legacy",
                match_condition=self._if_missing,
            )
        except Exception as exc:
            self._raise_mapped(exc, "non-overwriting HNS directory rename")

    def delete_directory(self, path: str) -> None:
        safe_path = self._validate_path(path)
        try:
            self._file_system.get_directory_client(safe_path).delete_directory()
        except Exception as exc:
            self._raise_mapped(exc, "owned staging cleanup")
