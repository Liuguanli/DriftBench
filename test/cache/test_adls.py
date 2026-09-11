from __future__ import annotations

import importlib.util
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from driftbench.cache.adls import AzureDataLakeBackend
from driftbench.cache.errors import (
    CacheAuthenticationError,
    CacheAuthorizationError,
    CacheTransportError,
    RemoteObjectConflict,
    RemoteObjectNotFound,
)
from driftbench.cache.models import AzureHNSCacheConfig


def _azure_available() -> bool:
    try:
        return importlib.util.find_spec("azure.storage.filedatalake") is not None
    except ModuleNotFoundError:
        return False


@unittest.skipUnless(_azure_available(), "Azure SDK optional extra is not installed")
class AzureDataLakeBackendTests(unittest.TestCase):
    class _Directory:
        def __init__(self) -> None:
            self.rename_call: tuple[str, dict[str, object]] | None = None
            self.delete_calls: list[tuple[tuple[object, ...], dict[str, object]]] = []
            self.create_calls = 0

        def create_directory(self) -> None:
            self.create_calls += 1

        def rename_directory(self, destination: str, **kwargs: object) -> None:
            self.rename_call = (destination, kwargs)

        def delete_directory(self, *args: object, **kwargs: object) -> None:
            self.delete_calls.append((args, kwargs))

    class _File:
        def __init__(self) -> None:
            self.operations: list[tuple[str, object]] = []
            self.create_response: dict[str, object] = {"etag": '"created-etag"'}
            self.create_error: Exception | None = None

        def create_file(self, *args: object, **kwargs: object) -> dict[str, object]:
            self.operations.append(("create", (args, kwargs)))
            if self.create_error is not None:
                raise self.create_error
            return self.create_response

        def upload_data(self, data: object, **kwargs: object) -> None:
            payload = data.read() if hasattr(data, "read") else data
            self.operations.append(("upload", (payload, kwargs)))

    class _FileSystem:
        def __init__(self) -> None:
            self.directory = AzureDataLakeBackendTests._Directory()
            self.file = AzureDataLakeBackendTests._File()

        def get_directory_client(self, path: str):
            return self.directory

        def get_file_client(self, path: str):
            return self.file

    class _Service:
        file_system = None

        def __init__(self, **kwargs: object) -> None:
            self.kwargs = kwargs

        def get_file_system_client(self, name: str):
            assert self.file_system is not None
            return self.file_system

    class _Response:
        def __init__(self, status_code: int) -> None:
            self.status_code = status_code
            self.reason = "injected"
            self.headers: dict[str, str] = {}
            self.content_type = "application/json"
            self.request = None

        def text(self) -> str:
            return ""

    def _backend(self) -> tuple[AzureDataLakeBackend, _FileSystem]:
        file_system = self._FileSystem()
        self._Service.file_system = file_system
        config = AzureHNSCacheConfig(
            account_url="https://unittest.dfs.core.windows.net",
            file_system="driftbench-cache",
            mode="read-write",
        )
        with mock.patch(
            "azure.storage.filedatalake.DataLakeServiceClient", self._Service
        ), mock.patch(
            "driftbench.cache.adls.build_azure_credential", return_value=object()
        ):
            backend = AzureDataLakeBackend(config)
        return backend, file_system

    def test_rename_is_explicit_legacy_if_missing_operation(self) -> None:
        from azure.core import MatchConditions

        backend, file_system = self._backend()
        backend.rename_directory("owned-staging", "immutable/final")
        self.assertEqual(
            file_system.directory.rename_call,
            (
                "driftbench-cache/immutable/final",
                {
                    "mode": "legacy",
                    "match_condition": MatchConditions.IfMissing,
                },
            ),
        )

    def test_delete_directory_relies_on_sdk_recursive_behavior(self) -> None:
        backend, file_system = self._backend()

        backend.delete_directory("owned-staging")

        self.assertEqual(file_system.directory.delete_calls, [((), {})])

    def test_upload_file_conditionally_creates_and_uses_created_etag(self) -> None:
        from azure.core import MatchConditions

        backend, file_system = self._backend()
        with tempfile.TemporaryDirectory() as temp_dir:
            source = Path(temp_dir) / "payload.bin"
            source.write_bytes(b"payload")

            backend.upload_file(source, "owned/payload.bin")

        self.assertEqual(
            file_system.file.operations,
            [
                (
                    "create",
                    ((), {"match_condition": MatchConditions.IfMissing}),
                ),
                (
                    "upload",
                    (
                        b"payload",
                        {
                            "length": 7,
                            "overwrite": False,
                            "etag": '"created-etag"',
                            "match_condition": MatchConditions.IfNotModified,
                        },
                    ),
                ),
            ],
        )

    def test_upload_bytes_conditionally_creates_and_preserves_empty_payload(self) -> None:
        from azure.core import MatchConditions

        backend, file_system = self._backend()

        backend.upload_bytes(b"", "empty.bin")

        self.assertEqual(
            file_system.file.operations,
            [
                (
                    "create",
                    ((), {"match_condition": MatchConditions.IfMissing}),
                ),
                (
                    "upload",
                    (
                        b"",
                        {
                            "length": 0,
                            "overwrite": False,
                            "etag": '"created-etag"',
                            "match_condition": MatchConditions.IfNotModified,
                        },
                    ),
                ),
            ],
        )

    def test_upload_fails_closed_when_conditional_create_returns_no_etag(self) -> None:
        backend, file_system = self._backend()
        file_system.file.create_response = {}

        with self.assertRaises(CacheTransportError):
            backend.upload_bytes(b"payload", "payload.bin")

        self.assertEqual(file_system.file.operations, [("create", ((), {"match_condition": backend._if_missing}))])

    def test_upload_create_conflict_does_not_write_data(self) -> None:
        from azure.core.exceptions import ResourceExistsError

        backend, file_system = self._backend()
        file_system.file.create_error = ResourceExistsError(
            message="exists", response=self._Response(409)
        )

        with self.assertRaises(RemoteObjectConflict):
            backend.upload_bytes(b"payload", "payload.bin")

        self.assertEqual(len(file_system.file.operations), 1)
        self.assertEqual(file_system.file.operations[0][0], "create")

    def test_real_sdk_conditionally_creates_before_upload_offline(self) -> None:
        from urllib.parse import parse_qs, urlsplit

        from azure.core.pipeline.transport import HttpResponse, HttpTransport
        from azure.storage.filedatalake import (
            DataLakeServiceClient as SDKDataLakeServiceClient,
        )

        class Response(HttpResponse):
            def __init__(self, request) -> None:
                super().__init__(request, None)
                action = parse_qs(urlsplit(request.url).query).get("action", [""])[0]
                self.status_code = (
                    201 if request.method == "PUT" else 202 if action == "append" else 200
                )
                self.headers = {
                    "ETag": '"created-etag"',
                    "Last-Modified": "Sun, 23 Aug 2026 00:00:00 GMT",
                    "x-ms-request-id": "offline-test",
                    "Content-Length": "0",
                }
                self.reason = "OK"
                self.content_type = "application/json"

            def body(self) -> bytes:
                return b""

            def text(self, encoding=None) -> str:
                return ""

        class Transport(HttpTransport):
            def __init__(self) -> None:
                self.requests = []

            def open(self) -> None:
                pass

            def close(self) -> None:
                pass

            def __enter__(self):
                return self

            def __exit__(self, *args) -> None:
                self.close()

            def send(self, request, **kwargs):
                self.requests.append(request)
                return Response(request)

        transport = Transport()

        def service_factory(*, account_url: str, credential):
            return SDKDataLakeServiceClient(
                account_url=account_url,
                credential=credential,
                transport=transport,
            )

        config = AzureHNSCacheConfig(
            account_url="https://unittest.dfs.core.windows.net",
            file_system="driftbench-cache",
            mode="read-write",
        )
        with mock.patch(
            "azure.storage.filedatalake.DataLakeServiceClient",
            side_effect=service_factory,
        ), mock.patch(
            "driftbench.cache.adls.build_azure_credential", return_value=None
        ):
            backend = AzureDataLakeBackend(config)
            backend.upload_bytes(b"abc", "probe.bin")
            backend.upload_bytes(b"", "empty.bin")

        self.assertEqual(
            [request.method for request in transport.requests],
            ["PUT", "PATCH", "PATCH", "PUT"],
        )
        create, append, flush, empty_create = transport.requests
        self.assertEqual(create.headers.get("If-None-Match"), "*")
        self.assertIsNone(create.headers.get("If-Match"))
        self.assertEqual(
            parse_qs(urlsplit(create.url).query), {"resource": ["file"]}
        )
        self.assertEqual(
            parse_qs(urlsplit(append.url).query),
            {"action": ["append"], "position": ["0"]},
        )
        self.assertEqual(
            parse_qs(urlsplit(flush.url).query),
            {"action": ["flush"], "position": ["3"], "close": ["true"]},
        )
        self.assertEqual(flush.headers.get("If-Match"), '"created-etag"')
        self.assertEqual(empty_create.headers.get("If-None-Match"), "*")

    def test_real_sdk_builds_non_overwriting_hns_rename_request_offline(self) -> None:
        from azure.core.pipeline.transport import HttpResponse, HttpTransport
        from azure.storage.filedatalake import (
            DataLakeServiceClient as SDKDataLakeServiceClient,
        )

        class Response(HttpResponse):
            def __init__(self, request) -> None:
                super().__init__(request, None)
                self.status_code = 201
                self.headers = {
                    "etag": '"offline-test"',
                    "last-modified": "Sun, 23 Aug 2026 00:00:00 GMT",
                    "x-ms-request-id": "offline-test",
                }
                self.reason = "Created"
                self.content_type = "application/json"

            def body(self) -> bytes:
                return b""

            def text(self, encoding=None) -> str:
                return ""

        class Transport(HttpTransport):
            def __init__(self) -> None:
                self.requests = []
                self.send_kwargs = []

            def open(self) -> None:
                pass

            def close(self) -> None:
                pass

            def __enter__(self):
                return self

            def __exit__(self, *args) -> None:
                self.close()

            def send(self, request, **kwargs):
                self.requests.append(request)
                self.send_kwargs.append(kwargs)
                return Response(request)

        transport = Transport()

        def service_factory(*, account_url: str, credential):
            return SDKDataLakeServiceClient(
                account_url=account_url,
                credential=credential,
                transport=transport,
            )

        config = AzureHNSCacheConfig(
            account_url="https://unittest.dfs.core.windows.net",
            file_system="driftbench-cache",
            mode="read-write",
        )
        with mock.patch(
            "azure.storage.filedatalake.DataLakeServiceClient",
            side_effect=service_factory,
        ), mock.patch(
            "driftbench.cache.adls.build_azure_credential", return_value=None
        ):
            backend = AzureDataLakeBackend(config)
            backend.rename_directory("owned-staging", "immutable/final")

        self.assertEqual(len(transport.requests), 1)
        request = transport.requests[0]
        self.assertEqual(request.method, "PUT")
        self.assertEqual(
            request.url,
            "https://unittest.dfs.core.windows.net/"
            "driftbench-cache/immutable/final?mode=legacy",
        )
        self.assertEqual(
            request.headers.get("x-ms-rename-source"),
            "/driftbench-cache/owned-staging",
        )
        self.assertEqual(request.headers.get("If-None-Match"), "*")
        self.assertNotIn("etag", transport.send_kwargs[0])

    def test_sdk_failures_are_classified_without_treating_auth_as_miss(self) -> None:
        from azure.core.exceptions import (
            ClientAuthenticationError,
            HttpResponseError,
            ResourceExistsError,
            ResourceNotFoundError,
            ServiceRequestError,
        )

        backend, _ = self._backend()
        cases = (
            (
                ResourceNotFoundError(
                    message="missing", response=self._Response(404)
                ),
                RemoteObjectNotFound,
            ),
            (
                ClientAuthenticationError(
                    message="rejected", response=self._Response(401)
                ),
                CacheAuthenticationError,
            ),
            (
                HttpResponseError(
                    message="forbidden", response=self._Response(403)
                ),
                CacheAuthorizationError,
            ),
            (
                ResourceExistsError(
                    message="exists", response=self._Response(409)
                ),
                RemoteObjectConflict,
            ),
            (
                HttpResponseError(
                    message="precondition", response=self._Response(412)
                ),
                RemoteObjectConflict,
            ),
            (ServiceRequestError("transport"), CacheTransportError),
            (
                HttpResponseError(
                    message="throttled", response=self._Response(429)
                ),
                CacheTransportError,
            ),
        )
        for error, expected in cases:
            with self.subTest(error=type(error).__name__, status=getattr(error, "status_code", None)):
                with self.assertRaises(expected):
                    backend._raise_mapped(error, "injected operation")


if __name__ == "__main__":
    unittest.main()
