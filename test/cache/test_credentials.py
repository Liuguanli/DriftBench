from __future__ import annotations

import os
import sys
import tempfile
import unittest
from pathlib import Path
from types import ModuleType
from unittest import mock

from driftbench.cache.credentials import (
    _parse_explicit_env,
    build_azure_credential,
)
from driftbench.cache.errors import CacheConfigurationError


class _DefaultCredential:
    constructed: list[dict[str, object]] = []

    def __init__(self, **kwargs: object) -> None:
        self.constructed.append(kwargs)


class _ClientSecretCredential:
    constructed: list[dict[str, str]] = []

    def __init__(self, **kwargs: str) -> None:
        self.constructed.append(kwargs)


class CredentialTests(unittest.TestCase):
    def test_explicit_env_parse_does_not_mutate_process_environment(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "azure.env"
            path.write_text(
                "AZURE_TENANT_ID=tenant\n"
                "AZURE_CLIENT_ID=client\n"
                "AZURE_CLIENT_SECRET='private-value'\n",
                encoding="utf-8",
            )
            before = dict(os.environ)
            values = _parse_explicit_env(path)
            self.assertEqual(values["AZURE_CLIENT_SECRET"], "private-value")
            self.assertEqual(dict(os.environ), before)

    def test_env_rejects_unknown_duplicate_missing_and_oversize_content(self) -> None:
        cases = (
            "UNKNOWN=value\n",
            "AZURE_TENANT_ID=a\nAZURE_TENANT_ID=b\n",
            "AZURE_TENANT_ID=a\nAZURE_CLIENT_ID=b\n",
            "x" * (16 * 1024 + 1),
        )
        for content in cases:
            with self.subTest(size=len(content)), tempfile.TemporaryDirectory() as tmp:
                path = Path(tmp) / "azure.env"
                path.write_text(content, encoding="utf-8")
                with self.assertRaises(CacheConfigurationError):
                    _parse_explicit_env(path)

    def test_env_rejects_valid_and_broken_symlinks(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            target = root / "target.env"
            target.write_text(
                "AZURE_TENANT_ID=tenant\n"
                "AZURE_CLIENT_ID=client\n"
                "AZURE_CLIENT_SECRET=secret\n",
                encoding="utf-8",
            )
            links = (root / "valid.env", root / "broken.env")
            try:
                links[0].symlink_to(target)
                links[1].symlink_to(root / "missing.env")
            except OSError as exc:
                self.skipTest(f"symlinks unavailable: {exc}")
            for link in links:
                with self.subTest(link=link.name), self.assertRaises(
                    CacheConfigurationError
                ):
                    _parse_explicit_env(link)

    def test_credential_file_explicitly_selects_service_principal_mode(self) -> None:
        azure = ModuleType("azure")
        azure.__path__ = []  # type: ignore[attr-defined]
        identity = ModuleType("azure.identity")
        identity.DefaultAzureCredential = _DefaultCredential  # type: ignore[attr-defined]
        identity.ClientSecretCredential = _ClientSecretCredential  # type: ignore[attr-defined]

        _DefaultCredential.constructed.clear()
        _ClientSecretCredential.constructed.clear()
        with mock.patch.dict(
            sys.modules,
            {"azure": azure, "azure.identity": identity},
        ), tempfile.TemporaryDirectory() as tmp:
            default = build_azure_credential(None)
            self.assertIsInstance(default, _DefaultCredential)
            self.assertEqual(
                _DefaultCredential.constructed,
                [{
                    "exclude_interactive_browser_credential": True,
                    "exclude_broker_credential": True,
                }],
            )
            self.assertEqual(_ClientSecretCredential.constructed, [])

            path = Path(tmp) / "azure.env"
            path.write_text(
                "AZURE_TENANT_ID=tenant\n"
                "AZURE_CLIENT_ID=client\n"
                "AZURE_CLIENT_SECRET=secret\n",
                encoding="utf-8",
            )
            explicit = build_azure_credential(path)
            self.assertIsInstance(explicit, _ClientSecretCredential)
            self.assertEqual(len(_DefaultCredential.constructed), 1)
            self.assertEqual(
                _ClientSecretCredential.constructed,
                [{
                    "tenant_id": "tenant",
                    "client_id": "client",
                    "client_secret": "secret",
                }],
            )


if __name__ == "__main__":
    unittest.main()
