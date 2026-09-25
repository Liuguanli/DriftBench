from __future__ import annotations

import os
import subprocess
import sys
import tarfile
import tempfile
import unittest
import zipfile
from pathlib import Path

from test.catalog_fixtures import OFFLINE_READER_SCRIPT


REPO_ROOT = Path(__file__).resolve().parents[1]


class CatalogPackagingTests(unittest.TestCase):
    def test_snapshot_ships_in_distributions_and_wheel_browses_offline(self) -> None:
        with tempfile.TemporaryDirectory(prefix="driftbench-catalog-package-") as directory:
            root = Path(directory)
            proc = subprocess.run(
                [sys.executable, "-m", "hatchling", "build", "--target", "wheel",
                 "--target", "sdist", "--directory", str(root)],
                cwd=REPO_ROOT, capture_output=True, text=True, check=False, timeout=120,
            )
            self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            wheels, sdists = tuple(root.glob("*.whl")), tuple(root.glob("*.tar.gz"))
            self.assertEqual(len(wheels), 1)
            self.assertEqual(len(sdists), 1)
            expected = (REPO_ROOT / "driftbench" / "catalog_snapshot.json").read_bytes()
            with zipfile.ZipFile(wheels[0]) as archive:
                self.assertEqual(archive.read("driftbench/catalog_snapshot.json"), expected)
                self.assertFalse(any(".private/" in name for name in archive.namelist()))
            with tarfile.open(sdists[0], "r:gz") as archive:
                member = next(item for item in archive.getmembers()
                              if item.name.endswith("/driftbench/catalog_snapshot.json"))
                with archive.extractfile(member) as stream:
                    self.assertEqual(stream.read(), expected)
                names = {name.split("/", 1)[1] for name in archive.getnames() if "/" in name}
                self.assertIn("docs/artifact_catalog.md", names)
                self.assertIn("scripts/export_azure_catalog.py", names)
            environment = dict(os.environ)
            environment.pop("PYTHONPATH", None)
            installed = root / "installed"
            proc = subprocess.run(
                [sys.executable, "-m", "pip", "--isolated", "--disable-pip-version-check",
                 "install", "--no-index", "--no-deps", "--no-compile", "--quiet",
                 "--target", str(installed), str(wheels[0])],
                cwd=root, env=environment, capture_output=True, text=True, check=False, timeout=120,
            )
            self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            script = (
                f"import sys; sys.path.insert(0, {str(installed)!r})\n"
                + OFFLINE_READER_SCRIPT
                + f"\nassert driftbench.__file__.startswith({str(installed)!r})\n"
            )
            proc = subprocess.run([sys.executable, "-B", "-c", script], cwd=root, env=environment,
                                  capture_output=True, text=True, check=False, timeout=60)
            self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertEqual(proc.stdout, "")
            self.assertEqual(proc.stderr, "")


if __name__ == "__main__":
    unittest.main()
