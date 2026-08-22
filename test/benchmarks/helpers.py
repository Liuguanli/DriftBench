from __future__ import annotations

import csv
import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Callable

from driftbench.data import GenerationResult


REPO_ROOT = Path(__file__).resolve().parents[2]
VALID_SPEC = REPO_ROOT / "driftspec" / "examples" / "demo_data_single.yaml"


class BenchmarkAdapterTestMixin:
    """Shared filesystem-contract setup for adapter tests."""

    def setUp(self) -> None:
        self._default_data_tmpdir = tempfile.mkdtemp(prefix="driftbench_test_")
        os.environ["DRIFTBENCH_DATA_DIR"] = self._default_data_tmpdir

    def tearDown(self) -> None:
        del os.environ["DRIFTBENCH_DATA_DIR"]
        shutil.rmtree(self._default_data_tmpdir, ignore_errors=True)

    def _assert_result_is_filesystem_contract(
        self,
        result: GenerationResult,
        output_root: Path,
    ) -> None:
        self.assertIsInstance(result, GenerationResult)
        self.assertIsInstance(result.output_dir, Path)
        self.assertTrue(result.output_dir.exists())
        self.assertEqual(result.output_dir.resolve(), output_root.resolve())
        self.assertTrue(result.metadata.exists())
        self.assertGreater(result.metadata.stat().st_size, 0)
        payload = json.loads(result.metadata.read_text(encoding="utf-8"))
        if "files" in payload:
            for rel in payload["files"]:
                self.assertFalse(str(rel).startswith("/"))
        if "cache" in payload:
            cache = payload["cache"]
            self.assertEqual(cache["version"], 2)
            self.assertEqual(
                [artifact["path"] for artifact in cache["artifacts"]],
                payload["files"],
            )
        self.assertGreaterEqual(len(result.files), 1)
        for index, path in enumerate(result.files):
            self.assertIsInstance(path, Path)
            self.assertTrue(path.exists())
            self.assertTrue(path.is_file())
            self.assertGreater(path.stat().st_size, 0)
            self.assertTrue(str(path).startswith(str(output_root.resolve())))
            if "cache" in payload:
                artifact = payload["cache"]["artifacts"][index]
                content = path.read_bytes()
                self.assertEqual(artifact["bytes"], len(content))
                self.assertEqual(
                    artifact["sha256"], hashlib.sha256(content).hexdigest()
                )


class ReliabilityTestMixin:
    """Run a reliability case in a fresh temporary directory."""

    def _run_case(self, case: Callable[[Path], None]) -> None:
        with tempfile.TemporaryDirectory(prefix="driftbench_reliability_") as tmpdir:
            case(Path(tmpdir))


def read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def csv_data_rows(path: Path) -> int:
    with path.open(encoding="utf-8", newline="") as stream:
        return max(0, sum(1 for _ in csv.reader(stream)) - 1)


def run_cli(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "driftbench.cli", *args],
        cwd=REPO_ROOT,
        capture_output=True,
        check=False,
        text=True,
    )
