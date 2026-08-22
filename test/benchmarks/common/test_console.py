from __future__ import annotations

import os
import subprocess
import sys
import unittest
from pathlib import Path

from ..helpers import REPO_ROOT, ReliabilityTestMixin


def _case_cp1252_console_handles_unicode_paths_for_generate_reuse_and_force(
    tmp_path: Path,
) -> None:
    output_dir = tmp_path / "benchmark_\u8def\u5f84"
    assert tuple(map(ord, output_dir.name[-2:])) == (0x8DEF, 0x5F84)
    code = (
        "import sys; "
        "from driftbench.data.ycsb import YCSBQueries; "
        "a=YCSBQueries(workload='A',run_seconds=1,target_rate=2); "
        "a.generate(sys.argv[1]); a.generate(sys.argv[1]); a.generate(sys.argv[1],force=True)"
    )
    env = os.environ.copy()
    env.update({"PYTHONUTF8": "0", "PYTHONIOENCODING": "cp1252"})
    proc = subprocess.run(
        [sys.executable, "-c", code, str(output_dir)],
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        check=False,
        text=True,
        encoding="cp1252",
    )
    assert proc.returncode == 0, proc.stderr
    assert "UnicodeEncodeError" not in proc.stderr
    assert " -> " in proc.stdout
    assert "Reusing." in proc.stdout

class BenchmarkReliabilityTests(ReliabilityTestMixin, unittest.TestCase):
    def test_cp1252_console_handles_unicode_paths_for_generate_reuse_and_force(self) -> None:
        self._run_case(
            _case_cp1252_console_handles_unicode_paths_for_generate_reuse_and_force
        )
