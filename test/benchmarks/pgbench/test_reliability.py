from __future__ import annotations

import unittest
from pathlib import Path

from driftbench.data.pgbench import PgBenchQueries
from ..helpers import ReliabilityTestMixin


def _case_pgbench_select_only_matches_pg16_builtin(tmp_path: Path) -> None:
    result = PgBenchQueries(workload="select_only").generate(output_dir=tmp_path)
    sql_path = next(path for path in result.files if path.suffix == ".sql")
    script = sql_path.read_text(encoding="utf-8")
    executable_lines = [
        line.strip()
        for line in script.splitlines()
        if line.strip() and not line.lstrip().startswith("--")
    ]
    assert executable_lines == [
        r"\set aid random(1, 100000 * :scale)",
        "SELECT abalance FROM pgbench_accounts WHERE aid = :aid;",
    ]
    assert "BEGIN;" not in script
    assert "END;" not in script

class BenchmarkReliabilityTests(ReliabilityTestMixin, unittest.TestCase):
    def test_pgbench_select_only_matches_pg16_builtin(self) -> None:
        self._run_case(_case_pgbench_select_only_matches_pg16_builtin)
