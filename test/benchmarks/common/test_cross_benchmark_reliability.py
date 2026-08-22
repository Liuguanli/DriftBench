from __future__ import annotations

import unittest
from pathlib import Path

from driftbench.data.dsb import DSBData
from driftbench.data.job import JOBData
from ..helpers import (
    ReliabilityTestMixin,
    csv_data_rows as _csv_data_rows,
    read_json as _read_json,
)


def _case_dsb_and_job_manifest_counts_match_csv_rows_at_two_scales(tmp_path: Path) -> None:
    for adapter_type in (DSBData, JOBData):
        for scale_factor in (1, 2):
            root = tmp_path / adapter_type.__name__ / str(scale_factor)
            result = adapter_type(scale_factor=scale_factor).generate(output_dir=root)
            manifest = _read_json(result.metadata)
            csv_files = {path.stem: path for path in result.files if path.suffix == ".csv"}
            assert set(manifest["tables"]) == set(csv_files)
            for table, declared_rows in manifest["tables"].items():
                assert csv_files[table].exists()
                assert _csv_data_rows(csv_files[table]) == declared_rows
            for relative_path in manifest["files"]:
                assert (root / relative_path).is_file()

    assert _read_json(
        (tmp_path / "DSBData" / "1" / "dsb" / "data" / "dsb_data_manifest.json")
    )["tables"]["date_dim"] == 3360
    assert _read_json(
        (tmp_path / "JOBData" / "1" / "job" / "data" / "job_data_manifest.json")
    )["tables"]["info_type"] == 10

class BenchmarkReliabilityTests(ReliabilityTestMixin, unittest.TestCase):
    def test_dsb_and_job_manifest_counts_match_csv_rows_at_two_scales(self) -> None:
        self._run_case(_case_dsb_and_job_manifest_counts_match_csv_rows_at_two_scales)
