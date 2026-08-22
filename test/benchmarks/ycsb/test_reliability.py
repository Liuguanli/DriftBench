from __future__ import annotations

import unittest
from pathlib import Path

from driftbench.data.ycsb import YCSBData
from ..helpers import (
    ReliabilityTestMixin,
    csv_data_rows as _csv_data_rows,
    read_json as _read_json,
)


def _case_ycsb_default_record_count_changes_with_scale_in_same_output_dir(
    tmp_path: Path,
) -> None:
    first = YCSBData(scale_factor=1).generate(output_dir=tmp_path)
    first_manifest = _read_json(first.metadata)
    first_fingerprint = first_manifest["cache"]["fingerprint"]
    first_size = first.files[0].stat().st_size
    assert _csv_data_rows(first.files[0]) == 1000
    assert first_manifest["cache"]["parameters"] == {
        "record_count": 1000,
        "scale_factor": 1,
    }

    second = YCSBData(scale_factor=2).generate(output_dir=tmp_path)
    second_manifest = _read_json(second.metadata)
    assert _csv_data_rows(second.files[0]) == 2000
    assert second.files[0].stat().st_size > first_size
    assert second_manifest["cache"]["parameters"] == {
        "record_count": 2000,
        "scale_factor": 2,
    }
    assert second_manifest["cache"]["fingerprint"] != first_fingerprint

class BenchmarkReliabilityTests(ReliabilityTestMixin, unittest.TestCase):
    def test_ycsb_default_record_count_changes_with_scale_in_same_output_dir(self) -> None:
        self._run_case(
            _case_ycsb_default_record_count_changes_with_scale_in_same_output_dir
        )
