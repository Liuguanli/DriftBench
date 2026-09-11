from __future__ import annotations

import unittest
from pathlib import Path

from driftbench.data.ycsb import (
    YCSBData,
    YCSBQueries,
    data as ycsb_data,
    queries as ycsb_queries,
)
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

    def test_ycsb_workload_uses_effective_record_count_in_content_and_cache(self) -> None:
        def case(tmp_path: Path) -> None:
            result = YCSBQueries(workload="B", scale_factor=2).generate(
                output_dir=tmp_path
            )
            properties = next(path for path in result.files if path.suffix == ".properties")
            assert "recordcount=2000\n" in properties.read_text(encoding="utf-8")
            manifest = _read_json(result.metadata)
            assert manifest["record_count"] == 2000
            assert manifest["cache"]["parameters"]["record_count"] == 2000

        self._run_case(case)

    def test_ycsb_conflicting_scale_and_count_fail_closed(self) -> None:
        def case(tmp_path: Path) -> None:
            for adapter in (
                YCSBData(scale_factor=2, record_count=1000),
                YCSBQueries(scale_factor=2, record_count=1000),
            ):
                try:
                    adapter.generate(output_dir=tmp_path)
                except ValueError as exc:
                    assert "conflict" in str(exc)
                else:
                    raise AssertionError("conflicting YCSB count was accepted")

        self._run_case(case)

    def test_ycsb_public_factories_keep_effective_counts_in_all_artifacts(self) -> None:
        def case(tmp_path: Path) -> None:
            data_result = ycsb_data(record_count=7).generate(
                output_dir=tmp_path / "data-out"
            )
            data_manifest = _read_json(data_result.metadata)
            assert _csv_data_rows(data_result.files[0]) == 7
            assert data_manifest["scale_factor"] is None
            assert data_manifest["record_count"] == 7
            assert data_manifest["tables"] == {"usertable": 7}
            assert data_manifest["cache"]["parameters"] == {
                "record_count": 7,
                "scale_factor": None,
            }

            query_result = ycsb_queries(
                workload="b",
                run_seconds=1,
                target_rate=10,
                scale_factor=2,
            ).generate(output_dir=tmp_path / "query-out")
            properties = next(
                path for path in query_result.files if path.suffix == ".properties"
            ).read_text(encoding="utf-8")
            assert "recordcount=2000\n" in properties
            query_manifest = _read_json(query_result.metadata)
            assert query_manifest["workload"] == "B"
            assert query_manifest["scale_factor"] == 2
            assert query_manifest["record_count"] == 2000
            assert query_manifest["cache"]["parameters"] == {
                "record_count": 2000,
                "run_seconds": 1,
                "scale_factor": 2,
                "target_rate": 10,
                "workload": "B",
            }
            assert len(query_manifest["cache"]["fingerprint"]) == 64

        self._run_case(case)

    def test_ycsb_conflicts_fail_before_creating_output(self) -> None:
        def case(tmp_path: Path) -> None:
            for index, adapter in enumerate((
                ycsb_data(scale_factor=2, record_count=1000),
                ycsb_queries(scale_factor=2, record_count=1000),
            )):
                output = tmp_path / f"conflict-{index}"
                try:
                    adapter.generate(output_dir=output)
                except ValueError:
                    pass
                else:
                    raise AssertionError("conflicting YCSB count was accepted")
                assert not output.exists()

        self._run_case(case)
