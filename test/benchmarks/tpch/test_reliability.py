from __future__ import annotations

import unittest
from pathlib import Path
from unittest import mock

from driftbench.data.tpch import TPCHQueries
from ..helpers import ReliabilityTestMixin, read_json as _read_json


def _case_tpch_one_shot_query_ids_are_fingerprinted_once(tmp_path: Path) -> None:
    adapter = TPCHQueries(
        query_ids=(query_id for query_id in (1, 3)),
        mode="qgen",
        shuffle=False,
    )
    first = adapter.generate(output_dir=tmp_path)
    params = _read_json(first.metadata)["cache"]["parameters"]
    assert params["query_ids"] == ["1", "3"]
    first_fingerprint = _read_json(first.metadata)["cache"]["fingerprint"]

    with mock.patch(
        "driftbench.data.tpch.generate_tpch_queries_indexed_qgen",
        side_effect=AssertionError("one-shot parameters should reuse intact cache"),
    ):
        second = adapter.generate(output_dir=tmp_path)
    assert second.files == first.files

    reordered = TPCHQueries(
        query_ids=[3, 1], mode="qgen", shuffle=False
    ).generate(output_dir=tmp_path)
    assert _read_json(reordered.metadata)["cache"]["parameters"]["query_ids"] == ["3", "1"]
    assert _read_json(reordered.metadata)["cache"]["fingerprint"] != first_fingerprint

class BenchmarkReliabilityTests(ReliabilityTestMixin, unittest.TestCase):
    def test_tpch_one_shot_query_ids_are_fingerprinted_once(self) -> None:
        self._run_case(_case_tpch_one_shot_query_ids_are_fingerprinted_once)
