"""Tests for GenerationResult.summary()."""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from driftbench.data.base import GenerationResult


class GenerationResultSummaryTests(unittest.TestCase):
    def _make_result(self, tmpdir: Path) -> GenerationResult:
        (tmpdir / "lineitem.csv").write_text("a,b\n1,2\n")
        (tmpdir / "orders.csv").write_text("a,b\n1,2\n")
        (tmpdir / "lineitem.tbl").write_text("1|2|\n")
        meta = tmpdir / "manifest.json"
        meta.write_text("{}")
        return GenerationResult(
            benchmark="tpch",
            artifact_type="data",
            output_dir=tmpdir,
            files=[
                tmpdir / "lineitem.csv",
                tmpdir / "orders.csv",
                tmpdir / "lineitem.tbl",
            ],
            metadata=meta,
        )

    def test_summary_keys_and_values(self):
        with tempfile.TemporaryDirectory() as td:
            tmp = Path(td)
            result = self._make_result(tmp)
            summary = result.summary()

            self.assertEqual(summary["benchmark"], "tpch")
            self.assertEqual(summary["artifact_type"], "data")
            self.assertEqual(summary["output_dir"], str(tmp))
            self.assertEqual(summary["file_count"], 3)
            self.assertEqual(summary["tables"], ["lineitem", "orders"])

    def test_summary_is_json_serializable(self):
        with tempfile.TemporaryDirectory() as td:
            tmp = Path(td)
            result = self._make_result(tmp)
            payload = json.dumps(result.summary())
            self.assertIn("\"benchmark\": \"tpch\"", payload)


if __name__ == "__main__":
    unittest.main()
