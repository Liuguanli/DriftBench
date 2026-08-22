from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from driftbench.spec.core import run_spec


REPO_ROOT = Path(__file__).resolve().parents[1]
FIXTURE_ROOT = REPO_ROOT / "test" / "fixtures"


class DataDriftSpecResultTests(unittest.TestCase):
    def _materialize(self, name: str, output_dir: Path) -> Path:
        template = FIXTURE_ROOT / "specs" / name
        text = template.read_text(encoding="utf-8").replace(
            "__OUTPUT_DIR__", output_dir.as_posix()
        )
        path = output_dir / name
        path.write_text(text, encoding="utf-8")
        return path

    def test_single_table_handler_returns_small_output_contract(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            result = run_spec(
                self._materialize("p0_single_table_template.yaml", root)
            )
            self.assertEqual(result["subtype"], "single_table")
            self.assertEqual(len(result["outputs"]), 1)
            self.assertEqual(result["outputs"][0]["rows"], 10)
            self.assertEqual(
                Path(result["outputs"][0]["path"]),
                root / "single_cardinality_scale_2.csv",
            )

    def test_multi_table_handler_returns_written_table_contracts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            result = run_spec(
                self._materialize("p0_multi_table_template.yaml", root)
            )
            self.assertEqual(result["subtype"], "multi_table")
            self.assertFalse(result["integrity_validated"])
            rows = {item["table"]: item["rows"] for item in result["outputs"]}
            self.assertEqual(rows, {"dim_users": 5, "fact_orders": 5})


if __name__ == "__main__":
    unittest.main()
