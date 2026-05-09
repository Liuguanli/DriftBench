import tempfile
import unittest
from pathlib import Path

import pandas as pd

from driftbench.spec.core import run_all


REPO_ROOT = Path(__file__).resolve().parents[1]
SINGLE_TEMPLATE = REPO_ROOT / "test/fixtures/specs/p0_single_table_template.yaml"
MULTI_TEMPLATE = REPO_ROOT / "test/fixtures/specs/p0_multi_table_template.yaml"


def _materialize_spec(template_path: Path, output_dir: Path) -> Path:
    text = template_path.read_text(encoding="utf-8")
    text = text.replace("__OUTPUT_DIR__", output_dir.as_posix())
    out_spec = output_dir / f"{template_path.stem}_materialized.yaml"
    out_spec.write_text(text, encoding="utf-8")
    return out_spec


class SpecExecutionIntegrationTests(unittest.TestCase):
    def test_single_table_fixture_spec_runs_and_is_deterministic(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            spec_path = _materialize_spec(SINGLE_TEMPLATE, tmp_path)
            out_csv = tmp_path / "single_cardinality_scale_2.csv"

            run_all(str(spec_path))
            self.assertTrue(out_csv.exists())
            first = pd.read_csv(out_csv)
            self.assertEqual(len(first), 10)

            first_bytes = out_csv.read_bytes()
            run_all(str(spec_path))
            second_bytes = out_csv.read_bytes()
            self.assertEqual(first_bytes, second_bytes)

    def test_multi_table_fixture_spec_runs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            spec_path = _materialize_spec(MULTI_TEMPLATE, tmp_path)

            dim_out = tmp_path / "dim_users_out.csv"
            fact_out = tmp_path / "fact_orders_out.csv"
            run_all(str(spec_path))

            self.assertTrue(dim_out.exists())
            self.assertTrue(fact_out.exists())

            dim_df = pd.read_csv(dim_out)
            fact_df = pd.read_csv(fact_out)
            self.assertEqual(len(dim_df), 5)  # base 4 + 1 outlier row
            self.assertEqual(len(fact_df), 5)  # unchanged


if __name__ == "__main__":
    unittest.main()
