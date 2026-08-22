from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import pandas as pd
from pandas.testing import assert_frame_equal

from driftbench.core.data.single_table import SingleTableDriftGenerator


class ConstantNumericCardinalityTests(unittest.TestCase):
    def test_vary_cardinality_handles_constant_numeric_column_deterministically(self) -> None:
        source = pd.DataFrame(
            {
                "constant_number": [7, 7, 7, 7],
                "category": ["a", "b", "a", "c"],
            }
        )
        schema = {
            "tables": {
                "items": {
                    "columns": {
                        "constant_number": {"logical_type": "numeric"},
                        "category": {"logical_type": "categorical"},
                    }
                }
            }
        }

        with tempfile.TemporaryDirectory() as tmp:
            source_path = Path(tmp) / "items.csv"
            source.to_csv(source_path, index=False)

            first = SingleTableDriftGenerator(
                source_path, schema, base_table="items", seed=23
            ).apply_drift(drift_type="vary_cardinality", scale=1.5)
            second = SingleTableDriftGenerator(
                source_path, schema, base_table="items", seed=23
            ).apply_drift(drift_type="vary_cardinality", scale=1.5)

        self.assertEqual(len(first), 6)
        self.assertTrue(first["constant_number"].eq(7).all())
        assert_frame_equal(first, second)


if __name__ == "__main__":
    unittest.main()
