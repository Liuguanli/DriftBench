"""Legacy manual script retained for reference.

This file intentionally avoids side effects during test discovery.
"""

import unittest


@unittest.skip("Legacy manual script placeholder; covered by P0 test suites.")
class LegacySingleTableDataGeneratorScript(unittest.TestCase):
    def test_placeholder(self) -> None:
        self.assertTrue(True)


if __name__ == "__main__":
    import json
    from driftbench.core.data.single_table import SingleTableDriftGenerator

    csv_path = "./data/census_original.csv"
    with open("./output/intermediate/census_original_schema.json", "r", encoding="utf-8") as f:
        schema = json.load(f)

    generator = SingleTableDriftGenerator(csv_path, schema, base_table="census_original")
    df_drifted = generator.apply_drift(drift_type="vary_cardinality", scale=1)
    df_drifted.to_csv("./output/data/cardinality/scale/census_original_cardinality_1.csv", index=False)

