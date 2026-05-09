"""Legacy manual script retained for reference.

This file intentionally does not execute extraction on import.
Use dedicated unit/integration tests for CI.
"""

import unittest


@unittest.skip("Legacy manual script placeholder; covered by P0 test suites.")
class LegacyCSVSchemaExtractorScript(unittest.TestCase):
    def test_placeholder(self) -> None:
        self.assertTrue(True)


if __name__ == "__main__":
    from driftbench.core.schema.factory import get_schema_extractor
    import json
    import pprint

    csv_path = "./data/census_original.csv"
    extractor = get_schema_extractor(source_type="csv", csv_path=csv_path, sample_size=1000)
    schema = extractor.extract_schema()
    pprint.pprint(schema)
    with open("./output/intermediate/census_original_schema.json", "w", encoding="utf-8") as f:
        json.dump(schema, f, indent=2, default=str)

