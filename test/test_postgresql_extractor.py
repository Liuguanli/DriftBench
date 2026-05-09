"""Legacy manual script retained for reference.

This file intentionally avoids DB access during test discovery.
"""

import unittest


@unittest.skip("Legacy manual script placeholder; DB integration is environment-specific.")
class LegacyPostgresExtractorScript(unittest.TestCase):
    def test_placeholder(self) -> None:
        self.assertTrue(True)


if __name__ == "__main__":
    import json
    import pprint
    from driftbench.core.schema.factory import get_schema_extractor

    with open("./data/PG_info.json", "r", encoding="utf-8") as f:
        db_config = json.load(f)
    extractor = get_schema_extractor(
        source_type="postgres",
        db_config=db_config,
        schema_name="public",
        sample_size=10000,
    )
    schema = extractor.extract_schema()
    pprint.pprint(schema)
    with open("./output/intermediate/tpcds_schema.json", "w", encoding="utf-8") as f:
        json.dump(schema, f, indent=2, default=str)

