"""Legacy manual script retained for reference.

This file intentionally avoids generation side effects during test discovery.
"""

import unittest


@unittest.skip("Legacy manual script placeholder; covered by dedicated integration tests.")
class LegacyTemplateGeneratorMultiScript(unittest.TestCase):
    def test_placeholder(self) -> None:
        self.assertTrue(True)


if __name__ == "__main__":
    import json
    from driftbench.core.utils import save_templates
    from driftbench.core.workload.template_generator import TemplateGeneratorMulti

    with open("./output/intermediate/tpcds_schema.json", "r", encoding="utf-8") as f:
        schema = json.load(f)

    gen = TemplateGeneratorMulti(
        schema,
        candidate_tables=["public.catalog_sales", "public.store_sales"],
        seed=42,
    )
    templates = gen.generate_templates(
        num_templates=5,
        selectivity={"age": [0.1, 0.2], "income": [0.1, 0.2]},
        max_predicates=3,
        max_payload_columns=2,
        join_count=2,
    )
    save_templates(templates, "./output/intermediate/tpcds_templates_multi_table.json")

