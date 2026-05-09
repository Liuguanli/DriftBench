"""Legacy manual script retained for reference.

This file intentionally avoids generation side effects during test discovery.
"""

import unittest


@unittest.skip("Legacy manual script placeholder; covered by dedicated integration tests.")
class LegacyTemplateGeneratorSingleScript(unittest.TestCase):
    def test_placeholder(self) -> None:
        self.assertTrue(True)


if __name__ == "__main__":
    import json
    from driftbench.core.utils import save_templates
    from driftbench.core.workload.template_generator import TemplateGenerator

    with open("./output/intermediate/census_original_schema.json", "r", encoding="utf-8") as f:
        schema = json.load(f)

    gen = TemplateGenerator(schema, base_table="census_original")
    templates = gen.generate_templates(
        num_templates=5,
        selectivity={"age": [0.1, 0.2], "hours_per_week": [0.1, 0.2]},
        value_range={"age": [17, 90], "hours_per_week": [1, 99]},
    )
    save_templates(templates, "./output/intermediate/census_original_templates.json")

