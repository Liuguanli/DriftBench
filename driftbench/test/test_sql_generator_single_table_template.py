import json
from pathlib import Path
from typing import Dict
from driftbench.core.workload.sql_generator import generate_sql_queries, save_queries_to_json, save_queries_to_csv

from driftbench.core.workload.template_generator import TemplateGenerator



# Example usage
if __name__ == "__main__":

# Load schema
    with open("./autoquerybench/data/sample_rich_schema.json") as f:
        schema = json.load(f)

    selectivity = {
        "age": [0.1, 0.2],
        "income": [0.1, 0.2]
    }

    value_range = {
        "age": [0, 100],
        "income": [2000, 100000]
    }

    # Create generator and generate templates
    gen = TemplateGenerator(schema, base_table="sample_rich", seed=42)
    templates = gen.generate_templates(
        num_templates=500,
        max_predicates = 5,
        max_payload_columns = 6,
        selectivity=selectivity
    )

    # Save to file
    gen.save_templates(templates, "./autoquerybench/data/sample_templates_predicates_2.json")

    gen = TemplateGenerator(schema, base_table="sample_rich", seed=42)
    templates = gen.generate_templates(
        num_templates=500,
        max_predicates = 7,
        max_payload_columns = 8,
        selectivity=selectivity
    )

    # Save to file
    gen.save_templates(templates, "./autoquerybench/data/sample_templates_predicates_3.json")



   