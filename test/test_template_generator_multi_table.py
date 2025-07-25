from driftbench.core.workload.template_generator import TemplateGenerator, TemplateGeneratorMulti
import json
from driftbench.core.utils import save_templates

# Load schema
with open("./output/intermediate/tpcds_schema.json") as f:
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
candidate_tables = ["public.customer_address", 
                    "public.customer_demographics",
                    "public.customer"]
candidate_tables = ["public.catalog_sales", "public.store_sales"]
gen = TemplateGeneratorMulti(schema, candidate_tables=candidate_tables, seed=42)
# gen = TemplateGeneratorMulti(schema, candidate_tables=None, seed=42)
templates = gen.generate_templates(
    num_templates=5,
    selectivity=selectivity,
    max_predicates=3,
    max_payload_columns=2,
    join_count=2
)

# # Save to file
save_templates(templates, "./output/intermediate/tpcds_templates_multi_table.json")

