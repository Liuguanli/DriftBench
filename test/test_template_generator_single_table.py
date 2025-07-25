from driftbench.core.workload.template_generator import TemplateGenerator
from driftbench.core.utils import save_templates
import json

# Load schema
with open("./output/intermediate/census_original_schema.json") as f:
    schema = json.load(f)


selectivity = {
    "age": [0.1, 0.2],
    "hours_per_week": [0.1, 0.2]
}

value_range = {
    "age": [17, 90],
    "hours_per_week": [1, 99]
}

# Generate templates 
gen = TemplateGenerator(schema, base_table="census_original")
templates = gen.generate_templates(num_templates=5, selectivity=selectivity, value_range=value_range)

# Save to file
save_templates(templates, "./output/intermediate/census_original_templates.json")

selectivity = {
    "age": [0.1, 0.25],
    "hours_per_week": [0.1, 0.3]
}
gen = TemplateGenerator(schema, base_table="census_original")
templates = gen.generate_templates(num_templates=5, selectivity=selectivity, value_range=value_range)
save_templates(templates, "./output/intermediate/census_original_templates_selectivity_2.json")

selectivity = {
    "age": [0.1, 0.4],
    "hours_per_week": [0.1, 0.3]
}
gen = TemplateGenerator(schema, base_table="census_original")
templates = gen.generate_templates(num_templates=5, selectivity=selectivity, value_range=value_range)
save_templates(templates, "./output/intermediate/census_original_templates_selectivity_3.json")


gen = TemplateGenerator(schema, base_table="census_original")
templates = gen.generate_templates(
    num_templates=500,
    max_predicates = 5,
    max_payload_columns = 6,
    selectivity=selectivity
)
save_templates(templates, "./output/intermediate/census_original_templates_structual_2.json")


gen = TemplateGenerator(schema, base_table="census_original")
templates = gen.generate_templates(
    num_templates=500,
    max_predicates = 7,
    max_payload_columns = 8,
    selectivity=selectivity
)
save_templates(templates, "./output/intermediate/census_original_templates_structual_3.json")
