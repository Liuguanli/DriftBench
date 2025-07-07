from autoquerybench.core.workload.template_generator import TemplateGenerator
import json

# Load schema
with open("./autoquerybench/data/sample_rich_schema.json") as f:
    schema = json.load(f)

# # Generate templates 
# gen = TemplateGenerator(schema, base_table="sample")
# templates = gen.generate_templates(num_templates=5)

# # Save templates
# gen.save_templates(templates, "./autoquerybench/data/sample_templates.json")


selectivity = {
    "age": [0.1, 0.2],
    "income": [0.1, 0.2]
}

value_range = {
    "age": [0, 100],
    "income": [2000, 100000]
}

# query_center_distribution = {
#     "age": {"mean": 35, "std": 5},
#     "income": {"mean": 50000, "std": 10000}
# }

# Create generator and generate templates
gen = TemplateGenerator(schema, base_table="sample_rich", seed=42)
templates = gen.generate_templates(
    num_templates=5,
    selectivity=selectivity
)

# Save to file
gen.save_templates(templates, "./autoquerybench/data/sample_templates.json")


selectivity = {
    "age": [0.1, 0.3],
    "income": [0.1, 0.3]
}
gen = TemplateGenerator(schema, base_table="sample_rich", seed=42)
templates = gen.generate_templates(
    num_templates=5,
    selectivity=selectivity
)

# Save to file
gen.save_templates(templates, "./autoquerybench/data/sample_templates_selectivity_2.json")

selectivity = {
    "age": [0.1, 0.4],
    "income": [0.1, 0.4]
}
gen = TemplateGenerator(schema, base_table="sample_rich", seed=42)
templates = gen.generate_templates(
    num_templates=5,
    selectivity=selectivity
)

# Save to file
gen.save_templates(templates, "./autoquerybench/data/sample_templates_selectivity_3.json")

selectivity = {
    "age": [0.1, 0.5],
    "income": [0.1, 0.5]
}
gen = TemplateGenerator(schema, base_table="sample_rich", seed=42)
templates = gen.generate_templates(
    num_templates=5,
    selectivity=selectivity
)

# Save to file
gen.save_templates(templates, "./autoquerybench/data/sample_templates_selectivity_4.json")