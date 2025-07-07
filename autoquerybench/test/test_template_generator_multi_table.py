from autoquerybench.core.workload.template_generator import TemplateGenerator, TemplateGeneratorMulti
import json

# Load schema
with open("./autoquerybench/data/tpcds_schema.json") as f:
    schema = json.load(f)

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

    # def generate_templates(
    #     self,
    #     num_templates: int = 5,
    #     max_predicates: int = 2,
    #     max_payload_columns: int = 3,
    #     selectivity: Optional[Dict[str, Tuple[float, float]]] = None,
    #     value_range: Optional[List[str, Any]] = None,
    #     join_count: int = 0,
    #     join_candidates: List[Dict[str, Any]] = None
    # ) -> List[Dict]:

# # Save to file
gen.save_templates(templates, "./autoquerybench/data/sample_templates_multi_table.json")

