
import json
from pathlib import Path
from typing import Dict
import pprint
import csv
from autoquerybench.core.data.single_table import SingleTableDriftGenerator
from autoquerybench.core.schema_extractor import get_schema_extractor


csv_path = "./autoquerybench/data/census_original.csv"  

# 1. get schema 
# extractor = get_schema_extractor(source_type="csv", csv_path=csv_path, sample_size=100)
# schema = extractor.extract_schema()
# # pprint.pprint(schema)
# with open("./autoquerybench/data/census_original_schema.json", "w") as f:
#     json.dump(schema, f, indent=2, default=str)

# 2. Load schema
with open("./autoquerybench/data/census_original_schema.json") as f:
    schema = json.load(f)


generator = SingleTableDriftGenerator(csv_path, schema, base_table="census_original")
# df_drifted = generator.apply_drift(drift_type="vary_cardinality", scale=1)
# output_file = "./autoquerybench/data/census_original_cardinality_1.csv"  
# df_drifted.to_csv(output_file, index=False)


# df_deleted = generator.apply_drift(drift_type="selective_deletion", n=100)
# df_deleted = generator.apply_drift(drift_type="selective_deletion", n=5000)
# output_file = "./autoquerybench/data/census_original_deletion_5000.csv"  
# df_deleted.to_csv(output_file, index=False)
    
df_deleted = generator.apply_drift(drift_type="value_skew", 
    columns=["age", "education_num", "capital_gain", "hours_per_week", "workclass", "education"], 
    portion=1.0)
output_file = "./autoquerybench/data/census_original_skew_2.csv"
df_deleted.to_csv(output_file, index=False)
