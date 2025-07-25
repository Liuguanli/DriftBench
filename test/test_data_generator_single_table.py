
import json
from pathlib import Path
from typing import Dict
import pprint
import csv
from driftbench.core.data.single_table import SingleTableDriftGenerator


from driftbench.core.schema.factory import get_schema_extractor

csv_path = "./data/census_original.csv"  


# 2. Load schema
with open("./output/intermediate/census_original_schema.json") as f:
    schema = json.load(f)

print(schema)

# 3.1 varying cardinality 
generator = SingleTableDriftGenerator(csv_path, schema, base_table="census_original")
df_drifted = generator.apply_drift(drift_type="vary_cardinality", scale=1)
output_file = "./output/data/cardinality/scale/census_original_cardinality_1.csv"  
df_drifted.to_csv(output_file, index=False)

# 3.2 updating cardinality 
df_deleted = generator.apply_drift(drift_type="selective_deletion", n=5000)
output_file = "./output/data/cardinality/update/census_original_deletion_5000.csv"  
df_deleted.to_csv(output_file, index=False)
    
# 3.3 shifting column distribution
df_shift = generator.apply_drift(drift_type="value_skew", 
    columns=["age", "hours_per_week", "workclass", "education"], 
    portion=1.0, skewness=2)
output_file = "./output/data/distributional/column/census_original_skew_2.csv"
df_shift.to_csv(output_file, index=False)

# 3.4 injecting outliers
df_outlier = generator.apply_drift(outlier_csv_path="./data/census_outliers.csv")
output_file = "./output/data/distributional/outlier/census_original_outlier.csv"
df_outlier.to_csv(output_file, index=False)
