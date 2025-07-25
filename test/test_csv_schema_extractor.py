from driftbench.core.schema.factory import get_schema_extractor

csv_path = "./data/census_original.csv"  

extractor = get_schema_extractor(source_type="csv", csv_path=csv_path, sample_size=1000)


schema = extractor.extract_schema()


import pprint
pprint.pprint(schema)

import json

with open("./output/intermediate/census_original_schema.json", "w") as f:
    json.dump(schema, f, indent=2, default=str)