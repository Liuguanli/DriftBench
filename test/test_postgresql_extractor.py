from driftbench.core.schema.factory import get_schema_extractor
import json


# Load db_config from JSON file
with open("./data/PG_info.json", "r") as f:
    db_config = json.load(f)

schema_name = "public"

extractor = get_schema_extractor(source_type="postgres", db_config=db_config, 
                                 schema_name=schema_name, sample_size=10000)

schema = extractor.extract_schema()

import pprint
pprint.pprint(schema)


import json

with open("./output/intermediate/tpcds_schema.json", "w") as f:
    json.dump(schema, f, indent=2, default=str)