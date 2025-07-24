from driftbench.core.schema_extractor import get_schema_extractor


db_config =  {
    "dbname": "pgdb",
    "user": "pguser",
    "password": "123456",
    "host": "localhost",
    "port": 5438
}

schema_name = "public"

extractor = get_schema_extractor(source_type="postgres", db_config=db_config, 
                                 schema_name=schema_name, sample_size=100)

# 提取 schema
schema = extractor.extract_schema()

# 打印结果
import pprint
pprint.pprint(schema)


import json

with open("./autoquerybench/data/tpcds_schema.json", "w") as f:
    json.dump(schema, f, indent=2, default=str)