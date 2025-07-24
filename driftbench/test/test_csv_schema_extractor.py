from driftbench.core.schema_extractor import get_schema_extractor


csv_path = "./autoquerybench/data/sample_rich.csv"  

extractor = get_schema_extractor(source_type="csv", csv_path=csv_path, sample_size=100)


schema = extractor.extract_schema()


import pprint
pprint.pprint(schema)


import json

with open("./autoquerybench/data/sample_rich_schema.json", "w") as f:
    json.dump(schema, f, indent=2, default=str)