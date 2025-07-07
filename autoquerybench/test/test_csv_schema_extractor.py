from autoquerybench.core.schema_extractor import get_schema_extractor

# 指定 CSV 文件路径
csv_path = "./autoquerybench/data/sample_rich.csv"  
# 获取 schema extractor 实例
extractor = get_schema_extractor(source_type="csv", csv_path=csv_path, sample_size=100)

# 提取 schema
schema = extractor.extract_schema()

# 打印结果
import pprint
pprint.pprint(schema)


import json

with open("./autoquerybench/data/sample_rich_schema.json", "w") as f:
    json.dump(schema, f, indent=2, default=str)