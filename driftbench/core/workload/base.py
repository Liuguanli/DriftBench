import random
from typing import Optional


class BaseTemplateGenerator:
    def __init__(self, schema: dict, seed: Optional[int] = 42):
        self.schema = schema
        self.tables = schema["tables"]
        self.source = schema.get("source", "")
        self.rnd = random.Random(seed)

        self.operator_pool = {
            "numeric": [">=", "<=", ">", "<", "BETWEEN"],
            "categorical": ["=", "!="],
            "datetime": [">=", "<="],
            "boolean": ["="],
            "string": ["LIKE", "="]
        }
