

class BaseSQLRenderer:
    def render(self, template: dict) -> str:
        raise NotImplementedError("Subclasses must implement render()")

import numpy as np
from typing import Dict, Callable, Any, Optional, List


class PredicateValueSampler:
    def __init__(self, dist_config: Optional[Dict[str, Dict[str, Any]]] = None, seed: Optional[int] = 42):
        self.dist_config = dist_config or {}
        self.rng = np.random.default_rng(seed)

    def sample(self, col: str, dtype: str) -> Any:
        config = self.dist_config.get(col, self.dist_config.get(dtype.lower(), {}))
        dist = config.get("distribution", "uniform")  # default strategy

        if dtype.lower() == "numeric":
            if dist == "normal":
                mean = config.get("mean", 50)
                std = config.get("std", 10)
                val = round(self.rng.normal(mean, std), 2)
                # print(f"mean={mean}, std={std}, val={val}")
                return val
            elif dist == "uniform":
                min_val = config.get("min", 1)
                max_val = config.get("max", 100)
                return round(self.rng.uniform(min_val, max_val), 2)
            elif dist == "zipf":
                a = config.get("a", 2.0)
                max_val = config.get("max", 100)
                min_val = config.get("min", 1)
                val = self.rng.zipf(a)
                val = max(min(val, max_val), min_val)
                return val
            elif dist == "fixed":
                return config["value"]
        
        elif dtype.lower() == "categorical":
            if dist == "choice":
                choices = config.get("choices", ["A", "B", "C"])
                return self.rng.choice(choices)
            elif dist == "fixed":
                return config["value"]

        elif dtype.lower() == "datetime":
            return config.get("default", "2022-01-01")  # more advanced logic can be added

        elif dtype.lower() == "boolean":
            return self.rng.choice(["TRUE", "FALSE"])

        elif dtype.lower() == "string":
            return config.get("default", "sample")

        return "NULL"


class PostgreSQLRenderer:
    def __init__(self, sampler: Optional[PredicateValueSampler] = None):
        self.sampler = sampler or PredicateValueSampler()

    def render(self, template: Dict) -> str:
        base_table = template["tables"]["base"]
        payload = template["payload"]
        predicates = template["predicate"]

        select_clause = "SELECT " + ", ".join(payload["columns"])
        from_clause = f"FROM {base_table}"

        # JOINs
        join_clauses = []
        for join in template["tables"].get("joins", []):
            join_type = join["type"]
            join_table = join["table"]
            join_condition = join["condition"]
            join_clauses.append(f"{join_type} {join_table} ON {join_condition}")
        
        # WHERE predicates
        where_clauses = []
        for pred in predicates:
            col = pred["column"]
            op = pred["operator"]
            dtype = pred["type"]
            selectivity = float(pred["selectivity"])

            # TODO when sample, pass val_min and val_max is also possible.
            val = self.sampler.sample(col, dtype)

            if op.lower() == "between":  # dtype must in {"numeric"}
                if dtype in {"numeric"}:
                    val_min = pred["range"]["min"]
                    val_max = pred["range"]["max"]
                    half_range = (val_max - val_min) * selectivity / 2
                    # TODO can bound the range of val via val_max and val_min from the schema
                    # TODO can also use the bound from user input
                    clause = f"{col} BETWEEN {round(val - half_range, 6)} AND {round(val + half_range, 6)}"
            elif dtype in {"string", "categorical"}:
                clause = f"{col} {op} '{val}'"
            elif dtype == "datetime":
                clause = f"{col} {op} TIMESTAMP '{val}'"
            elif dtype == "boolean":
                clause = f"{col} {op} {val}"
            else:
                clause = f"{col} {op} {val}"
            where_clauses.append(clause)

        join_clause = " ".join(join_clauses)
        where_clause = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""
        order_by_clause = f"ORDER BY {payload['order_by']}" if payload.get("order_by") else ""
        limit_clause = f"LIMIT {payload['limit']}" if payload.get("limit") else ""

        return f"{select_clause} {from_clause} {join_clause} {where_clause} {order_by_clause} {limit_clause};"


class CSVRenderer(BaseSQLRenderer):
    def render(self, template: dict) -> str:
        predicates = template["predicate"]
        filters = []

        for pred in predicates:
            col = pred["column"].split(".")[-1]  # drop table prefix
            op = pred["operator"]
            val = pred["value"]
            dtype = pred["type"]

            if op == "BETWEEN" and isinstance(val, list):
                clause = f"(df['{col}'] >= {val[0]}) & (df['{col}'] <= {val[1]})"
            elif dtype in {"string", "categorical", "datetime"}:
                clause = f"(df['{col}'] {op} '{val}')"
            elif dtype == "boolean":
                clause = f"(df['{col}'] == {str(val)})"
            else:
                clause = f"(df['{col}'] {op} {val})"
            filters.append(clause)

        where_expr = " & ".join(filters)
        payload_cols = [c.split(".")[-1] for c in template["payload"]["columns"]]
        return f"df[{where_expr}][{payload_cols}]"
