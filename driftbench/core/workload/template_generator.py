import json
import random
from pathlib import Path
from typing import Any, List, Dict, Optional, Tuple
import pprint

class TemplateGenerator:

    def __init__(self, schema: dict, base_table: str = "sample", seed: Optional[int] = 42):

        self.schema = schema
        self.base_table = base_table
        self.source = schema["source"]
        self.table = schema["tables"][base_table]
        self.columns = self.table["columns"]
        self.rnd = random.Random(seed)
        self.operator_pool = {
            # "numeric": [">=", "<=", ">", "<", "BETWEEN"],
            "numeric": [">", "<", "BETWEEN"],
            "categorical": ["=", "!="],
            "datetime": [">=", "<="],
            "boolean": ["="],
            "string": ["LIKE", "="]
        }


    def _generate_predicate(self, col_name: str, col_info: dict, selectivity_lower_bound: int = 1e-6,
                            selectivity_upper_bound: int = 0.1, col_value_range: Any = None) -> Optional[dict]:

        dtype = col_info.get("logical_type")
        full_col = f"{self.base_table}.{col_name}"

        selectivity_lower_bound = max(selectivity_lower_bound, 0.05)
        selectivity_upper_bound = min(selectivity_upper_bound, 1)

        if selectivity_lower_bound == selectivity_upper_bound:
            selectivity = selectivity_lower_bound
        else:
            selectivity = round(self.rnd.uniform(selectivity_lower_bound, selectivity_upper_bound), 2)

        op = self.rnd.choice(self.operator_pool.get(dtype, ["="]))
        if dtype == "numeric":
            return {
                "column": full_col,
                "operator": op,
                "type": dtype,
                "value": "",
                "range": col_info.get("range"),
                "selectivity": selectivity
            }

        elif dtype == "categorical":
            # values = col_info.get("sample_values", ["A"])
            return {
                "column": full_col,
                "operator": op,
                "type": dtype,
                "value": "",
                "range": col_value_range if col_value_range else col_info.get("sample_values"),
                "selectivity": selectivity
            }

        elif dtype == "datetime":
            # values = col_info.get("sample_values", ["2020-01-01"])
            return {
                "column": full_col,
                "operator": op,
                "type": dtype,
                "value": "",
                "range": col_info.get("range"),
                "selectivity": selectivity
            }

        # Boolean predicate
        elif dtype == "boolean":
            return {
                "column": full_col,
                "operator": op,
                "type": dtype,
                "value": "",
                "range": col_info.get("sample_values"),
                "selectivity": selectivity
            }

        # String: sample fixed literal
        elif dtype == "string":
            # values = col_info.get("sample_values", ["sample"])
            return {
                "column": full_col,
                "operator": op,
                "type": dtype,
                # "value": f"%{self.rnd.choice(values)}%",
                "value": "",
                "range": col_value_range if col_value_range else col_info.get("sample_values"),
                "selectivity": selectivity
            }

        return None

    def generate_templates(
        self,
        num_templates: int = 5,
        max_predicates: int = 3,
        max_payload_columns: int = 3,
        selectivity: Optional[Dict[str, Tuple[float, float]]] = None,
        value_range: Optional[Dict[str, Any]] = None
    ) -> List[Dict]:
        
        assert selectivity is not None and len(selectivity) > 0, "selectivity must be provided and not be empty"

        templates = []
        col_names = sorted(list(self.columns.keys()))  # sorted for deterministic output

        for i in range(num_templates):
            pred_num = self.rnd.randint(1, min(max_predicates, len(col_names)))
            pred_cols = self.rnd.sample(col_names, k=pred_num)
            payload_num = self.rnd.randint(1, min(max_payload_columns, len(col_names)))
            payload_cols = self.rnd.sample(col_names, k=payload_num)
            predicates = list(filter(None, [
                self._generate_predicate(
                    col,
                    self.columns[col],
                    selectivity_lower_bound=selectivity[col][0] if selectivity and col in selectivity else 1e-6,
                    selectivity_upper_bound=selectivity[col][1] if selectivity and col in selectivity else 0.1,
                    col_value_range=value_range[col] if value_range and col in value_range else None
                )
                for col in pred_cols
            ]))

            payload = {
                "columns": [f"{self.base_table}.{col}" for col in payload_cols],
                "aggregation": None,
                "order_by": f"{self.base_table}.{self.rnd.choice(payload_cols)}",
                "limit": 100
            }

            template = {
                "template_id": f"T{i:03}",
                "cardinality": self.table["num_rows"],
                "tables": {
                    "base": self.base_table,
                    "joins": []
                },
                "predicate": predicates,
                "payload": payload
            }

            templates.append(template)

        return templates

    # def save_templates(self, templates: List[Dict], output_path: str):
    #     Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    #     with open(output_path, "w") as f:
    #         json.dump(templates, f, indent=2)


class TemplateGeneratorMulti:
    def __init__(self, schema: dict, candidate_tables: List[str] = None, seed: Optional[int] = 42):

        if candidate_tables:
            schema["tables"] = {
                table: schema["tables"][table]
                for table in schema["tables"]
                if table in candidate_tables
            }

        self.tables = schema["tables"]
        self.table_names = list(schema["tables"].keys())

        from itertools import combinations
        import difflib

        join_candidates = []

        for t1, t2 in combinations(schema["tables"], 2):
            cols1 = schema["tables"][t1]["columns"]
            cols2 = schema["tables"][t2]["columns"]

            name_cutoff = 0.9
            for col1 in cols1:
                for col2 in cols2:

                    name_sim = difflib.SequenceMatcher(None, col1, col2).ratio()
                    type1 = cols1[col1]["logical_type"]
                    type2 = cols2[col2]["logical_type"]

                    if name_sim >= name_cutoff and type1 == type2:
                        join_candidates.append({
                            "column1": f"{t1}.{col1}",
                            "column2": f"{t2}.{col2}",
                            "name_similarity": round(name_sim, 2)
                        })

        pprint.pprint(join_candidates)
        print(len(join_candidates))

        self.join_candidates = join_candidates

        self.source = schema["source"]
        # self.columns = schema["tables"][base_table]["columns"]
        self.rnd = random.Random(seed)

        self.operator_pool = {
            "numeric": [">=", "<=", ">", "<", "BETWEEN"],
            "categorical": ["=", "!="],
            "datetime": [">=", "<="],
            "boolean": ["="],
            "string": ["LIKE", "="]
        }
        # For JOIN conditions, we only consider equality predicates (i.e., column1 = column2).
        # This aligns with common join semantics such as foreign key relationships and allows efficient index-based execution.
        # Non-equi joins (e.g., <, >, BETWEEN) are not considered in this benchmark.
        self.join_type_pool = [
            "INNER JOIN",
            "LEFT JOIN",
            "FULL JOIN",
            "RIGHT JOIN",
            "FULL JOIN",
        ]

    def _generate_predicate(self, base_table: str, col_name: str, col_info: dict, selectivity_lower_bound: int = 1e-6,
                            selectivity_upper_bound: int = 0.1, col_value_range: Any = None) -> Optional[dict]:

        dtype = col_info.get("logical_type")
        full_col = f"{base_table}.{col_name}"

        selectivity_lower_bound = max(selectivity_lower_bound, 0.1)
        selectivity_upper_bound = min(selectivity_upper_bound, 1)

        if selectivity_lower_bound == selectivity_upper_bound:
            selectivity = selectivity_lower_bound
        else:
            selectivity = round(self.rnd.uniform(selectivity_lower_bound, selectivity_upper_bound), 2)

        op = self.rnd.choice(self.operator_pool.get(dtype, ["="]))
        if dtype == "numeric":
            return {
                "column": full_col,
                "operator": op,
                "type": dtype,
                "value": "",
                "range": col_info.get("range"),
                "selectivity": selectivity
            }

        elif dtype == "categorical":
            # values = col_info.get("sample_values", ["A"])
            return {
                "column": full_col,
                "operator": op,
                "type": dtype,
                "value": "",
                "range": col_value_range if col_value_range else col_info.get("sample_values"),
                "selectivity": selectivity
            }

        elif dtype == "datetime":
            # values = col_info.get("sample_values", ["2020-01-01"])
            return {
                "column": full_col,
                "operator": op,
                "type": dtype,
                "value": "",
                "range": col_info.get("range"),
                "selectivity": selectivity
            }

        # Boolean predicate
        elif dtype == "boolean":
            return {
                "column": full_col,
                "operator": op,
                "type": dtype,
                "value": "",
                "range": col_info.get("sample_values"),
                "selectivity": selectivity
            }

        # String: sample fixed literal
        elif dtype == "string":
            # values = col_info.get("sample_values", ["sample"])
            return {
                "column": full_col,
                "operator": op,
                "type": dtype,
                # "value": f"%{self.rnd.choice(values)}%",
                "value": "",
                "range": col_value_range if col_value_range else col_info.get("sample_values"),
                "selectivity": selectivity
            }

        return None

    def generate_templates(
        self,
        num_templates: int = 5,
        max_predicates: int = 2,
        max_payload_columns: int = 3,
        selectivity: Optional[Dict[str, Tuple[float, float]]] = None,
        value_range: Optional[List[Tuple[str, Any]]] = None,
        join_count: int = 0,
        join_candidates: List[Dict[str, Any]] = None
    ) -> List[Dict]:

        if join_candidates:
            self.join_candidates = join_candidates

        assert selectivity is not None and len(selectivity) > 0, "selectivity must be provided and not be empty"

        if join_count == 0:
            pass
            #TODO use one table way

        # TODO get at most {join_count} items from join_candidates 

        templates = []
        # col_names = sorted(list(self.columns.keys()))  # sorted for deterministic output

        for i in range(num_templates):
            # 1. Randomly select a main table
            main_table = self.rnd.sample(list(self.tables.keys()), 1)[0]
            main_columns = list(self.tables[main_table]["columns"].keys())

            # 2. Generate SELECT clause (payload columns)
            payload_num = self.rnd.randint(1, min(max_payload_columns, len(main_columns)))
            payload_cols = self.rnd.sample(main_columns, k=payload_num)

            # 3. Generate predicate columns
            pred_num = self.rnd.randint(1, min(max_predicates, len(main_columns)))
            pred_cols = self.rnd.sample(main_columns, k=pred_num)

            template_value_range = {}

            if not value_range:
                for col in pred_cols:
                    if "range" in self.tables[main_table]["columns"][col]:
                        col_value_range = self.tables[main_table]["columns"][col]["range"]
                        template_value_range[col] = [col_value_range["min"], col_value_range["max"]]
                        # print(col, template_value_range)
            else:
                template_value_range = value_range

            # print(template_value_range)
            # print("pred_cols", pred_cols)
            predicates = list(filter(None, [
                self._generate_predicate(
                    main_table,
                    col,
                    self.tables[main_table]["columns"][col],
                    selectivity_lower_bound=selectivity[col][0] if selectivity and col in selectivity else 1e-6,
                    selectivity_upper_bound=selectivity[col][1] if selectivity and col in selectivity else 0.1,
                    col_value_range=template_value_range[col] if template_value_range and col in template_value_range else None
                )
                for col in pred_cols
            ]))

            join_entries = []
            used_tables = {main_table}
            current_table = main_table

            random_join_count = self.rnd.sample(range(1, join_count + 1), 1)[0]
            for _ in range(random_join_count):
                # print(current_table)
                # for c in self.join_candidates:
                #     print("c", c)
                joinable = [c for c in self.join_candidates if current_table in c["column1"]]
                if not joinable:
                    break
                join = random.choice(joinable)
                left = join["column1"]
                right = join["column2"]
                right_table = ".".join(right.split(".")[:-1])

                if right_table in used_tables:
                    continue

                join_type = random.choice(self.join_type_pool)
                join_condition = f"{left} = {right}"
                join_entries.append({
                    "type": join_type,
                    "table": right_table,
                    "condition": join_condition
                })

                used_tables.add(right_table)
                current_table = right_table

            payload = {
                "columns": [f"{main_table}.{col}" for col in payload_cols],
                "aggregation": None,
                "order_by": f"{main_table}.{self.rnd.choice(payload_cols)}",
                "limit": 100
            }

            template = {
                "template_id": f"T{i:03}",
                "cardinality": self.tables[main_table]["num_rows"],
                "tables": {
                    "base": main_table,
                    "joins": join_entries
                },
                "predicate": predicates,
                "payload": payload
            }

            templates.append(template)

        return templates

    # def save_templates(self, templates: List[Dict], output_path: str):
    #     Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    #     with open(output_path, "w") as f:
    #         json.dump(templates, f, indent=2)
