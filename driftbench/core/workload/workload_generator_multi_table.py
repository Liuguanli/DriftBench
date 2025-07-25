from typing import List, Dict, Tuple, Optional
from itertools import combinations
import difflib
from .base import BaseTemplateGenerator
from .predicate_generator import generate_predicate
from .payload_generator import generate_payload
from .join_strategy import generate_joins


class MultiTableTemplateGenerator(BaseTemplateGenerator):
    def __init__(self, schema: dict, candidate_tables: Optional[List[str]] = None, seed: Optional[int] = 42):
        if candidate_tables:
            schema["tables"] = {k: v for k, v in schema["tables"].items() if k in candidate_tables}
        super().__init__(schema, seed)

        self.table_names = list(self.tables.keys())
        self.join_type_pool = ["INNER JOIN", "LEFT JOIN", "FULL JOIN", "RIGHT JOIN"]
        self.join_candidates = self._infer_join_candidates()

    def _infer_join_candidates(self) -> List[Dict]:
        join_candidates = []
        for t1, t2 in combinations(self.tables, 2):
            cols1 = self.tables[t1]["columns"]
            cols2 = self.tables[t2]["columns"]

            for c1 in cols1:
                for c2 in cols2:
                    type1 = cols1[c1]["logical_type"]
                    type2 = cols2[c2]["logical_type"]
                    if type1 != type2:
                        continue
                    name_sim = difflib.SequenceMatcher(None, c1, c2).ratio()
                    if name_sim > 0.9:
                        join_candidates.append({
                            "column1": f"{t1}.{c1}",
                            "column2": f"{t2}.{c2}",
                            "name_similarity": round(name_sim, 2)
                        })
        return join_candidates

    def generate_templates(
        self,
        num_templates: int = 5,
        max_predicates: int = 2,
        max_payload_columns: int = 3,
        selectivity: Optional[Dict[str, Tuple[float, float]]] = None,
        value_range: Optional[Dict[str, List]] = None,
        join_count: int = 0,
        join_candidates: Optional[List[Dict]] = None
    ) -> List[Dict]:
        assert selectivity is not None and len(selectivity) > 0
        if join_candidates:
            self.join_candidates = join_candidates

        templates = []

        for i in range(num_templates):
            main_table = self.rnd.choice(self.table_names)
            main_columns = list(self.tables[main_table]["columns"].keys())

            pred_num = self.rnd.randint(1, min(max_predicates, len(main_columns)))
            pred_cols = self.rnd.sample(main_columns, k=pred_num)

            payload_num = self.rnd.randint(1, min(max_payload_columns, len(main_columns)))
            payload_cols = self.rnd.sample(main_columns, k=payload_num)

            predicates = [
                generate_predicate(
                    self.rnd,
                    main_table,
                    col,
                    self.tables[main_table]["columns"][col],
                    self.operator_pool,
                    selectivity.get(col, (1e-6, 0.1)),
                    value_range.get(col) if value_range and col in value_range else None
                )
                for col in pred_cols
            ]

            payload = generate_payload(self.rnd, main_table, payload_cols, max_payload_columns)

            join_entries = generate_joins(self.rnd, main_table, self.join_candidates, join_count, self.join_type_pool)

            templates.append({
                "template_id": f"T{i:03}",
                "cardinality": self.tables[main_table]["num_rows"],
                "tables": {
                    "base": main_table,
                    "joins": join_entries
                },
                "predicate": predicates,
                "payload": payload
            })

        return templates
