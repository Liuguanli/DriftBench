from typing import List, Dict, Tuple, Optional
from .base import BaseTemplateGenerator
from .predicate_generator import generate_predicate
from .payload_generator import generate_payload


class SingleTableTemplateGenerator(BaseTemplateGenerator):
    def __init__(self, schema: dict, base_table: str, seed: Optional[int] = 42):
        super().__init__(schema, seed)
        self.base_table = base_table
        self.table = self.tables[base_table]
        self.columns = self.table["columns"]

    def generate_templates(
        self,
        num_templates: int = 5,
        max_predicates: int = 2,
        max_payload_columns: int = 3,
        selectivity: Optional[Dict[str, Tuple[float, float]]] = None,
        value_range: Optional[Dict[str, List]] = None
    ) -> List[Dict]:
        assert selectivity is not None and len(selectivity) > 0

        col_names = sorted(list(self.columns.keys()))
        templates = []

        for i in range(num_templates):
            pred_num = self.rnd.randint(1, min(max_predicates, len(col_names)))
            pred_cols = self.rnd.sample(col_names, k=pred_num)

            payload_num = self.rnd.randint(1, min(max_payload_columns, len(col_names)))
            payload_cols = self.rnd.sample(col_names, k=payload_num)

            predicates = [
                generate_predicate(
                    self.rnd,
                    self.base_table,
                    col,
                    self.columns[col],
                    self.operator_pool,
                    selectivity.get(col, (1e-6, 0.1)),
                    value_range.get(col) if value_range and col in value_range else None
                )
                for col in pred_cols
            ]

            payload = generate_payload(self.rnd, self.base_table, payload_cols, max_payload_columns)

            templates.append({
                "template_id": f"T{i:03}",
                "cardinality": self.table["num_rows"],
                "tables": {
                    "base": self.base_table,
                    "joins": []
                },
                "predicate": predicates,
                "payload": payload
            })

        return templates
