from typing import List


def generate_payload(rnd, table_name: str, columns: List[str], max_cols: int) -> dict:
    payload_cols = rnd.sample(columns, k=rnd.randint(1, min(max_cols, len(columns))))
    return {
        "columns": [f"{table_name}.{col}" for col in payload_cols],
        "aggregation": None,
        "order_by": f"{table_name}.{rnd.choice(payload_cols)}",
        "limit": 100
    }
