from typing import List, Dict


def generate_joins(rnd, start_table: str, join_candidates: List[Dict], join_count: int, join_type_pool: List[str]) -> List[Dict]:
    joins = []
    used_tables = {start_table}
    current_table = start_table

    for _ in range(join_count):
        candidates = [c for c in join_candidates if current_table in c["column1"]]
        if not candidates:
            break

        join = rnd.choice(candidates)
        right_table = ".".join(join["column2"].split(".")[:-1])
        if right_table in used_tables:
            continue

        joins.append({
            "type": rnd.choice(join_type_pool),
            "table": right_table,
            "condition": f"{join['column1']} = {join['column2']}"
        })
        used_tables.add(right_table)
        current_table = right_table

    return joins
