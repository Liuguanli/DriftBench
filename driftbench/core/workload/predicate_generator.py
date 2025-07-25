from typing import Any, Tuple


def generate_predicate(
    rnd,
    table_name: str,
    col_name: str,
    col_info: dict,
    operator_pool: dict,
    selectivity_range: Tuple[float, float],
    value_range: Any = None
) -> dict:
    dtype = col_info.get("logical_type")
    full_col = f"{table_name}.{col_name}"

    sel_lo, sel_hi = max(selectivity_range[0], 1e-6), min(selectivity_range[1], 1)
    selectivity = round(rnd.uniform(sel_lo, sel_hi), 2)

    op = rnd.choice(operator_pool.get(dtype, ["="]))
    return {
        "column": full_col,
        "operator": op,
        "type": dtype,
        "value": "",
        "range": value_range if value_range else col_info.get("sample_values") or col_info.get("range"),
        "selectivity": selectivity
    }
