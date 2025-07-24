import json
from pathlib import Path
from typing import Dict
from driftbench.core.workload.sql_generator import generate_sql_queries, save_queries_to_json, save_queries_to_csv


# Example usage
if __name__ == "__main__":

    # uniform
    dist_config = {
        "sample_rich.age": {"distribution": "uniform", "max":75, "min": 18},
        "sample_rich.category": {"distribution": "choice", "choices": ["A", "B", "C"]},
        "sample_rich.signup_date": {"distribution": "fixed", "value": "2021-01-01"}
    }

    timestamps, sqls = generate_sql_queries(
        template_file="./autoquerybench/data/sample_templates_multi_table.json",
        dist_config=dist_config,
        timestamp_start="2025-07-01T00:00:00",
        queries_per_template=300,
        total_duration_sec=300
    )

    # print(sqls)
    # for sql in sqls:
    #     if "JOIN" in sql:
    #         print(sql)
    #         break

    save_queries_to_json(timestamps, sqls, "./autoquerybench/data/generated_queries_multi_table.json")

    save_queries_to_csv(timestamps, sqls, "./autoquerybench/data/generated_queries_multi_table.csv")
