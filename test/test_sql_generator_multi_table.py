import json
from pathlib import Path
from typing import Dict
from driftbench.core.workload.sql_generator import generate_sql_queries, save_queries_to_json, save_queries_to_csv
from driftbench.core.utils import save_sqls, save_sqls_with_timestamps
from driftbench.core.temporal.time_stamp_generator import generate_timestamps


# Example usage
if __name__ == "__main__":

    # # uniform
    # dist_config = {
    #     "sample_rich.age": {"distribution": "uniform", "max":75, "min": 18},
    #     "sample_rich.category": {"distribution": "choice", "choices": ["A", "B", "C"]},
    #     "sample_rich.signup_date": {"distribution": "fixed", "value": "2021-01-01"}
    # }

    sqls = generate_sql_queries(
        template_file="./output/intermediate/tpcds_templates_multi_table.json",
        # dist_config=dist_config,
        queries_per_template=300,
    )

    # sqls = generate_sql_queries(
    #     template_file="./output/intermediate/census_original_templates.json",
    #     dist_config=dist_config,
    #     queries_per_template=300
    # )
    save_sqls(sqls, "./output/workload/tpcds_sqls_default.csv")

    timestamps = generate_timestamps(
        count=len(sqls),
        start_time="2025-07-01T00:00:00",
        pattern="uniform",
        queries_per_minute=300
    )
    save_sqls_with_timestamps(sqls, timestamps, "./output/temporal/tpcds_sqls_uniform_timestamp.csv")
