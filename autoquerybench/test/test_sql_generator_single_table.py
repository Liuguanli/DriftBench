import json
from pathlib import Path
from typing import Dict
from autoquerybench.core.workload.sql_generator import generate_sql_queries, save_queries_to_json, save_queries_to_csv


# Example usage
if __name__ == "__main__":

    # uniform
    dist_config = {
        "sample_rich.age": {"distribution": "uniform", "max":75, "min": 18},
        "sample_rich.category": {"distribution": "choice", "choices": ["A", "B", "C"]},
        "sample_rich.signup_date": {"distribution": "fixed", "value": "2021-01-01"}
    }
        
    timestamps, sqls = generate_sql_queries(
        template_file="./autoquerybench/data/sample_templates.json",
        dist_config=dist_config,
        timestamp_start="2025-07-01T00:00:00",
        queries_per_template=300,
        total_duration_sec=300
    )

    save_queries_to_json(timestamps, sqls, "./autoquerybench/data/generated_queries_age_uniform.json")

    save_queries_to_csv(timestamps, sqls, "./autoquerybench/data/generated_queries_age_uniform.csv")

    timestamps, sqls = generate_sql_queries(
        template_file="./autoquerybench/data/sample_templates.json",
        dist_config=dist_config,
        timestamp_start="2025-07-01T00:05:00",
        queries_per_template=300,
        timestamp_pattern="periodic",
        total_duration_sec=300
    )

    save_queries_to_json(timestamps, sqls, "./autoquerybench/data/generated_queries_age_uniform_periodic.json")

    save_queries_to_csv(timestamps, sqls, "./autoquerybench/data/generated_queries_age_uniform_periodic.csv")

    timestamps, sqls = generate_sql_queries(
        template_file="./autoquerybench/data/sample_templates.json",
        dist_config=dist_config,
        timestamp_start="2025-07-01T00:10:00",
        queries_per_template=300,
        timestamp_pattern="trend",
        total_duration_sec=300
    )

    save_queries_to_json(timestamps, sqls, "./autoquerybench/data/generated_queries_age_uniform_trend.json")

    save_queries_to_csv(timestamps, sqls, "./autoquerybench/data/generated_queries_age_uniform_trend.csv")


    timestamps, sqls = generate_sql_queries(
        template_file="./autoquerybench/data/sample_templates.json",
        dist_config=dist_config,
        timestamp_start="2025-07-01T00:15:00",
        queries_per_template=300,
        timestamp_pattern="long_tail",
        total_duration_sec=300
    )

    save_queries_to_json(timestamps, sqls, "./autoquerybench/data/generated_queries_age_uniform_long_tail.json")

    save_queries_to_csv(timestamps, sqls, "./autoquerybench/data/generated_queries_age_uniform_long_tail.csv")

    # normal
    dist_config = {
        "sample_rich.age": {"distribution": "normal", "mean": 50, "std": 10},
        "sample_rich.category": {"distribution": "choice", "choices": ["A", "B", "C"]},
        "sample_rich.signup_date": {"distribution": "fixed", "value": "2021-01-01"}
    }
    
    timestamps, sqls = generate_sql_queries(
        template_file="./autoquerybench/data/sample_templates.json",
        dist_config=dist_config,
        timestamp_start="2025-07-01T00:05:00",
        queries_per_template=300,
        total_duration_sec=300
    )

    save_queries_to_json(timestamps, sqls, "./autoquerybench/data/generated_queries_age_normal_50.json")

    save_queries_to_csv(timestamps, sqls, "./autoquerybench/data/generated_queries_age_normal_50.csv")

    # zipf
    dist_config = {
        "sample_rich.age": {"distribution": "zipf", "a": 2, "max":75, "min": 18},
        "sample_rich.category": {"distribution": "choice", "choices": ["A", "B", "C"]},
        "sample_rich.signup_date": {"distribution": "fixed", "value": "2021-01-01"}
    }

    timestamps, sqls = generate_sql_queries(
        template_file="./autoquerybench/data/sample_templates.json",
        dist_config=dist_config,
        timestamp_start="2025-07-01T00:10:00",
        queries_per_template=300,
        total_duration_sec=300
    )

    save_queries_to_json(timestamps, sqls, "./autoquerybench/data/generated_queries_age_zipf_50.json")

    save_queries_to_csv(timestamps, sqls, "./autoquerybench/data/generated_queries_age_zipf_50.csv")
