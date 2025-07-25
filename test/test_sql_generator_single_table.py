import json
from pathlib import Path
from typing import Dict
from driftbench.core.workload.sql_generator import generate_sql_queries
from driftbench.core.temporal.time_stamp_generator import generate_timestamps
from driftbench.core.utils import save_sqls, save_sqls_with_timestamps


def gen_temporal_patterns():

    # uniform
    dist_config = {
        "census_original.age": {"distribution": "uniform", "max":75, "min": 18},
        "census_original.workclass": {"distribution": "choice", "choices": ["State-gov", "Private", "Local-gov"]}
    }
        
    sqls = generate_sql_queries(
        template_file="./output/intermediate/census_original_templates.json",
        dist_config=dist_config,
        queries_per_template=300
    )
    save_sqls(sqls, "./output/workload/census_original_sqls_default.csv")

    timestamps = generate_timestamps(
        count=len(sqls),
        start_time="2025-07-01T00:00:00",
        pattern="uniform",
        queries_per_minute=300
    )
    save_sqls_with_timestamps(sqls, timestamps, "./output/temporal/census_original_sqls_uniform_timestamp.csv")

    timestamps = generate_timestamps(
        count=len(sqls),
        start_time="2025-07-01T00:05:00",
        pattern="periodic",
        queries_per_minute=300
    )
    save_sqls_with_timestamps(sqls, timestamps, "./output/temporal/census_original_sqls_periodic_timestamp.csv")

    timestamps = generate_timestamps(
        count=len(sqls),
        start_time="2025-07-01T00:10:00",
        pattern="trend",
        queries_per_minute=300
    )
    save_sqls_with_timestamps(sqls, timestamps, "./output/temporal/census_original_sqls_trend_timestamp.csv")

    timestamps = generate_timestamps(
        count=len(sqls),
        start_time="2025-07-01T00:15:00",
        pattern="long_tail",
        queries_per_minute=300
    )
    save_sqls_with_timestamps(sqls, timestamps, "./output/temporal/census_original_sqls_long_tail_timestamp.csv")


def gen_parametric_distributional():

    # uniform
    dist_config = {
        "census_original.age": {"distribution": "uniform", "max":75, "min": 18},
        "census_original.workclass": {"distribution": "choice", "choices": ["State-gov", "Private", "Local-gov"]}
    }
        
    sqls = generate_sql_queries(
        template_file="./output/intermediate/census_original_templates.json",
        dist_config=dist_config,
        queries_per_template=300
    )

    timestamps = generate_timestamps(
        count=len(sqls),
        start_time="2025-07-01T00:00:00",
        pattern="uniform",
        queries_per_minute=300
    )
    save_sqls_with_timestamps(sqls, timestamps, "./output/workload/parametric/distribution/census_original_uniform_sqls.csv")

    # normal
    dist_config = {
        "census_original.age": {"distribution": "normal", "mean": 50, "std": 5},
        "census_original.workclass": {"distribution": "choice", "choices": ["State-gov", "Private", "Local-gov"]}
    }
    
    sqls = generate_sql_queries(
        template_file="./output/intermediate/census_original_templates.json",
        dist_config=dist_config,
        queries_per_template=300
    )

    timestamps = generate_timestamps(
        count=len(sqls),
        start_time="2025-07-01T00:05:00",
        pattern="uniform",
        queries_per_minute=300
    )

    save_sqls_with_timestamps(sqls, timestamps, "./output/workload/parametric/distribution/census_original_normal_sqls.csv")

    # zipf
    dist_config = {
        "census_original.age": {"distribution": "zipf", "a": 2, "max":75, "min": 18},
        "census_original.workclass": {"distribution": "choice", "choices": ["State-gov", "Private", "Local-gov"]}
    }

    sqls = generate_sql_queries(
        template_file="./output/intermediate/census_original_templates.json",
        dist_config=dist_config,
        queries_per_template=300
    )

    timestamps = generate_timestamps(
        count=len(sqls),
        start_time="2025-07-01T00:10:00",
        pattern="uniform",
        queries_per_minute=300
    )

    save_sqls_with_timestamps(sqls, timestamps, "./output/workload/parametric/distribution/census_original_skew_sqls.csv")


def gen_parametric_selectivity():

    dist_config = {
        "census_original.age": {"distribution": "uniform", "max":75, "min": 18},
    }

    # selectivity 1
    sqls = generate_sql_queries(
        template_file="./output/intermediate/census_original_templates.json",
        dist_config=dist_config,
        queries_per_template=300
    )

    timestamps = generate_timestamps(
        count=len(sqls),
        start_time="2025-07-01T00:00:00",
        pattern="uniform",
        queries_per_minute=300
    )

    save_sqls_with_timestamps(sqls, timestamps, "./output/workload/parametric/selectivity/census_original_sqls_selectivity_1.csv")

    # selectivity 2
    sqls = generate_sql_queries(
        template_file="./output/intermediate/census_original_templates_selectivity_2.json",
        dist_config=dist_config,
        queries_per_template=300
    )

    timestamps = generate_timestamps(
        count=len(sqls),
        start_time="2025-07-01T00:05:00",
        pattern="uniform",
        queries_per_minute=300
    )

    save_sqls_with_timestamps(sqls, timestamps, "./output/workload/parametric/selectivity/census_original_sqls_selectivity_2.csv")

    # selectivity 3
    sqls = generate_sql_queries(
        template_file="./output/intermediate/census_original_templates_selectivity_3.json",
        dist_config=dist_config,
        queries_per_template=300
    )

    timestamps = generate_timestamps(
        count=len(sqls),
        start_time="2025-07-01T00:10:00",
        pattern="uniform",
        queries_per_minute=300
    )

    save_sqls_with_timestamps(sqls, timestamps, "./output/workload/parametric/selectivity/census_original_sqls_selectivity_3.csv")


def gen_structural():
    pass


# Example usage
if __name__ == "__main__":
    gen_temporal_patterns()
    gen_parametric_distributional()
    gen_parametric_selectivity()
    gen_structural()
# save_queries_to_json(timestamps, sqls, "./autoquerybench/data/generated_queries_age_uniform_periodic.json")

# save_queries_to_csv(timestamps, sqls, "./autoquerybench/data/generated_queries_age_uniform_periodic.csv")

# timestamps, sqls = generate_sql_queries(
#     template_file="./autoquerybench/data/sample_templates.json",
#     dist_config=dist_config,
#     timestamp_start="2025-07-01T00:10:00",
#     queries_per_template=300,
#     timestamp_pattern="trend",
#     total_duration_sec=300
# )

# save_queries_to_json(timestamps, sqls, "./autoquerybench/data/generated_queries_age_uniform_trend.json")

# save_queries_to_csv(timestamps, sqls, "./autoquerybench/data/generated_queries_age_uniform_trend.csv")


# timestamps, sqls = generate_sql_queries(
#     template_file="./autoquerybench/data/sample_templates.json",
#     dist_config=dist_config,
#     timestamp_start="2025-07-01T00:15:00",
#     queries_per_template=300,
#     timestamp_pattern="long_tail",
#     total_duration_sec=300
# )

# save_queries_to_json(timestamps, sqls, "./autoquerybench/data/generated_queries_age_uniform_long_tail.json")

# save_queries_to_csv(timestamps, sqls, "./autoquerybench/data/generated_queries_age_uniform_long_tail.csv")

# # normal
# dist_config = {
#     "sample_rich.age": {"distribution": "normal", "mean": 50, "std": 10},
#     "sample_rich.category": {"distribution": "choice", "choices": ["A", "B", "C"]},
#     "sample_rich.signup_date": {"distribution": "fixed", "value": "2021-01-01"}
# }

# timestamps, sqls = generate_sql_queries(
#     template_file="./autoquerybench/data/sample_templates.json",
#     dist_config=dist_config,
#     timestamp_start="2025-07-01T00:05:00",
#     queries_per_template=300,
#     total_duration_sec=300
# )

# save_queries_to_json(timestamps, sqls, "./autoquerybench/data/generated_queries_age_normal_50.json")

# save_queries_to_csv(timestamps, sqls, "./autoquerybench/data/generated_queries_age_normal_50.csv")

# # zipf
# dist_config = {
#     "sample_rich.age": {"distribution": "zipf", "a": 2, "max":75, "min": 18},
#     "sample_rich.category": {"distribution": "choice", "choices": ["A", "B", "C"]},
#     "sample_rich.signup_date": {"distribution": "fixed", "value": "2021-01-01"}
# }

# timestamps, sqls = generate_sql_queries(
#     template_file="./autoquerybench/data/sample_templates.json",
#     dist_config=dist_config,
#     timestamp_start="2025-07-01T00:10:00",
#     queries_per_template=300,
#     total_duration_sec=300
# )

# save_queries_to_json(timestamps, sqls, "./autoquerybench/data/generated_queries_age_zipf_50.json")

# save_queries_to_csv(timestamps, sqls, "./autoquerybench/data/generated_queries_age_zipf_50.csv")
