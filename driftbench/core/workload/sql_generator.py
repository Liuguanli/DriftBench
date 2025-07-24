import json
import random
import datetime
import csv
import math
import numpy as np
from typing import List, Dict, Optional
from pathlib import Path
from driftbench.core.workload.sql_renderer import PostgreSQLRenderer, PredicateValueSampler


# def generate_timestamps(
#     count: int,
#     start_time: str,
#     pattern: str = "uniform",
#     queries_per_minute: int = 60
# ) -> List[str]:
#     interval_sec = 60.0 / queries_per_minute
#     base_ts = datetime.datetime.fromisoformat(start_time)
#     timestamps = []
#     for i in range(count):
#         if pattern == "uniform":
#             offset = datetime.timedelta(seconds=i * interval_sec)
#         elif pattern == "periodic":
#             offset = datetime.timedelta(seconds=interval_sec * (1 + 0.5 * (1 + math.sin(i))))
#         elif pattern == "bursty":
#             if i % 20 < 3:
#                 offset = datetime.timedelta(seconds=i * interval_sec / 10)
#             else:
#                 offset = datetime.timedelta(seconds=i * interval_sec * 2)
#         elif pattern == "long_tail":
#             offset = datetime.timedelta(seconds=interval_sec * (1 + math.log1p(i)))
#         else:
#             offset = datetime.timedelta(seconds=i * interval_sec)
#         timestamps.append((base_ts + offset).isoformat(timespec="microseconds"))
#     return timestamps

def generate_timestamps_fixed_duration(
    count: int,
    start_time: str,
    pattern: str = "uniform",
    total_duration_sec: float = 300.0  # e.g., 10 minutes
) -> List[str]:
    base_ts = datetime.datetime.fromisoformat(start_time)

    # Generate unnormalized step weights
    raw_steps = []
    cumulative = 0.0
    timestamps = []
    periodic_query_num = 100
    trend_time_gap = 10 # second

    step = total_duration_sec / count

    for i in range(count):
        if pattern == "uniform":
            cumulative += step
            # raw_steps.append(1.0)
            ts = base_ts + datetime.timedelta(seconds=cumulative)
            timestamps.append(ts.isoformat(timespec="microseconds"))
        elif pattern == "periodic":
        
            if i % periodic_query_num == 0:
                # cumulative = step * periodic_query_num * (i / periodic_query_num)
                cumulative = step * i

                for _ in range(periodic_query_num):
                    ts = base_ts + datetime.timedelta(seconds=cumulative)
                    timestamps.append(ts.isoformat(timespec="microseconds"))
            # raw_steps.append(1.0 + 0.5 * math.sin(i / 5))
        # elif pattern == "random":
            
        #     cumulative = random.uniform(0, total_duration_sec)
        #     ts = base_ts + datetime.timedelta(seconds=cumulative)
        #     timestamps.append(ts.isoformat(timespec="microseconds"))
        # elif pattern == "long_tail":
        #     raw_steps.append(1.0 + math.log1p(i + 1))
        # else:
        #     cumulative += step
        #     # raw_steps.append(1.0)
        #     ts = base_ts + datetime.timedelta(seconds=cumulative)
        #     timestamps.append(ts.isoformat(timespec="microseconds"))

    if pattern == "trend":
        # timestamps.sort()
        # seconds_offsets = np.linspace(0, total_duration_sec, count)
        # np.random.shuffle(seconds_offsets)

        trends = total_duration_sec // trend_time_gap
        last = count * 2 // trends
        increment = last // trends
        current_num = 0
        for _ in range(trends):
            for i in range(current_num):
                ts = base_ts + datetime.timedelta(seconds=cumulative)
                timestamps.append(ts.isoformat(timespec="microseconds"))
            current_num += increment
            cumulative += trend_time_gap

    if pattern == "long_tail":

        raw = np.log1p(np.arange(1, count + 1))  # log(1), log(2), ..., log(N)
        raw = raw[::-1]


        norm = (raw - raw.min()) / (raw.max() - raw.min())
        
        seconds_offsets = norm * total_duration_sec

        timestamps = [(base_ts + datetime.timedelta(seconds=float(total_duration_sec - s))).isoformat(timespec="microseconds")
                    for s in seconds_offsets]
    # if pattern == "periodic":
        # print(timestamps)
    
    return timestamps


def generate_timestamps(
    count: int,
    start_time: str,
    pattern: str = "uniform",
    queries_per_minute: int = 60
) -> List[str]:
    interval_sec = 60.0 / queries_per_minute
    base_ts = datetime.datetime.fromisoformat(start_time)
    timestamps = []

    # Step sizes
    steps = []

    for i in range(count):
        if pattern == "uniform":
            steps.append(interval_sec)
        elif pattern == "periodic":
            steps.append(interval_sec * (1 + 0.5 * math.sin(i / 5)))  # smoother wave
        elif pattern == "bursty":
            # Simulate burst every 20 queries
            if i % 20 < 3:
                steps.append(interval_sec / 10)
            else:
                steps.append(interval_sec * 2)
        elif pattern == "long_tail":
            steps.append(interval_sec * (1 + math.log1p(i + 1)))
        else:
            steps.append(interval_sec)

    # Cumulative offset
    cumulative = 0.0
    for step in steps:
        cumulative += step
        ts = base_ts + datetime.timedelta(seconds=cumulative)
        timestamps.append(ts.isoformat(timespec="microseconds"))

    return timestamps



def generate_sql_queries(
    template_file: str,
    dist_config: Optional[Dict] = None,
    seed: int = 42,
    queries_per_template: int = 10,
    timestamp_start: Optional[str] = "2023-01-01T00:00:00",
    timestamp_pattern: str = "uniform",
    total_duration_sec: float = 60
) -> List[Dict[str, str]]:
    """
    Load templates and generate SQL queries with associated timestamps.

    timestamp_pattern: one of ["uniform", "periodic", "trend", "long_tail"]
    """
    random.seed(seed)
    sampler = PredicateValueSampler(dist_config=dist_config, seed=seed)
    renderer = PostgreSQLRenderer(sampler)

    # Load template file
    with open(template_file) as f:
        templates = json.load(f)

    sqls = []

    for template in templates:
        for _ in range(queries_per_template):
            sql = renderer.render(template)
            sqls.append(sql)
    random.shuffle(sqls)

    timestamps = generate_timestamps_fixed_duration(
        count=len(sqls),
        start_time=timestamp_start,
        pattern=timestamp_pattern,
        total_duration_sec=total_duration_sec
    )

    return timestamps, sqls


def save_queries_to_csv(timestamps: List[str], sqls: List[str], output_file: str):
    queries = list(zip(timestamps, sqls))
    Path(output_file).parent.mkdir(parents=True, exist_ok=True)
    with open(output_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["timestamp", "sql"])
        writer.writerows(queries)


def save_queries_to_json(timestamps: List[str], sqls: List[str], output_file: str):
    queries = [
        {"timestamp": ts, "sql": sql}
        for ts, sql in zip(timestamps, sqls)
    ]
    Path(output_file).parent.mkdir(parents=True, exist_ok=True)
    with open(output_file, "w") as f:
        json.dump(queries, f, indent=2)
