import datetime
import math
import numpy as np
from typing import List


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


class TimestampGenerator:
    def __init__(self, start_time: str):
        self.base_ts = datetime.datetime.fromisoformat(start_time)

    def generate_fixed_duration(self, count: int, pattern: str = "uniform", total_duration_sec: float = 300.0) -> List[str]:
        timestamps = []
        cumulative = 0.0
        step = total_duration_sec / count
        periodic_query_num = 100
        trend_time_gap = 10

        if pattern == "uniform":
            for i in range(count):
                cumulative += step
                ts = self.base_ts + datetime.timedelta(seconds=cumulative)
                timestamps.append(ts.isoformat(timespec="microseconds"))

        elif pattern == "periodic":
            for i in range(count):
                if i % periodic_query_num == 0:
                    cumulative = step * i
                    for _ in range(periodic_query_num):
                        ts = self.base_ts + datetime.timedelta(seconds=cumulative)
                        timestamps.append(ts.isoformat(timespec="microseconds"))

        elif pattern == "trend":
            trends = total_duration_sec // trend_time_gap
            last = count * 2 // trends
            increment = last // trends
            current_num = 0
            for _ in range(int(trends)):
                for _ in range(current_num):
                    ts = self.base_ts + datetime.timedelta(seconds=cumulative)
                    timestamps.append(ts.isoformat(timespec="microseconds"))
                current_num += increment
                cumulative += trend_time_gap

        elif pattern == "long_tail":
            raw = np.log1p(np.arange(1, count + 1))[::-1]
            norm = (raw - raw.min()) / (raw.max() - raw.min())
            seconds_offsets = norm * total_duration_sec
            timestamps = [
                (self.base_ts + datetime.timedelta(seconds=float(total_duration_sec - s))).isoformat(timespec="microseconds")
                for s in seconds_offsets
            ]

        return timestamps

    def generate_by_rate(self, count: int, pattern: str = "uniform", queries_per_minute: int = 60) -> List[str]:
        interval_sec = 60.0 / queries_per_minute
        timestamps = []
        steps = []

        for i in range(count):
            if pattern == "uniform":
                steps.append(interval_sec)
            elif pattern == "periodic":
                steps.append(interval_sec * (1 + 0.5 * math.sin(i / 5)))
            elif pattern == "bursty":
                if i % 20 < 3:
                    steps.append(interval_sec / 10)
                else:
                    steps.append(interval_sec * 2)
            elif pattern == "long_tail":
                steps.append(interval_sec * (1 + math.log1p(i + 1)))
            else:
                steps.append(interval_sec)

        cumulative = 0.0
        for step in steps:
            cumulative += step
            ts = self.base_ts + datetime.timedelta(seconds=cumulative)
            timestamps.append(ts.isoformat(timespec="microseconds"))

        return timestamps
