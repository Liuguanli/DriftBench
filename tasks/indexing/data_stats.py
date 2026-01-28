#!/usr/bin/env python3
import argparse
import csv
import os
from pathlib import Path

import numpy as np


def load_array(path, dtype, has_size):
    offset = 8 if has_size else 0
    return np.fromfile(path, dtype=dtype, offset=offset)


def sample_unique_ratio(data, sample_size, rng):
    if data.size == 0:
        return 0.0
    if data.size <= sample_size:
        sample = data
    else:
        idx = rng.choice(data.size, size=sample_size, replace=False)
        sample = data[idx]
    return float(np.unique(sample).size) / float(sample.size)


def quantiles(data, probs):
    if data.size == 0:
        return [None for _ in probs]
    return np.quantile(data, probs).tolist()


def histogram(data, bins):
    if data.size == 0:
        return [0 for _ in range(bins)]
    counts, _ = np.histogram(data, bins=bins)
    total = counts.sum()
    if total == 0:
        return [0 for _ in range(bins)]
    return (counts / total).tolist()


def main():
    parser = argparse.ArgumentParser(description="Compute basic stats for key datasets.")
    parser.add_argument("--files", nargs="+", required=True, help="Binary key files.")
    parser.add_argument("--has-size", type=int, default=0, help="1 if files have uint64 size header.")
    parser.add_argument("--type", choices=["u64", "i64"], default="u64")
    parser.add_argument("--sample-size", type=int, default=200000)
    parser.add_argument("--exact-unique-max", type=int, default=5000000)
    parser.add_argument("--hist-bins", type=int, default=10)
    parser.add_argument("--out", default="", help="Optional CSV output path.")
    args = parser.parse_args()

    dtype = np.uint64 if args.type == "u64" else np.int64
    rng = np.random.default_rng(42)

    rows = []
    for file_path in args.files:
        path = Path(file_path)
        data = load_array(path, dtype, args.has_size == 1)
        count = int(data.size)
        if count == 0:
            row = {
                "file": str(path),
                "count": 0,
                "min": None,
                "max": None,
                "mean": None,
                "std": None,
                "unique_count": None,
                "unique_ratio": None,
                "sample_unique_ratio": 0.0,
                "p01": None,
                "p10": None,
                "p50": None,
                "p90": None,
                "p99": None,
                "hist": "",
            }
            rows.append(row)
            continue

        min_val = int(data.min())
        max_val = int(data.max())
        mean_val = float(data.mean())
        std_val = float(data.std())
        sample_ratio = sample_unique_ratio(data, args.sample_size, rng)

        if count <= args.exact_unique_max:
            uniq = int(np.unique(data).size)
            uniq_ratio = uniq / float(count)
        else:
            uniq = None
            uniq_ratio = None

        p01, p10, p50, p90, p99 = quantiles(data, [0.01, 0.10, 0.50, 0.90, 0.99])
        hist = histogram(data, args.hist_bins)
        hist_str = ";".join(f"{v:.4f}" for v in hist)

        row = {
            "file": str(path),
            "count": count,
            "min": min_val,
            "max": max_val,
            "mean": f"{mean_val:.3f}",
            "std": f"{std_val:.3f}",
            "unique_count": uniq,
            "unique_ratio": f"{uniq_ratio:.6f}" if uniq_ratio is not None else "",
            "sample_unique_ratio": f"{sample_ratio:.6f}",
            "p01": f"{p01:.3f}",
            "p10": f"{p10:.3f}",
            "p50": f"{p50:.3f}",
            "p90": f"{p90:.3f}",
            "p99": f"{p99:.3f}",
            "hist": hist_str,
        }
        rows.append(row)

    fieldnames = list(rows[0].keys()) if rows else []
    if args.out:
        out_path = Path(args.out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with open(out_path, "w", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
    else:
        writer = csv.DictWriter(os.sys.stdout, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


if __name__ == "__main__":
    main()
