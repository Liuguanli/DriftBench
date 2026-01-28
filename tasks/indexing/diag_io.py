#!/usr/bin/env python3
import argparse
import random
import struct
import sys


def iter_values(path, has_size, fmt):
    with open(path, "rb") as handle:
        if has_size:
            header = handle.read(8)
            if len(header) != 8:
                raise ValueError("missing size header")
        chunk_size = 8 * 8192
        while True:
            chunk = handle.read(chunk_size)
            if not chunk:
                break
            for (value,) in struct.iter_unpack(fmt, chunk):
                yield value


def reservoir_sample(iterable, k, rng):
    sample = []
    count = 0
    min_val = None
    max_val = None
    for value in iterable:
        count += 1
        if min_val is None or value < min_val:
            min_val = value
        if max_val is None or value > max_val:
            max_val = value
        if len(sample) < k:
            sample.append(value)
        else:
            j = rng.randrange(count)
            if j < k:
                sample[j] = value
    return count, min_val, max_val, sample


def load_all(path, has_size, fmt):
    return list(iter_values(path, has_size, fmt))


def main():
    parser = argparse.ArgumentParser(description="Diagnose data/query binary files.")
    parser.add_argument("--data", required=True)
    parser.add_argument("--data-has-size", type=int, default=0)
    parser.add_argument("--query", default="")
    parser.add_argument("--query-has-size", type=int, default=0)
    parser.add_argument("--type", choices=["u64", "i64"], default="u64")
    parser.add_argument("--sample-size", type=int, default=200000)
    parser.add_argument("--op-type", default="")
    parser.add_argument("--r-size", type=int, default=0)
    args = parser.parse_args()

    fmt = "<Q" if args.type == "u64" else "<q"
    rng = random.Random(42)

    data_iter = iter_values(args.data, args.data_has_size == 1, fmt)
    count, min_val, max_val, sample = reservoir_sample(data_iter, args.sample_size, rng)
    if count == 0:
        print("[diag][error] empty data file")
        return 0

    sample_unique_ratio = len(set(sample)) / len(sample) if sample else 0.0
    print(f"[diag] data_count: {count}")
    print(f"[diag] data_min: {min_val}")
    print(f"[diag] data_max: {max_val}")
    print(f"[diag] data_sample_size: {len(sample)}")
    print(f"[diag] data_sample_unique_ratio: {sample_unique_ratio:.6f}")
    if sample_unique_ratio < 0.9:
        print("[diag][warn] data has many duplicates (sample_unique_ratio < 0.9)")

    if not args.query:
        return 0

    queries = load_all(args.query, args.query_has_size == 1, fmt)
    if not queries:
        print("[diag][warn] empty query file")
        return 0

    q_min = min(queries)
    q_max = max(queries)
    q_unique_ratio = len(set(queries)) / len(queries)
    in_range = sum(1 for q in queries if min_val <= q <= max_val)
    in_range_ratio = in_range / len(queries)

    data_sample_set = set(sample)
    hit_sample = sum(1 for q in queries if q in data_sample_set)
    hit_sample_ratio = hit_sample / len(queries)

    print(f"[diag] query_count: {len(queries)}")
    print(f"[diag] query_min: {q_min}")
    print(f"[diag] query_max: {q_max}")
    print(f"[diag] query_unique_ratio: {q_unique_ratio:.6f}")
    print(f"[diag] query_in_data_range_ratio: {in_range_ratio:.6f}")
    print(f"[diag] query_hit_sample_ratio: {hit_sample_ratio:.6f}")
    if args.op_type:
        print(f"[diag] op_type: {args.op_type}")
    if args.r_size:
        print(f"[diag] r_size: {args.r_size}")
        if args.r_size >= count:
            print("[diag][warn] r_size >= data_count; scans will be degenerate")

    if in_range_ratio < 1.0:
        print("[diag][warn] queries outside data min/max range")

    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        print(f"[diag][error] {exc}", file=sys.stderr)
        sys.exit(0)
