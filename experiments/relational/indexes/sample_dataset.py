#!/usr/bin/env python3
import argparse
import os
import random
import struct
import sys


def iter_values(handle, fmt):
    chunk_size = 8 * 8192
    while True:
        chunk = handle.read(chunk_size)
        if not chunk:
            break
        for (value,) in struct.iter_unpack(fmt, chunk):
            yield value


def reservoir_sample(values, k, seed):
    rng = random.Random(seed)
    sample = []
    for i, v in enumerate(values):
        if i < k:
            sample.append(v)
        else:
            j = rng.randint(0, i)
            if j < k:
                sample[j] = v
    return sample


def main():
    parser = argparse.ArgumentParser(description="Sample uint64/int64 keys from a large binary file.")
    parser.add_argument("--input", required=True, help="Input binary file.")
    parser.add_argument("--output", required=True, help="Output binary file.")
    parser.add_argument("--count", type=int, required=True, help="Number of keys to sample.")
    parser.add_argument("--type", choices=["u64", "i64"], default="u64")
    parser.add_argument("--input-has-size", action="store_true", help="Input starts with uint64 size header.")
    parser.add_argument("--output-has-size", action="store_true", help="Write uint64 size header.")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--sort", action="store_true", help="Sort sampled keys.")
    parser.add_argument("--dedupe", action="store_true", help="Drop duplicate keys after sampling.")
    args = parser.parse_args()

    if args.count <= 0:
        raise ValueError("count must be > 0")

    fmt = "<Q" if args.type == "u64" else "<q"
    total_count = None

    with open(args.input, "rb") as handle:
        if args.input_has_size:
            header = handle.read(8)
            if len(header) != 8:
                raise ValueError("input size header missing")
            total_count = struct.unpack("<Q", header)[0]
        values = iter_values(handle, fmt)
        sample = reservoir_sample(values, args.count, args.seed)

    if total_count is not None and total_count < args.count:
        print(f"[warn] input has {total_count} keys, sampled {len(sample)}", file=sys.stderr)

    if args.dedupe:
        sample = list(dict.fromkeys(sample))
    if args.sort:
        sample.sort()

    os.makedirs(os.path.dirname(args.output) or ".", exist_ok=True)
    with open(args.output, "wb") as handle:
        if args.output_has_size:
            handle.write(struct.pack("<Q", len(sample)))
        for v in sample:
            if args.type == "u64" and v < 0:
                raise ValueError("u64 output cannot contain negative values")
            handle.write(struct.pack(fmt, v))

    print(f"[ok] wrote {len(sample)} keys to {args.output}")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"[error] {exc}", file=sys.stderr)
        sys.exit(1)
