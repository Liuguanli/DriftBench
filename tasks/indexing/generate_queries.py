#!/usr/bin/env python3
import argparse
import bisect
import random
import struct
import sys


def load_keys(path, has_size, fmt):
    keys = []
    with open(path, "rb") as handle:
        if has_size:
            header = handle.read(8)
            if len(header) != 8:
                raise ValueError("input size header missing")
            _ = struct.unpack("<Q", header)[0]
        chunk_size = 8 * 8192
        while True:
            chunk = handle.read(chunk_size)
            if not chunk:
                break
            for (value,) in struct.iter_unpack(fmt, chunk):
                keys.append(value)
    if not keys:
        raise ValueError("no keys loaded")
    return keys


def build_zipf_cdf(n, alpha):
    weights = [1.0 / ((i + 1) ** alpha) for i in range(n)]
    total = 0.0
    cdf = []
    for w in weights:
        total += w
        cdf.append(total)
    return cdf, total


def sample_indices_uniform(rng, n, count):
    return [rng.randrange(n) for _ in range(count)]


def sample_indices_hotspot(rng, n, count, hotspot_frac):
    hotspot_size = max(1, int(n * hotspot_frac))
    return [rng.randrange(hotspot_size) for _ in range(count)]


def sample_indices_zipf(rng, n, count, alpha):
    cdf, total = build_zipf_cdf(n, alpha)
    out = []
    for _ in range(count):
        r = rng.random() * total
        idx = bisect.bisect_left(cdf, r)
        out.append(min(idx, n - 1))
    return out


def main():
    parser = argparse.ArgumentParser(description="Generate point/range query keys from a binary dataset.")
    parser.add_argument("--input", required=True, help="Input binary key file.")
    parser.add_argument("--output", required=True, help="Output binary query file.")
    parser.add_argument("--count", type=int, required=True, help="Number of queries to generate.")
    parser.add_argument("--type", choices=["point", "range"], default="point")
    parser.add_argument("--distribution", choices=["uniform", "zipf", "hotspot"], default="uniform")
    parser.add_argument("--zipf-alpha", type=float, default=1.2)
    parser.add_argument("--hotspot-frac", type=float, default=0.1)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--input-has-size", action="store_true")
    parser.add_argument("--output-has-size", action="store_true")
    parser.add_argument("--type-format", choices=["u64", "i64"], default="u64")
    parser.add_argument("--sort-keys", action="store_true", help="Sort input keys before sampling.")
    parser.add_argument("--r-size", type=int, default=0, help="Range size hint for range queries.")
    args = parser.parse_args()

    if args.count <= 0:
        raise ValueError("count must be > 0")
    if args.distribution == "zipf" and args.zipf_alpha <= 1.0:
        raise ValueError("zipf-alpha must be > 1.0")

    fmt = "<Q" if args.type_format == "u64" else "<q"
    keys = load_keys(args.input, args.input_has_size, fmt)
    if args.sort_keys:
        keys.sort()

    n = len(keys)
    max_index = n - 1
    if args.type == "range" and args.r_size > 0 and n > args.r_size:
        max_index = max(0, n - args.r_size - 1)

    rng = random.Random(args.seed)
    if args.distribution == "uniform":
        indices = sample_indices_uniform(rng, max_index + 1, args.count)
    elif args.distribution == "hotspot":
        indices = sample_indices_hotspot(rng, max_index + 1, args.count, args.hotspot_frac)
    else:
        indices = sample_indices_zipf(rng, max_index + 1, args.count, args.zipf_alpha)

    with open(args.output, "wb") as handle:
        if args.output_has_size:
            handle.write(struct.pack("<Q", len(indices)))
        for idx in indices:
            handle.write(struct.pack(fmt, keys[idx]))

    print(f"[ok] wrote {len(indices)} queries to {args.output}")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"[error] {exc}", file=sys.stderr)
        sys.exit(1)
