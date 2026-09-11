#!/usr/bin/env python3
import argparse
import struct
import sys


def main():
    parser = argparse.ArgumentParser(description="Convert binary keys to single-column CSV.")
    parser.add_argument("--input", required=True, help="Input binary file.")
    parser.add_argument("--output", required=True, help="Output CSV file.")
    parser.add_argument("--type", choices=["u64", "i64"], default="u64")
    parser.add_argument("--input-has-size", action="store_true")
    parser.add_argument("--limit", type=int, default=0, help="Optional max rows (0=all).")
    parser.add_argument("--column", default="key", help="CSV column name.")
    args = parser.parse_args()

    fmt = "<Q" if args.type == "u64" else "<q"
    max_rows = args.limit if args.limit and args.limit > 0 else None

    with open(args.input, "rb") as handle, open(args.output, "w", encoding="utf-8") as out:
        if args.input_has_size:
            header = handle.read(8)
            if len(header) != 8:
                raise ValueError("input size header missing")
        out.write(f"{args.column}\n")
        count = 0
        chunk_size = 8 * 8192
        while True:
            chunk = handle.read(chunk_size)
            if not chunk:
                break
            for (value,) in struct.iter_unpack(fmt, chunk):
                out.write(f"{value}\n")
                count += 1
                if max_rows is not None and count >= max_rows:
                    print(f"[ok] wrote {count} rows to {args.output}")
                    return
    print(f"[ok] wrote {count} rows to {args.output}")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"[error] {exc}", file=sys.stderr)
        sys.exit(1)
