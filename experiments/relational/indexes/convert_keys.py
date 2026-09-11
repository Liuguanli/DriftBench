#!/usr/bin/env python3
import argparse
import csv
import struct
import sys


def _parse_column(value):
    try:
        return int(value)
    except ValueError:
        return value


def _parse_int(value, allow_float):
    text = value.strip()
    if text == "":
        raise ValueError("empty value")
    try:
        return int(text)
    except ValueError:
        if not allow_float:
            raise
        return int(float(text))


def _read_text(path, args):
    values = []
    with open(path, "r", newline="") as handle:
        for line in handle:
            text = line.strip()
            if not text:
                continue
            try:
                values.append(_parse_int(text, args.allow_float))
            except ValueError:
                if args.skip_invalid:
                    continue
                raise
    return values


def _read_csv(path, args):
    values = []
    column = _parse_column(args.column)
    with open(path, "r", newline="") as handle:
        if isinstance(column, int):
            reader = csv.reader(handle, delimiter=args.delimiter)
            if args.has_header:
                next(reader, None)
            for row in reader:
                if not row or column >= len(row):
                    if args.skip_invalid:
                        continue
                    raise ValueError("column index out of range")
                try:
                    values.append(_parse_int(row[column], args.allow_float))
                except ValueError:
                    if args.skip_invalid:
                        continue
                    raise
        else:
            reader = csv.DictReader(handle, delimiter=args.delimiter)
            if not reader.fieldnames or column not in reader.fieldnames:
                raise ValueError(f"column '{column}' not found in header")
            for row in reader:
                try:
                    values.append(_parse_int(row[column], args.allow_float))
                except ValueError:
                    if args.skip_invalid:
                        continue
                    raise
    return values


def _dedupe(values):
    seen = set()
    out = []
    for v in values:
        if v in seen:
            continue
        seen.add(v)
        out.append(v)
    return out


def main():
    parser = argparse.ArgumentParser(description="Convert CSV/text keys to binary u64/i64.")
    parser.add_argument("--input", required=True, help="Input CSV or text file.")
    parser.add_argument("--output", required=True, help="Output binary file.")
    parser.add_argument("--format", choices=["csv", "text"], default="csv")
    parser.add_argument("--column", default="0", help="CSV column name or 0-based index.")
    parser.add_argument("--delimiter", default=",", help="CSV delimiter.")
    parser.add_argument("--has-header", action="store_true", help="Skip CSV header row for index column.")
    parser.add_argument("--has-size", action="store_true", help="Write uint64 size header.")
    parser.add_argument("--type", choices=["u64", "i64"], default="u64")
    parser.add_argument("--sort", action="store_true", help="Sort keys before writing.")
    parser.add_argument("--dedupe", action="store_true", help="Drop duplicate keys.")
    parser.add_argument("--limit", type=int, default=0, help="Limit number of keys (0=all).")
    parser.add_argument("--allow-float", action="store_true", help="Allow float values and cast to int.")
    parser.add_argument("--clip-u64", action="store_true", help="Clamp values to [0, 2^64-1] for u64.")
    parser.add_argument("--skip-invalid", action="store_true", help="Skip rows with invalid values.")
    args = parser.parse_args()

    if args.format == "text":
        values = _read_text(args.input, args)
    else:
        values = _read_csv(args.input, args)

    if args.dedupe:
        values = _dedupe(values)
    if args.sort:
        values.sort()
    if args.limit and args.limit > 0:
        values = values[: args.limit]

    if args.type == "u64":
        max_u64 = (1 << 64) - 1
        clipped = 0
        out_values = []
        for v in values:
            if args.clip_u64:
                if v < 0:
                    v = 0
                    clipped += 1
                elif v > max_u64:
                    v = max_u64
                    clipped += 1
            else:
                if v < 0:
                    raise ValueError("u64 output cannot contain negative values")
                if v > max_u64:
                    raise ValueError("u64 output cannot exceed 2^64-1")
            out_values.append(v)
        values = out_values
        if clipped:
            print(f"[warn] clipped {clipped} values to u64 range")
        fmt = "<Q"
    else:
        fmt = "<q"

    with open(args.output, "wb") as handle:
        if args.has_size:
            handle.write(struct.pack("<Q", len(values)))
        for v in values:
            handle.write(struct.pack(fmt, v))

    print(f"[ok] wrote {len(values)} keys to {args.output}")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"[error] {exc}", file=sys.stderr)
        sys.exit(1)
