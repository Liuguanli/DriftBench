"""Convert one canonical TPC-H lineitem.tbl into a new, headered CSV."""
from __future__ import annotations

import argparse
import csv
from pathlib import Path


LINEITEM_COLUMNS = (
    "l_orderkey", "l_partkey", "l_suppkey", "l_linenumber",
    "l_quantity", "l_extendedprice", "l_discount", "l_tax",
    "l_returnflag", "l_linestatus", "l_shipdate", "l_commitdate",
    "l_receiptdate", "l_shipinstruct", "l_shipmode", "l_comment",
)


def _rows(source: Path):
    with source.open(encoding="utf-8", newline="") as stream:
        for number, row in enumerate(csv.reader(stream, delimiter="|"), 1):
            if row and row[-1] == "":
                row.pop()  # dbgen's trailing delimiter
            if len(row) != len(LINEITEM_COLUMNS):
                raise ValueError(f"lineitem row {number} has {len(row)} fields; expected 16")
            yield row


def convert_lineitem(source: Path, destination: Path) -> int:
    if source.is_symlink() or not source.is_file():
        raise ValueError("--input must be a regular, non-symlink lineitem.tbl file")
    if destination.exists() or destination.is_symlink():
        raise ValueError("--output already exists; choose a new CSV path")
    count = sum(1 for _ in _rows(source))
    if count == 0:
        raise ValueError("lineitem input is empty")
    destination.parent.mkdir(parents=True, exist_ok=True)
    # Exclusive creation preserves any existing file, including a racing writer.
    created = False
    try:
        with destination.open("x", encoding="utf-8", newline="") as stream:
            created = True
            writer = csv.writer(stream)
            writer.writerow(LINEITEM_COLUMNS)
            writer.writerows(_rows(source))
    except BaseException:
        if created:
            destination.unlink(missing_ok=True)
        raise
    return count


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args(argv)
    try:
        count = convert_lineitem(args.input, args.output)
    except (OSError, ValueError) as exc:
        parser.exit(1, f"error: {exc}\n")
    print(f"Wrote {count} rows with 16 column headers to {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
