#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SPEC="${SPEC:-$ROOT/specs/indexing_query_drift.yaml}"

DATA_FILE="${DATA_FILE:-$ROOT/data/keys_u64.bin}"
CSV_FILE="${CSV_FILE:-$ROOT/data/keys_u64.csv}"
TYPE_FORMAT="${TYPE_FORMAT:-u64}"
INPUT_HAS_SIZE="${INPUT_HAS_SIZE:-0}"
KEY_LIMIT="${KEY_LIMIT:-0}"
FORCE="${FORCE:-0}"

if [[ "$FORCE" == "1" || ! -f "$CSV_FILE" || "$DATA_FILE" -nt "$CSV_FILE" ]]; then
  echo "[step] convert binary to CSV: $CSV_FILE"
  BIN_ARGS=(--input "$DATA_FILE" --output "$CSV_FILE" --type "$TYPE_FORMAT" --column key)
  if [[ "$INPUT_HAS_SIZE" == "1" ]]; then
    BIN_ARGS+=(--input-has-size)
  fi
  if [[ "$KEY_LIMIT" -gt 0 ]]; then
    BIN_ARGS+=(--limit "$KEY_LIMIT")
  fi
  python3 "$ROOT/bin_to_csv.py" "${BIN_ARGS[@]}"
fi

echo "[step] run query spec: $SPEC"
python3 -m driftbench.cli run-yaml "$SPEC"

echo "[done] queries in $ROOT/queries"
