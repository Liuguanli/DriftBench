#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SPEC="${SPEC:-$ROOT/specs/indexing_data_drift.yaml}"

DATA_FILE="${DATA_FILE:-$ROOT/data/keys_u64.bin}"
CSV_FILE="${CSV_FILE:-$ROOT/data/keys_u64.csv}"
TYPE_FORMAT="${TYPE_FORMAT:-u64}"
INPUT_HAS_SIZE="${INPUT_HAS_SIZE:-0}"
ALLOW_FLOAT="${ALLOW_FLOAT:-1}"
CLIP_U64="${CLIP_U64:-1}"

echo "[step] convert binary to CSV: $CSV_FILE"
BIN_ARGS=(--input "$DATA_FILE" --output "$CSV_FILE" --type "$TYPE_FORMAT" --column key)
if [[ "$INPUT_HAS_SIZE" == "1" ]]; then
  BIN_ARGS+=(--input-has-size)
fi
python3 "$ROOT/bin_to_csv.py" "${BIN_ARGS[@]}"

echo "[step] run driftbench spec: $SPEC"
python3 -m driftbench.cli run-yaml "$SPEC"

if [[ -d "$ROOT/data/drift" ]]; then
  echo "[step] convert drift CSVs to binary"
  EXTRA_ARGS=()
  if [[ "$ALLOW_FLOAT" == "1" ]]; then
    EXTRA_ARGS+=(--allow-float)
  fi
  if [[ "$TYPE_FORMAT" == "u64" && "$CLIP_U64" == "1" ]]; then
    EXTRA_ARGS+=(--clip-u64)
  fi
  while IFS= read -r -d '' csv; do
    out_bin="${csv%.csv}.bin"
    python3 "$ROOT/convert_keys.py" \
      --input "$csv" \
      --output "$out_bin" \
      --format csv \
      --column key \
      --has-header \
      --type "$TYPE_FORMAT" \
      "${EXTRA_ARGS[@]}"
  done < <(find "$ROOT/data/drift" -name "*.csv" -print0)
fi

echo "[done] drift datasets in $ROOT/data/drift"
