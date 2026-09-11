#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_FILE="${DATA_FILE:-$ROOT/data/keys_u64.bin}"
OUT_DIR="${OUT_DIR:-$ROOT/queries}"

COUNT="${COUNT:-200000}"
R_SIZE="${R_SIZE:-100}"
SEED="${SEED:-42}"
TYPE_FORMAT="${TYPE_FORMAT:-u64}"

mkdir -p "$OUT_DIR"

python3 "$ROOT/generate_queries.py" \
  --input "$DATA_FILE" \
  --output "$OUT_DIR/point_uniform.bin" \
  --count "$COUNT" \
  --type point \
  --distribution uniform \
  --seed "$SEED" \
  --type-format "$TYPE_FORMAT"

python3 "$ROOT/generate_queries.py" \
  --input "$DATA_FILE" \
  --output "$OUT_DIR/range_uniform.bin" \
  --count "$COUNT" \
  --type range \
  --distribution uniform \
  --r-size "$R_SIZE" \
  --seed "$SEED" \
  --type-format "$TYPE_FORMAT"

echo "[done] baseline queries in $OUT_DIR"
