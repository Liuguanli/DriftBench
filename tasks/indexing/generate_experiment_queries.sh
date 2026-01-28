#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_FILE="${DATA_FILE:-$ROOT/data/keys_u64.bin}"
OUT_DIR="${OUT_DIR:-$ROOT/queries}"

COUNT="${COUNT:-10000}"
SEED="${SEED:-42}"
TYPE_FORMAT="${TYPE_FORMAT:-u64}"

mkdir -p "$OUT_DIR"

# Point queries
python3 "$ROOT/generate_queries.py" \
  --input "$DATA_FILE" \
  --output "$OUT_DIR/point_uniform_10k.bin" \
  --count "$COUNT" \
  --type point \
  --distribution uniform \
  --seed "$SEED" \
  --type-format "$TYPE_FORMAT"

python3 "$ROOT/generate_queries.py" \
  --input "$DATA_FILE" \
  --output "$OUT_DIR/point_zipf_a2_10k.bin" \
  --count "$COUNT" \
  --type point \
  --distribution zipf \
  --zipf-alpha 2 \
  --seed "$SEED" \
  --type-format "$TYPE_FORMAT"

python3 "$ROOT/generate_queries.py" \
  --input "$DATA_FILE" \
  --output "$OUT_DIR/point_hotspot_1pct_10k.bin" \
  --count "$COUNT" \
  --type point \
  --distribution hotspot \
  --hotspot-frac 0.01 \
  --seed "$SEED" \
  --type-format "$TYPE_FORMAT"

# Range queries (uniform start, different range sizes)
for r in 100 1000 10000; do
  python3 "$ROOT/generate_queries.py" \
    --input "$DATA_FILE" \
    --output "$OUT_DIR/range_uniform_r${r}_10k.bin" \
    --count "$COUNT" \
    --type range \
    --distribution uniform \
    --r-size "$r" \
    --seed "$SEED" \
    --type-format "$TYPE_FORMAT" \
    --sort-keys
done

echo "[done] experiment queries in $OUT_DIR"
