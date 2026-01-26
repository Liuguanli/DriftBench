#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

DATA_DIR="${DATA_DIR:-$ROOT/data}"
DRIFT_DIR="${DRIFT_DIR:-$ROOT/data/drift}"
QUERY_DIR="${QUERY_DIR:-$ROOT/queries}"
LOG_DIR="${LOG_DIR:-$ROOT/logs}"

QUERY_COUNT="${QUERY_COUNT:-10000}"
SEARCH_COUNT="${SEARCH_COUNT:-$QUERY_COUNT}"
SEED="${SEED:-42}"
HAS_SIZE="${HAS_SIZE:-0}"
QUERY_HAS_SIZE="${QUERY_HAS_SIZE:-0}"

BASE_DATA_TAG="${BASE_DATA_TAG:-keys_u64}"
BASE_RANGE_SIZE="${BASE_RANGE_SIZE:-100}"

ZIPF_ALPHA="${ZIPF_ALPHA:-}"
ZIPF_ALPHAS="${ZIPF_ALPHAS:-}"
if [[ -z "$ZIPF_ALPHAS" ]]; then
  if [[ -n "$ZIPF_ALPHA" ]]; then
    ZIPF_ALPHAS="$ZIPF_ALPHA"
  else
    ZIPF_ALPHAS="2 3 4 5"
  fi
fi
HOTSPOT_FRAC="${HOTSPOT_FRAC:-0.01}"
R_SIZES="${R_SIZES:-100 200 400 800 1600 3200 6400 12800 25600 51200 102400}"

REGEN_QUERIES="${REGEN_QUERIES:-1}"
RUN_INDEXES="${RUN_INDEXES:-1}"
RUN_PGM="${RUN_PGM:-0}"
INCLUDE_BASE="${INCLUDE_BASE:-1}"
INCLUDE_DRIFT="${INCLUDE_DRIFT:-1}"
COUNT_OVERRIDE="${COUNT_OVERRIDE:-}"

mkdir -p "$QUERY_DIR" "$LOG_DIR"

data_files=()
if [[ "$INCLUDE_BASE" == "1" && -d "$DATA_DIR" ]]; then
  while IFS= read -r -d '' file; do
    data_files+=("$file")
  done < <(find "$DATA_DIR" -maxdepth 1 -type f -name "*.bin" -print0)
fi

if [[ "$INCLUDE_DRIFT" == "1" && -d "$DRIFT_DIR" ]]; then
  while IFS= read -r -d '' file; do
    data_files+=("$file")
  done < <(find "$DRIFT_DIR" -type f -name "*.bin" -print0)
fi

if [[ ${#data_files[@]} -eq 0 ]]; then
  echo "[error] no data files found under $DATA_DIR or $DRIFT_DIR" >&2
  exit 1
fi

calc_count() {
  local file="$1"
  if [[ -n "$COUNT_OVERRIDE" ]]; then
    echo "$COUNT_OVERRIDE"
    return
  fi
  if [[ "$HAS_SIZE" == "1" ]]; then
    python3 - <<PY
import struct
with open("${file}", "rb") as f:
    raw = f.read(8)
    if len(raw) != 8:
        raise SystemExit(0)
    print(struct.unpack("<Q", raw)[0])
PY
  else
    python3 - <<PY
import os
size = os.path.getsize("${file}")
print(size // 8)
PY
  fi
}

gen_point_queries() {
  local data_file="$1"
  local out_dir="$2"
  local hotspot_tag
  hotspot_tag="$(echo "$HOTSPOT_FRAC" | tr '.' '_')"
  python3 "$ROOT/generate_queries.py" \
    --input "$data_file" \
    --output "$out_dir/point_uniform_${QUERY_COUNT}.bin" \
    --count "$QUERY_COUNT" --type point --distribution uniform --seed "$SEED"
  for alpha in $ZIPF_ALPHAS; do
    local zipf_tag
    zipf_tag="$(echo "$alpha" | tr '.' '_')"
    python3 "$ROOT/generate_queries.py" \
      --input "$data_file" \
      --output "$out_dir/point_zipf_a${zipf_tag}_${QUERY_COUNT}.bin" \
      --count "$QUERY_COUNT" --type point --distribution zipf --zipf-alpha "$alpha" --seed "$SEED"
  done
  python3 "$ROOT/generate_queries.py" \
    --input "$data_file" \
    --output "$out_dir/point_hotspot_${hotspot_tag}_${QUERY_COUNT}.bin" \
    --count "$QUERY_COUNT" --type point --distribution hotspot --hotspot-frac "$HOTSPOT_FRAC" --seed "$SEED"
}

gen_point_queries_min() {
  local data_file="$1"
  local out_dir="$2"
  python3 "$ROOT/generate_queries.py" \
    --input "$data_file" \
    --output "$out_dir/point_uniform_${QUERY_COUNT}.bin" \
    --count "$QUERY_COUNT" --type point --distribution uniform --seed "$SEED"
}

gen_range_queries() {
  local data_file="$1"
  local out_dir="$2"
  for r in $R_SIZES; do
    python3 "$ROOT/generate_queries.py" \
      --input "$data_file" \
      --output "$out_dir/range_uniform_r${r}_${QUERY_COUNT}.bin" \
      --count "$QUERY_COUNT" --type range --distribution uniform --r-size "$r" --sort-keys --seed "$SEED"
  done
}

gen_range_queries_min() {
  local data_file="$1"
  local out_dir="$2"
  python3 "$ROOT/generate_queries.py" \
    --input "$data_file" \
    --output "$out_dir/range_uniform_r${BASE_RANGE_SIZE}_${QUERY_COUNT}.bin" \
    --count "$QUERY_COUNT" --type range --distribution uniform --r-size "$BASE_RANGE_SIZE" --sort-keys --seed "$SEED"
}

build_done=0
for data_file in "${data_files[@]}"; do
  tag="$(basename "$data_file" .bin)"
  out_dir="$QUERY_DIR/$tag"
  mkdir -p "$out_dir"
  is_base=0
  if [[ "$tag" == "$BASE_DATA_TAG" ]]; then
    is_base=1
  fi

  count="$(calc_count "$data_file")"
  if [[ -z "$count" || "$count" == "0" ]]; then
    echo "[warn] skip $data_file (count=0)"
    continue
  fi

  if [[ "$REGEN_QUERIES" == "1" ]]; then
    echo "[step] generate queries for $tag"
    if [[ "$is_base" == "1" ]]; then
      gen_point_queries "$data_file" "$out_dir"
      gen_range_queries "$data_file" "$out_dir"
    else
      gen_point_queries_min "$data_file" "$out_dir"
      gen_range_queries_min "$data_file" "$out_dir"
    fi
  fi

  if [[ "$RUN_INDEXES" != "1" ]]; then
    continue
  fi

  if [[ "$build_done" == "0" ]]; then
    run_build=1
    build_done=1
  else
    run_build=0
  fi

  log_tag="run_$(date +%Y%m%d_%H%M%S)_${RANDOM}_${tag}_bulk_only"
  RUN_PGM="$RUN_PGM" RUN_BUILD="$run_build" RUN_BULK=1 RUN_LOOKUP=0 RUN_STRICT=0 \
  DATA_FILE="$data_file" COUNT="$count" HAS_SIZE="$HAS_SIZE" \
  LOG_TAG="$log_tag" \
  bash "$ROOT/run_learned_index.sh"

  hotspot_tag="$(echo "$HOTSPOT_FRAC" | tr '.' '_')"
  point_workloads=("point_uniform_${QUERY_COUNT}")
  range_sizes=("$BASE_RANGE_SIZE")
  if [[ "$is_base" == "1" ]]; then
    for alpha in $ZIPF_ALPHAS; do
      zipf_tag="$(echo "$alpha" | tr '.' '_')"
      point_workloads+=("point_zipf_a${zipf_tag}_${QUERY_COUNT}")
    done
    point_workloads+=("point_hotspot_${hotspot_tag}_${QUERY_COUNT}")
    range_sizes=($R_SIZES)
  fi

  for point_name in "${point_workloads[@]}"; do
    query_file="$out_dir/${point_name}.bin"
    log_tag="run_$(date +%Y%m%d_%H%M%S)_${RANDOM}_${tag}_${point_name}"
    if [[ ! -f "$query_file" ]]; then
      echo "[error] missing query_file: $query_file" >&2
      exit 1
    fi
    RUN_PGM="$RUN_PGM" RUN_BUILD="$run_build" RUN_BULK=0 RUN_LOOKUP=1 RUN_STRICT=0 \
    DATA_FILE="$data_file" QUERY_FILE="$query_file" \
    COUNT="$count" SEARCH_COUNT="$SEARCH_COUNT" \
    HAS_SIZE="$HAS_SIZE" QUERY_HAS_SIZE="$QUERY_HAS_SIZE" \
    OP_TYPE=lookup R_SIZE=0 LOG_TAG="$log_tag" \
    bash "$ROOT/run_learned_index.sh"
  done

  for r in "${range_sizes[@]}"; do
    range_name="range_uniform_r${r}_${QUERY_COUNT}"
    query_file="$out_dir/${range_name}.bin"
    log_tag="run_$(date +%Y%m%d_%H%M%S)_${RANDOM}_${tag}_${range_name}"
    if [[ ! -f "$query_file" ]]; then
      echo "[error] missing query_file: $query_file" >&2
      exit 1
    fi
    RUN_PGM="$RUN_PGM" RUN_BUILD="$run_build" RUN_BULK=0 RUN_LOOKUP=1 RUN_STRICT=0 \
    DATA_FILE="$data_file" QUERY_FILE="$query_file" \
    COUNT="$count" SEARCH_COUNT="$SEARCH_COUNT" \
    HAS_SIZE="$HAS_SIZE" QUERY_HAS_SIZE="$QUERY_HAS_SIZE" \
    OP_TYPE=scan R_SIZE="$r" LOG_TAG="$log_tag" \
    bash "$ROOT/run_learned_index.sh"
  done
done
