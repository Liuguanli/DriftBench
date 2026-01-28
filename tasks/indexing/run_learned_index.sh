#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$ROOT/../.." && pwd)"
BENCH_ROOT="$REPO_ROOT/existing_benchmarks/LearnedIndexDiskExp"
CODE_DIR="$BENCH_ROOT/code"

DATA_DIR="${DATA_DIR:-$ROOT/data}"
QUERY_DIR="${QUERY_DIR:-$ROOT/queries}"
LOG_DIR="${LOG_DIR:-$ROOT/logs}"
OUT_DIR="${OUT_DIR:-$ROOT/out}"

mkdir -p "$DATA_DIR" "$QUERY_DIR" "$LOG_DIR" "$OUT_DIR"

COUNT="${COUNT:-100000}"
SEARCH_COUNT="${SEARCH_COUNT:-20000}"
HAS_SIZE="${HAS_SIZE:-0}"
QUERY_HAS_SIZE="${QUERY_HAS_SIZE:-0}"
OP_TYPE="${OP_TYPE:-lookup}"
CASE_ID="${CASE_ID:-1}"
R_SIZE="${R_SIZE:-100}"
TYPE_FORMAT="${TYPE_FORMAT:-u64}"
RUN_DIAG="${RUN_DIAG:-1}"
DIAG_SAMPLE_SIZE="${DIAG_SAMPLE_SIZE:-200000}"
RUN_STRICT="${RUN_STRICT:-1}"
ALEX_DEDUPE="${ALEX_DEDUPE:-0}"
PGM_DEDUPE="${PGM_DEDUPE:-1}"

DATA_FILE="${DATA_FILE:-$DATA_DIR/keys_u64.bin}"
QUERY_FILE="${QUERY_FILE:-$QUERY_DIR/queries_u64.bin}"

RUN_BUILD="${RUN_BUILD:-1}"
RUN_BULK="${RUN_BULK:-1}"
RUN_LOOKUP="${RUN_LOOKUP:-1}"
RUN_ALEX="${RUN_ALEX:-1}"
RUN_BPLUS="${RUN_BPLUS:-1}"
RUN_FITING="${RUN_FITING:-1}"
RUN_PGM="${RUN_PGM:-0}"

LOG_TAG="${LOG_TAG:-run_$(date +%Y%m%d_%H%M%S)}"

if [[ -f "$QUERY_FILE" ]]; then
  QUERY_FLAGS=(--query_file="$QUERY_FILE" --query_has_size="$QUERY_HAS_SIZE")
else
  QUERY_FLAGS=(--case_id="$CASE_ID")
fi

OP_FLAGS=(--op_type="$OP_TYPE" --search_count="$SEARCH_COUNT")
if [[ "$OP_TYPE" == "scan" || "$OP_TYPE" == "bulk_search_range" ]]; then
  OP_FLAGS+=(--r_size="$R_SIZE")
fi

run_cmd() {
  local log_file="$1"
  shift
  echo "[cmd] $*" | tee "$log_file"
  if [[ "$RUN_STRICT" == "1" ]]; then
    "$@" 2>&1 | tee -a "$log_file"
  else
    set +e
    "$@" 2>&1 | tee -a "$log_file"
    local status=${PIPESTATUS[0]}
    set -e
    if [[ $status -ne 0 ]]; then
      echo "[warn] command failed (exit=$status); continuing" | tee -a "$log_file"
      return 0
    fi
  fi
}

diag_data() {
  local log_file="$1"
  if [[ "$RUN_DIAG" != "1" ]]; then
    return
  fi
  python3 "$ROOT/diag_io.py" \
    --data "$DATA_FILE" \
    --data-has-size "$HAS_SIZE" \
    --type "$TYPE_FORMAT" \
    --sample-size "$DIAG_SAMPLE_SIZE" \
    2>&1 | tee -a "$log_file" || true
}

diag_data_query() {
  local log_file="$1"
  if [[ "$RUN_DIAG" != "1" ]]; then
    return
  fi
  python3 "$ROOT/diag_io.py" \
    --data "$DATA_FILE" \
    --data-has-size "$HAS_SIZE" \
    --query "$QUERY_FILE" \
    --query-has-size "$QUERY_HAS_SIZE" \
    --type "$TYPE_FORMAT" \
    --sample-size "$DIAG_SAMPLE_SIZE" \
    --op-type "$OP_TYPE" \
    --r-size "$R_SIZE" \
    2>&1 | tee -a "$log_file" || true
}

echo "[info] repo: $REPO_ROOT"
echo "[info] data: $DATA_FILE (has_size=$HAS_SIZE)"
echo "[info] query: $QUERY_FILE (has_size=$QUERY_HAS_SIZE)"
echo "[info] op_type: $OP_TYPE case_id: $CASE_ID r_size: $R_SIZE"

if [[ "$RUN_ALEX" == "1" ]]; then
  if [[ "$RUN_BUILD" == "1" ]]; then
    (cd "$CODE_DIR/ALEX" && ./build.sh)
  fi
  ALEX_BIN="$CODE_DIR/ALEX/build/benchmark"
  if [[ "$RUN_BULK" == "1" ]]; then
    diag_data "$LOG_DIR/${LOG_TAG}_alex_bulk.log"
    run_cmd "$LOG_DIR/${LOG_TAG}_alex_bulk.log" \
      "$ALEX_BIN" \
      --keys_file="$DATA_FILE" \
      --op_type=bulk \
      --index_file="$OUT_DIR/alex" \
      --total_count="$COUNT" \
      --has_size="$HAS_SIZE" \
      --dedupe="$ALEX_DEDUPE"
  fi
  if [[ "$RUN_LOOKUP" == "1" ]]; then
    diag_data_query "$LOG_DIR/${LOG_TAG}_alex_lookup.log"
    run_cmd "$LOG_DIR/${LOG_TAG}_alex_lookup.log" \
      "$ALEX_BIN" \
      --keys_file="$DATA_FILE" \
      "${OP_FLAGS[@]}" \
      --index_file="$OUT_DIR/alex" \
      --total_count="$COUNT" \
      --has_size="$HAS_SIZE" \
      --dedupe="$ALEX_DEDUPE" \
      "${QUERY_FLAGS[@]}"
  fi
fi

if [[ "$RUN_BPLUS" == "1" ]]; then
  if [[ "$RUN_BUILD" == "1" ]]; then
    (cd "$CODE_DIR/b+_tree" && g++ -O3 test2.cpp -std=c++17 -I./stx-btree-0.9/include -o test)
  fi
  BPLUS_BIN="$CODE_DIR/b+_tree/test"
  if [[ "$RUN_BULK" == "1" ]]; then
    diag_data "$LOG_DIR/${LOG_TAG}_bplus_bulk.log"
    run_cmd "$LOG_DIR/${LOG_TAG}_bplus_bulk.log" \
      "$BPLUS_BIN" \
      --keys_file="$DATA_FILE" \
      --op_type=bulk \
      --index_file="$OUT_DIR/bplus" \
      --total_count="$COUNT" \
      --has_size="$HAS_SIZE"
  fi
  if [[ "$RUN_LOOKUP" == "1" ]]; then
    diag_data_query "$LOG_DIR/${LOG_TAG}_bplus_lookup.log"
    run_cmd "$LOG_DIR/${LOG_TAG}_bplus_lookup.log" \
      "$BPLUS_BIN" \
      --keys_file="$DATA_FILE" \
      "${OP_FLAGS[@]}" \
      --index_file="$OUT_DIR/bplus" \
      --total_count="$COUNT" \
      --has_size="$HAS_SIZE" \
      "${QUERY_FLAGS[@]}"
  fi
fi

if [[ "$RUN_FITING" == "1" ]]; then
  if [[ "$RUN_BUILD" == "1" ]]; then
    (cd "$CODE_DIR/fiting_tree" && g++ -O3 test2.cpp -std=c++17 -I./stx-btree-0.9/include -o test)
  fi
  FIT_BIN="$CODE_DIR/fiting_tree/test"
  if [[ "$RUN_BULK" == "1" ]]; then
    diag_data "$LOG_DIR/${LOG_TAG}_fiting_bulk.log"
    run_cmd "$LOG_DIR/${LOG_TAG}_fiting_bulk.log" \
      "$FIT_BIN" \
      --keys_file="$DATA_FILE" \
      --op_type=bulk \
      --index_file="$OUT_DIR/fiting" \
      --total_count="$COUNT" \
      --has_size="$HAS_SIZE"
  fi
  if [[ "$RUN_LOOKUP" == "1" ]]; then
    diag_data_query "$LOG_DIR/${LOG_TAG}_fiting_lookup.log"
    run_cmd "$LOG_DIR/${LOG_TAG}_fiting_lookup.log" \
      "$FIT_BIN" \
      --keys_file="$DATA_FILE" \
      "${OP_FLAGS[@]}" \
      --index_file="$OUT_DIR/fiting" \
      --total_count="$COUNT" \
      --has_size="$HAS_SIZE" \
      "${QUERY_FLAGS[@]}"
  fi
fi

if [[ "$RUN_PGM" == "1" ]]; then
  PGM_RUNNER="$OUT_DIR/pgm_runner"
  if [[ "$RUN_BUILD" == "1" ]]; then
    g++ -O3 -std=c++17 "$ROOT/pgm_runner.cpp" -I"$CODE_DIR/PGM-index/include" -o "$PGM_RUNNER"
  fi
  if [[ "$RUN_BULK" == "1" ]]; then
    diag_data "$LOG_DIR/${LOG_TAG}_pgm_bulk.log"
    run_cmd "$LOG_DIR/${LOG_TAG}_pgm_bulk.log" \
      "$PGM_RUNNER" \
      --keys_file="$DATA_FILE" \
      --op_type=bulk \
      --total_count="$COUNT" \
      --has_size="$HAS_SIZE" \
      --dedupe="$PGM_DEDUPE"
  fi
  if [[ "$RUN_LOOKUP" == "1" ]]; then
    diag_data_query "$LOG_DIR/${LOG_TAG}_pgm_lookup.log"
    run_cmd "$LOG_DIR/${LOG_TAG}_pgm_lookup.log" \
      "$PGM_RUNNER" \
      --keys_file="$DATA_FILE" \
      "${OP_FLAGS[@]}" \
      --total_count="$COUNT" \
      --has_size="$HAS_SIZE" \
      --dedupe="$PGM_DEDUPE" \
      "${QUERY_FLAGS[@]}"
  fi
fi

echo "[done] outputs in $OUT_DIR"
