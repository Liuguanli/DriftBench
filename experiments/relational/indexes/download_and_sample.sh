#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_DIR="${DATA_DIR:-$ROOT/data}"
QUERY_DIR="${QUERY_DIR:-$ROOT/queries}"

SAMPLE_COUNT="${SAMPLE_COUNT:-1000000}"
QUERY_COUNT="${QUERY_COUNT:-0}"
TYPE="${TYPE:-u64}"
INPUT_HAS_SIZE="${INPUT_HAS_SIZE:-0}"
SEED="${SEED:-42}"
SORT_KEYS="${SORT_KEYS:-1}"
DEDUPE_KEYS="${DEDUPE_KEYS:-0}"
DELETE_RAW="${DELETE_RAW:-1}"

mkdir -p "$DATA_DIR" "$QUERY_DIR"

if [[ -n "${SOSD_DATASET:-}" ]]; then
  case "$SOSD_DATASET" in
    wiki_ts_200M_uint64|fb_200M_uint64|books_800M_uint64|osm_cellids_800M_uint64|books_200M_uint32)
      RAW_FILE="$DATA_DIR/$SOSD_DATASET"
      ;;
    *)
      echo "[error] unknown SOSD_DATASET: $SOSD_DATASET" >&2
      exit 1
      ;;
  esac
fi

if [[ -n "${DATASET_URL:-}" ]]; then
  RAW_FILE="$DATA_DIR/$(basename "$DATASET_URL")"
fi

if [[ -z "${RAW_FILE:-}" ]]; then
  echo "[error] set SOSD_DATASET or DATASET_URL" >&2
  exit 1
fi

echo "[step] download dataset"
DATA_DIR="$DATA_DIR" \
SOSD_DATASET="${SOSD_DATASET:-}" \
DATASET_URL="${DATASET_URL:-}" \
OUT_FILE="${OUT_FILE:-}" \
DECOMPRESS="${DECOMPRESS:-0}" \
VERIFY_SHA256="${VERIFY_SHA256:-}" \
bash "$ROOT/download_dataset.sh"

if [[ ! -f "$RAW_FILE" ]]; then
  echo "[error] expected raw dataset not found: $RAW_FILE" >&2
  exit 1
fi

SAMPLE_ARGS=(--input "$RAW_FILE" --output "$DATA_DIR/keys_u64.bin" --count "$SAMPLE_COUNT" --type "$TYPE" --seed "$SEED")
if [[ "$INPUT_HAS_SIZE" == "1" ]]; then
  SAMPLE_ARGS+=(--input-has-size)
fi
if [[ "$SORT_KEYS" == "1" ]]; then
  SAMPLE_ARGS+=(--sort)
fi
if [[ "$DEDUPE_KEYS" == "1" ]]; then
  SAMPLE_ARGS+=(--dedupe)
fi

echo "[step] sample data -> $DATA_DIR/keys_u64.bin"
python3 "$ROOT/sample_dataset.py" "${SAMPLE_ARGS[@]}"

if [[ "$QUERY_COUNT" -gt 0 ]]; then
  QUERY_ARGS=(--input "$RAW_FILE" --output "$QUERY_DIR/queries_u64.bin" --count "$QUERY_COUNT" --type "$TYPE" --seed "$SEED")
  if [[ "$INPUT_HAS_SIZE" == "1" ]]; then
    QUERY_ARGS+=(--input-has-size)
  fi
  echo "[step] sample queries -> $QUERY_DIR/queries_u64.bin"
  python3 "$ROOT/sample_dataset.py" "${QUERY_ARGS[@]}"
fi

if [[ "$DELETE_RAW" == "1" ]]; then
  echo "[step] delete raw dataset $RAW_FILE"
  rm -f "$RAW_FILE"
fi

echo "[done] sampled data in $DATA_DIR and queries in $QUERY_DIR"
