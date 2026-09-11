#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_DIR="${DATA_DIR:-$ROOT/data}"
DATASET_URL="${DATASET_URL:-}"
OUT_FILE="${OUT_FILE:-}"
DECOMPRESS="${DECOMPRESS:-0}"
VERIFY_SHA256="${VERIFY_SHA256:-}"
SOSD_DATASET="${SOSD_DATASET:-}"

mkdir -p "$DATA_DIR"

get_checksum() {
  local file_path="$1"
  if command -v md5sum >/dev/null 2>&1; then
    md5sum "$file_path" | awk '{ print $1 }'
  else
    md5 -q "$file_path"
  fi
}

download_stream() {
  local url="$1"
  if command -v curl >/dev/null 2>&1; then
    curl -L --fail "$url"
  elif command -v wget >/dev/null 2>&1; then
    wget -O - "$url"
  else
    echo "[error] need curl or wget to download" >&2
    exit 1
  fi
}

download_file_zst() {
  local file_path="$1"
  local checksum="$2"
  local url="$3"

  if ! command -v zstd >/dev/null 2>&1; then
    echo "[error] zstd is required to download SOSD .zst datasets" >&2
    exit 1
  fi

  if [[ -f "$file_path" ]]; then
    local current
    current="$(get_checksum "$file_path")"
    if [[ "$current" != "$checksum" ]]; then
      download_stream "$url" | zstd -d > "$file_path"
    fi
  else
    download_stream "$url" | zstd -d > "$file_path"
  fi

  local final
  final="$(get_checksum "$file_path")"
  if [[ "$final" != "$checksum" ]]; then
    echo "error checksum does not match: run download again" >&2
    exit 1
  fi
  echo "$file_path checksum ok"
}

if [[ -n "$SOSD_DATASET" ]]; then
  case "$SOSD_DATASET" in
    wiki_ts_200M_uint64)
      FILE_NAME="wiki_ts_200M_uint64"
      CHECKSUM="4f1402b1c476d67f77d2da4955432f7d"
      URL="https://dataverse.harvard.edu/api/access/datafile/:persistentId?persistentId=doi:10.7910/DVN/JGVF9A/SVN8PI"
      ;;
    fb_200M_uint64)
      FILE_NAME="fb_200M_uint64"
      CHECKSUM="3b0f820caa0d62150e87ce94ec989978"
      URL="https://dataverse.harvard.edu/api/access/datafile/:persistentId?persistentId=doi:10.7910/DVN/JGVF9A/EATHF7"
      ;;
    books_800M_uint64)
      FILE_NAME="books_800M_uint64"
      CHECKSUM="8708eb3e1757640ba18dcd3a0dbb53bc"
      URL="https://www.dropbox.com/s/y2u3nbanbnbmg7n/books_800M_uint64.zst?dl=1"
      ;;
    osm_cellids_800M_uint64)
      FILE_NAME="osm_cellids_800M_uint64"
      CHECKSUM="70670bf41196b9591e07d0128a281b9a"
      URL="https://www.dropbox.com/s/j1d4ufn4fyb4po2/osm_cellids_800M_uint64.zst?dl=1"
      ;;
    books_200M_uint32)
      FILE_NAME="books_200M_uint32"
      CHECKSUM="9f3e578671e5c0348cdddc9c68946770"
      URL="https://dataverse.harvard.edu/api/access/datafile/:persistentId?persistentId=doi:10.7910/DVN/JGVF9A/5YTV8K"
      ;;
    *)
      echo "[error] unknown SOSD_DATASET: $SOSD_DATASET" >&2
      exit 1
      ;;
  esac
  OUT_PATH="$DATA_DIR/$FILE_NAME"
  echo "[download] SOSD ${SOSD_DATASET} -> $OUT_PATH"
  download_file_zst "$OUT_PATH" "$CHECKSUM" "$URL"
  echo "[done] dataset saved at $OUT_PATH"
  exit 0
fi

if [[ -z "$DATASET_URL" ]]; then
  echo "[error] set SOSD_DATASET or DATASET_URL" >&2
  exit 1
fi

if [[ -z "$OUT_FILE" ]]; then
  OUT_FILE="$DATA_DIR/$(basename "$DATASET_URL")"
fi

echo "[download] $DATASET_URL -> $OUT_FILE"
download_stream "$DATASET_URL" > "$OUT_FILE"

if [[ -n "$VERIFY_SHA256" ]]; then
  echo "${VERIFY_SHA256}  ${OUT_FILE}" | shasum -a 256 -c -
fi

if [[ "$DECOMPRESS" == "1" ]]; then
  case "$OUT_FILE" in
    *.gz)
      gunzip -k "$OUT_FILE"
      ;;
    *.zst)
      if command -v unzstd >/dev/null 2>&1; then
        unzstd -k "$OUT_FILE"
      else
        echo "[warn] unzstd not found; skip decompress" >&2
      fi
      ;;
    *.xz)
      xz -dk "$OUT_FILE"
      ;;
    *)
      echo "[info] no decompress rule for $OUT_FILE"
      ;;
  esac
fi

echo "[done] dataset saved at $OUT_FILE"
