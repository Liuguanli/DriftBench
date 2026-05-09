#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV_DIR="${1:-${REPO_ROOT}/.venv-p0}"
REQ_FILE="${REPO_ROOT}/requirements/p0.lock.txt"
PY_BIN="${P0_PYTHON:-}"

pick_python() {
  if [ -n "${PY_BIN}" ]; then
    if ! command -v "${PY_BIN}" >/dev/null 2>&1; then
      echo "[bootstrap][error] P0_PYTHON is set but not found: ${PY_BIN}" >&2
      exit 2
    fi
    echo "${PY_BIN}"
    return
  fi

  for cand in python3.10 python3.11 python3.12 python3; do
    if ! command -v "${cand}" >/dev/null 2>&1; then
      continue
    fi
    ver="$("${cand}" - <<'PY'
import sys
print(f"{sys.version_info.major}.{sys.version_info.minor}")
PY
)"
    case "${ver}" in
      3.10|3.11|3.12)
        echo "${cand}"
        return
        ;;
    esac
  done

  echo "[bootstrap][error] No supported Python found (need 3.10/3.11/3.12)." >&2
  echo "[bootstrap][error] Current default is: $(python3 --version 2>/dev/null || echo 'python3 not found')" >&2
  echo "[bootstrap][hint] Set explicit interpreter, e.g.:" >&2
  echo "  P0_PYTHON=/Users/guanlil1/anaconda3/bin/python3.10 ./scripts/bootstrap_p0_env.sh" >&2
  exit 2
}

PY_BIN="$(pick_python)"
PY_VER="$("${PY_BIN}" - <<'PY'
import sys
print(f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}")
PY
)"

echo "[bootstrap] repo=${REPO_ROOT}"
echo "[bootstrap] venv=${VENV_DIR}"
echo "[bootstrap] requirements=${REQ_FILE}"
echo "[bootstrap] python=${PY_BIN} (${PY_VER})"

"${PY_BIN}" -m venv "${VENV_DIR}"
source "${VENV_DIR}/bin/activate"

python -m pip install --upgrade pip setuptools wheel
python -m pip install -r "${REQ_FILE}"
python -m pip check

echo "[bootstrap] done"
echo "[bootstrap] activate with: source ${VENV_DIR}/bin/activate"
