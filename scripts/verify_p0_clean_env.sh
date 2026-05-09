#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV_DIR="${1:-${REPO_ROOT}/.venv-p0}"

echo "[verify] bootstrapping clean env"
# Optional override:
#   P0_PYTHON=/path/to/python3.10 ./scripts/verify_p0_clean_env.sh
"${REPO_ROOT}/scripts/bootstrap_p0_env.sh" "${VENV_DIR}"
source "${VENV_DIR}/bin/activate"

cd "${REPO_ROOT}"

echo "[verify] focused P0 suites"
python -m unittest -v \
  test.test_cli_commands \
  test.test_spec_core_unit \
  test.test_spec_execution_integration \
  test.test_smoke_pipeline

echo "[verify] full discovery sanity"
python -m unittest discover -s test -p 'test_*.py' -v

echo "[verify] CLI sanity"
python -m driftbench.cli validate-spec driftspec/examples/demo_data_single.yaml --json >/dev/null
python -m driftbench.cli dry-run driftspec/examples/demo_data_single.yaml --json >/dev/null

echo "[verify] success"
