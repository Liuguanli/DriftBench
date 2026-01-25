#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PYTHON_BIN="${PYTHON_BIN:-python}"

exec "$PYTHON_BIN" "$REPO_ROOT/tasks/cardinality_estimation/ce_runner.py" "$@"
