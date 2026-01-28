#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

bash "$ROOT/tasks/cardinality_estimation/run_ce_timeline_census.sh" apply-drift-and-workload-skew
bash "$ROOT/tasks/cardinality_estimation/run_ce_timeline_census.sh" apply-drift-and-workload-cardinality

# bash tasks/cardinality_estimation/run_ce_timeline_census.sh apply-drift-and-workload-cardinality
