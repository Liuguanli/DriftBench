#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"

bash "$ROOT/experiments/relational/cardinality_estimation/run_ce_timeline_census.sh" apply-drift-and-workload-skew
bash "$ROOT/experiments/relational/cardinality_estimation/run_ce_timeline_census.sh" apply-drift-and-workload-cardinality

# bash experiments/relational/cardinality_estimation/run_ce_timeline_census.sh apply-drift-and-workload-cardinality
