#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${BASE_URL:-http://127.0.0.1:8000}"

echo "[1/5] health"
curl -s "${BASE_URL}/api/health"
echo

echo "[2/5] validate + dry-run (CLI)"
driftbench-db validate-spec driftspec/examples/demo_data_single.yaml --json
driftbench-db dry-run driftspec/examples/demo_data_single.yaml --json

echo "[3/5] trace-to-spec (service job)"
TRACE_RESP="$(curl -s -X POST "${BASE_URL}/api/trace-to-spec" \
  -H 'Content-Type: application/json' \
  -d '{
    "trace_path":"driftspec/trace_inputs/trace_data_mock.csv",
    "output_path":"driftspec/generated/trace_data_from_api.yaml",
    "trace_type":"data"
  }')"
echo "${TRACE_RESP}"

echo "[4/5] run generated spec (service job)"
RUN_RESP="$(curl -s -X POST "${BASE_URL}/api/run" \
  -H 'Content-Type: application/json' \
  -d '{"spec_path":"driftspec/generated/trace_data_from_api.yaml"}')"
echo "${RUN_RESP}"

echo "[5/5] list jobs + list outputs (CLI)"
curl -s "${BASE_URL}/api/jobs"
echo
driftbench-db list-outputs --root output --glob "**/*.csv" --limit 20 --json

echo "[DONE] P0 MCP examples completed."
