# P0 MCP Command Matrix (Intent -> Service API -> CLI Fallback)

## Purpose

This document defines the P0 automation contract for MCP-style usage.
Each workflow intent maps to:
- preferred `driftbench_service` API endpoint,
- request/response shape,
- CLI fallback when service is unavailable.

Base URL examples below assume local service:
- `http://127.0.0.1:8000`

Start service:

```bash
python driftbench_service/server.py --port 8000
```

## Command Matrix

| Intent | Service API | Method | Required Payload/Params | Success Output | CLI Fallback |
|---|---|---|---|---|---|
| Health check | `/api/health` | GET | none | `{"ok": true, "time": ...}` | `driftbench-db --help` (process check) |
| List available specs | `/api/specs` | GET | none | `{"specs": [...]}` | `driftbench-db list-outputs --root driftspec --glob "**/*.yaml" --json` |
| List public/shared specs | `/api/public-specs` | GET | optional query: `tag`, `query`, `limit` | `{"specs":[...], "count":N}` | MCP: `list_public_specs` |
| Build DriftSpec from JSON object | `/api/spec/build` | POST | `{"spec": {...}, "output_path": "..."}` | `{"ok": true, "path": "..."}` | write YAML locally, then use CLI commands below |
| Validate spec without execution | n/a (service does not expose direct endpoint yet) | n/a | n/a | n/a | `driftbench-db validate-spec <spec_path> --json` |
| Dry-run spec summary | n/a (service does not expose direct endpoint yet) | n/a | n/a | n/a | `driftbench-db dry-run <spec_path> --json` |
| Execute DriftSpec | `/api/run` | POST | `{"spec_path": "driftspec/...yaml"}` | `{"job": {..., "id": N}}` | `driftbench-db run-yaml <spec_path>` |
| Trace summary -> DriftSpec | `/api/trace-to-spec` | POST | `{"trace_path":"...","output_path":"...","trace_type":"data|workload?","mapping_path":"...?"}` | `{"job": {..., "id": N}}` | `driftbench-db trace-to-spec <trace> <output> [--trace-type ...] [--mapping ...]` |
| Extract schema | `/api/schema/extract` | POST | CSV: `{"source_type":"csv","path":"...","output_path":"...?"}`; PG: `{"source_type":"postgres","db_config_path":"...","schema_name":"public","output_path":"...?"}` | `{"job": {..., "id": N}}` | Python API: `get_schema_extractor(...).extract_schema()` |
| Read schema file | `/api/schema/read?path=...` | GET | query param `path` | `{"schema": {...}, "path":"..."}` | read JSON directly from filesystem |
| Preview table samples from schema | `/api/schema/table-preview` | POST | `{"schema_path":"...","table_name":"...","limit":1..5,...}` | preview rows + columns JSON | n/a |
| List jobs | `/api/jobs` | GET | none | `{"jobs":[...]}` | n/a |
| Get job details/logs | `/api/jobs/{id}` | GET | path `id` | `{"job": {...}}` | n/a |
| Delete/stop job | `/api/jobs/delete` | POST | `{"job_id": N}` | `{"ok": true, ...}` | n/a |
| List schema files | `/api/schemas` | GET | none | `{"files":[...]}` | `driftbench-db list-outputs --root driftbench_service/schemas --glob "*.json" --json` |
| List uploads | `/api/uploads` | GET | optional `?ext=.json&prefix=db_config_` | `{"files":[...]}` | `driftbench-db list-outputs --root driftbench_service/uploads --json` |
| Upload file (base64) | `/api/files/upload` | POST | `{"filename":"...","content_b64":"..."}` | `{"path":"..."}` | write local file directly |
| Save text file | `/api/files/save-text` | POST | `{"filename":"...","content":"..."}` | `{"path":"..."}` | write local file directly |
| List generated outputs | n/a (service does not expose dedicated endpoint yet) | n/a | n/a | n/a | `driftbench-db list-outputs --root output --glob "**/*" --json` |
| Import public spec and run | `/api/public-specs/import-run` | POST | `{"spec_id":"..."}` or `{"spec_path":"..."}`, optional `target_path`, `overwrite`, `execute` | `{"ok":true, "imported_spec_path":"...", "job":...}` when execute | MCP: `import_spec_and_run` |

## Runnable Automation Examples

### 1) Validate and dry-run a spec (CLI path)

```bash
driftbench-db validate-spec driftspec/examples/demo_data_single.yaml --json
driftbench-db dry-run driftspec/examples/demo_data_single.yaml --json
```

### 2) Trace -> Spec -> Execute (service path)

```bash
# generate spec in background job
curl -s -X POST http://127.0.0.1:8000/api/trace-to-spec \
  -H 'Content-Type: application/json' \
  -d '{
    "trace_path":"driftspec/trace_inputs/trace_data_mock.csv",
    "output_path":"driftspec/generated/trace_data_from_api.yaml",
    "trace_type":"data"
  }'

# run generated spec in background job
curl -s -X POST http://127.0.0.1:8000/api/run \
  -H 'Content-Type: application/json' \
  -d '{"spec_path":"driftspec/generated/trace_data_from_api.yaml"}'
```

### 3) Poll job status/logs

```bash
curl -s http://127.0.0.1:8000/api/jobs
curl -s http://127.0.0.1:8000/api/jobs/<job_id>
```

### 4) Fallback execution path when service is unavailable

```bash
driftbench-db trace-to-spec \
  driftspec/trace_inputs/trace_data_mock.csv \
  driftspec/generated/trace_data_cli.yaml \
  --trace-type data

driftbench-db run-yaml driftspec/generated/trace_data_cli.yaml
driftbench-db list-outputs --root output --glob "**/*.csv" --limit 20 --json
```

## Contract Notes

- Service workflows for `run` and `trace-to-spec` are asynchronous and return job IDs.
- Validation and dry-run are currently CLI-only in P0.
- Paths must stay inside repository root for service endpoints.
- Service and CLI can be combined in one automation pipeline safely.
