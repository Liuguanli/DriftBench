# DriftBench CLI + MCP Implementation Notes (CIDR 2027)

This note documents the current production-facing interfaces used to operationalize DriftBench as a reusable system rather than a vision-only prototype.

## 1) CLI Implementation

Primary file:
- `driftbench/cli.py`

### Command surface

1. `run-yaml <spec>`
- Executes a DriftSpec end-to-end via `run_all`.

2. `trace-to-spec <trace> <output> [--trace-type data|workload] [--mapping ...]`
- Generates a DriftSpec YAML from summarized trace input via `trace_to_spec`.

3. `validate-spec <spec> [--json]`
- Loads spec, applies migration, validates schema and handler availability.
- Returns normalized type triple + declared outputs.

4. `dry-run <spec> [--json]`
- Performs full validation and returns execution summary without executing generators.

5. `list-outputs [--root output] [--glob **/*] [--limit N] [--include-dirs] [--json]`
- Scans output trees for automation and downstream pipelines.

### Reliability contract

- Exit codes:
  - `0`: success
  - `3`: validation errors
  - `4`: runtime errors
- JSON output mode is supported for machine orchestration (`validate-spec`, `dry-run`, `list-outputs`).
- Path traversal is guarded by explicit path validation at service/MCP boundaries.

## 2) Stable Python API boundary

Primary files:
- `driftbench/api.py`
- `driftbench/__init__.py`

Public integration entrypoints:
- `run_spec`
- `run_spec_and_return_summary`
- `trace_to_spec`
- `load_and_validate_spec`
- schema/filter registry access (`get_schema_extractor`, `register_filter`, `get_filter`)

This boundary avoids dependence on deep internal modules in external projects.

## 3) MCP Server Implementation

Primary files:
- `driftbench_mcp/server.py`
- `scripts/run_driftbench_mcp.sh`
- `docs/mcp_config_example.json`

Transport/protocol:
- JSON-RPC over stdio with MCP framing (`Content-Length` headers).
- Implements `initialize`, `tools/list`, `tools/call`, `ping`.

### Implemented MCP tools

P0 foundation tools:
1. `driftbench_health`
2. `trace_to_spec`
3. `validate_spec`
4. `dry_run_spec`
5. `run_spec`
6. `list_outputs`

P1 productization tools:
7. `save_spec`
8. `list_public_specs`
9. `import_spec_and_run`

### MCP data-plane behavior

- All tool inputs are schema-validated and type-checked.
- Paths are constrained to repository root (`DRIFTBENCH_ROOT`).
- Tool replies include:
  - human-readable text in `content`
  - machine-usable JSON in `structuredContent`

## 4) Shareable Spec Catalog and Import Flow

### MCP-side catalog

Default storage:
- shared specs: `driftspec/shared/`
- catalog: `driftbench_service/state/public_specs_catalog.json`

Config overrides:
- `DRIFTBENCH_MCP_SHARED_SPECS_DIR`
- `DRIFTBENCH_MCP_CATALOG_PATH`

Operations:
- `save_spec`: validates + copies spec into shared dir + upserts catalog metadata.
- `list_public_specs`: filtering by tag/query/limit.
- `import_spec_and_run`: imports by `spec_id` or path into local target and optionally executes.

### Service-side HTTP endpoints

Primary file:
- `driftbench_service/server.py`

Implemented endpoints:
- `GET /api/public-specs?tag=&query=&limit=`
- `POST /api/public-specs/import-run`

`import-run` behavior:
- imports spec from catalog/path into repository-safe target location;
- optionally launches async CLI run job (`python -m driftbench.cli run-yaml ...`).

## 5) Validation & Reproducibility

Test files:
- `test/test_cli_commands.py`
- `test/test_mcp_server_tools.py`
- `test/test_public_specs_service.py`

Validated properties:
- CLI command correctness and error code semantics.
- MCP tool invocation correctness (including sharing/import tools).
- Public spec catalog listing and import helper logic.

## 6) Suggested paper positioning (CIDR demo style)

For CIDR demo framing, emphasize:
1. Interface unification: same workflow across Python API, CLI, MCP, and HTTP service.
2. Shareability: DriftSpec as portable artifact with catalog and import/run semantics.
3. Reproducibility: deterministic spec execution + machine-readable outputs + automated tests.

