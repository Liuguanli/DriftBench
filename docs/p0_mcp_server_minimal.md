# P0 Minimal DriftBench MCP Server

This is the first real MCP server runtime for DriftBench (stdio transport).

## Location

- Server: `driftbench_mcp/server.py`
- Launcher script: `scripts/run_driftbench_mcp.sh`
- Example config: `docs/mcp_config_example.json`

## Core Tools

P0 foundation tools:

1. `driftbench_health`
2. `trace_to_spec`
3. `validate_spec`
4. `dry_run_spec`
5. `run_spec`
6. `list_outputs`

P1 sharing tools:

7. `save_spec`
8. `list_public_specs`
9. `import_spec_and_run`

## Quick Local Check

```bash
python3 -m driftbench_mcp.server
```

If it starts without errors and waits on stdin, the server is ready.

## Ready-to-Paste MCP Config

Use this shape in your MCP client config and adjust absolute paths:

```json
{
  "mcpServers": {
    "driftbench": {
      "command": "/Users/guanlil1/Dropbox/PostDoc/topics/WorkloadDatasetGenerator/scripts/run_driftbench_mcp.sh",
      "env": {
        "DRIFTBENCH_ROOT": "/Users/guanlil1/Dropbox/PostDoc/topics/WorkloadDatasetGenerator"
      }
    }
  }
}
```

Alternative using Python module directly:

```json
{
  "mcpServers": {
    "driftbench": {
      "command": "python3",
      "args": [
        "-m",
        "driftbench_mcp.server"
      ],
      "cwd": "/Users/guanlil1/Dropbox/PostDoc/topics/WorkloadDatasetGenerator",
      "env": {
        "DRIFTBENCH_ROOT": "/Users/guanlil1/Dropbox/PostDoc/topics/WorkloadDatasetGenerator"
      }
    }
  }
}
```

## Notes

- For safety, tool paths are restricted to the repository root (`DRIFTBENCH_ROOT` or current working directory).
- `list_outputs` defaults to root `output`, but can be pointed to `driftspec` for spec discovery.
- Public spec catalog defaults:
  - catalog JSON: `driftbench_service/state/public_specs_catalog.json`
  - shared specs dir: `driftspec/shared/`
- Optional overrides:
  - `DRIFTBENCH_MCP_CATALOG_PATH`
  - `DRIFTBENCH_MCP_SHARED_SPECS_DIR`
- Tool results include both:
  - human-readable text (`content`)
  - machine-readable JSON (`structuredContent`)
