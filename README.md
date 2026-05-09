<p align="center">
  <img src="./res/icon.png" alt="DriftBench logo" width="360"/>
</p>

# DriftBench

DriftBench is a toolkit for generating and replaying **data drift** and **workload drift** with DriftSpec.

This README is intentionally focused on **how to use the latest DriftBench**.

---

## Web Frontend

- Production site: [driftbench.com](https://driftbench.com)
- Frontend source repo: [driftbench-web](https://github.com/Liuguanli/driftbench-web)

---

## Install (Latest)

### From PyPI (recommended)

```bash
python3 -m pip install -U driftbench-db
```

### From source (latest `main`)

```bash
git clone https://github.com/Liuguanli/DriftBench.git
cd DriftBench
python3 -m pip install -e .
```

### Verify installation

```bash
driftbench --help
driftbench-service --help
driftbench-mcp --help
```

---

## CLI Quickstart

Use this flow for most users:

```bash
# 1) Validate a DriftSpec
python -m driftbench.cli validate-spec driftspec/examples/demo_data_single.yaml --json

# 2) Preview execution plan
python -m driftbench.cli dry-run driftspec/examples/demo_data_single.yaml --json

# 3) Execute
python -m driftbench.cli run-yaml driftspec/examples/demo_data_single.yaml

# 4) Inspect outputs
python -m driftbench.cli list-outputs --root output --glob "**/*" --limit 30 --json
```

### Trace to DriftSpec

```bash
python -m driftbench.cli trace-to-spec \
  driftspec/trace_inputs/trace_data_mock.csv \
  driftspec/generated/from_trace.yaml \
  --trace-type data
```

---

## MCP Quickstart

Start MCP server (stdio):

```bash
python3 -m driftbench_mcp.server
```

Client config template:

- `docs/mcp_config_example.json`

Minimal MCP guide:

- `docs/p0_mcp_server_minimal.md`

Core MCP workflow:

1. `trace_to_spec`
2. `validate_spec`
3. `run_spec`
4. `list_outputs`

Spec sharing tools:

- `save_spec`
- `list_public_specs`
- `import_spec_and_run`

---

## MCP Chat Demo (Codex / Claude Code)

After MCP is configured, you can ask your coding assistant in plain language.
Use prompts like these:

```bash
[Prompt 1]
Read docs/p0_integration_quickstart.md, then run a full DriftBench MCP workflow:
1) build a spec from driftspec/trace_inputs/trace_data_mock.csv
2) validate the generated spec
3) execute it
4) list generated outputs
If a step fails, fix it and continue.
```

```bash
[Prompt 2]
Use MCP tools to save the generated spec as a public spec named "demo-trace-spec",
then list public specs and import-run it to verify the sharing flow works.
```

What you should expect from the assistant:

1. It will call MCP tools in sequence (`trace_to_spec` -> `validate_spec` -> `run_spec` -> `list_outputs`).
2. It will return concrete artifact paths (e.g., generated YAML under `driftspec/generated/` and output files under `output/`).
3. It will summarize what was produced (which stages ran, which files were created).
4. It may suggest the next experiment change (for example, adjusting drift intensity or running a different template).

Typical outputs to look for:

1. A new DriftSpec YAML file.
2. Generated drifted data/workload artifacts.
3. A machine-readable output list for logging or CI.

## Python API (Stable Entry Points)

Use top-level APIs instead of internal modules:

```python
from driftbench import run_spec, trace_to_spec, get_schema_extractor

run_spec("driftspec/examples/demo_data_single.yaml")
trace_to_spec("driftspec/trace_inputs/trace_data_mock.csv", "driftspec/generated/from_trace.yaml")
```

---

## Where to find examples

- Example specs: `driftspec/examples/`
- Trace inputs: `driftspec/trace_inputs/`
- Integration tests with runnable fixtures: `test/fixtures/specs/`

---

## Core docs

- API boundary: `docs/p0_api_boundary_freeze.md`
- CLI/MCP command matrix: `docs/p0_mcp_command_matrix.md`
- Integration quickstart: `docs/p0_integration_quickstart.md`
- MCP examples script: `docs/p0_mcp_examples.sh`
- Release branch/tag policy: `docs/release_branch_policy.md`

---

## Testing

Run all tests:

```bash
python3 -m unittest discover -s test -p 'test_*.py' -v
```

---

## License

MIT (see `LICENSE`).
