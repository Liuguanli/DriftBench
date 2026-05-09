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

After MCP is configured, the best pattern is to give your assistant a **case type**
plus **what change you want to simulate**.

### Case A: Data Drift (data changes)

Use when you care about data size/distribution changes (scaling, skew, outliers, updates).

```bash
[Prompt: Data Drift]
Read docs/p0_integration_quickstart.md.
I want a DATA drift case on <my dataset path>.
Goal: <e.g., scale 2x + stronger skew on column amount>.
Please use MCP tools to:
1) build a DriftSpec (or trace_to_spec if needed),
2) validate it,
3) run it,
4) list outputs.
Then summarize what data files were generated and what changed.
```

### Case B: Workload Drift (query changes)

Use when you care about query behavior changes (predicate distribution, selectivity, structure, payload).

```bash
[Prompt: Workload Drift]
I want a WORKLOAD drift case.
Query goal: <e.g., predicates shift from uniform to city-focused, selectivity from 10% to 60%>.
Please create/run a spec via MCP and report:
- generated workload files,
- how query distribution/selectivity changed,
- suggested next workload variant.
```

### Case C: Temporal Drift (time changes)

Use when you care about how events/queries evolve over time (uniform, periodic, trend, long-tail).

```bash
[Prompt: Temporal Drift]
I want a TEMPORAL drift case with pattern <uniform|periodic|trend|long_tail>.
Please run the MCP workflow end-to-end and summarize:
1) generated spec path,
2) output artifacts,
3) expected temporal behavior in plain language.
```

### What users should expect

1. The assistant executes MCP tools in order (`trace_to_spec/build_spec` -> `validate_spec` -> `run_spec` -> `list_outputs`).
2. You get concrete artifact paths (generated YAML + output files).
3. You get a short interpretation of what changed for your selected case (data/query/time).
4. You usually get one or two suggested next iterations for deeper benchmarking.

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
