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
- Release branch note: pushes to `release/**` with user-facing DriftBench changes auto-dispatch a docs update event to `driftbench-web`.
- Dispatch verification note (2026-05-10): this README line is used to validate cross-repo release notifications.

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

### Temporal Overlay (applied on top of Case A or B)

Temporal drift is usually an overlay, not a standalone base case.
Use it to add time evolution (uniform / periodic / trend / long-tail) on top of data drift or workload drift.

```bash
[Prompt: Temporal Overlay]
Take my <DATA or WORKLOAD> drift case and add TEMPORAL pattern <uniform|periodic|trend|long_tail>.
Please run the MCP workflow and summarize:
1) generated spec path,
2) output artifacts,
3) expected temporal behavior in plain language,
4) how temporal behavior changes the base (data/workload) case.
```

### What users should expect

1. The assistant executes MCP tools in order (`trace_to_spec/build_spec` -> `validate_spec` -> `run_spec` -> `list_outputs`).
2. You get concrete artifact paths (generated YAML + output files).
3. You get a short interpretation of what changed for your selected case (data/query), plus temporal overlay effects when requested.
4. You usually get one or two suggested next iterations for deeper benchmarking.

## Python API (Stable Entry Points)

Use top-level APIs instead of internal modules:

```python
from driftbench import run_spec, trace_to_spec, get_schema_extractor

run_spec("driftspec/examples/demo_data_single.yaml")
trace_to_spec("driftspec/trace_inputs/trace_data_mock.csv", "driftspec/generated/from_trace.yaml")
```

## Benchmark Objects (`driftbench.data.xxx`)

Use benchmark-specific objects to generate artifacts into a user-chosen directory.

### 1) Choose an output directory

`output_dir` is required. DriftBench will write files only under this directory.

### 2) Generate data and queries

```python
from pathlib import Path
from driftbench.data.tpch import data as tpch_data, queries as tpch_queries
from driftbench.data.ycsb import data as ycsb_data, queries as ycsb_queries
from driftbench.data.tpcds import data as tpcds_data, queries as tpcds_queries
from driftbench.data.dsb import data as dsb_data, queries as dsb_queries

out = Path("./artifacts")

tpch_data(scale_factor=1).generate(output_dir=out)
tpch_queries(query_ids=[1, 3, 5], queries_per_template=2, mode="qgen").generate(output_dir=out)

# For very large scale factors, generate a server-side execution plan only.
tpch_data(scale_factor=1000, mode="plan").generate(output_dir=out)

ycsb_data(scale_factor=1).generate(output_dir=out)
ycsb_queries(workload="B").generate(output_dir=out)

tpcds_data(scale_factor=10).generate(output_dir=out)
tpcds_queries().generate(output_dir=out)

dsb_data(scale_factor=10).generate(output_dir=out)
dsb_queries().generate(output_dir=out)
```

### 3) Find generated files

Artifacts are written to:

```text
<output_dir>/
  tpch/
    data/
    queries/
  ycsb/
    data/
    queries/
  tpcds/
    data/
    queries/
  dsb/
    data/
    queries/
```

Each generation creates a manifest (`*_manifest.json`) in its folder.  
Use the manifest `files` field to see exactly which files were generated.

### 4) Programmatic path retrieval

`generate()` returns a `GenerationResult` with:
- `result.files`: generated file paths
- `result.metadata`: manifest path

This is the recommended way to chain into downstream benchmarking scripts.

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
