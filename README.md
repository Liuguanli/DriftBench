<p align="center">
  <img src="./res/icon.png" alt="DriftBench logo" width="360"/>
</p>

# DriftBench

DriftBench generates benchmark datasets where **data and queries change in controlled ways** — simulating the distribution shifts, skew, and workload changes that real database systems encounter over time. You give it a DriftSpec (a YAML file describing what should change and how much), and it produces data files and SQL workloads ready for benchmarking.

Works via **CLI** (`driftbench-db`) or **MCP** (Claude / Codex assistant). Supports TPC-H, TPC-DS, YCSB, and DSB out of the box — no external data-generation tools required.

**Who this is for:**
- `Researcher` — reproduce drift scenarios, run ablations, compare estimators under shift.
- `Database Vendor / Performance Team` — run drift regression checks across benchmark targets.
- `New User` — start from a working example and see output in under 5 minutes.

## Start Here (5-minute path)

```bash
pip install -U driftbench-db
driftbench-db validate-spec driftspec/examples/demo_data_single.yaml --json
driftbench-db dry-run driftspec/examples/demo_data_single.yaml --json
driftbench-db run-yaml driftspec/examples/demo_data_single.yaml
driftbench-db list-outputs --root output --glob "**/*" --limit 20 --json
```

**What you get:** a folder under `output/` containing generated data files, a SQL workload, and a manifest (`*_manifest.json`) listing every artifact path.

> **Stuck?** See [Troubleshooting](#troubleshooting) below.

More:
- Version-by-version updates and service coverage: [CHANGELOG.md](./CHANGELOG.md)
- Production site: [driftbench.com](https://driftbench.com)
- Frontend source: [driftbench-web](https://github.com/Liuguanli/driftbench-web)

---

## Quick Paths by Role

### Researcher
```bash
pip install -U driftbench-db
driftbench-db validate-spec driftspec/examples/demo_data_single.yaml --json
driftbench-db run-yaml driftspec/examples/demo_data_single.yaml
```
→ Outputs drift datasets + workload files ready for estimator evaluation.

### Database Vendor / Performance Team
```bash
pip install -U driftbench-db
driftbench-db orchestrate \
  --spec driftspec/examples/demo_data_single.yaml \
  --targets driftspec/examples/adapters/benchmark_targets_mvp.yaml \
  --manifest-out output/orchestrate_manifest.json --json
driftbench-db list-outputs --root output --glob "**/*" --limit 30 --json
```
→ Runs one DriftSpec across multiple benchmark targets; outputs per-target manifests.

### New User
```bash
pip install -U driftbench-db
driftbench-db --help
driftbench-db validate-spec driftspec/examples/demo_data_single.yaml --json
```
→ Validates the example spec and shows you what a passing spec looks like before running anything.

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
driftbench-db validate-spec driftspec/examples/demo_data_single.yaml --json

# 2) Preview execution plan
driftbench-db dry-run driftspec/examples/demo_data_single.yaml --json

# 3) Execute
driftbench-db run-yaml driftspec/examples/demo_data_single.yaml

# 4) Inspect outputs
driftbench-db list-outputs --root output --glob "**/*" --limit 30 --json
```

### Trace to DriftSpec

```bash
driftbench-db trace-to-spec \
  driftspec/trace_inputs/trace_data_mock.csv \
  driftspec/generated/from_trace.yaml \
  --trace-type data
```

### Orchestrate Across Benchmark Targets (MVP)

Use one DriftSpec across multiple benchmark targets defined in `benchmark_target.yaml`.

```bash
driftbench-db orchestrate \
  --spec driftspec/examples/demo_data_single.yaml \
  --targets driftspec/examples/adapters/benchmark_targets_mvp.yaml \
  --manifest-out output/orchestrate_manifest.json \
  --json
```

Execute setup/run commands for each target:

```bash
driftbench-db orchestrate \
  --spec driftspec/examples/demo_data_single.yaml \
  --targets driftspec/examples/adapters/benchmark_targets_mvp.yaml \
  --manifest-out output/orchestrate_manifest.json \
  --execute \
  --json
```

### Bootstrap Dataset (download/copy + checksum + schema extract)

Bootstrap from preset, local path, or URL:

```bash
driftbench-db bootstrap dataset \
  --source census_original \
  --output-dir output/bootstrap/datasets \
  --json
```

With checksum verification:

```bash
driftbench-db bootstrap dataset \
  --source /path/to/my_dataset.csv \
  --output-dir output/bootstrap/datasets \
  --checksum sha256:<hex> \
  --json
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
tpch_queries().generate(output_dir=out)  # all query ids

# For very large scale factors, generate a server-side execution plan only.
tpch_data(scale_factor=1000, mode="plan").generate(output_dir=out)

ycsb_data(scale_factor=1).generate(output_dir=out)
ycsb_queries(workload="B").generate(output_dir=out)

tpcds_data(scale_factor=10).generate(output_dir=out)            # any scale (synthetic local generation)
tpcds_queries().generate(output_dir=out)                        # all query ids (1..99)
tpcds_queries(query_ids=[1, 5, 42]).generate(output_dir=out)    # selected query ids

dsb_data(scale_factor=10).generate(output_dir=out)
dsb_queries().generate(output_dir=out)
```

`tpch_data(scale_factor=...)` default mode is `auto`:
- try local `.tbl` source (if available);
- if missing and `scale_factor == 1`, try built-in Python download path;
- otherwise fall back to integrated synthetic generation.

This means users can call the Python API directly without manually running external download commands.

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

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `command not found: driftbench-db` | Entry point not on PATH | Run `pip install -U driftbench-db` again; check your venv is active |
| `[VALIDATION ERROR] Spec root must be a YAML mapping` | YAML file is a list or scalar, not a mapping | Open the spec file and ensure the top level is `type: ...` / `variables: ...` |
| `[VALIDATION ERROR] Invalid 'type': expected mapping` | `type:` field is a plain string, not a nested object | Use `type: {family: ..., category: ..., subtype: ...}` |
| `[VALIDATION ERROR] No such file or directory` | Wrong spec path | Check the path with `ls driftspec/examples/` and retry |
| `Missing 'type' in spec` | Spec file is empty or missing the `type` key | Add `type:` block; see `driftspec/examples/demo_data_single.yaml` for reference |
| Output folder is empty after `run-yaml` | Spec has no enabled variables | Ensure at least one variable in `variables:` is not commented out |

For anything not listed here, run with `--json` to get a machine-readable error, then check `docs/p0_known_issues.md`.

---

## Testing

Run all tests:

```bash
python3 -m unittest discover -s test -p 'test_*.py' -v
```

---

## License

MIT (see `LICENSE`).
