<p align="center">
  <img src="./res/icon.png" alt="DriftBench logo" width="420"/>
</p>

# DriftBench

DriftBench is a benchmarking toolkit that quantifies how data drift and workload drift influence database behavior. It powers the experiments in the DriftBench paper with reproducible drift generation, query synthesis, and downstream evaluation utilities.

---

## Highlights
- Unified handling of data and workload drift with shared abstractions.
- Declarative experiment definitions through DriftSpec YAML files.
- Support for CSV, Parquet, and PostgreSQL sources plus downstream workloads.
- End-to-end assets (schemas, templates, workloads, plots) for paper reproduction.

---

## Web Frontend

The deployable frontend (Home, Get Started, Drift Lab, Generator playbook, Case Studies) lives in a sibling repo: [`driftbench-web`](https://github.com/Liuguanli/driftbench-web). Clone that repo to build / run the UI.

## DriftSpec at a Glance

DriftSpec is the YAML contract that tells DriftBench which drift scenario to build. A single file captures the drift family, data source, variables, and optional workload generation hooks, making experiments portable and versionable.

### Key Elements
- `type`: declares whether the pattern targets data or workloads (single-table or multi-table).
- `data_source`: describes how to access the base data and optionally extract a schema.
- `variables`: parameterizes the drift operators, output paths, and workload knobs.
- Optional sections cover temporal stamps, query generation, and downstream processors.

### Minimal Example

```yaml
pattern_id: census-cardinality-demo
seed: 42

type:
  family: data
  category: drift
  subtype: single_table

data_source:
  kind: csv
  path: ./data/census_original.csv

variables:
  base_table: census_original
  drifts:
    - name: vary_cardinality_scale_1
      drift_type: vary_cardinality
      scale: 1.0
      output_path: ./output/data/cardinality/scale/census_original_cardinality_1.csv
```

Run any specification with:

```bash
python -m driftbench.cli run-yaml <path-to-yaml>
```

Additional operational CLI commands:

```bash
# Validate spec structure and handler availability
python -m driftbench.cli validate-spec driftspec/examples/demo_data_single.yaml --json

# Preview what a spec would execute (without running handlers)
python -m driftbench.cli dry-run driftspec/examples/demo_data_single.yaml --json

# Generate a spec from trace summary
python -m driftbench.cli trace-to-spec driftspec/trace_inputs/trace_data_mock.csv driftspec/generated/trace_data_mock.yaml

# List generated outputs for inspection/automation
python -m driftbench.cli list-outputs --root output --glob "**/*.csv" --limit 20 --json
```

### Python Integration API (P0)

For downstream project integration, prefer the stable top-level API:

```python
from driftbench import run_spec, trace_to_spec, get_schema_extractor

run_spec("driftspec/examples/demo_data_single.yaml")
trace_to_spec("driftspec/trace_inputs/trace_data_mock.csv", "driftspec/generated/from_trace.yaml")
```

Public API details and boundary rules:
- `docs/p0_api_boundary_freeze.md`
- `docs/p0_mcp_command_matrix.md`

MCP runnable example script:
- `docs/p0_mcp_examples.sh`

Minimal MCP server runtime (stdio):
- `driftbench_mcp/server.py`
- `scripts/run_driftbench_mcp.sh`
- `docs/p0_mcp_server_minimal.md`
- `docs/mcp_config_example.json`

Spec sharing MCP tools are included:
- `save_spec`
- `list_public_specs`
- `import_spec_and_run`

## Testing (P0 Foundation)

Run the full test suite:

```bash
python3 -m unittest discover -s test -p 'test_*.py' -v
```

Run focused P0 suites:

```bash
python3 -m unittest -v \
  test.test_cli_commands \
  test.test_spec_core_unit \
  test.test_spec_execution_integration \
  test.test_smoke_pipeline
```

Clean-environment bootstrap and verification:

```bash
./scripts/bootstrap_p0_env.sh
./scripts/verify_p0_clean_env.sh
```

### Custom Deletion Filters (registry + DriftSpec)

DriftSpec cannot serialize Python callables, so use the filter registry to reference a filter by name.

1) Register a filter in code:

```python
# my_project/filters.py
from driftbench.core.data.filter_registry import register_filter

@register_filter("age_gt_60")
def age_gt_60(series, config):
    return series > 60
```

2) Import the module and reference it in YAML:

```yaml
filter_registry_modules:
  - my_project.filters

variables:
  base_table: census_original
  drifts:
    - name: delete_age_gt_60
      drift_type: selective_deletion
      n: 5000
      filter:
        column: age
        func_name: age_gt_60
      output_path: ./output/data/cardinality/update/census_original_deletion_age_gt_60.csv
```

You can also use simple declarative filters without registration:

```yaml
filter:
  column: timestamp
  min: "2025-07-02T00:00:00"
  max: "2025-07-03T00:00:00"
```

### Trace to DriftSpec (mock flow)

If you already parsed a real database trace into a compact CSV/JSON summary, you can generate a DriftSpec YAML directly:

```bash
python -m driftbench.cli trace-to-spec driftspec/trace_inputs/trace_data_mock.csv driftspec/generated/trace_data_mock.yaml
python -m driftbench.cli trace-to-spec driftspec/trace_inputs/trace_workload_mock.json driftspec/generated/trace_workload_mock.yaml
```

The mock inputs live in `driftspec/trace_inputs/` and show the minimal fields the generator expects.

Explore complete templates in `driftspec/examples/`, including:
- Data drift patterns: [`demo_data_single.yaml`](driftspec/examples/demo_data_single.yaml)
- PostgreSQL single-table: [`demo_postgres.yaml`](driftspec/examples/demo_postgres.yaml)
- PostgreSQL multi-table: [`demo_postgres_multi.yaml`](driftspec/examples/demo_postgres_multi.yaml)
- Workload drift: [`workload_census.yaml`](driftspec/examples/workload_census.yaml)

### Census Temporal Demos (Data)

These specs create timestamped census data and time growth scenarios under `output/data/time_demo/`.

- [`demo_data_census_timestamp.yaml`](driftspec/examples/demo_data_census_timestamp.yaml): add a `timestamp` column with uniform arrivals.
- [`demo_data_census_time_growth.yaml`](driftspec/examples/demo_data_census_time_growth.yaml): combine a base day with an age-skewed day to show a distribution shift.
- [`demo_data_census_time_growth_3x.yaml`](driftspec/examples/demo_data_census_time_growth_3x.yaml): 3-day time growth (uniform + periodic + bursty).
- [`demo_data_census_time_growth_4x.yaml`](driftspec/examples/demo_data_census_time_growth_4x.yaml): 4-day time growth (adds a long-tail day).

Run them with:

```bash
python -m driftbench.cli run-yaml driftspec/examples/demo_data_census_timestamp.yaml
python -m driftbench.cli run-yaml driftspec/examples/demo_data_census_time_growth.yaml
python -m driftbench.cli run-yaml driftspec/examples/demo_data_census_time_growth_3x.yaml
python -m driftbench.cli run-yaml driftspec/examples/demo_data_census_time_growth_4x.yaml
```

Behind the scenes, the runner loads type handlers registered in `driftbench/spec/types/` and executes them through `driftbench/spec/core.py`.

---

## Paper Artifacts

Everything required to reproduce the results in the DriftBench paper lives in this repository.

- **Input datasets**
  - `data/census_original.csv`: baseline census table.
  - `data/census_outliers.csv`: injected outliers for case studies.
  - `data/PG_info.json`: PostgreSQL connection metadata.
- **Schemas and templates**
  - `output/intermediate/census_original_schema.json`: extracted single-table schema.
  - `output/intermediate/census_original_templates.json`: workload templates for the census data.
  - `output/intermediate/tpcds_schema.json`: inferred multi-table schema from TPC-DS.
- **Data drift outputs**
  - `output/data/cardinality/scale/`: scaled datasets (e.g., ×0.1, ×1).
  - `output/data/cardinality/update/`: selective deletion scenarios.
  - `output/data/distributional/column/`: column distribution shifts.
  - `output/data/distributional/outlier/`: rare value injections.
- **Workload drift outputs**
  - `output/workload/parametric/distribution/`: predicate distribution changes.
  - `output/workload/parametric/selectivity/`: workloads with varying selectivity.
  - `output/workload/tpcds_sqls_default.csv`: multi-table workload derived from TPC-DS.

---

## Case Study Gallery

All visuals were generated from notebooks in `driftbench/notebooks/` using the assets listed above.

- **Varying Cardinality**
  <p align="center">
    <img src="driftbench/notebooks/case_study/case_study_cardinality_scale_1.png" alt="Cardinality scale comparison (numeric)" width="600"/>
  </p>
  <p align="center">
    <img src="driftbench/notebooks/case_study/case_study_cardinality_scale_1_categorical.png" alt="Cardinality scale comparison (categorical)" width="600"/>
  </p>

- **Selective Deletions**
  <p align="center">
    <img src="driftbench/notebooks/case_study/case_study_cardinality_delete_5000.png" alt="Selective deletion effect (numeric)" width="600"/>
  </p>
  <p align="center">
    <img src="driftbench/notebooks/case_study/case_study_cardinality_delete_5000_categorical.png" alt="Selective deletion effect (categorical)" width="600"/>
  </p>

- **Column Distribution Shifts**
  <p align="center">
    <img src="driftbench/notebooks/case_study/case_study_census_original_skew_2.png" alt="Value skew impact (numeric)" width="600"/>
  </p>
  <p align="center">
    <img src="driftbench/notebooks/case_study/case_study_census_original_skew_2_categorical.png" alt="Value skew impact (categorical)" width="600"/>
  </p>

- **Outlier Injection**
  <p align="center">
    <img src="driftbench/notebooks/output_histograms/case_study_histogram_age_comparison.png" alt="Outlier injection histogram" width="600"/>
  </p>

- **Workload Drift**
  <p align="center">
    <img src="driftbench/notebooks/case_study/case_study_predicate_center.png" alt="Predicate distribution shift" width="600"/>
  </p>
  <p align="center">
    <img src="driftbench/notebooks/case_study/case_study_predicate_range_size.png" alt="Predicate selectivity shift" width="600"/>
  </p>
  <p align="center">
    <img src="driftbench/notebooks/case_study/tsne_pred_payload.png" alt="Predicate and payload t-SNE" width="600"/>
  </p>

- **Q-Error Benchmarks**
  <p align="center">
    <img src="driftbench/notebooks/case_study/case_study_pg.png" alt="PostgreSQL Q-error distribution" width="600"/>
  </p>
  <p align="center">
    <img src="driftbench/notebooks/case_study/case_study_naru.png" alt="Naru Q-error distribution" width="600"/>
  </p>
  <p align="center">
    <img src="driftbench/notebooks/case_study/case_study_mscn.png" alt="MSCN Q-error distribution" width="600"/>
  </p>

- **Join-Aware Drift Templates**
  <details>
  <summary>Click to expand a generated multi-table template</summary>

  ```json
  {
    "template_id": "T000",
    "cardinality": 1441548,
    "tables": {
      "base": "public.catalog_sales",
      "joins": [
        {
          "type": "FULL JOIN",
          "table": "public.store_sales",
          "condition": "public.catalog_sales.cs_net_profit = public.store_sales.ss_net_profit"
        }
      ]
    },
    "predicate": [
      {
        "column": "public.catalog_sales.cs_warehouse_sk",
        "operator": "<=",
        "type": "numeric",
        "value": "",
        "range": {
          "min": 1,
          "max": 5
        },
        "selectivity": 0.1
      }
    ],
    "payload": {
      "columns": [
        "public.catalog_sales.cs_order_number"
      ],
      "aggregation": null,
      "order_by": "public.catalog_sales.cs_order_number",
      "limit": 100
    }
  }
  ```
  </details>

---

## Installation

DriftBench requires **Python 3.10 / 3.11 / 3.12** (3.13 not yet supported).

```bash
# from PyPI (after a release tag is published)
pip install driftbench-db

# from source (current state of main)
pip install git+https://github.com/Liuguanli/DriftBench.git

# editable / development
git clone https://github.com/Liuguanli/DriftBench.git
cd DriftBench
pip install -e .
```

> The PyPI distribution is named **`driftbench-db`** to disambiguate from
> existing PyPI projects. The Python import name is still `driftbench`
> — same package, different convention, like `scikit-learn` / `sklearn`.

A single `pip install` brings in:

- The `driftbench` engine and its public Python API.
- The `driftbench` CLI (entry point: `driftbench`).
- The `driftbench-service` HTTP server (entry point: `driftbench-service`).
- The `driftbench-mcp` MCP server (entry point: `driftbench-mcp`).

Optional: a PostgreSQL instance if you plan to run the postgres-backed examples (`psycopg2-binary` is already pulled in).

---

## Quickstart Workflow

After install, the bundled CLI exposes the full P0 workflow:

```bash
# 1) Validate a spec without executing it
driftbench validate-spec driftspec/examples/demo_data_single.yaml --json

# 2) Preview the planned stages
driftbench dry-run driftspec/examples/demo_data_single.yaml --json

# 3) Execute the spec end-to-end
driftbench run-yaml driftspec/examples/demo_data_single.yaml

# 4) Convert a real trace into a runnable spec
driftbench trace-to-spec \
  driftspec/trace_inputs/trace_data_mock.csv \
  driftspec/generated/from_trace.yaml \
  --trace-type data

# 5) List generated outputs for inspection / automation
driftbench list-outputs --root output --glob "**/*.csv" --limit 20 --json
```

Bring the HTTP service up with `driftbench-service --port 8000` and the MCP server with `driftbench-mcp`. See [`docs/mcp_config_example.json`](docs/mcp_config_example.json) for an MCP client configuration template.

Substitute any of the YAML files in [`driftspec/examples/`](driftspec/examples/) to explore alternative drift scenarios.

---

## Tutorials

Three end-to-end walkthroughs you can copy-paste verbatim. Each assumes
`pip install driftbench-db` is already done (Python 3.10 / 3.11 / 3.12).

### Tutorial 1 — Drift your own CSV in 60 seconds (no clone, no agent)

The wheel does not bundle the `driftspec/examples/` YAML fixtures, so the
self-contained way is to write a tiny spec inline against a CSV you already
have.

```bash
mkdir -p drift_demo && cd drift_demo

# 1) The data we want to drift
cat > sales.csv <<'CSV'
sale_id,store_id,amount,quantity,sale_date
1,3,49.99,2,2024-01-15
2,1,12.50,1,2024-01-16
3,2,89.00,3,2024-01-17
4,1,25.75,1,2024-01-18
5,3,150.00,5,2024-01-19
CSV

# 2) A minimal DriftSpec — two stages: scale 2x + skew the amount column
cat > sales_drift.yaml <<'YAML'
pattern_id: tutorial-sales-drift
seed: 42
type:
  family: data
  category: drift
  subtype: single_table
data_source:
  kind: csv
  path: ./sales.csv
  schema_extractor:
    source_type: csv
    sample_size: 5
    schema_output_path: ./sales_schema.json
variables:
  base_table: sales
  drifts:
    - name: scale_2x
      drift_type: vary_cardinality
      output_path: ./out/sales_2x.csv
      scale: 2
    - name: skew_amount
      drift_type: value_skew
      output_path: ./out/sales_skewed.csv
      columns: ["amount"]
      portion: 1.0
      skewness: 2
YAML

# 3) Validate, dry-run, then execute
driftbench validate-spec sales_drift.yaml --json
driftbench dry-run sales_drift.yaml --json
driftbench run-yaml sales_drift.yaml

# 4) Inspect the drifted artifacts
ls out/
wc -l sales.csv out/sales_2x.csv      # 5 -> 10 rows after 2x scaling
```

### Tutorial 2 — Vibe coding with Cursor / Claude Code (MCP)

Skip writing YAML by hand: an MCP-aware agent can use `extract_schema`,
`build_spec`, `validate_spec`, and `run_spec` to author and execute a spec
from a natural-language prompt.

**Configure the MCP client.** In Cursor or Claude Code, add to your
`mcp.json`:

```json
{
  "mcpServers": {
    "driftbench": {
      "command": "driftbench-mcp"
    }
  }
}
```

If the client cannot find `driftbench-mcp` on its `PATH` (Cursor and
Claude Desktop sometimes launch with a stripped environment), use the
absolute path inside your venv. Get it with `which driftbench-mcp`:

```json
{
  "mcpServers": {
    "driftbench": {
      "command": "/abs/path/to/your/venv/bin/driftbench-mcp"
    }
  }
}
```

**Drive it in natural language.** Example prompts that exercise the full
chain:

> Inspect `~/Documents/sales.csv` with driftbench (extract_schema), then
> propose a single-table drift spec that scales cardinality 2x and skews
> the amount column. Build, validate, and run it.

> Run `extract_schema` on `data/census_original.csv`, then `build_spec` a
> multi-stage spec covering cardinality + value-skew + outlier injection.
> Save under `driftspec/generated/` and dry-run.

> Convert `~/traces/redbench_slice.csv` into a workload-drift spec via
> `trace_to_spec`, then run it and report row counts per stage.

The agent will call the MCP tools in sequence, write the YAML to disk,
and report run summaries (paths, row counts) inline. Since 0.1.0b2 the
MCP server accepts any absolute path, so your data does not need to live
inside the DriftBench install directory.

The full tool inventory is: `driftbench_health`, `trace_to_spec`,
`validate_spec`, `dry_run_spec`, `run_spec`, `list_outputs`,
`extract_schema`, `build_spec`, `save_spec`, `list_public_specs`,
`import_spec_and_run`.

### Tutorial 3 — Run a curated example spec

The repository ships several mature DriftSpecs under
[`driftspec/examples/`](driftspec/examples/). For pip-installed users
who don't want to clone the whole repo, fetch them from the raw URL:

```bash
mkdir -p data
curl --create-dirs -o data/census_original.csv \
  https://raw.githubusercontent.com/Liuguanli/DriftBench/main/data/census_original.csv
curl --create-dirs -o data/census_outliers.csv \
  https://raw.githubusercontent.com/Liuguanli/DriftBench/main/data/census_outliers.csv
curl -O https://raw.githubusercontent.com/Liuguanli/DriftBench/main/driftspec/examples/demo_data_single.yaml

driftbench validate-spec demo_data_single.yaml --json
driftbench run-yaml demo_data_single.yaml
ls output/data/                 # cardinality / distributional outputs
```

Other curated specs you can substitute for `demo_data_single.yaml`:

| Spec | What it demonstrates |
|---|---|
| [`demo_data_census_time_patterns.yaml`](driftspec/examples/demo_data_census_time_patterns.yaml) | Uniform / periodic / trend / long-tail temporal arrival patterns |
| [`workload_census.yaml`](driftspec/examples/workload_census.yaml) | Workload templates × time × selectivity (full SQL workload) |
| [`demo_postgres.yaml`](driftspec/examples/demo_postgres.yaml) | Same workflow shape but from a live PostgreSQL source |
| [`demo_template_mix_drift.yaml`](driftspec/examples/demo_template_mix_drift.yaml) | Distribution + structural + selectivity drift composed in one workload |

### Tutorial 4 — Embed the Python API in your own project

If you'd rather call DriftBench from your existing pipeline, the public
API is intentionally small:

```python
from driftbench import (
    load_and_validate_spec,
    run_spec,
    run_spec_and_return_summary,
    trace_to_spec,
    get_schema_extractor,
)

# Validate without executing
spec, type_info = load_and_validate_spec("sales_drift.yaml")
print(type_info)   # {'family': 'data', 'category': 'drift', 'subtype': 'single_table'}

# Execute end-to-end (returns a structured summary)
summary = run_spec_and_return_summary("sales_drift.yaml")

# Derive a spec from a trace summary
trace_to_spec(
    "/path/to/trace.csv",
    "driftspec/generated/from_trace.yaml",
    trace_type="data",
)

# Use a schema extractor directly
extractor = get_schema_extractor("csv", csv_path="sales.csv", sample_size=100)
schema = extractor.extract_schema()
```

Stick to imports from `driftbench` or `driftbench.api`. Internal modules
under `driftbench.core.*` are not part of the supported surface and may
move between releases.
