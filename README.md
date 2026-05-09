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
# from source (current state of main)
pip install git+https://github.com/Liuguanli/DriftBench.git

# editable / development
git clone https://github.com/Liuguanli/DriftBench.git
cd DriftBench
pip install -e .
```

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

Bring the HTTP service up with `driftbench-service --port 8000` and the MCP server with `driftbench-mcp` (or via [`scripts/run_driftbench_mcp.sh`](scripts/run_driftbench_mcp.sh) for Cursor / Claude Code MCP clients — see [`docs/mcp_config_example.json`](docs/mcp_config_example.json)).

Substitute any of the YAML files in [`driftspec/examples/`](driftspec/examples/) to explore alternative drift scenarios.
