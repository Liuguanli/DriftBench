# DriftSpec examples

Start with the two paper-ready, executable TPC-H examples:

- [Data distribution and synthetic schedule drift](paper_tpch_data_drift.yaml)
- [Query template, Python qgen-style parameter, and synthetic schedule drift](paper_tpch_query_workload_drift.yaml)

Both use registered DriftBench handlers and a fixed seed of `42`. They run from a
source checkout with local inputs only; they do not download data or connect to a
database. The concise canonical paper examples and their Visualization traceability
are documented in [`paper/README.md`](paper/README.md).

## What the flagship examples produce

| Example | Baseline | Drifted phase | Outputs |
| --- | --- | --- | --- |
| Data | Original TPC-H `lineitem` rows with a Day 1 uniform 60 rows/minute schedule | The same row count after `l_extendedprice` skew, with a Day 2 periodic nominal 180 rows/minute schedule | Baseline, skew intermediate, drifted, and sorted two-phase timeline CSVs |
| Query workload | Q1/Q3/Q6/Q11/Q14, 20 instances each, qgen-style scale `0.01`, Day 1 uniform 60 queries/minute | Q2/Q5/Q8/Q11/Q21, 20 instances each, qgen-style scale `1.0`, Day 2 periodic nominal 180 queries/minute | Two temporal CSVs with `timestamp,sql` columns |

The query example uses the TPC-H templates and `dists.dss` packaged under
`driftbench/data/resources/tpch/`. Its parameter sampler is implemented in Python;
it does not execute a native `qgen` binary.

## How YAML becomes data and files

`driftbench.api.run_spec()` is a recipe executor, not a benchmark data loader. Its
actual path is:

1. Open the YAML as UTF-8 and parse it with PyYAML `yaml.safe_load()` into Python
   mappings and lists.
2. Recursively replace exact scalar-string `${NAME}` bindings, including mapping
   keys. It does not expand environment variables or interpolate a placeholder
   embedded inside a larger string. Missing and unused bindings fail.
3. Apply the current migration default and shallow structural validation, then save
   and seed the process-global Python `random` and NumPy RNG states from the YAML's
   top-level `seed`.
4. Convert `type.family`, `type.category`, and `type.subtype` into a type triple and
   select its registered handler.
5. The handler reads explicitly configured inputs—local CSV/schema/template files
   and, for supported handlers, PostgreSQL schema sources—or creates an in-memory
   workload sample. Depending on the handler, ordinary Python, Pandas, and NumPy
   perform the sampling and transformation.
6. The handler writes the configured `output_path` or output paths, usually CSV or
   JSON, and `run_spec()` restores the caller's Python and NumPy RNG states.
   Relative paths are resolved against the process's current working directory,
   not the YAML file's directory.

Baseline generation is a separate phase. A YAML field such as
`data_source: {kind: benchmark_adapter, benchmark: tpch}` supplies benchmark identity
for deep preflight and provenance. **`benchmark_adapter` does not generate data** at
runtime, and `run_spec()` does not call the adapter's `.generate()` method. Likewise,
`variables.baseline` in a `template_mix` YAML is a probability
distribution used to sample the comparison workload. It does not create benchmark
tables or measure a performance baseline, and it does not automatically create a
separate `baseline.json`.

For a data-drift YAML, first import and call the appropriate adapter factory—for
example, `from driftbench.data.ycsb import data` followed by
`data(scale_factor=1).generate(output_dir="artifacts")`—or provide audited existing
files. Then bind those explicit input and output paths into `run_spec()`. The current
`run-yaml` CLI does not accept placeholder bindings, so the examples below use the
public Python API. See the canonical
[provenance and execution boundaries](../../docs/benchmark_reference.md#provenance-conformance-and-execution-boundaries)
for the adapter conformance, TPC-H toolchain, FK-integrity, and real-database limits.

## Validate

Validation does not resolve paths or execute either workflow:

```bash
python -m driftbench.cli validate-spec driftspec/examples/paper_tpch_data_drift.yaml --json
python -m driftbench.cli validate-spec driftspec/examples/paper_tpch_query_workload_drift.yaml --json
```

The current CLI summary reports `declared_outputs: 0` for the query example
because it does not count nested `outputs[*].path` entries. This is a reporting
limitation; execution still writes the two configured temporal CSVs.

## Reproduce through the public Python API

Bindings replace scalar `${NAME}` placeholders exactly. The data input must be a
local CSV with a header and an `l_extendedprice` column. The following paths are
relative to the repository root and are examples; create the input CSV before
running the code.

```python
from pathlib import Path

from driftbench.api import run_spec

work = Path("work/paper-tpch")
resources = Path("driftbench/data/resources/tpch")

run_spec(
    "driftspec/examples/paper_tpch_data_drift.yaml",
    bindings={
        "DRIFTBENCH_INPUT": work / "lineitem.csv",
        "DRIFTBENCH_SCHEMA": work / "lineitem.schema.json",
        "DRIFTBENCH_BASELINE_OUTPUT": work / "data-baseline.csv",
        "DRIFTBENCH_SKEW_INTERMEDIATE": work / "data-skew.csv",
        "DRIFTBENCH_DRIFTED_OUTPUT": work / "data-drifted.csv",
        "DRIFTBENCH_COMBINED_OUTPUT": work / "data-timeline.csv",
    },
)

run_spec(
    "driftspec/examples/paper_tpch_query_workload_drift.yaml",
    bindings={
        "DRIFTBENCH_TPCH_TEMPLATE_DIR": resources / "queries",
        "DRIFTBENCH_TPCH_DISTS_FILE": resources / "dists.dss",
        "DRIFTBENCH_BASELINE_OUTPUT": work / "query-baseline.csv",
        "DRIFTBENCH_DRIFTED_OUTPUT": work / "query-drifted.csv",
    },
)
```

Run into a second output directory and compare file hashes to audit reproducibility.
With identical input bytes, bindings, packaged resources, seed, and supported
environment, each generated CSV is byte-stable.

## Interpretation limits

These workflows create synthetic input and scheduling labels. They do not establish
real arrival duration, predicate selectivity, database execution, ingestion behavior,
performance, or causal effects. The query CSVs do not serialize query IDs, and generated
SQL is not evidence that a target database executed it successfully.
