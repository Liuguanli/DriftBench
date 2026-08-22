# DriftSpec examples

Start with the two paper-ready, executable TPC-H examples:

- [Data distribution and synthetic schedule drift](paper_tpch_data_drift.yaml)
- [Query template, qgen-parameter, and synthetic schedule drift](paper_tpch_query_workload_drift.yaml)

Both use registered DriftBench handlers and a fixed seed of `42`. They run from a
source checkout with local inputs only; they do not download data or connect to a
database. The concise canonical paper examples and their Visualization traceability
are documented in [`paper/README.md`](paper/README.md).

## What the flagship examples produce

| Example | Baseline | Drifted phase | Outputs |
| --- | --- | --- | --- |
| Data | Original TPC-H `lineitem` rows with a Day 1 uniform 60 rows/minute schedule | The same row count after `l_extendedprice` skew, with a Day 2 periodic nominal 180 rows/minute schedule | Baseline, skew intermediate, drifted, and sorted two-phase timeline CSVs |
| Query workload | Q1/Q3/Q6/Q11/Q14, 20 instances each, qgen scale `0.01`, Day 1 uniform 60 queries/minute | Q2/Q5/Q8/Q11/Q21, 20 instances each, qgen scale `1.0`, Day 2 periodic nominal 180 queries/minute | Two temporal CSVs with `timestamp,sql` columns |

The query example uses the TPC-H templates and `dists.dss` packaged under
`driftbench/data/resources/tpch/`.

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
