# TPC-H integration

This folder wires the TPC-H dbgen SQL templates into DriftBench via a DriftSpec.

Run the sample spec:

```bash
python -m driftbench.cli run-yaml driftspec/integration/tpch/specs/tpch_workload.yaml
```

Run dedicated specs:

```bash
python -m driftbench.cli run-yaml driftspec/integration/tpch/specs/tpch_workload_qgen.yaml
python -m driftbench.cli run-yaml driftspec/integration/tpch/specs/tpch_workload_custom.yaml
```

Run TPC-H multi-table data drift specs (BenchBase .tbl inputs):

```bash
python -m driftbench.cli run-yaml driftspec/integration/tpch/specs/tpch_data_scale.yaml
python -m driftbench.cli run-yaml driftspec/integration/tpch/specs/tpch_data_cardinality.yaml
python -m driftbench.cli run-yaml driftspec/integration/tpch/specs/tpch_data_skew.yaml
python -m driftbench.cli run-yaml driftspec/integration/tpch/specs/tpch_data_outliers.yaml
```

Schema resolution (DDL):
- These specs use `ddl_path` + `use_ddl_columns: true` to derive column lists
  from `existing_benchmarks/TPC-H V3.0.1/dbgen/dss.ddl` before loading `.tbl`.

Python entrypoint (equivalent):

```python
from driftbench.spec.core import run_all
run_all("driftspec/integration/tpch/specs/tpch_workload.yaml")
```

Notes on parameters:
- `params` maps query id -> ordered list of parameter definitions.
- Each list entry matches `:1`, `:2`, ... in the SQL template.
- Values are inserted as-is; keep quotes in the template files.
- Supported types: `int_range`, `float_range`, `choice`, `fixed`, `date_range`, `dss_dist`.
- `dss_dist` reads weighted tokens from `dists.dss`:
  ```yaml
  - type: dss_dist
    dist_file: ./existing_benchmarks/TPC-H V3.0.1/dbgen/dists.dss
    dist_name: msegmnt
  ```
  Use `nations2` if you need a uniform nations list (the `nations` distribution includes non-positive weights).

Parameter modes:
- `param_mode: custom` (default) uses `params` from the YAML.
- `param_mode: qgen` uses built-in qgen-like rules (Q1–Q22).
  Optionally set `qgen_dist_file` to point at `dists.dss` and `qgen_scale`
  to control scale-sensitive parameters (e.g., Q11).

Output options:
- `type: workload`: write all SQLs into one file at `path`.
- `type: split`: write one file per query at directory `path`.
  - `filename_template` supports `{query_id}` and `{index}` (1-based).

Outputs land in `driftspec/integration/tpch/output/` by default.

Outlier placement rationale (TPC-H):
- `l_extendedprice` appears in payload/aggregates, not in predicates, by design.
  It primarily affects revenue-like aggregates while keeping query selectivity stable.
- To test predicate-sensitive drift, inject outliers into filter columns such as
  `l_quantity`, `l_discount`, `l_shipdate`, or `o_orderdate`.
- To test join/FK-sensitive drift, use key skew or new keys while preserving
  referential integrity (for example, `skew_fk` on `orders_customer` or
  `add_dimension_keys` on `customer` with `reassign`).
- If you want `l_extendedprice` in a predicate, add it in a custom workload
  template (this deviates from the standard TPC-H semantics).
