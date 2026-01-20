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
- `param_mode: qgen` uses built-in qgen-like rules (currently Q1–Q3).
  Optionally set `qgen_dist_file` to point at `dists.dss`.

Output options:
- `type: workload`: write all SQLs into one file at `path`.
- `type: split`: write one file per query at directory `path`.
  - `filename_template` supports `{query_id}` and `{index}` (1-based).

Outputs land in `driftspec/integration/tpch/output/` by default.
