# TPC-H integration

This folder wires the TPC-H dbgen SQL templates into DriftBench via a DriftSpec.

Run the sample spec:

```bash
python -m driftbench.cli run-yaml driftspec/integration/tpch/specs/tpch_workload.yaml
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
- Supported types: `int_range`, `float_range`, `choice`, `fixed`, `date_range`.

Output options:
- `type: workload`: write all SQLs into one file at `path`.
- `type: split`: write one file per query at directory `path`.
  - `filename_template` supports `{query_id}` and `{index}` (1-based).

Outputs land in `driftspec/integration/tpch/output/` by default.
