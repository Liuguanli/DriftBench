# Multi-Table Drift (FK-Aware)

This mode keeps referential integrity by applying drift steps with explicit
relationships (fact -> dimension). It avoids independent per-table edits.

Example DriftSpec:

```yaml
pattern_id: demo-multi-table
seed: 42

type:
  family: data
  category: drift
  subtype: multi_table

variables:
  validate_integrity: true

  tables:
    - name: customers
      path: ./data/customers.csv
      format: csv
      key_column: c_custkey
      output_path: ./output/data/customers_drift.csv
    - name: orders
      path: ./data/orders.csv
      key_column: o_orderkey
      output_path: ./output/data/orders_drift.csv

  relationships:
    - name: orders_customer
      fact: orders
      fk: o_custkey
      dim: customers
      pk: c_custkey

  drift_steps:
    - op: delete_keys
      target: customers
      key_column: c_custkey
      fraction: 0.05
      propagate:
        relationship: orders_customer
        policy: drop  # or reassign

    - op: reassign_fk
      relationship: orders_customer
      fraction: 0.10
```

Notes:
- `delete_keys` removes dimension keys, then propagates to fact via `policy`.
- `reassign_fk` rewires a fraction of fact rows to valid dimension keys.
- `scale_tables` clones the full dataset with key offsets (preserves FK integrity).
- `scale_sample` scales by sampling existing rows with replacement (preserves distribution).
- `add_dimension_keys` increases dimension cardinality and can reassign fact FKs.
- `skew_fk` skews FK distribution in the fact table (no PK changes).
- `insert_outliers` appends rows with extreme values in a numeric column.
- `rewrite_columns` rewrites columns (template, numeric_jitter, categorical_resample).
- `validate_integrity` checks FK values remain in the dimension PK set.

File formats:
- For `.tbl` inputs, set `format: tbl`, `delimiter: "|"`, provide `columns`,
  and `drop_last_empty: true` to remove the trailing pipe column.
- If you have a DDL file, you can derive columns automatically:
  ```yaml
  variables:
    ddl_path: ./existing_benchmarks/TPC-H V3.0.1/dbgen/dss.ddl
    use_ddl_columns: true
  ```
