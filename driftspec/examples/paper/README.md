# Paper-ready DriftSpec examples

This directory contains exactly five concise paper examples. Each parsed YAML
payload is identical to its linked canonical Visualization spec; the numbered
comments add scientific context without changing execution semantics.

For richer executable TPC-H workflows, use the root-level
[data drift](../paper_tpch_data_drift.yaml) and
[query-workload drift](../paper_tpch_query_workload_drift.yaml) examples. Their
bindings and offline reproduction steps are in the [examples guide](../README.md).

## Traceability

### Data value skew

- Paper spec: [`data_value_skew.yaml`](data_value_skew.yaml)
- Canonical spec: [TPC-H `price_skew`](../../../visualization/specs/data/tpch/price_skew.yaml)
- Figure: [distribution comparison](../../../visualization/figures/data/tpch/price_skew.png)
- Manifest: [generation evidence](../../../visualization/manifests/data/tpch/price_skew.json)
- Gallery entry: [TPC-H / `price_skew`](../../../visualization/GALLERY.md#tpc-h-tpch)

### Data outlier injection

- Paper spec: [`data_outlier_injection.yaml`](data_outlier_injection.yaml)
- Canonical spec: [TPC-DS `price_outliers`](../../../visualization/specs/data/tpcds/price_outliers.yaml)
- Figure: [distribution comparison](../../../visualization/figures/data/tpcds/price_outliers.png)
- Manifest: [generation evidence](../../../visualization/manifests/data/tpcds/price_outliers.json)
- Gallery entry: [TPC-DS / `price_outliers`](../../../visualization/GALLERY.md#tpc-ds-tpcds)

### Data cardinality change

- Paper spec: [`data_cardinality_change.yaml`](data_cardinality_change.yaml)
- Canonical spec: [YCSB `record_cardinality_growth`](../../../visualization/specs/data/ycsb/record_cardinality_growth.yaml)
- Figure: [row-count comparison](../../../visualization/figures/data/ycsb/record_cardinality_growth.png)
- Manifest: [generation evidence](../../../visualization/manifests/data/ycsb/record_cardinality_growth.json)
- Gallery entry: [YCSB / `record_cardinality_growth`](../../../visualization/GALLERY.md#ycsb-ycsb)

### FK-safe selective deletion

- Paper spec: [`data_fk_safe_selective_deletion.yaml`](data_fk_safe_selective_deletion.yaml)
- Canonical spec: [JOB `post_2000_title_deletion`](../../../visualization/specs/data/job/post_2000_title_deletion.yaml)
- Figure: [distribution comparison](../../../visualization/figures/data/job/post_2000_title_deletion.png)
- Manifest: [generation evidence](../../../visualization/manifests/data/job/post_2000_title_deletion.json)
- Gallery entry: [JOB / `post_2000_title_deletion`](../../../visualization/GALLERY.md#job-job)

### Query template mix

- Paper spec: [`query_template_mix.yaml`](query_template_mix.yaml)
- Canonical spec: [TPC-H `complexity_mix_shift`](../../../visualization/specs/query/tpch/complexity_mix_shift.yaml)
- Figure: [workload comparison](../../../visualization/figures/query/tpch/complexity_mix_shift.png)
- Manifest: [generation evidence](../../../visualization/manifests/query/tpch/complexity_mix_shift.json)
- Gallery entry: [TPC-H / `complexity_mix_shift`](../../../visualization/GALLERY.md#tpc-h-tpch)

## Validate and reproduce

Validate an example from the repository root:

```bash
python -m driftbench.cli validate-spec driftspec/examples/paper/data_value_skew.yaml --json
```

The `${NAME}` values are exact scalar placeholders, not environment-variable
expansion. Execute bound specs through the public `driftbench.api.run_spec` API.
The root-level [examples guide](../README.md) provides a complete local invocation.
The current `run-yaml` command does not accept placeholder bindings. Source
distributions include `driftspec/examples/`; installed wheels do not expose this
tree as a package resource.

## Interpretation limits

The root-level `driftspec/examples/*.yaml` files also contain broader or legacy
demos; `demo_template_mix_drift.yaml` is not the canonical
`workload/drift/template_mix` example. These examples compare generated input or
query-workload distributions. They do not establish predicate selectivity,
observed traffic, database execution, performance, or causality. The YCSB
cardinality example does not claim database loadability or key integrity.
