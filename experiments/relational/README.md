# Relational Database Experiments

**Website applicability:** [Supported](https://driftbench.com/where-drift-applies)
**Repository progress:** 2 existing / verification pending; 4 stubs

The website lists these typical drift dimensions: data volume, skew, correlation,
query mix, and read/write ratio.

## Components

- [Cardinality estimation](cardinality_estimation/README.md)
- [Optimizer](optimizer/README.md)
- [Indexes](indexes/README.md)
- [Execution engine](execution_engine/README.md)
- [Buffer pool](buffer_pool/README.md)
- [Transactions](transactions/README.md)

## Metrics from the scope map

q-error, plan change, p95–p99 latency, TPS, cache hit rate, and abort rate.

## Related website scenarios

- [TPC-H Cardinality Drift](https://driftbench.com/scenarios#tpch)
- [JOB Join/Workload Drift](https://driftbench.com/scenarios#job)
- [TPC-C Hotspot Drift](https://driftbench.com/scenarios#tpcc)
- [Indexing under Drift](https://driftbench.com/scenarios#indexing)
- [Multi-target Regression](https://driftbench.com/scenarios#multi-target)

`Supported` is a website applicability label, not proof that every listed component is
implemented or that any database performance result has been validated. Component
progress is maintained in the [central tracker](../README.md#progress-tracker).
