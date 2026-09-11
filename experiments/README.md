# Database Experiments

This directory replaces the former top-level `tasks/` directory with a hierarchy that
describes what each experiment targets:

```text
experiments/<database-family>/<component>/
```

The initial taxonomy is copied from DriftBench's
[Where Drift Applies](https://driftbench.com/where-drift-applies) page as observed on
2026-08-23. The initial catalog intentionally includes only the relational and vector families.
Website applicability describes DriftBench's public scope map; repository progress
describes what is actually present here. They are not interchangeable.

## Progress tracker

This table is the single source of truth for detailed component progress.

| Database family | Component | Website applicability | Repository progress | Current evidence / next step |
|---|---|---|---|---|
| Relational | [Cardinality estimation](relational/cardinality_estimation/README.md) | Supported | Offline TPC-H-derived plan / runtime verification pending | SF1 SPJ probe and estimator-registry contracts exist; database execution and every academic adapter remain review-blocked. |
| Relational | [Optimizer](relational/optimizer/README.md) | Supported | Stub | Define the first optimizer scenario and its plan-change evidence. |
| Relational | [Indexes](relational/indexes/README.md) | Supported | Existing / verification pending | Existing learned/PGM/B-tree experiment was migrated; verify external-tool provenance and repeatability before changing status. |
| Relational | [Execution engine](relational/execution_engine/README.md) | Supported | Stub | Define an execution-engine scenario, controlled inputs, and metrics. |
| Relational | [Buffer pool](relational/buffer_pool/README.md) | Supported | Stub | Define a working-set/cache scenario and a repeatable cache-evidence contract. |
| Relational | [Transactions](relational/transactions/README.md) | Supported | Stub | Define a contention or read/write-ratio scenario and abort/latency evidence. |
| Vector | [ANN index](vector/ann_index/README.md) | Proposed | Stub | Select an ANN target and define recall, latency, and rebuild evidence. |
| Vector | [Quantization](vector/quantization/README.md) | Proposed | Stub | Define a quantization comparison with accuracy and resource evidence. |
| Vector | [Reranking](vector/reranking/README.md) | Proposed | Stub | Define candidate generation, reranking inputs, and quality/latency evidence. |
| Vector | [Ingestion](vector/ingestion/README.md) | Proposed | Stub | Define a controlled growth stream and freshness/throughput evidence. |
| Vector | [Partitioning](vector/partitioning/README.md) | Proposed | Stub | Define placement drift, balance signals, and cross-partition evidence. |

Status vocabulary:

- `Stub`: documentation placeholder only; no component implementation or result evidence.
- `Existing / verification pending`: pre-existing experiment files are present, but this
  catalog has not established correctness, reproducibility, database execution, or
  performance.
- `Verified`: reserved for a future change with a named protocol and review evidence;
  no component currently has this status.

When progress changes, update this table first and keep the root README summary aligned.
Do not infer repository readiness from a website `Supported` label.

## Migration map

This is an intentional path change; no compatibility symlink is provided.

| Former path | Current path |
|---|---|
| `tasks/cardinality_estimation/` | [`experiments/relational/cardinality_estimation/`](relational/cardinality_estimation/README.md) |
| `tasks/indexing/` | [`experiments/relational/indexes/`](relational/indexes/README.md) |

## Families

- [Relational databases](relational/README.md)
- [Vector databases](vector/README.md)
