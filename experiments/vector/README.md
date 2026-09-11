# Vector Database Experiments

**Website applicability:** [Proposed](https://driftbench.com/where-drift-applies)
**Repository progress:** 0 existing; 5 stubs

The website lists these typical drift dimensions: embedding distribution, data growth,
query-vector distribution, and filter selectivity.

## Components

- [ANN index](ann_index/README.md)
- [Quantization](quantization/README.md)
- [Reranking](reranking/README.md)
- [Ingestion](ingestion/README.md)
- [Partitioning](partitioning/README.md)

## Metrics from the scope map

Recall@K, QPS, p95 latency, rebuild cost, and freshness.

## Related website scenarios

- [Indexing under Drift](https://driftbench.com/scenarios#indexing)
- [Multi-target Regression](https://driftbench.com/scenarios#multi-target)

`Proposed` is a research-direction label, not an implementation or validation claim.
All vector components are documentation stubs. Component progress is maintained in the
[central tracker](../README.md#progress-tracker).
