# Planned experiment matrix

Every row is review-only. `Existing / verification pending` means files already
exist in `experiments/`; it does not mean the Docker stack or results work.
`Stub` rows have no Compose profile and cannot be run.

| ID | GitHub benchmark/tool (pinned in manifest) | Dataset and bounded workload | Planned stack | Tier / status |
|---|---|---|---|---|
| `rel-ce-tpch` | gregrahn/tpch-kit + estimator registry | TPC-H-derived SF1; deterministic Q3/Q5/Q9 SPJ probes; static and adaptive CE arms | PostgreSQL + offline-first Python runner | local-lite review plan; runtime verification pending |
| `rel-optimizer-job` | gregrahn/join-order-benchmark | IMDb/JOB; fixed SQL set, query-mix/selectivity phases, JSON plans | PostgreSQL + runner | devbox-only; Stub |
| `rel-indexes-sosd` | rmitbggroup/LearnedIndexDiskExp | deterministic 1M sample of SOSD OSM keys; point/range uniform, Zipf, hotspot | native C++ runner | local-lite after sample exists; Existing / verification pending |
| `rel-execution-tpch` | gregrahn/tpch-kit | TPC-H SF0.1; Q1/Q6/Q12; operator/buffer/spill evidence | PostgreSQL + runner | local-lite; Stub |
| `rel-buffer-sysbench` | akopytov/sysbench | synthetic 4×250K rows; working set fits/exceeds cache; read-only hotspot shift | PostgreSQL + sysbench runner | local-lite; Stub |
| `rel-transactions-tpcc` | cmu-db/benchbase | TPC-C lite, 2 warehouses/4 terminals; baseline, hotspot and mix shift | PostgreSQL + Java runner | local-lite; Stub |
| `vec-ann-sift` | erikbern/ann-benchmarks | SIFT1M deterministic 100K subset; exact vs selected ANN, K=10 | CPU runner | local-lite; Stub |
| `vec-quantization-sift` | facebookresearch/faiss | SIFT 100K; Flat, SQ8, PQ under distribution/outlier drift | CPU runner | local-lite; Stub |
| `vec-reranking-scifact` | beir-cellar/beir | SciFact; immutable top-100 candidates and a fixed reranker | CPU runner | local-lite; Stub |
| `vec-ingestion-qdrant` | zilliztech/VectorDBBench + qdrant/qdrant | SIFT 100K; bounded batch ingestion plus fixed-rate search | Qdrant + runner | local-lite; Stub |
| `vec-partition-qdrant` | VectorDBBench + Qdrant | 1M-vector plan; balanced vs Zipf partition keys and routing | 3 Qdrant nodes + runner | devbox-only; Stub |

## Why these choices

- PostgreSQL is shared as the relational target to avoid maintaining several DB
  configurations. Each run still gets a dedicated, disposable database state.
- SIFT is reused for ANN, quantization, ingestion, and partition studies so one
  verified cache can later serve several questions without mixing result files.
- The local index experiment remains a learned/B-tree/PGM study. It is not
  relabelled as a vector ANN benchmark.
- Qdrant partition evidence stays devbox-only because a single node cannot
  substantiate cross-node balance or routing claims.
- JOB/IMDb remains devbox-only because the full prepared database is well above
  the 4 GiB local-lite memory envelope.
- The CE row is explicitly TPC-H-derived, not an official TPC-H score. Only
  PostgreSQL native-statistics controls are plan anchors; academic methods remain
  adapter candidates until separately reproduced on the common probe contract.

## Inputs, execution, and evidence

Each manifest records the 40-character source commit, source/dataset license
status, expected sizes, seed, warmup, repetitions, exact future method, image
digest, service resources, metrics, and blockers. Checksums remain
`pending-not-downloaded` until a later approved prepare step computes them.

All runs write outside the repository:

```text
${DRIFTBENCH_LAB_STATE}/cache/<dataset>/
${DRIFTBENCH_LAB_STATE}/runs/<experiment>/<run-id>/logs/
${DRIFTBENCH_LAB_STATE}/runs/<experiment>/<run-id>/results/
${DRIFTBENCH_LAB_STATE}/runs/<experiment>/<run-id>/metrics/
${DRIFTBENCH_LAB_STATE}/runs/<experiment>/<run-id>/figures/
```

The local page reads `status.json` and `events.ndjson`. Optional Prometheus and
Grafana consume explicitly emitted metrics. Grafana PNGs go to `figures/`, but
raw logs/results and the final `run-manifest.json` remain authoritative.
