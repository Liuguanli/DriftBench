# DriftBench Benchmark Reference

Complete reference for all 9 benchmark adapters, including generated artifacts, query characteristics, and selection guidance.

Related docs: [README and quickstart](../README.md),
[target orchestration contract](benchmark_target_contract.md), and
[hands-on testing guide](benchmark_testing_guide.html).

---

## Provenance, Conformance, and Execution Boundaries

Unless a section explicitly says otherwise, these adapters create DriftBench
synthetic fixtures and workload artifacts for testing DriftBench itself. They are
not official, audited, or benchmark-spec-compliant implementations, and their
outputs must not be reported as official TPC, YCSB, JOB, DSB, or BenchBase scores.
The benchmark names identify the modeled workload family; they do not establish
conformance.

### TPC-H data and query provenance

TPC-H data and queries use separate generation paths:

- Data `mode="copy"` copies existing `.tbl` files. Data `mode="generate"` runs a
  `dbgen` executable, resolving it in this order: explicit `dbgen_path`, `PATH`,
  the repository-local TPC-H build, the DriftBench user cache, then a one-time
  clone and `make` of `electrum/tpch-dbgen` in that cache.
- The automatic clone follows an **unpinned revision**: DriftBench does not select
  a commit or tag. The manifest hashes generated table files, but it records
  neither the upstream revision nor the `dbgen` binary SHA-256. Consequently,
  auto-built output is not evidence of a fixed toolchain, cross-machine
  byte-for-byte reproducibility, or official TPC-H conformance.
- Query `mode="qgen"` is a DriftBench **Python qgen-style** parameter generator.
  It reads SQL templates and `dists.dss`, samples parameters with Python, and
  renders SQL. It does not execute the native or official `qgen` binary.

### How a DriftSpec YAML becomes files

The public `driftbench.api.run_spec()` path performs this sequence:

1. Open the YAML as UTF-8 and parse it with `yaml.safe_load()`.
2. Recursively replace exact scalar-string `${NAME}` bindings, including mapping
   keys. Partial string interpolation is not supported; missing and unused
   bindings fail.
3. Run `migrate_spec()`, then the current shallow `validate_spec()` check for the
   required `type` and `variables` fields.
4. Seed Python `random` and NumPy from the top-level `seed`, while saving and later
   restoring their process-global states.
5. Resolve the registered handler from the `(family, category, subtype)` type
   triple and invoke it, optionally with explicit `runtime_inputs` when that
   handler supports them.
6. The handler reads explicitly configured inputs—local files and, for supported
   handlers, PostgreSQL schema sources—or creates an in-memory sample. It applies
   its Python/Pandas/NumPy transformation and writes the configured `output_path`
   or output paths, commonly as CSV or JSON. Relative paths are resolved from the
   process's current working directory, not from the YAML file's directory.

This is distinct from benchmark baseline generation. In deep preflight,
`data_source.kind: benchmark_adapter` identifies a benchmark and checks whether
its adapter module can be imported. At runtime, **`benchmark_adapter` does not
generate data**, and `run_spec()` does not call the adapter's `.generate()` method.
Generate baseline files separately through an adapter factory—for example,
`from driftbench.data.ycsb import data; data(scale_factor=1).generate(output_dir="artifacts")`—or
bind existing files before running a data DriftSpec. Similarly,
`variables.baseline` in a workload `template_mix` spec is only a comparison
distribution from which the handler samples; it does not create a benchmark
dataset or a performance baseline, and it does not automatically create a separate
`baseline.json`.

### Conditional FK-safety

The paper JOB deletion example is FK-safe only within its explicitly modeled
multi-table operation. It loads 11 tables, declares 7 single-column relationships,
uses seed 42 to select the rounded 40% of unique non-null `title.id` candidates
whose `production_year >= 2001`, and requests direct `drop` propagation along the
4 declared relationships that point into `title`. With
`validate_integrity: true`, the final check covers all 7 declared relationships
and verifies that non-null fact-table FK values occur in the declared dimension
PK column.

That guarantee requires all relevant tables to be loaded and every affected
direct incoming edge to appear in both `variables.relationships` and the step's
`propagate` list with `drop` or `reassign`. It covers only declared relationships
and direct propagation. It does not discover a database schema, enforce other
database constraints, support composite FKs, find omitted tables or edges, or
perform recursive cascade propagation. Single-table `selective_deletion` is a
different operation and provides no FK cascade guarantee.

### Real-database coverage

The current **only real database gate** with no mocks is PostgreSQL 16
`select-only`: `pgbench -i -s 1` initializes the database, native
`pgbench -b select-only` is the baseline, and a DriftBench-generated script is the
candidate. Three paired rounds each run warmup plus measurement, and five opt-in
integration tests are forced on by the Benchmark Regression workflow. All other
adapter, Visualization, paper-example, and DriftSpec tests validate generated
files, manifests, handler behavior, distributions, or declared integrity only;
they do not prove database loadability, successful SQL execution, performance,
or benchmark conformance. BenchBase produces configuration and driver scripts,
not a local dataset or a DriftBench-run live-database gate.

---

## Quick Selection Guide

| Goal | Recommended Benchmark |
|------|-----------------------|
| Synthetic OLAP / query optimizer fixtures | TPC-H or TPC-DS |
| Synthetic OLTP transaction fixtures | TPC-C |
| OLTP with hot-spot / skew drift | TPC-C Skew |
| Join-order sensitivity / cardinality estimation | JOB |
| Key-value / NoSQL workloads | YCSB |
| Decision support with complex schemas | DSB |
| PostgreSQL throughput/regression gating | pgbench |
| Generate configs for an external Java benchmark driver | BenchBase |

---

## TPC-H

**Type:** OLAP · **Tables:** 8 · **Queries:** 22 templates

### Data features
| Table | Scale-factor rows (sf=1) | Notes |
|-------|--------------------------|-------|
| lineitem | ~6 M | Largest fact table; high I/O cost |
| orders | 1.5 M | — |
| customer | 150 K | — |
| part | 200 K | — |
| supplier | 10 K | — |
| partsupp | 800 K | — |
| nation | 25 | Fixed |
| region | 5 | Fixed |

### Query features
- 22 templates × configurable instances per template
- Range predicates, aggregations, nested subqueries, GROUP BY, ORDER BY
- Selectivity: low (Q6: ~2%) to high (Q1: full scan)
- Result cardinality: 1 row (Q1 total) to millions (Q3 top-N)

### Query categories
| Category | Query IDs |
|----------|-----------|
| Pricing / revenue | Q1, Q6, Q14 |
| Supplier / part | Q2, Q11, Q16, Q17, Q20 |
| Shipping / logistics | Q3, Q7, Q8, Q10, Q12 |
| Customer / market | Q4, Q5, Q9, Q13, Q18, Q22 |
| Forecasting | Q15, Q19, Q21 |

### Modes
- `mode="qgen"` — Python qgen-style parameterization using packaged templates and `dists.dss`; no native `qgen` binary
- `mode="custom"` — custom parameter specs via `param_specs` dict
- `mode="copy"` for data — copies `.tbl` files from `source_dir`
- `mode="generate"` for data — runs dbgen to produce `.tbl` files locally

### Python API
```python
from driftbench.data.tpch import data as tpch_data, queries as tpch_queries

tpch_data(scale_factor=10, mode="generate").generate(output_dir="./artifacts")
tpch_queries(query_ids=[1, 6, 14], queries_per_template=5, mode="qgen").generate(output_dir="./artifacts")
```

---

## TPC-DS

**Type:** OLAP / Decision Support · **Tables:** 5 (synthetic subset) · **Queries:** 99 IDs (SQL text not included)

### Data features (synth subset)
| Table | Rows at sf=1 |
|-------|--------------|
| date_dim | 3 360 (fixed) |
| store | 1 |
| item | 1 000 |
| customer | 1 000 |
| store_sales | 10 000 |

### Query artifacts
- `query_ids.txt` lists `query01` through `query99`.
- `sample_tpcds_config.xml` is a BenchBase starter profile.
- The adapter does **not** ship TPC-DS SQL bodies; attach templates from a licensed/local TPC-DS kit.

### Python API
```python
from driftbench.data.tpcds import data as tpcds_data, queries as tpcds_queries

tpcds_data(scale_factor=3).generate(output_dir="./artifacts")
tpcds_queries().generate(output_dir="./artifacts")
```

---

## TPC-C

**Type:** OLTP · **Tables:** 9 · **Transactions:** 5 types

### Data features
| Table | Rows at W=1 | Scales with W |
|-------|-------------|---------------|
| warehouse | 1 | Yes |
| district | 10 | Yes |
| customer | 3 000 | Yes |
| history | 3 000 | Yes |
| item | 10 000 | Capped at 100 K |
| stock | 10 000 | Capped at 100 K |
| orders | 3 000 | Yes |
| new_order | ~900 | Yes |
| order_line | ~30 000 | Yes |

W = `scale_factor` (integer number of warehouses).

### Transaction types
| Transaction | Adapter default mix % | Description |
|-------------|-------------------|-------------|
| New Order | 45 % | Insert order across warehouse/district/item/stock |
| Payment | 43 % | Update customer balance, insert history |
| Order Status | 4 % | Read most recent order for a customer |
| Delivery | 4 % | Batch deliver oldest undelivered orders per district |
| Stock Level | 4 % | Count low-stock items in last 20 orders |

### Modes
- `mode="synth"` — generates 9 CSV files locally

### Python API
```python
from driftbench.data.tpcc import data as tpcc_data, queries as tpcc_queries

tpcc_data(scale_factor=4).generate(output_dir="./artifacts")
tpcc_queries().generate(output_dir="./artifacts")
```

---

## TPC-C Skew

**Type:** OLTP with workload drift · **Tables:** 9 + weight manifest · **Transactions:** 5 types

### What it adds over TPC-C
TPC-C Skew generates identical table data plus a `warehouse_access_weights.csv` file
encoding a Zipf probability distribution over warehouses. Drivers use this manifest to
bias new-order and payment transactions toward "hot" warehouses, creating lock contention
and simulating real-world access skew drift.

### Parameters
| Parameter | Default | Description |
|-----------|---------|-------------|
| `scale_factor` | 1 | Number of warehouses (W) |
| `hot_warehouse_fraction` | 0.2 | Fraction of warehouses labeled "hot" |
| `skew_factor` | 0.99 | Zipf α (higher = more skewed toward rank-1 warehouse) |

### Skew distribution examples
| skew_factor | hot_warehouse_fraction=0.2 (W=10) | Hot warehouses receive |
|-------------|-----------------------------------|------------------------|
| 0.5 | 2 of 10 warehouses | ~38% of traffic |
| 0.99 | 2 of 10 warehouses | ~52% of traffic |
| 1.5 | 2 of 10 warehouses | ~65% of traffic |

### Python API
```python
from driftbench.data.tpcc_skew import data as tpcc_skew_data, queries as tpcc_skew_queries

tpcc_skew_data(scale_factor=10, hot_warehouse_fraction=0.2, skew_factor=0.99).generate(output_dir="./artifacts")
tpcc_skew_queries(scale_factor=10, hot_warehouse_fraction=0.2, skew_factor=0.99).generate(output_dir="./artifacts")
```

---

## JOB (Join Order Benchmark)

**Type:** OLAP / Join-order sensitivity · **Tables:** 11 synthetic CSVs / 21 full IMDB · **Queries:** 20 templates (original: 113)

Reference: Leis et al., "How Good Are Query Optimizers, Really?", VLDB 2015.

### Data features (synth subset, sf=1)
| Table | Rows at sf=1 | Notes |
|-------|--------------|-------|
| title | 500 | Main movie table |
| name | 1 000 | Person names |
| cast_info | 5 000 | Actor-movie links (largest synth table) |
| movie_info | 3 000 | Movie metadata (genres, ratings, etc.) |
| movie_keyword | 2 000 | Movie-keyword links |
| keyword | 200 | Keyword vocabulary |
| movie_companies | 1 000 | Movie-company links |
| company_name | 100 | Production companies |
| kind_type | 7 | Fixed lookup |
| info_type | 10 | Fixed lookup |
| company_type | 4 | Fixed lookup |

Full IMDB snapshot: 21 tables, ~36M rows in cast_info alone. The synthetic adapter writes 8 primary tables plus 3 lookup tables (11 CSVs total).

### Query features (20 representative templates)
All queries use standard SQL (no proprietary extensions). Join depths:

| Join depth | Queries |
|------------|---------|
| 2-table | 3a, 7a |
| 3-table | 1a, 2a, 13a, 15a, 19a |
| 4-table | 4a, 8a, 9a, 12a |
| 5-table | 6a, 11a, 16a |
| 6-table | 5a, 14a |
| 7-table | 17a, 18a |
| 8-table | 10a, 20a |

### Query categories
| Category | Query IDs |
|----------|-----------|
| Keyword filter | 1a, 7a, 9a, 15a |
| Company / country | 2a, 5a, 19a |
| Genre / info type | 3a, 6a, 13a, 18a |
| Cast / actor | 4a, 8a, 12a, 16a |
| Multi-way join | 10a, 11a, 14a, 17a, 20a |

### Python API
```python
from driftbench.data.job import data as job_data, queries as job_queries

job_data(scale_factor=1).generate(output_dir="./artifacts")
job_queries().generate(output_dir="./artifacts")
```

---

## YCSB

**Type:** Key-value / NoSQL · **Tables:** 1 (usertable) · **Workloads:** 6 modeled mixes

### Data features
Row count = `scale_factor × 1000`. Each row has 10 string fields of 100 chars.

### Workload mixes
| Workload | Read | Insert | Scan | Update | RMW | Description |
|----------|------|--------|------|--------|-----|-------------|
| A | 50% | — | — | 50% | — | Update heavy |
| B | 95% | — | — | 5% | — | Read heavy |
| C | 100% | — | — | — | — | Read only |
| D | 95% | 5% | — | — | — | Read latest |
| E | — | 5% | 95% | — | — | Short ranges |
| F | 50% | — | — | — | — | 50% RMW |

### Python API
```python
from driftbench.data.ycsb import data as ycsb_data, queries as ycsb_queries

ycsb_data(scale_factor=10).generate(output_dir="./artifacts")
ycsb_queries(workload="A", run_seconds=60).generate(output_dir="./artifacts")
```

---

## DSB

**Type:** Decision Support Benchmark · **Tables:** 3 synthetic star-schema CSVs · **Queries:** 3 SQL templates

DSB extends TPC-DS with more complex queries and a richer schema designed to stress
modern query optimizers beyond TPC-DS.

| Table | Rows at sf=1 | Scaling |
|-------|--------------|---------|
| date_dim | 3 360 | Fixed |
| customer | 1 000 | × sf |
| lineorder | 5 000 | × sf |

### Python API
```python
from driftbench.data.dsb import data as dsb_data, queries as dsb_queries

dsb_data(scale_factor=2).generate(output_dir="./artifacts")
dsb_queries().generate(output_dir="./artifacts")
```

---

## pgbench

**Type:** PostgreSQL TPC-B-like OLTP · **Tables:** 4 · **Workloads:** 3

The artifact adapter writes synthetic CSV/DDL and transaction scripts for `tpcb`,
`simple_update`, and `select_only`. At sf=1 it writes 1 branch, 10 tellers,
100 000 accounts, and an empty history table.

For real regression measurement, `driftbench benchmark pgbench` uses PostgreSQL 16
initialized by `pgbench -i -s 1`. It measures a native `-b select-only` baseline and
the generated `select_only` script in alternating paired rounds. `BenchmarkRunResult`
v1 records median TPS, mean/p50/p95/p99 transaction-log latency, errors, full versions,
run configuration, and raw-evidence paths. Each result also points to SHA-256/byte-counted
copies of the canonical loaded policy, the exact candidate SQL executed from the bundle,
and an environment snapshot. That snapshot records the DriftBench version and source SHA,
Python and OS/CPU identity, password-free connection identity, PostgreSQL/pgbench
versions, key PostgreSQL settings, and the scale inferred from `pgbench_branches` and
validated against all four initialized pgbench table counts. A producer run requires the
DriftBench runtime-source scope (`driftbench/`, `driftbench_service/`,
`driftbench_mcp/`, and `pyproject.toml`) to be clean at the initial preflight and after
the final measurement, with the same full 40-character HEAD. `DRIFTBENCH_GIT_SHA` is a
full-SHA assertion against that real HEAD, not an override and never a dirty-state bypass;
wheel installs do not borrow a caller repository SHA. Missing, dirty, changed, or
incomplete provenance fails closed and leaves a failure bundle for diagnosis.

Measurement TPS is `successful transactions / runner elapsed measurement seconds`. The
unrounded value must agree with the TPS parsed from hashed pgbench stdout within an
inclusive 5%, using the reported TPS as the denominator. This check is measurement-only;
failure makes the evidence invalid before aggregation and is exit 4, not a threshold
regression. The approved `pgbench-ci-v1` thresholds are:

- candidate median TPS ≥ 70% of baseline;
- candidate median p95 latency ≤ 150% of baseline;
- baseline and candidate error rates = 0.

Persisted evidence can be checked on another machine with no Git, database, or network:

```bash
driftbench benchmark verify --bundle benchmark-artifacts/results --json
```

The verifier validates safe relative descriptors and reconstructs raw counts, R-7 latency,
TPS, aggregates, execution order, and the bundled-policy decision. It establishes internal
bundle consistency, not the external authenticity or identity of the bundle producer.

```python
from driftbench.data.pgbench import data as pgbench_data, queries as pgbench_queries

pgbench_data(scale_factor=1).generate(output_dir="./artifacts")
pgbench_queries(workload="select_only", clients=2, duration=5).generate(output_dir="./artifacts")
```

---

## BenchBase

**Type:** External Java benchmark configuration generator · **Benchmarks:** 10

BenchBase does not generate a local dataset. It creates XML plus `load.sh`/`execute.sh`
for TPC-C, TPC-H, YCSB, SEATS, AuctionMark, Smallbank, Epinions, Wikipedia, Twitter,
and Voter. A BenchBase JAR and live database are required to execute those files.
Connection values are XML-escaped; JDBC URLs and passwords are redacted from cache
parameters while still participating in cache invalidation.

```python
from driftbench.data.benchbase import data as bb_data, queries as bb_queries

bb_data(benchmark="tpcc", scale_factor=1).generate(output_dir="./artifacts")
bb_queries(benchmark="tpcc", terminals=4, duration=60).generate(output_dir="./artifacts")
```

---

## Table-Focused Query Index

Use this index to find queries that heavily access a specific table.

| Table | Benchmarks with heavy access |
|-------|------------------------------|
| lineitem | TPC-H (Q1, Q3, Q5, Q6, Q7, Q10, Q12, Q14, Q17, Q18, Q19, Q20, Q21) |
| orders | TPC-H (Q3, Q4, Q5, Q7, Q8, Q10, Q12, Q18, Q21) |
| customer | TPC-H (Q5, Q7, Q8, Q10, Q13, Q18, Q22); TPC-C (new_order, payment) |
| store_sales | TPC-DS (most queries) |
| warehouse | TPC-C (all transactions); TPC-C Skew (biased) |
| title | JOB (all queries via join spine) |
| cast_info | JOB (4a, 5a, 8a, 10a, 12a, 14a, 16a, 17a, 20a) |
| usertable | YCSB (all operations) |

---

## Benchmark Comparison Matrix

| Benchmark | Schema complexity | Query complexity | Scale control | Drift type |
|-----------|-------------------|------------------|---------------|------------|
| TPC-H | Medium (8 tables) | High (22 templates) | Scale factor | Data volume |
| TPC-DS | Medium (5 synthetic; 24 full) | Query IDs only in adapter | Scale factor | Data volume |
| TPC-C | High (9 tables) | Medium (5 txn types) | Warehouses | Throughput |
| TPC-C Skew | High (9 tables + weights) | Medium | Warehouses + Zipf α | Access skew |
| JOB | High (11 synthetic; 21 full) | Very high (20 included; 113 original) | Proportional | Join-order sensitivity |
| YCSB | Minimal (1 table) | Low | Record count | Read/write ratio |
| DSB | Low (3 synthetic tables) | Medium (3 templates) | Scale factor | Query complexity |
| pgbench | Low (4 tables) | Low/medium (3 workloads) | Scale factor + clients | Throughput / latency |
| BenchBase | External benchmark dependent | External benchmark dependent | Benchmark config | Live-driver execution |
