# DriftBench Benchmark Reference

Complete reference for all 9 benchmark adapters, including data features, query characteristics, and selection guidance.

Support claims follow the [adapter support contract](adapter_support_contract.md). Tiers describe each generated artifact and mode; they are not official benchmark compliance claims.

## Current Support Summary

| Adapter | Data support | Query/workload support |
|---|---|---|
| TPC-H | Tier 2 `copied-official-format`; Tier 3 `dbgen` | Tier 3 `qgen`; Tier 2 `custom-parameterized-sql` |
| TPC-DS | Tier 1 `synthetic-subset` (5/24 tables) | Tier 0 `query-ids-and-config-only` (0/99 SQL queries shipped) |
| TPC-C | Tier 1 `synthetic-subset` (9 tables) | Tier 1 `sql-transaction-templates` (5 transactions) |
| TPC-C Skew | Tier 1 `synthetic-subset-with-inert-weights` | Tier 1 `annotated-sql-transaction-templates` |
| JOB | Tier 1 `synthetic-subset` (11 physical tables) | Tier 2 `executable-sql-subset` (20/113 queries) |
| YCSB | Tier 1 `synthetic-usertable` | Tier 1 `workload-config-only` (6 operation types) |
| DSB | Tier 1 `synthetic-toy-subset` (3 tables) | Tier 2 `executable-sql-toy-subset` (3 queries) |
| pgbench | Tier 1 `synthetic-pgbench-shape` (4 tables) | Tier 2 `executable-pgbench-script` (3 selectable workloads) |
| BenchBase | Tier 2 `external-benchbase-load-config` | Tier 2 `external-benchbase-execute-config` |

---

## Quick Selection Guide

| Goal | Recommended Benchmark |
|------|-----------------------|
| Standard OLAP / query optimizer research | TPC-H or TPC-DS |
| Standard OLTP throughput | TPC-C |
| OLTP with hot-spot / skew drift | TPC-C Skew |
| Join-order sensitivity / cardinality estimation | JOB |
| Key-value / NoSQL workloads | YCSB |
| Decision support with complex schemas | DSB |
| PostgreSQL-native transaction scripts | pgbench |
| External multi-benchmark execution | BenchBase |

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
- `mode="qgen"` — generates parameterized SQL via qgen (packaged template dir)
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

**Type:** OLAP / Decision Support · **Tables:** 5/24 (synth subset) · **Queries:** 99 IDs, no SQL text

### Data features (synth subset)
| Table | Rows at sf=1 |
|-------|--------------|
| date_dim | 3 360 (fixed) |
| store | 1 |
| item | 1 000 |
| customer | 1 000 |
| store_sales | 10 000 |

### Query features
- Emits IDs `query01` through `query99` and a sample BenchBase XML profile.
- Does not ship TPC-DS SQL text; attach templates from a local kit.

### Modes
- Synthetic generation emits 5 pipe-delimited `.dat` tables locally.

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
| Transaction | Mix % (standard) | Description |
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

**Type:** OLAP / Join-order sensitivity · **Tables:** 8 (synth) / 21 (full IMDB) · **Queries:** 20 templates (original: 113)

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

Full IMDB snapshot: 21 tables, ~36M rows in cast_info alone. The synth adapter generates an 8-table subset.

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

### Modes
- `mode="synth"` — generates 8-table CSV subset locally

### Python API
```python
from driftbench.data.job import data as job_data, queries as job_queries

job_data(scale_factor=1).generate(output_dir="./artifacts")
job_queries().generate(output_dir="./artifacts")
```

---

## YCSB

**Type:** Key-value / NoSQL · **Tables:** 1 (usertable) · **Workloads:** 6 standard mixes

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

**Type:** Decision-support toy subset · **Tables:** 3 · **Queries:** 3 executable SQL templates

This adapter emits a deterministic 3-table star-schema toy dataset (`date_dim`,
`customer`, `lineorder`) and 3 SQL queries. DriftBench does not claim an official
DSB table or query count.

### Python API
```python
from driftbench.data.dsb import data as dsb_data, queries as dsb_queries

dsb_data(scale_factor=2).generate(output_dir="./artifacts")
dsb_queries().generate(output_dir="./artifacts")
```

---

## pgbench

**Type:** PostgreSQL TPC-B-like OLTP · **Tables:** 4 · **Workloads:** 3 selectable scripts

Data generation emits synthetic `pgbench_branches`, `pgbench_tellers`,
`pgbench_accounts`, and empty `pgbench_history` CSVs. Query generation selects one
of `tpcb`, `simple_update`, or `select_only` and emits an executable pgbench script.
DriftBench does not run `pgbench` during generation.

```python
from driftbench.data.pgbench import data as pgbench_data, queries as pgbench_queries

pgbench_data(scale_factor=1).generate(output_dir="./artifacts")
pgbench_queries(workload="tpcb").generate(output_dir="./artifacts")
```

---

## BenchBase

**Type:** External Java benchmark framework · **Targets:** 10 · **Artifacts:** load/execute XML and shell scripts

The adapter supports TPC-C, TPC-H, YCSB, SEATS, AuctionMark, Smallbank, Epinions,
Wikipedia, Twitter, and Voter configuration. Set `BENCHBASE_JAR` and provide a
database before running the generated script; DriftBench does not invoke the jar
during generation.

```python
from driftbench.data.benchbase import data as benchbase_data, queries as benchbase_queries

benchbase_data(benchmark="tpcc").generate(output_dir="./artifacts")
benchbase_queries(benchmark="tpcc", terminals=4).generate(output_dir="./artifacts")
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
| TPC-DS | High (26 tables full) | Very high | Scale factor | Data volume |
| TPC-C | High (9 tables) | Medium (5 txn types) | Warehouses | Throughput |
| TPC-C Skew | High (9 tables + weights) | Medium | Warehouses + Zipf α | Access skew |
| JOB | High (21 tables full) | Very high (113 queries) | Proportional | Join-order sensitivity |
| YCSB | Minimal (1 table) | Low | Record count | Read/write ratio |
| DSB | Small (3 tables) | Low (3 templates) | Scale factor | Query complexity |
| pgbench | Small (4 tables) | Low (3 scripts) | Scale factor | Transaction mix |
| BenchBase | Target-dependent | Target-dependent | Config parameters | External execution |
