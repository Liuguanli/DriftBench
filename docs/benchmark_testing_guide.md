# Benchmark Adapter Testing Guide

Run each snippet in a Python shell or script from the repo root.
All artifacts land in `./test_artifacts/` so you can inspect them easily.
Delete `./test_artifacts/` between runs if you want a completely fresh start.

```python
from pathlib import Path
OUT = Path("./test_artifacts")
```

---

## 0. Automated test suite (all 78 tests)

```bash
python -m unittest discover -s test -v
```

**Expected result:**
```
Ran 78 tests in ~25s
OK (skipped=5)
```

The 5 skipped are legacy placeholder tests — that is normal.

---

## 1. TPC-H

### 1a. Data — plan mode (large scale, no local files generated)

```python
from driftbench.data.tpch import data as tpch_data
result = tpch_data(scale_factor=1000, mode="plan").generate(output_dir=OUT)
print("files:", [f.name for f in result.files])
```

**Expected console output:**
```
[driftbench] Generating TPC-H data (sf=1000) → .../test_artifacts/tpch/data/sf_1000
```

**Expected files (2):**
- `test_artifacts/tpch/data/sf_1000/generate_tpch_data.sh`
- `test_artifacts/tpch/data/sf_1000/tpch_data_manifest.json`

**Verify script content:**
```python
script = OUT / "tpch/data/sf_1000/generate_tpch_data.sh"
assert "SCALE_FACTOR=1000" in script.read_text()
assert "dbgen" in script.read_text()
print("✓ TPC-H plan script OK")
```

---

### 1b. Queries — qgen mode

```python
from driftbench.data.tpch import queries as tpch_queries
result = tpch_queries(query_ids=[1, 6, 14], queries_per_template=2, mode="qgen").generate(output_dir=OUT)
print("files:", [f.name for f in result.files])
print("query count:", len(result.files))
```

**Expected console output:**
```
[driftbench] Generating TPC-H queries → .../test_artifacts/tpch/queries
```

**Expected files (2):**
- `tpch/queries/tpch_queries.sql`
- `tpch/queries/tpch_queries.csv`

**Expected manifest fields:**
```python
import json
manifest = json.loads((OUT / "tpch/queries/tpch_queries_manifest.json").read_text())
assert manifest["count"] == 6          # 3 query IDs × 2 instances each
assert manifest["queries_per_template"] == 2
assert manifest["query_ids"] == ["1", "6", "14"]
print("✓ TPC-H queries manifest OK")
```

---

### 1c. Lazy reuse

```python
# Run again — should skip generation
result2 = tpch_queries(query_ids=[1, 6, 14], queries_per_template=2, mode="qgen").generate(output_dir=OUT)
```

**Expected console output:**
```
[driftbench] TPC-H queries already exist at ... Reusing.
```

---

## 2. TPC-DS

```python
from driftbench.data.tpcds import data as tpcds_data, queries as tpcds_queries
r_data = tpcds_data(scale_factor=5).generate(output_dir=OUT)
r_queries = tpcds_queries().generate(output_dir=OUT)
print("data files:", [f.name for f in r_data.files])
print("query files:", [f.name for f in r_queries.files])
```

**Expected console output:**
```
[driftbench] Generating TPC-DS data (sf=5) → .../test_artifacts/tpcds/data
[driftbench] Generating TPC-DS queries → .../test_artifacts/tpcds/queries
```

**Expected data files (2):**
- `tpcds/data/generate_tpcds_data.sh`
- `tpcds/data/README.md`

**Expected query files (2):**
- `tpcds/queries/query_ids.txt`
- `tpcds/queries/sample_tpcds_config.xml`

**Verify:**
```python
script = OUT / "tpcds/data/generate_tpcds_data.sh"
assert "SCALE_FACTOR=5" in script.read_text()
manifest = json.loads((OUT / "tpcds/queries/tpcds_queries_manifest.json").read_text())
assert manifest["query_count"] == 99
print("✓ TPC-DS OK")
```

---

## 3. YCSB

```python
from driftbench.data.ycsb import data as ycsb_data, queries as ycsb_queries
r_data = ycsb_data(scale_factor=2).generate(output_dir=OUT)
r_queries = ycsb_queries(workload="B", run_seconds=30).generate(output_dir=OUT)
print("data files:", [f.name for f in r_data.files])
print("query files:", [f.name for f in r_queries.files])
```

**Expected console output:**
```
[driftbench] Generating YCSB data (sf=2) → .../test_artifacts/ycsb/data
[driftbench] Generating YCSB queries (workload=B) → .../test_artifacts/ycsb/queries
```

**Expected data files (2):**
- `ycsb/data/load.properties` (contains `recordcount=2000`)
- `ycsb/data/generate_ycsb_data.sh`

**Expected query files (2):**
- `ycsb/queries/workload_b.properties`
- `ycsb/queries/sample_ycsb_config.xml`

**Verify:**
```python
props = (OUT / "ycsb/data/load.properties").read_text()
assert "recordcount=2000" in props    # 2 * 1000
manifest = json.loads((OUT / "ycsb/queries/ycsb_queries_manifest.json").read_text())
assert manifest["workload"] == "B"
assert manifest["weights"]["ReadRecord"] == 95   # Workload B: 95% reads
assert manifest["weights"]["UpdateRecord"] == 5
print("✓ YCSB OK")
```

---

## 4. DSB

```python
from driftbench.data.dsb import data as dsb_data, queries as dsb_queries
r_data = dsb_data(scale_factor=3).generate(output_dir=OUT)
r_queries = dsb_queries().generate(output_dir=OUT)
print("data files:", [f.name for f in r_data.files])
print("query files:", [f.name for f in r_queries.files])
```

**Expected console output:**
```
[driftbench] Generating DSB data (sf=3) → .../test_artifacts/dsb/data
[driftbench] Generating DSB queries → .../test_artifacts/dsb/queries
```

**Expected data files (2):**
- `dsb/data/schema_blueprint.sql`
- `dsb/data/seed_plan.yaml`

**Expected query files (3):**
- `dsb/queries/q1_revenue_by_year.sql`
- `dsb/queries/q2_revenue_by_region.sql`
- `dsb/queries/q3_margin_trend.sql`

**Verify:**
```python
seed = (OUT / "dsb/data/seed_plan.yaml").read_text()
assert "300000" in seed    # customer: 100_000 * 3
manifest = json.loads((OUT / "dsb/queries/dsb_queries_manifest.json").read_text())
assert manifest["query_count"] == 3
print("✓ DSB OK")
```

---

## 5. TPC-C

```python
from driftbench.data.tpcc import data as tpcc_data, queries as tpcc_queries
r_data = tpcc_data(scale_factor=2).generate(output_dir=OUT)
r_queries = tpcc_queries().generate(output_dir=OUT)
print("data files:", sorted(f.name for f in r_data.files))
print("query files:", sorted(f.name for f in r_queries.files))
```

**Expected console output:**
```
[driftbench] Generating TPC-C data (W=2) → .../test_artifacts/tpcc/data
[driftbench] Generating TPC-C queries → .../test_artifacts/tpcc/queries
```

**Expected data files (11):**
`customer.csv`, `district.csv`, `history.csv`, `item.csv`, `new_order.csv`,
`order_line.csv`, `orders.csv`, `stock.csv`, `tpcc_data_manifest.json`,
`tpcc_schema.sql`, `warehouse.csv`

**Expected query files (7):**
`delivery.sql`, `new_order.sql`, `order_status.sql`, `payment.sql`,
`stock_level.sql`, `tpcc_all_transactions.sql`, `tpcc_queries_manifest.json`

**Verify row counts (W=2):**
```python
import csv as csv_mod

def row_count(name):
    f = OUT / "tpcc/data" / name
    return sum(1 for _ in f.open()) - 1  # subtract header

assert row_count("warehouse.csv") == 2
assert row_count("district.csv") == 20     # 10 * W
assert row_count("customer.csv") == 6000   # 3000 * W
assert row_count("orders.csv") == 6000     # 300 orders * 10 districts * W
print("✓ TPC-C row counts OK")
```

---

## 6. TPC-C Skew

```python
from driftbench.data.tpcc_skew import data as tpcc_skew_data, queries as tpcc_skew_queries
r_data = tpcc_skew_data(scale_factor=5, hot_warehouse_fraction=0.2, skew_factor=0.99).generate(output_dir=OUT)
r_queries = tpcc_skew_queries(scale_factor=5, hot_warehouse_fraction=0.2, skew_factor=0.99).generate(output_dir=OUT)
print("data files:", sorted(f.name for f in r_data.files))
```

**Expected console output:**
```
[driftbench] Generating TPC-C Skew data (W=5, skew=0.99) → .../test_artifacts/tpcc_skew/data
[driftbench] Generating TPC-C Skew queries → .../test_artifacts/tpcc_skew/queries
```

**Expected extra file (beyond TPC-C):**
- `tpcc_skew/data/warehouse_access_weights.csv`

**Verify weights:**
```python
weights_file = OUT / "tpcc_skew/data/warehouse_access_weights.csv"
lines = weights_file.read_text().splitlines()
assert lines[0] == "warehouse_id,access_probability,is_hot"
assert len(lines) == 6   # header + 5 warehouses
# 1 hot warehouse (ceil(5 * 0.2) = 1)
hot = [l for l in lines[1:] if l.endswith(",1")]
assert len(hot) == 1
# Probabilities sum to 1
total = sum(float(l.split(",")[1]) for l in lines[1:])
assert abs(total - 1.0) < 1e-5
print("✓ TPC-C Skew weights OK")
```

---

## 7. JOB (Join Order Benchmark)

```python
from driftbench.data.job import data as job_data, queries as job_queries
r_data = job_data(scale_factor=1).generate(output_dir=OUT)
r_queries = job_queries().generate(output_dir=OUT)
print("data tables:", sorted(f.name for f in r_data.files if f.suffix == ".csv"))
print("query count:", len([f for f in r_queries.files if f.suffix == ".sql"]))
```

**Expected console output:**
```
[driftbench] Generating JOB data (sf=1, mode=synth) → .../test_artifacts/job/data
[driftbench] Generating JOB queries → .../test_artifacts/job/queries
```

**Expected data CSV tables (11):**
`cast_info.csv`, `company_name.csv`, `company_type.csv`, `info_type.csv`,
`keyword.csv`, `kind_type.csv`, `movie_companies.csv`, `movie_info.csv`,
`movie_keyword.csv`, `name.csv`, `title.csv`

**Expected query count: 21** (20 individual + 1 bundle)

**Verify:**
```python
title_rows = sum(1 for _ in open(OUT / "job/data/title.csv")) - 1
assert title_rows == 500   # 500 * sf=1
cast_rows = sum(1 for _ in open(OUT / "job/data/cast_info.csv")) - 1
assert cast_rows == 5000   # 5000 * sf=1
manifest = json.loads((OUT / "job/queries/job_queries_manifest.json").read_text())
assert "8_table" in manifest["join_complexity"]
print("✓ JOB OK")
```

---

## 8. pgbench

```python
from driftbench.data.pgbench import data as pgbench_data, queries as pgbench_queries
r_data = pgbench_data(scale_factor=1).generate(output_dir=OUT)
r_tpcb = pgbench_queries(workload="tpcb", clients=4, duration=30).generate(output_dir=OUT)
r_ro = pgbench_queries(workload="select_only").generate(output_dir=OUT / "pgbench_ro")
print("data files:", sorted(f.name for f in r_data.files))
```

**Expected console output:**
```
[driftbench] Generating pgbench data (sf=1, mode=synth) → .../test_artifacts/pgbench/data
[driftbench] Generating pgbench queries (workload=tpcb) → .../test_artifacts/pgbench/queries
[driftbench] Generating pgbench queries (workload=select_only) → .../pgbench_ro/pgbench/queries
```

**Expected data files (5):**
`pgbench_accounts.csv`, `pgbench_branches.csv`, `pgbench_history.csv`,
`pgbench_schema.sql`, `pgbench_tellers.csv`

**Verify row counts (sf=1):**
```python
def row_count(name):
    return sum(1 for _ in open(OUT / "pgbench/data" / name)) - 1

assert row_count("pgbench_branches.csv") == 1
assert row_count("pgbench_tellers.csv") == 10
assert row_count("pgbench_accounts.csv") == 100_000
assert row_count("pgbench_history.csv") == 0    # starts empty

# Verify run script has correct flags
run_script = (OUT / "pgbench/queries/run_pgbench.sh").read_text()
assert "-c 4" in run_script
assert "-T 30" in run_script
print("✓ pgbench OK")
```

---

## 9. BenchBase

```python
from driftbench.data.benchbase import data as bb_data, queries as bb_queries

# TPC-C
r_load = bb_data(benchmark="tpcc", scale_factor=10).generate(output_dir=OUT)
r_exec = bb_queries(benchmark="tpcc", terminals=8, duration=120, rate=5000).generate(output_dir=OUT)

# Also try a second benchmark
r_seats = bb_data(benchmark="seats", scale_factor=1).generate(output_dir=OUT)
print("tpcc load files:", [f.name for f in r_load.files])
print("tpcc exec files:", [f.name for f in r_exec.files])
```

**Expected console output:**
```
[driftbench] Generating BenchBase tpcc load config → .../benchbase/tpcc/data
[driftbench] Generating BenchBase tpcc execute config → .../benchbase/tpcc/queries
[driftbench] Generating BenchBase seats load config → .../benchbase/seats/data
```

**Expected load files (2 per benchmark):**
- `benchbase/tpcc/data/tpcc_load_config.xml`
- `benchbase/tpcc/data/load.sh`

**Expected execute files (2):**
- `benchbase/tpcc/queries/tpcc_execute_config.xml`
- `benchbase/tpcc/queries/execute.sh`

**Verify XML content:**
```python
load_xml = (OUT / "benchbase/tpcc/data/tpcc_load_config.xml").read_text()
assert "<scalefactor>10</scalefactor>" in load_xml
assert "load=true" in load_xml
assert "execute=false" in load_xml
assert "NewOrder" in load_xml

exec_xml = (OUT / "benchbase/tpcc/queries/tpcc_execute_config.xml").read_text()
assert "<terminals>8</terminals>" in exec_xml
assert "<time>120</time>" in exec_xml
assert "<rate>5000</rate>" in exec_xml
assert "execute=true" in exec_xml
assert "load=false" in exec_xml

# All 10 supported benchmarks
for bm in ["tpcc", "tpch", "ycsb", "seats", "auctionmark",
           "smallbank", "epinions", "wikipedia", "twitter", "voter"]:
    bb_data(benchmark=bm).generate(output_dir=OUT)
print("✓ All BenchBase benchmarks OK")
```

---

## 10. Lazy reuse (cross-adapter verification)

Run any adapter twice and confirm the second call does not regenerate:

```python
from driftbench.data.tpcc import data as tpcc_data

r1 = tpcc_data(scale_factor=1).generate(output_dir=OUT)
# Corrupt a file to prove reuse does NOT re-read content
(OUT / "tpcc/data/warehouse.csv").write_text("CORRUPTED")
r2 = tpcc_data(scale_factor=1).generate(output_dir=OUT)

# File should still be corrupted — reuse skipped generation
assert (OUT / "tpcc/data/warehouse.csv").read_text() == "CORRUPTED"
print("✓ Lazy reuse confirmed — second call did not regenerate")

# force=True should restore it
r3 = tpcc_data(scale_factor=1).generate(output_dir=OUT, force=True)
assert (OUT / "tpcc/data/warehouse.csv").read_text() != "CORRUPTED"
print("✓ force=True regenerated the data")
```

**Expected console output:**
```
[driftbench] TPC-C data (W=1) already exists at ... Reusing.
[driftbench] Generating TPC-C data (W=1) → ...   ← only on force=True call
```

---

## 11. Default output directory

When `output_dir` is omitted, artifacts go to `~/.driftbench/data/`
(or `DRIFTBENCH_DATA_DIR` if set):

```python
import os
os.environ["DRIFTBENCH_DATA_DIR"] = "/tmp/my_driftbench"

from driftbench.data.ycsb import data as ycsb_data
result = ycsb_data().generate()   # no output_dir
print("wrote to:", result.output_dir)
```

**Expected console output:**
```
[driftbench] No output_dir specified. Writing ycsb/data to: /tmp/my_driftbench
[driftbench] Generating YCSB data (sf=1) → /tmp/my_driftbench/ycsb/data
```

---

## Summary table

| # | Benchmark | Test snippet above | Key assertion |
|---|-----------|-------------------|---------------|
| 1 | TPC-H | §1a + §1b + §1c | manifest count==6, script has SCALE_FACTOR=1000, reuse message |
| 2 | TPC-DS | §2 | query_count==99, SCALE_FACTOR=5 in script |
| 3 | YCSB | §3 | recordcount==2000, ReadRecord weight==95 |
| 4 | DSB | §4 | seed plan has 300000 customers, query_count==3 |
| 5 | TPC-C | §5 | warehouse==2, district==20, customer==6000 |
| 6 | TPC-C Skew | §6 | 5 weight rows, 1 hot, sum≈1.0 |
| 7 | JOB | §7 | title==500, cast_info==5000, 21 SQL files |
| 8 | pgbench | §8 | accounts==100K, tellers==10, -c 4 -T 30 in script |
| 9 | BenchBase | §9 | correct XML flags, all 10 benchmarks generate |
| 10 | Lazy reuse | §10 | second call reuses, force=True regenerates |
| 11 | Default dir | §11 | prints env-var path |
