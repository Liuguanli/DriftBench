<p align="center">
  <img src="https://raw.githubusercontent.com/Liuguanli/DriftBench/main/res/icon.png" alt="DriftBench logo" width="360"/>
</p>

# DriftBench

DriftBench is a toolkit for generating and replaying **data drift** and **workload drift** with DriftSpec.

Who uses DriftBench:
- **Researcher** — design reproducible drift experiments and ablations.
- **Database Vendor / Performance Team** — run drift regression checks across targets before release.
- **New User** — start from validated examples and get first outputs quickly.

Version history: [CHANGELOG](https://github.com/Liuguanli/DriftBench/blob/main/CHANGELOG.md) · Production site: [driftbench.com](https://driftbench.com)

---

## Install

```bash
pip install -U driftbench-db
```

Or from source:

```bash
git clone https://github.com/Liuguanli/DriftBench.git
cd DriftBench
pip install -e .
```

Verify:

```bash
driftbench --help
```

---

## Benchmark Adapters (`driftbench.data`)

Nine adapters generate real data files and SQL query workloads with no external dependencies
(TPC-H `mode="generate"` auto-downloads and builds `dbgen` on first use).

| Adapter | Workload type | Data format | Tables | Queries |
|---------|--------------|-------------|--------|---------|
| `tpch` | OLAP | `.tbl` (pipe-delimited) | 8 | 22 SQL via qgen |
| `tpcds` | OLAP / Decision support | `.dat` (pipe-delimited) | 5 synthetic | 99 query IDs |
| `tpcc` | OLTP | `.csv` | 9 | 5 transaction types |
| `tpcc_skew` | OLTP + hotspot | `.csv` + weight manifest | 9 | 5 transaction types |
| `job` | OLAP / join-order | `.csv` | 11 (IMDB-like) | 20 SQL templates |
| `ycsb` | Key-value | `.csv` | 1 | 6 workload mixes (A–F) |
| `dsb` | Decision support | `.csv` | 3 star-schema | 3 SQL templates |
| `pgbench` | TPC-B (OLTP) | `.csv` | 4 | 3 workloads |
| `benchbase` | Multi-benchmark | XML + shell script | via live DB | 10 benchmarks |

### Generate data and queries

```python
from pathlib import Path
from driftbench.data.tpch import data as tpch_data, queries as tpch_queries
from driftbench.data.tpcds import data as tpcds_data, queries as tpcds_queries
from driftbench.data.tpcc import data as tpcc_data, queries as tpcc_queries
from driftbench.data.tpcc_skew import data as tpcc_skew_data, queries as tpcc_skew_queries
from driftbench.data.job import data as job_data, queries as job_queries
from driftbench.data.ycsb import data as ycsb_data, queries as ycsb_queries
from driftbench.data.dsb import data as dsb_data, queries as dsb_queries
from driftbench.data.pgbench import data as pgbench_data, queries as pgbench_queries
from driftbench.data.benchbase import data as bb_data, queries as bb_queries

out = Path("./artifacts")

# TPC-H — auto-builds dbgen on first use; converts .tbl to .csv with .as_csv()
tpch_data(scale_factor=1, mode="generate").generate(output_dir=out)
tpch_queries(query_ids=[1, 3, 5], queries_per_template=2).generate(output_dir=out)

# TPC-DS — synthetic .dat files; converts to .csv with .as_csv()
tpcds_data(scale_factor=10).generate(output_dir=out)
tpcds_queries().generate(output_dir=out)

# TPC-C — scale_factor = number of warehouses
tpcc_data(scale_factor=4).generate(output_dir=out)
tpcc_queries().generate(output_dir=out)

# TPC-C Skew — Zipf hot-warehouse access distribution
tpcc_skew_data(scale_factor=10, hot_warehouse_fraction=0.2, skew_factor=0.99).generate(output_dir=out)
tpcc_skew_queries(scale_factor=10, hot_warehouse_fraction=0.2).generate(output_dir=out)

# JOB, YCSB, DSB, pgbench
job_data(scale_factor=1).generate(output_dir=out)
ycsb_data(scale_factor=1).generate(output_dir=out)
ycsb_queries(workload="B").generate(output_dir=out)
dsb_data(scale_factor=10).generate(output_dir=out)
pgbench_data(scale_factor=1).generate(output_dir=out)
pgbench_queries(workload="tpcb").generate(output_dir=out)

# BenchBase — generates XML configs + shell scripts for a live database
bb_data(benchmark="tpcc", scale_factor=10).generate(output_dir=out)
bb_queries(benchmark="tpcc", terminals=8, duration=120).generate(output_dir=out)
```

### Output layout

```
artifacts/
  tpch/data/sf_1/tables/   tpch/queries/
  tpcds/data/              tpcds/queries/
  tpcc/data/               tpcc/queries/
  tpcc_skew/data/          tpcc_skew/queries/
  job/data/                job/queries/
  ycsb/data/               ycsb/queries/
  dsb/data/                dsb/queries/
  pgbench/data/            pgbench/queries/
  benchbase/tpcc/data/     benchbase/tpcc/queries/
```

Each folder contains a `*_manifest.json` listing the generated files.

### GenerationResult

`generate()` returns a `GenerationResult`:

```python
result = tpch_data(scale_factor=1, mode="generate").generate(output_dir=out)
result.files      # list of generated file paths
result.metadata   # path to the manifest JSON

# Convert pipe-delimited .tbl / .dat to standard CSV (both kept on disk)
csv_result = result.as_csv()
```

Second call reuses existing files automatically. Pass `force=True` to regenerate.

---

## CLI Quickstart

```bash
# Validate a DriftSpec
python -m driftbench.cli validate-spec driftspec/examples/demo_data_single.yaml --json

# Dry-run (preview execution plan)
python -m driftbench.cli dry-run driftspec/examples/demo_data_single.yaml --json

# Execute
python -m driftbench.cli run-yaml driftspec/examples/demo_data_single.yaml
```

---

## Python API

```python
from driftbench import run_spec, trace_to_spec

run_spec("driftspec/examples/demo_data_single.yaml")
trace_to_spec("driftspec/trace_inputs/trace_data_mock.csv", "driftspec/generated/from_trace.yaml")
```

---

## MCP Server

```bash
python3 -m driftbench_mcp.server
```

Core workflow via MCP: `trace_to_spec` → `validate_spec` → `run_spec` → `list_outputs`

---

## Testing

```bash
python -m unittest discover -s test -p 'test_*.py' -v
```

---

## License

MIT — see [LICENSE](https://github.com/Liuguanli/DriftBench/blob/main/LICENSE).
