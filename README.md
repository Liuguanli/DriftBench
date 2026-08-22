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
python -c "from driftbench.data.ycsb import data; print(data(record_count=10).generate('./artifacts').summary())"
```

---

## Benchmark Adapters (`driftbench.data`)

Nine adapters generate local benchmark artifacts. Most synthetic generators need no
external tool; TPC-H `mode="generate"` may auto-build an unpinned upstream `dbgen`,
pgbench regression runs need PostgreSQL/pgbench, and BenchBase generates configuration
rather than a local dataset.

| Adapter | Workload type | Data format | Tables | Queries |
|---------|--------------|-------------|--------|---------|
| `tpch` | OLAP | `.tbl` (pipe-delimited) | 8 | 22 Python qgen-style SQL templates |
| `tpcds` | OLAP / Decision support | `.dat` (pipe-delimited) | 5 synthetic | 99 query IDs |
| `tpcc` | OLTP | `.csv` | 9 | 5 transaction types |
| `tpcc_skew` | OLTP + hotspot | `.csv` + weight manifest | 9 | 5 transaction types |
| `job` | OLAP / join-order | `.csv` | 11 (IMDB-like) | 20 SQL templates |
| `ycsb` | Key-value | `.csv` | 1 | 6 workload mixes (A–F) |
| `dsb` | Decision support | `.csv` | 3 star-schema | 3 SQL templates |
| `pgbench` | TPC-B (OLTP) | `.csv` | 4 | 3 workloads |
| `benchbase` | Multi-benchmark | XML + shell script | via live DB | 10 benchmarks |

Related benchmark docs: [complete adapter reference](docs/benchmark_reference.md),
[target orchestration contract](docs/benchmark_target_contract.md), and
[hands-on testing guide](docs/benchmark_testing_guide.html).

Adapter names describe modeled workload families, not official benchmark conformance.
Most outputs are DriftBench synthetic fixtures, and the TPC-H auto-build neither pins the
upstream revision nor records the `dbgen` binary hash. The canonical
[provenance, conformance, YAML-execution, and live-database boundaries](docs/benchmark_reference.md#provenance-conformance-and-execution-boundaries)
explain what is generated, what `benchmark_adapter` means inside a DriftSpec, and which
claims the artifacts do and do not support.

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
pgbench_queries(workload="select_only", clients=2, duration=5).generate(output_dir=out)

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

# Convert pipe-delimited .tbl / .dat to standard CSV (both kept on disk).
# Known TPC-H (8 tables) and TPC-DS (5 synthetic tables) get a proper
# header row, so the CSV is self-describing and usable directly by .drift().
csv_result = result.as_csv()

# Lightweight JSON-serializable summary for logs / dashboards / quick asserts.
result.summary()
# {'benchmark': 'tpch', 'artifact_type': 'data',
#  'output_dir': '/tmp/...', 'file_count': 8,
#  'tables': ['customer', 'lineitem', 'nation', ...]}
```

A second call reuses files only when its normalized generation parameters match the
manifest and every managed file has the recorded path, byte count, and SHA-256. Older
manifests rebuild once. Pass `force=True` to regenerate unconditionally; if an external
`source_dir` changes in place, use `force=True` because source contents are not checksummed.

### Only real-database regression gate: pgbench

This is currently DriftBench's only no-mock live-database regression gate. It covers
PostgreSQL 16 `select-only`; the other adapter and example tests validate artifacts and
do not establish database execution, performance, or benchmark conformance.
`driftbench benchmark pgbench` runs a native `pgbench -b select-only` baseline and a
DriftBench-generated `select_only` candidate against the same PostgreSQL instance.
Each of three paired rounds runs an isolated warmup and measurement, retaining raw
stdout/stderr and transaction logs. The version-controlled CI policy requires
PostgreSQL/pgbench 16, scale 1, two clients, 3-second warmups and 5-second measurements.
The PostgreSQL server must be running and its role/authentication configured first.

```bash
# Confirm that the pgbench client is major version 16, then create and initialize
# a new database using an already configured PostgreSQL role.
pgbench --version
createdb --host localhost --port 5432 --username driftbench driftbench_ci
pgbench --initialize --scale=1 \
  --host localhost --port 5432 --username driftbench driftbench_ci

# Generate the candidate used by the checked-in default policy.
python -c "from driftbench.data.pgbench import queries; queries(workload='select_only', clients=2, duration=5).generate(output_dir='artifacts', force=True)"

driftbench benchmark pgbench \
  --candidate-script artifacts/pgbench/queries/pgbench_select_only.sql \
  --output-dir benchmark-artifacts/results \
  --database driftbench_ci --host localhost --port 5432 --username driftbench \
  --json

# Verify a copied bundle later, without PostgreSQL, network access, or Git.
driftbench benchmark verify --bundle benchmark-artifacts/results --json
```

The command requires a new or empty output directory and writes `baseline.json`,
`candidate.json`, `decision.json`, `execution_order.json`, hashed raw evidence, the
canonical policy at `inputs/policy.json`, the exact executed candidate at
`inputs/candidate.sql`, and `environment.json`. Both results record byte counts and
SHA-256 digests for those inputs and the environment snapshot. The snapshot includes the
DriftBench version/source revision, Python, OS/CPU, password-free connection identity,
PostgreSQL/pgbench versions, key server settings, and the scale inferred from initialized
pgbench tables. Producing a valid bundle requires a clean DriftBench source checkout:
the runner checks the runtime-source paths before the first phase and again after the last
measurement, requiring the same full 40-character HEAD both times. `DRIFTBENCH_GIT_SHA`
is only an assertion that must equal that HEAD; it cannot bypass a dirty checkout, and an
installed wheel never borrows the caller repository's SHA.

For each measurement (not warmup), authoritative TPS is successful transactions divided
by runner-measured elapsed seconds. It must be within an inclusive 5% of pgbench-reported
TPS before any aggregation or threshold decision; a mismatch is invalid evidence and exits
4. Exit codes are 0 (verified pass), 3 (configuration/path), 4
(execution/parser/provenance/integrity), and 5 (complete verified threshold regression).
The offline verifier proves that a bundle is internally self-consistent with its captured
bytes and policy; it does not authenticate who produced the bundle or where it originated.
Incomplete capture fails closed while retaining evidence. DriftBench never updates a
baseline or policy automatically.

### Applying drift to benchmark data

`GenerationResult` exposes `.drift()` and `.drift_multi()` to apply data drift directly — no manual schema extraction or generator setup needed.

**Single-table drift:**

```python
from driftbench.data.tpch import TPCHData

result = TPCHData(scale_factor=1, source_dir="path/to/tbls").generate().as_csv()

# Inject outliers into lineitem.l_quantity
drifted = result.drift("lineitem", "outlier_injection", column="l_quantity", n=500)

# Skew the price/discount distribution
drifted = result.drift("lineitem", "value_skew",
                       columns=["l_extendedprice", "l_discount"], skewness=2)
```

`drift()` writes the drifted CSV to `<output_dir>/<table>_<drift_type>.csv` by default. Pass `output_path=` to override. Returns a new `GenerationResult` pointing at the drifted file.

Every `.drift()` call also emits a reproducible DriftSpec YAML (`<output_stem>.driftspec.yaml`) next to the CSV — kept out of `result.files` but recorded under the manifest's `driftspec` key. Running that YAML through `driftbench.spec.core.run_all` regenerates **byte-identical** output, so a Python-generated drift can be shared or automated as a spec without rework. The function-call path (fast, imperative) and the spec path (declarative, version-controllable, reproducible) are the same engine and produce identical results for the same seed and parameters.

**Multi-table drift:**

```python
# FK relationships for tpch / job are wired automatically
drifted = result.drift_multi([
    {"op": "skew_column", "target": "lineitem", "column": "l_quantity",
     "fraction": 0.2, "skewness": 2},
    {"op": "delete_keys", "target": "orders", "key_column": "o_orderkey",
     "fraction": 0.05,
     "propagate": [{"relationship": "lineitem_orders", "policy": "drop"}]},
])
```

Pass `relationships=[]` or a custom list to override the built-in FK maps. Supported benchmarks with auto-wiring: `tpch`, `job`. `tpcc` and `tpcc_skew` require explicit relationship definitions because their joins use composite keys.

**DriftSpec YAMLs** — five ready-to-run benchmark drift examples are in `driftspec/examples/`:
- `tpch_lineitem_drift.yaml`
- `tpcc_drift.yaml`
- `job_drift.yaml`
- `ycsb_drift.yaml`
- `pgbench_drift.yaml`

Paper-ready executable TPC-H examples: [data drift](driftspec/examples/paper_tpch_data_drift.yaml),
[query-workload drift](driftspec/examples/paper_tpch_query_workload_drift.yaml), and the
[examples reproduction guide](driftspec/examples/README.md).

---

## CLI Quickstart

The commands below use repository fixtures and must be run from a source checkout.
PyPI installations do not include `driftspec/examples/` or its fixture data.

```bash
# Validate a DriftSpec
python -m driftbench.cli validate-spec driftspec/examples/demo_data_single.yaml --json

# Deep local readiness check (schema, handler parameters, inputs, outputs, adapter import)
python -m driftbench.cli validate-spec driftspec/examples/demo_data_single.yaml --deep --json

# Dry-run (preview execution plan)
python -m driftbench.cli dry-run driftspec/examples/demo_data_single.yaml --json

# Execute
python -m driftbench.cli run-yaml driftspec/examples/demo_data_single.yaml
```

Deep validation is an opt-in, read-only preflight. Relative paths resolve from
the current working directory, just as they do during execution. It checks
local files/directories, output collisions and writability, all registered
DriftSpec handler contracts, and benchmark-adapter availability. It does not
generate data, execute handlers, create directories, connect to databases, or
probe external tools. Existing outputs and unchecked external services are
reported as warnings. Exit code `0` means no validation errors, `3` means the
spec is not locally ready, and `4` means the validator itself failed.

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
pip install -e ".[test]"
python -m unittest discover -s test -p 'test_*.py' -v
```

The five real PostgreSQL 16 integration tests are discovered by this command but skip by
default whenever `DRIFTBENCH_REQUIRE_PG_INTEGRATION` is not exactly `1`. The remote
[Benchmark Regression workflow](.github/workflows/benchmark-regression-pgbench.yml)
explicitly sets it to `1` and runs those tests against PostgreSQL 16 with no mocks or
skips.

---

Visualization results: [DriftBench Visualization Gallery](visualization/GALLERY.md).

---

## License

MIT — see [LICENSE](https://github.com/Liuguanli/DriftBench/blob/main/LICENSE).
