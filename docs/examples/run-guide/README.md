# Run guide: supported inputs and a real database demonstration

For your own CSV and SQL, start with the [independent own-input tutorial](#use-your-own-data-and-queries).

Run these commands from a DriftBench **source checkout** with Python 3.10–3.12
and `python -m pip install -e .`. The guide files and demo helper are source
resources, not installed console commands. All examples use local artifacts;
they do not require Azure or a remote cache.

## 1. Choose one data acquisition route

Each request uses the real TPC-H adapter at scale factor 0.001. Select a route:

| Request | Prerequisite | Materialized table location |
| --- | --- | --- |
| `tpch-data-generate.yaml` | Native `dbgen` on PATH, repository build or cache; otherwise git, make and a C compiler for automatic build | `output/run-guide/data/tpch/data/sf_0.001/tables/` |
| `tpch-data-local-dbgen.yaml` | Edit `dbgen_path` to your executable, including `.exe` on Windows | Same `tables/` directory |
| `tpch-data-copy.yaml` | Edit `source_dir` to existing `region`, `nation`, `supplier`, `customer`, `part`, `partsupp`, `orders`, `lineitem` `.tbl` files | `output/run-guide/data/tpch/data/sf_0.001/` |

```console
python -m driftbench.cli cache materialize --request docs/examples/run-guide/tpch-data-generate.yaml --output-dir output/run-guide/data --cache-mode off --json
```

Replace the request filename to use either alternative. `dbgen` generation
executes a native generator and can need network access/build tools on first use.
Copy validates all eight nonempty canonical files before writing outputs.
The source directory must contain data at the scale you declare; the adapter's
scale label does not validate the scientific provenance of supplied data.

Both routes produce `tpch_data_manifest.json`. The local manifest records declared
source/binary paths and parameters plus generated output hashes, not source-file
or binary content identity. Parameter changes invalidate reuse. If valid source
files or a generator are replaced at the same path, choose a fresh output directory
or add top-level `force: true` to the artifact request to regenerate its outputs.
Local manifest reuse is available;
TPC-H **data is not eligible for the remote artifact cache**. Packaged TPC-H query
artifacts can use configured remote cache modes; external template/distribution
paths cannot. See [the cache guide](../../azure_hns_cache.md).

## 2. Choose queries or a scheduled workload

Fixed parameters for the bundled Q6 template:

```console
python -m driftbench.cli cache materialize --request docs/examples/run-guide/tpch-queries.yaml --output-dir output/run-guide/queries --cache-mode off --json
```

This writes `output/run-guide/queries/tpch/queries/tpch_queries.sql`,
`tpch_queries.csv` (headers `query_id,index,sql`) and a manifest.
There is no `static` mode: this request selects `custom` with explicit fixed
parameters. `tpch-queries-qgen.yaml` instead generates two instances of each of
the 22 bundled queries, using seed 42 and the built-in Python qgen-style sampler.
It does not execute native official `qgen`.

For your own numbered templates and synthetic timestamps:

```console
python -m driftbench.cli validate-spec docs/examples/run-guide/tpch-workload.yaml --deep --json
python -m driftbench.cli run-yaml docs/examples/run-guide/tpch-workload.yaml
```

`templates/6.sql` is a supplied **teaching adaptation of the Q6 shape**, with
explicit date boundaries for PostgreSQL and SQLite. It is not an official TPC-H
query compliance claim. The spec substitutes `:1`…`:4` and writes
`output/run-guide/workload.sql` plus `workload.csv` (`timestamp,sql`).
Templates must be named by query ID, such as `6.sql`. Blank lines, comments and
qgen control lines are stripped before substitution. Custom parameter types are
`fixed`, `choice`, `int_range`, `float_range`, `date_range` and `dss_dist`; scalars
mean fixed values and lists mean random choices. Quote string/date placeholders
in the template itself; substitution is generation, not database parameter binding.

Supported timestamp patterns are `uniform`, `periodic`, `bursty` and `long_tail`.
These annotate a synthetic schedule. `run-yaml` does not replay arrivals or
execute the generated SQL. A `workload` output contains plain SQL even if someone
gives it a `.csv` extension; use `temporal` for actual timestamped CSV.

## 3. Optionally apply data Drift

Convert native generated data to a common headered CSV path:

```console
python docs/examples/run-guide/prepare_lineitem.py --input output/run-guide/data/tpch/data/sf_0.001/tables/lineitem.tbl --output output/run-guide/data/tpch/data/sf_0.001/lineitem.csv
python -m driftbench.cli validate-spec docs/examples/run-guide/tpch-data-drift.yaml --deep --json
python -m driftbench.cli run-yaml docs/examples/run-guide/tpch-data-drift.yaml
```

For the copy route, remove `/tables` from the converter's `--input` only.
The converter requires a new output filename and preserves the original `.tbl`.
The DriftSpec reads that CSV and writes `output/run-guide/lineitem-drifted.csv`,
changing 80% of `l_extendedprice` values with seed 42. Compare the baseline and
drifted files separately. This example keeps key columns unchanged. Arbitrary
single-table transformations do not establish referential integrity across a
whole benchmark or guarantee a particular performance effect.

## 4. Convert an external trace summary

`workload-trace.json` shows the accepted normalized summary contract. Replace its
run configuration with the summary you derived from your own observations:

```console
python -m driftbench.cli trace-to-spec docs/examples/run-guide/workload-trace.json --output output/run-guide/trace-spec.yaml --trace-type workload
python -m driftbench.cli validate-spec output/run-guide/trace-spec.yaml --deep --json
python -m driftbench.cli run-yaml output/run-guide/trace-spec.yaml
```

The result is `output/run-guide/trace-workload.csv`, with `timestamp,sql` headers.
The JSON explicitly selects the `workload/sql_templates/tpch` handler and contains
`variables.template_dir`, `query_ids`, `defaults`, `params`, and `query_runs`.
Trace conversion accepts supported JSON/CSV summaries; raw database query logs
need preprocessing. It does not promise literal replay fidelity. RedBench
statistics have a separate existing [mapping example](../../../driftspec/trace_inputs/redbench_mapping.json);
without mapping, converted statistics alone are not a runnable workload.

## 5. Actually execute SQL in an isolated teaching database

The helper below prepares its own eight-table miniature fixture, copies it
through the data adapter, generates SQL, converts lineitem, applies the DriftSpec,
converts the trace summary and loads the **selected lineitem table** into an
actual database. It executes one fixed query and three trace-derived queries
against each of the baseline and drifted phases: eight SELECT executions total.
Other fixture tables exercise the acquisition contract but are not loaded for Q6.

With Docker Desktop or a local Docker Engine running:

```console
python scripts/run_learning_demo.py --engine postgres --output-dir output/run-guide-demo
```

The helper pulls `postgres:16` if needed, records its exact image ID and runs that
inspected ID. It creates
a unique labeled container, a temporary random password, a dynamic port bound to
`127.0.0.1`, a dedicated database and tmpfs storage (512 MiB container memory,
256 MiB tmpfs, one CPU). It uses bounded startup/query timeouts and removes only
its own verified container in cleanup. There is no existing-database or arbitrary
DSN option. Docker Desktop itself and the pulled image remain available after
the demonstration; container storage is removed. Remote Docker contexts are rejected.

Without Docker, explicitly select a real SQLite teaching simulation:

```console
python scripts/run_learning_demo.py --engine sqlite --output-dir output/run-guide-demo-sqlite
```

`--engine` is required. There is no silent fallback. SQLite runs the same supplied
date-boundary template, not the full bundled PostgreSQL/TPC-H SQL dialect.
Use a **new or empty** output directory for each run. Existing files are preserved.

Outputs include:

- `results.json`: engine/version, phase row counts, actual SQL/result rows, elapsed
  times and input artifact hashes; PostgreSQL includes verified container removal.
- `commands.json`: actual CLI arguments, exit status, stdout and stderr.
- `source/fixture-provenance.json`: explicit hand-authored miniature provenance
  and table counts. The `sf_0.001` directory name is only an adapter tag for this
  fixture; it is not a claim that the fixture is official scale-factor 0.001 data.
- `data/`, `queries/`, `requests/`, `specs/`, `templates/`, `trace/`, `lineitem.csv`,
  `lineitem-drifted.csv` and `trace-workload.csv`: the concrete inputs and outputs.
- `teaching.sqlite` for SQLite, or `failure.json` if a preparation/runtime step fails.

The timestamp column is an annotation; execution is sequential with no arrival
replay. Elapsed times demonstrate instrumentation only. This fixture and helper
do not establish TPC-H compliance, benchmark performance or a regression gate.
For a research experiment, supply native data, appropriate schema/loading scripts,
dialect-verified SQL, an execution driver and a measurement policy. The existing
`orchestrate --execute` route can invoke your explicit external commands and
capture their outputs; without `--execute` it only prepares a plan. Its command
placeholders are `{spec_path}`, `{target_name}`, `{manifest_path}` and
`{manifest_dir}`. The separate `benchmark pgbench` runner measures pgbench, not TPC-H.

## Use your own data and queries

This independent path needs the current source checkout and installed Python
dependencies described above, with commands run from the repository root.
It does not require TPC-H acquisition. Use Data Drift, Query Drift or both.
The files are local development tutorial resources, not a claim about an
already published wheel or a public dataset upload service.

### Prepare matching inputs

`own-inputs/orders.csv` is a hand-authored 12-row teaching fixture with
`order_id`, `region`, and numeric `amount`. `own_query_drift.py` supplies two SQL
queries for that schema: an order lookup and a regional sum. Your research
question determines which queries to supply; the CSV does not choose them.

To use your own data, replace `data_source.path` in `own-data-drift.yaml` with
your headered CSV, and set `base_table` to its filename without the extension
(the CSV extractor's table name). Change `columns`, `portion`, `skewness`, and
`seed` to suit your experiment. This numeric example needs a non-key numeric
column with multiple distinct, non-null values. Convert other formats to CSV
for this path. Set fresh output and schema paths, separate from every input:
`run-yaml` can overwrite its declared output, and existing schema files are reused.

### Data Drift through the CLI

Run each command only after the previous command succeeds:

```console
python -m driftbench.cli validate-spec docs/examples/run-guide/own-data-drift.yaml --deep --json
python -m driftbench.cli dry-run docs/examples/run-guide/own-data-drift.yaml --json
python -m driftbench.cli run-yaml docs/examples/run-guide/own-data-drift.yaml
```

The spec applies `value_skew` to 75% of the `amount` rows with seed 42. It writes
`output/run-guide/own-inputs/orders-drifted.csv` (12 rows) and
`orders-schema.json`, preserving the source CSV, `order_id` and `region`.
Check exact column names and execution errors; deep validation is not a
scientific-validity check. Inspect changed values before loading. This
transformation does not enforce domain bounds or cross-table constraints.

### Query Drift through the public Python API

Edit the `QueryTemplate` SQL strings in `own_query_drift.py` to match your
dataset's table, columns and target SQL dialect. Assign unique IDs and cover
every ID in both weight maps. The script calls
`driftbench.api.apply_query_workload_mix_drift`, shifting lookup/report
probabilities from 80/20 to 20/80 with 100 samples per phase and seed 42.

```console
python docs/examples/run-guide/own_query_drift.py --output-dir ./output/run-guide/own-query-mix
```

Choose a new directory: this script refuses an existing output directory.
The API returns objects in memory; the script writes `baseline.sql`,
`drifted.sql` and `mix.json`. Each SQL file contains 100 statements whose SQL
text is unchanged. JSON records original SQL, normalized weights, observed
counts, sample size, seed, algorithm and semantic hash. Probabilities do not
guarantee exact finite-sample counts (this fixture yields baseline 80/20 and
drifted 19/81). Reuse identical inputs, settings and dependency versions to
reproduce the samples.

Query-frequency drift does not read or alter the CSV, rewrite predicates,
estimate selectivity/cost, add arrival timestamps, validate SQL semantics or
execute a workload. You can hold data fixed while changing the query mix, or
hold queries fixed while changing data, then combine both if needed. Retain
the original CSV, SQL, spec, seed, dependency versions and generated metadata.
Use your own database schema, loader and query driver for execution; adapt the
earlier TPC-H loading example's table/schema and paths to these inputs.
