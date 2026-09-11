# sysbench, SSB and LDBC artifact support

These adapters are available in the local development source. They are not yet
included in a published package release. Each follows
`data(...).generate(output_dir=...)` / `queries(...).generate(output_dir=...)`
and returns a `GenerationResult` with managed files and a cache-v3 manifest.

Generation here means preparing files. No adapter in this document connects to a
database, executes SQL or a workload, installs native tools, downloads datasets,
or establishes benchmark conformance. Source and destination directories must be
separate. Supplied source files remain unchanged, and manifests record their
SHA-256 hashes. Keep the source version and sharing permissions with the artifacts.

## sysbench: native preparation and run plans

```python
from driftbench.data.sysbench import data, queries

data(tables=4, table_size=250000, db_driver="pgsql", seed=42).generate("./artifacts")
queries(
    workload="oltp_read_only", tables=4, table_size=250000,
    threads=8, duration=60, rate=1000, db_driver="pgsql",
    distribution="uniform", seed=42,
).generate("./artifacts")
```

Each phase exports `sysbench.cnf`, `command.json`, `README.md` and a manifest under
`artifacts/sysbench/data/` or `artifacts/sysbench/queries/`. The data result is a
**prepare plan**: it contains zero generated database rows. `command.json` provides
an argument array relative to its own directory. A user runs it separately after
supplying local connection options and preparing a dedicated target database.

Supported drivers: `mysql`, `pgsql`. Supported stock Lua workloads:
`oltp_read_only`, `oltp_read_write`, `oltp_write_only`, `oltp_point_select`.
The workload's table count and size must match the native preparation phase.
Supported distributions: `uniform`, `gaussian`, `pareto`, `zipfian`.

Integers reject booleans. Tables and table size are positive signed INT32 values;
threads are 1–1024, duration 1–86400 seconds, rate 0–INT32_MAX (0 means unlimited),
and seed 1–INT32_MAX. Seed 0 is excluded because native sysbench uses the clock for it.
No credentials or database endpoints are embedded in generated defaults.

The native option and script contract is pinned to
[sysbench 3ceba0b1](https://github.com/akopytov/sysbench/tree/3ceba0b1e115f8c50d1d045a4574d8ed643bd497).
That source accepts the four distributions above; its README's legacy `special`
entry does not match the native parser. Deterministic config files do not imply
identical concurrent database schedules or measured performance.

## SSB: import five tables and export thirteen analytical queries

Obtain or generate data separately with an SSB dbgen tool. Point `source_dir` at
the directory containing `customer.tbl`, `part.tbl`, `supplier.tbl`, `date.tbl`
and `lineorder.tbl`. These must be the canonical nonempty, headerless,
pipe-delimited SSB tables, not the TPC-H files with similar names.

```python
from driftbench.data.ssb import data, queries

baseline = data(source_dir="./inputs/ssb").generate("./artifacts")
all_queries = queries().generate("./artifacts")
selected = queries(query_ids=["1.1", "3.2", "4.3"]).generate("./selected-artifacts")
```

Data imports produce five **headered CSV files** and `schema.sql` under
`ssb/data/`. Source field values and integer price units are preserved. Import
validates column counts and numeric fields; it does not verify primary/foreign
keys, benchmark distributions or source-generator provenance beyond the supplied
files' hashes. The source contract is
[SSB dbgen 219403ad](https://github.com/electrum/ssb-dbgen/tree/219403ad7d1dd32ae1f97b5553abf92129fccd7f).
Its legacy `dss.ddl` is a TPC-H file and is not the exported SSB schema.

Queries use IDs `1.1`–`1.3`, `2.1`–`2.3`, `3.1`–`3.4`, `4.1`–`4.3` and filenames
such as `q1_1.sql`. They are DriftBench's PostgreSQL-compatible renderings of the
[SSB query definitions](https://www.cs.umb.edu/~poneil/StarSchemaB.PDF), not native
qgen output. The generated schema matches the SQL, including `p_brand1` and the
quoted `"date"` table. Load the CSVs and run the SQL with your own database tooling.

Literal choices follow the Rev.3 paper's **SQL blocks**: Q1.3 uses quantity 26–35
and Q2.3 uses `MFGR#2221`. The adjacent prose contains different values, and some
downstream suites use other variants. Record this query source when comparing
results; adapter generation does not reconcile results from different variants.

The headered CSVs can be inputs to existing single-table Drift operations. Such
changes can break joins; relationship preservation requires explicit modeling
and validation. This adapter adds no automatic relational integrity guarantee.

## LDBC: SNB Interactive v1 data and native driver handoff

The supported source is **Hadoop Datagen v1.0.0, `csv_merge_foreign`**. This is a
specific SNB Interactive v1 integration, not support for every LDBC suite.
No graph is synthesized, and no Cypher/SQL templates are invented by this adapter.

Source pins:

- [Hadoop Datagen 37d35f40](https://github.com/ldbc/ldbc_snb_datagen_hadoop/tree/37d35f40f5023fcf1afd3b6d0984f71c202f4bca) (v1.0.0).
- [SNB Interactive driver 4cd13735](https://github.com/ldbc/ldbc_snb_interactive_v1_driver/tree/4cd13735f964406ad34f34ccd5bef4d6e6c284d0) (v1.2.0).
- [Official PostgreSQL configuration example](https://github.com/ldbc/ldbc_snb_interactive_v1_impls/blob/11db98cc2ba14c33492f6c0c34e68c8be7e22e5f/postgres/driver/benchmark.properties).

### Data inputs

Point at a local `social_network/` directory containing `static/` and `dynamic/`.
All 20 CSV file families used by the pinned serializer are required; partition
suffixes such as `_0_0.csv` are preserved. Headers are checked against that
serializer, including its lowercase embedded foreign-key names such as `place`.
Relationship partitions may contain only a header; at least one person is required.

```python
from driftbench.data.ldbc import data, queries

graph = data(
    source_dir="./inputs/ldbc/social_network",
    format="csv_merge_foreign",
    benchmark_version="snb-interactive-v1",
    date_format="epoch_millis",
).generate("./artifacts")
```

Use `date_format="iso8601"` instead for Datagen's string date/date-time output.
The declaration is validated against birthday/creation/join dates; files are
copied byte-for-byte without date conversion. Initial graph files are staged in
`ldbc/data/social_network/`. Data import does not include the separate update
streams or substitution parameters. Neither graph referential validity nor a
matching scale factor is established by checking these files.

### Query parameters and driver configuration

Supply all `interactive_1_param.txt` through `interactive_14_param.txt` in a
separate directory, each with a pipe-delimited header and at least one data row.
The native driver requires all 14 even when some queries are disabled. Parameter
dates use integer epoch milliseconds. For example, query 1 uses
`personId|firstName`; queries 13/14 use `person1Id|person2Id`.

Supply your existing native `.properties` file for the database implementation
you intend to use. It must specify `db`, `mode=execute_benchmark`, positive
`thread_count`, `operation_count`, `time_compression_ratio`, a supported
`ldbc.snb.interactive.scale_factor`, and all complex/short-read enable flags.
Enabled short reads also require `short_read_dissipation` in 0–1.
Connection settings, query directory and implementation-specific options stay
in that file. Supported scale labels are 0.1, 0.3, 1, 3, 10, 30, 100, 300 and 1000.

```python
read_plan = queries(
    parameters_dir="./inputs/ldbc/substitution_parameters",
    driver_config="./local/benchmark.properties",
    read_only=True,
).generate("./artifacts")

mixed_plan = queries(
    parameters_dir="./inputs/ldbc/substitution_parameters",
    driver_config="./local/benchmark.properties",
    updates_dir="./inputs/ldbc/social_network",
    read_only=False,
).generate("./mixed-artifacts")
```

The mixed plan requires matching, nonempty
`updateStream_<partition>_<part>_forum.csv` / `_person.csv` pairs, all eight native
update enable flags in the config, and at least one enabled update operation.
The files are preserved; event decoding, timing, data/stream/parameter scale
compatibility and DB-specific validity remain the external driver's responsibility.

Outputs under `ldbc/queries/` include the original `user.properties`, all 14
parameter files, `overrides.properties`, `command.json`, instructions and a manifest.
The native v1.2.0 parser gives the **first** `-P` file priority, so the plan orders
the overrides before the user config. Read-only mode explicitly disables all
eight update operations and supplies a directory without update streams, even
if the original config enabled writes. Mixed mode preserves the user's operation
policy and points at staged update streams.

Replace the classpath placeholder in `command.json` with your installed driver
and database implementation. Use the recorded working directory to preserve
relative paths in your original config, and load the matching graph separately.
Native overrides use absolute local paths; regenerate after moving the artifacts.
The preserved user config may contain credentials: keep these local artifacts
private. Its values are not included in the DriftBench manifest, only its hash.

## Local caching and request files

Identical parameters and verified source/output hashes reuse local artifacts;
changed sources or tampered outputs invalidate reuse. `force=True` regenerates
managed files while retaining unrelated files. Symlink/junction escapes,
hardlinked output files and input/output overlap are rejected.
LDBC also rejects extra CSV partitions in its staged graph directories or extra
update streams in the selected native updates directory, even on cache hits or
with `force=True`. If the source partition set shrinks, choose a clean
`output_dir`; existing files are preserved so native tools cannot silently mix
old and current inputs.

All three families are in the public artifact-request registry. For example:

```yaml
schema: driftbench.artifact-request/v1
benchmark: ssb
artifact_type: queries
parameters:
  query_ids: ["1.1", "3.2"]
force: false
```

```console
driftbench cache materialize --request ssb.yaml --output-dir ./artifacts --cache-mode off
```

These adapters are **not remote-cache eligible**. Their addition does not extend
the fixed eight-family persona smoke protocol or create a new MCP generation tool.
DriftSpec `benchmark_adapter` preflight checks import/readiness only; generate the
baseline separately and bind supported input files when applying optional Drift.
