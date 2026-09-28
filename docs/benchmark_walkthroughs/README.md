# Benchmark walkthroughs: Notebook + Python

This directory provides paired `.ipynb` and `.py` walkthroughs for all 12
adapters. Each pair calls the same Python workflow; the notebook does not carry
a second drift or sampling implementation. In these walkthroughs, **load means
local file or DataFrame inspection, not loading data into a database**.

Each walkthrough checks package identity, baseline files, local data reading,
cardinality drift, query artifacts, a baseline query stream, a changed query
mix, parameter or profile changes, and a final JSON report. By default it does
not access Azure, download data, connect to a database, or execute generated
SQL, shell, Java, or benchmark tools.

## Choose a walkthrough

| Benchmark | Notebook / Python | Data prerequisite and inspected table | Meaning of query artifacts and streams |
|---|---|---|---|
| TPC-H | [Notebook](tpch.ipynb) / [Python](tpch.py) | Local `.tbl` files, or an explicitly prepared Azure SF0.01 cache copied into the run; `lineitem` | qgen-style SQL instances; query IDs, instance count, and seed are configurable |
| TPC-DS | [Notebook](tpcds.ipynb) / [Python](tpcds.py) | Five small SF1 synthetic tables; 10,000 `store_sales` rows | 99 query IDs and configuration; **no SQL bodies** |
| TPC-C | [Notebook](tpcc.ipynb) / [Python](tpcc.py) | One warehouse and nine synthetic tables; 3,000 `customer` rows | Five transaction templates; changing frequency is not arbitrary SQL rewriting |
| TPC-C Skew | [Notebook](tpcc_skew.ipynb) / [Python](tpcc_skew.py) | Two warehouses; 6,000 `customer` rows | Templates and hotspot annotations; annotations are not executed hotspot traffic |
| JOB | [Notebook](job.ipynb) / [Python](job.py) | Eleven synthetic tables; 500 `title` rows | Twenty SQL templates; no general predicate rewrite API |
| YCSB | [Notebook](ycsb.ipynb) / [Python](ycsb.py) | 200 `usertable` records by default | Operation names plus XML/properties; not SQL |
| DSB | [Notebook](dsb.ipynb) / [Python](dsb.py) | Three synthetic tables; 5,000 `lineorder` rows | Three SQL templates |
| pgbench | [Notebook](pgbench.ipynb) / [Python](pgbench.py) | SF1 still means 100,000 accounts; the drift example inspects 10 teller rows | Native script references; pgbench is not executed |
| BenchBase | [Notebook](benchbase.ipynb) / [Python](benchbase.py) | **A load plan only; no table data** | Operation names extracted from XML; Java is not executed |
| sysbench | [Notebook](sysbench.ipynb) / [Python](sysbench.py) | **A prepare plan only; no table data** | References to two native profiles, not individual SQL statements |
| SSB | [Notebook](ssb.ipynb) / [Python](ssb.py) | User-supplied five-table SSB `.tbl` input; `lineorder` | Thirteen SQL templates with subset selection |
| LDBC | [Notebook](ldbc.ipynb) / [Python](ldbc.py) | User-supplied 20 CSV file families; pipe-delimited `person` partitions | Handoff of fourteen parameter files and native driver configuration; no SQL/Cypher bodies |

The table states the designed boundaries, not proof that every step works in
every published version. The run report is the authority for that run. See the
[adapter extension guide](../benchmark_extensions.md) for SSB and LDBC input
formats; do not substitute same-named TPC-H tables for SSB. TPC-H SF10 is about
11 GB and is intentionally rejected by the walkthrough's 64 MiB external-input
limit.

### Evidence and limitations

The source-checkout test suite verifies all 12 default workflows, local-input
branches, standalone CLIs, generated notebook/source parity, notebook schema,
empty outputs, and ordered execution of raw Python cells with explicit test
output roots. These are source tests, not evidence of a published wheel, a real
Jupyter front end, database execution, official benchmark conformance, or
performance. `NEEDS_INPUT`, `UNAVAILABLE`, and `NOT_SUPPORTED` remain capability
results; a passing test does not turn them into benchmark success.

TPC-H, SSB, and LDBC input-path tests use small local fixtures. They verify the
import and inspection mechanism, not official scale or conformance. Cold-start
regressions also cover Windows host-architecture initialization before guarded
package imports. A standard-library host probe may invoke the Windows `ver`
command; it is not a benchmark tool, network request, or database operation.
Generated-artifact execution remains blocked.

## Distinguish an installed package from a source checkout

The distribution is **`driftbench-db`** and the import package is `driftbench`.
Do not install an unrelated similarly named project. Similar version strings do
not prove that two imports expose the same API. Every run records:

- `distribution_version`: installed distribution metadata;
- `module_version`: the imported module's version declaration;
- `import_file`: the package file actually imported;
- `selected_target` and `editable_install`: the requested target and whether
  it is editable; and
- `host_architecture`: the architecture observed during explicit
  initialization, or an empty string if the standard library cannot identify it.

`create()` records host metadata before importing DriftBench and NumPy inside
the operation guards. Importing a companion module alone does not probe the
host. The `installed` target rejects source shadowing, editable installs, and
imports without matching distribution metadata. A missing adapter or API is
reported as `UNAVAILABLE`; the workflow does not silently fall back to checkout
code or invent a replacement algorithm. This check locates an installation but
does not prove that its wheel came from PyPI; retain installation provenance and
hashes when that distinction matters. Restart the notebook kernel after changing
the selected package or target.

### A. Inspect an installed package

Copy this whole directory outside the repository, for example to
`C:\DriftBenchWalkthroughs\companions`. Do not copy only one notebook: its
paired `.py` file and `_walkthrough.py` are required. The companions are
included in the source distribution, not guaranteed in the wheel.

Create an isolated environment outside the checkout. Installation is an
explicit preparation step and is never performed by a notebook:

```powershell
Set-Location C:\DriftBenchWalkthroughs
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install driftbench-db jupyterlab
Set-Location .\companions
..\.venv\Scripts\python.exe -m jupyter lab
```

Open `ycsb.ipynb`, leave `PACKAGE_TARGET = "installed"`, inspect the identity
cell first, and then run the cells in order. For reproducible work, pin the
chosen `driftbench-db==<version>` and retain the package source, wheel SHA-256,
report, and input provenance.

The paired script does not require Jupyter:

```powershell
..\.venv\Scripts\python.exe .\ycsb.py --package-target installed --output-dir C:\DriftBenchWalkthroughs\runs
```

### B. Inspect this development checkout

From the repository root in the selected development environment:

```powershell
python -m pip install -e .
python .\docs\benchmark_walkthroughs\ycsb.py --package-target checkout --output-dir "$HOME\DriftBenchWalkthroughs"
```

Start notebooks from this directory, use the same Python environment, and set
`PACKAGE_TARGET = "checkout"` explicitly. The identity check fails if the
actual import path is outside this repository.

The same steps can be called from a notebook or Python program. Run from this
directory so the companion import is available; do not add the repository root
to an environment intended to test the installed package:

```python
from pathlib import Path
from ycsb import create

demo = create(
    output_dir=Path.home() / "DriftBenchWalkthroughs",
    package_target="installed",
    record_count=200,
    sample_size=200,
    seed=42,
)
demo.identity
demo.generate_data()
demo.load_data()
demo.preview()
demo.drift_data(scale=0.5)
demo.generate_queries()
demo.query_stream()
demo.change_mix()
demo.change_query_parameters(workload="A", target_rate=20)
report = demo.report()
```

### C. Explicitly prepare the Azure TPC-H SF0.01 dataset

TPC-H offers one optional preparation step that is off by default. It calls the
selected package's public `catalog.materialize` API; it does not relabel locally
generated data as Azure data. If the selected installed package lacks that API,
the step reports `UNAVAILABLE` without loading checkout code. A first download
requires an identity already authorized for the dataset and the
`driftbench-db[azure]` optional dependencies. The example does not install
dependencies, log in, or change permissions.

In the `tpch.ipynb` settings cell, choose the target package explicitly:

```python
PACKAGE_TARGET = "checkout"  # Use only when intentionally testing this checkout.
AZURE_DATASET_ID = (
    "tpch/data/immutable-dataset-v1/"
    "72764dad47622f7d5e0387b6faf623f2a4257648533b6f08040adcc6393cba2c"
)
DATASET_CACHE_DIR = Path.home() / "DriftBenchDatasets"
SOURCE_DIR = None
SOURCE_SCALE_FACTOR = None
```

Run the identity cell and **1a. Azure preparation**, then continue through the
local data, drift, and query steps. The ID identifies all eight SF0.01 tables,
about 10.6 MB, not an SF10 sample. Scale comes from the selected catalog and
provenance instead of local size inference. Committed notebooks remain
unexecuted; a Python test is not evidence that a Jupyter kernel ran them.

The companion object and CLI use the same preparation method:

```python
from tpch import create

demo = create(output_dir=Path.home() / "DriftBenchWalkthroughs", package_target="checkout")
source = demo.prepare_azure_data(AZURE_DATASET_ID, cache_dir=DATASET_CACHE_DIR)
print(source["cache_outcome"], source["payload_dir"])
report = demo.run_all()
```

```powershell
python -B -m docs.benchmark_walkthroughs.tpch --package-target checkout `
  --output-dir "$HOME\DriftBenchWalkthroughs" `
  --azure-catalog-id tpch/data/immutable-dataset-v1/72764dad47622f7d5e0387b6faf623f2a4257648533b6f08040adcc6393cba2c `
  --dataset-cache-dir "$HOME\DriftBenchDatasets"
```

The first successful population reports `downloaded`. Later calls verify the
complete file set, sizes, SHA-256 values, metadata, and source binding before
reporting `hit`; a valid hit downloads zero bytes, creates no Azure client, and
does not read credentials. A damaged cache fails without automatic replacement.
Keep cache and run-output roots separate so local transforms preserve the
original files.

Only an explicit preparation call may use the network. A successful download
sets `network_or_database_execution` to `true`; a purely local hit with no prior
network attempt sets it to `false`; an interrupted attempt whose network effect
is unknown reports `null`. `database_execution` stays `false`. A valid cache
does not prove that the remote object, current permission, or catalog freshness
is unchanged. Revoking remote access does not erase local copies. The 64 MiB
limit covers materialized payload and metadata, not HTTP retry or billing
traffic. See the [catalog guide](../artifact_catalog.md) for the complete API,
authentication, limits, and guarantees.

## Parameters, inputs, and safety boundaries

- `record_count` changes only YCSB data volume. Other walkthroughs use their
  actual minimum or small scale; stream length is not data volume.
- `sample_size` is the sampled stream length, default 200 and maximum 100,000.
  `seed` controls public query/drift APIs. It does not invent a seed parameter
  for a data adapter that owns a fixed seed.
- `query_stream(weights=...)` and `change_mix(weights=...)` require a mapping
  containing every actual template ID. Inspect `demo.templates`; do not guess
  template or dataset identity from defaults.
- Allowed `change_query_parameters(...)` values are documented in each
  notebook. Each change writes a new output stage and must materially change an
  artifact; original parameters remain intact.
- TPC-H, SSB, and LDBC use `source_dir`. LDBC queries also require
  `parameters_dir` and `driver_config`. Missing input reports `NEEDS_INPUT`;
  invalid paths, formats, and parameters are errors.
- Local TPC-H input also requires `source_scale_factor` (CLI:
  `--source-scale-factor`). It is a user declaration, not a scale inferred from
  file size. Azure preparation obtains `"0.01"` from the selected catalog entry
  and cannot be combined with a local source or scale.
- Native LDBC configuration can contain credentials. Do not commit, share, or
  display it. Reports record only the path, status, and necessary hashes or
  statistics, never configuration contents.
- Each run creates a new owned directory under the specified output root. It
  does not overwrite or automatically delete user files. Input/output overlap,
  link paths, and returned artifacts outside the owned directory are rejected.
  Walkthrough adapter calls use `force=False` and guard network and process
  entry points.
- On Windows, keep the output-root path at most 100 characters so nested adapter
  paths fit. Use a short dedicated root instead of changing system path policy.

## Interpret status values

| Status | Meaning | Action |
|---|---|---|
| `PASS` | The step completed and inspected concrete artifacts or counts | Inspect the statistics, files, and current package identity |
| `NEEDS_INPUT` | A declared input, prerequisite step, or sufficient sample is missing | Supply valid local input or run the prerequisite |
| `UNAVAILABLE` | The selected package lacks a public API or required dependency | Verify the import and choose a package exposing the API or install its declared dependency |
| `NOT_SUPPORTED` | The adapter or walkthrough has no such capability | Read the limitation; do not treat another artifact as a successful substitute |

A normal overall result is `PASS` or `PARTIAL`; any requested step that cannot
complete for a declared prerequisite or capability reason makes the run
`PARTIAL`. Standalone scripts exit `0` when all requested steps pass and `2` for
declared missing input, unsupported behavior, or unavailable API. Invalid
configuration and unexpected failures are nonzero errors. After a caught
exception, the step and overall report remain `FAIL`; previous success is not
reused. Regenerating data or queries invalidates dependent stages.

`vary_cardinality` targets `int(baseline_rows * scale)` and regenerates column
values; it is not a simple row deletion and does not guarantee key constraints.
A query mix is a seeded sample, not an exact frequency quota, QPS schedule,
arrival process, selectivity claim, or performance result. The presence of SQL,
configuration, or IDs does not prove successful database execution or official
benchmark certification.

## Maintain paired files

`_walkthrough.py` contains shared steps. `_build_notebooks.py` contains the
paired templates and explanations. After changes, regenerate explicitly or
check that committed pairs still match:

```powershell
python .\docs\benchmark_walkthroughs\_build_notebooks.py --write
python .\docs\benchmark_walkthroughs\_build_notebooks.py
```

Commit notebooks with empty outputs and execution counts. Keep run artifacts,
environments, real inputs, configurations, and reports outside the repository.
