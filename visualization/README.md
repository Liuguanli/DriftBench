# DriftBench Visualization

This module builds a reproducible, manifest-backed Gallery for exactly eight
local DriftBench adapters: TPC-H, TPC-DS, TPC-C, TPC-C Skew, JOB, YCSB, DSB,
and pgbench. It compares benchmark inputs only; it never runs a target database
or collects performance measurements.

The canonical Gallery contains 40 validated results: three Data Drift and two
Query Drift scenarios per benchmark. Every PNG has exactly one tracked
DriftSpec and one schema-v4 manifest. A figure is rejected instead of written
when its observed drift misses the effect policy recorded in its DriftSpec.

## Install

```bash
pip install -e ".[visualization]"
```

Matplotlib is optional and imported only while rendering. Core `driftbench`
imports and CLI help do not load it. The renderer uses a fixed colorblind-safe
palette and deterministic 1600×1000 output.

## TPC-H prerequisite

TPC-H data preparation requires a local SF0.01 source containing all eight
`.tbl` files:

```powershell
$env:DRIFTBENCH_TPCH_SOURCE_DIR = "C:\path\to\tpch-sf0.01"
```

Visualization never downloads, builds, or invokes `dbgen`. Without this
variable it may reuse a content-verified managed cache. `--force` requires the
local source because it must regenerate rather than reuse. TPC-H query-only
generation is local and does not use the data source.

## Reproduce the canonical Gallery

```bash
python -m visualization.cli prepare --benchmark all --kind all --seed 42
python -m visualization.cli generate --benchmark all --kind all --scenario all --seed 42 --sample-size 1000
python -m visualization.cli build-gallery
```

Generate one scenario with its stable ID:

```bash
python -m visualization.cli generate --benchmark tpch --kind data --scenario price_outliers --seed 42 --sample-size 1000
python -m visualization.cli generate --benchmark tpch --kind query --scenario hotset_concentration --seed 42 --sample-size 1000
```

The canonical tracked DriftSpecs fix `seed=42` and query `sample_size=1000`.
The CLI retains explicit `--seed` and `--sample-size` validation and rejects a
value that disagrees with the selected canonical spec. Existing valid
spec/figure/manifest triples are reused; `--force` re-executes only the selected
targets. Exit codes are 0 success/reuse, 2 invalid configuration, 3 missing
optional/offline prerequisite, and 4 execution, integrity, or effect failure.

## Output layout

```text
visualization/
  specs/{data|query}/<benchmark>/<scenario>.yaml  # tracked, portable authority
  figures/{data|query}/<benchmark>/<scenario>.png
  manifests/{data|query}/<benchmark>/<scenario>.json
  data/                                           # ignored adapter/executor data
  cache/                                          # ignored query/schema/download cache
  GALLERY.md
```

Tracked specs use symbolic runtime path bindings; they never contain user
machine paths. Generation calls `driftbench.api.run_spec` on the selected
tracked spec, binds only prepared Adapter inputs and managed outputs, and plots
the returned executor result. Manifests record raw and semantic spec hashes,
resolved semantics, executor/algorithm identity, input hashes, effect evidence,
and figure hashes using relative POSIX paths only.

## Diagnostics and effect gates

Numeric dashboards combine a shared-bin distribution, ECDF or log-tail CCDF,
the Baseline P99 threshold, P05–P95 quantile shifts, row-count change, exact
empirical KS-D, and 1D Wasserstein distance. Outlier scenarios must show both a
tail-rate gain above the observed Baseline P99 and robust normalized W1.

Categorical dashboards show deterministic Top-K + Other frequency, symmetric
signed movement, a full-support concentration curve, JSD, TVD, Top-3 share, and
effective category count. Query dashboards add template movers and lexical SQL
complexity distributions when public SQL exists. All probability metrics use
the complete observed support before display aggregation.

The canonical effect policies are evaluated on deterministic plotted samples
(maximum 1,000); row counts, injected cardinality, and JOB integrity use full
frames. Inclusive boundaries pass. These are descriptive diagnostics, not
p-values, optimizer estimates, or performance conclusions.

The third Data scenario for each benchmark adds a distinct, strongly gated
volume, outlier, or relational-deletion case. Cardinality reductions must remove
at least 40% of rows, YCSB growth must add at least 45%, and new outlier cases
must add 9.5–10.5% rows while also passing tail and robust Wasserstein gates.
Single-table cardinality generation preserves adapter-shaped columns but does
not claim primary-key or foreign-key integrity.

## Capability limits

- Query template/operation frequency is supported for all eight adapters.
- SQL complexity is lexical only. TPC-DS exposes IDs and YCSB exposes
  operations, so their SQL complexity panels are explicitly Unsupported.
- Predicate/selectivity distributions are Unsupported because no target
  database is executed.
- Arrival-rate/inter-arrival distributions are Unsupported because the selected
  artifacts do not materialize observed timestamps.
- BenchBase is excluded because it requires a live database.
