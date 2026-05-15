# Changelog

All notable user-facing changes to DriftBench are documented in this file.

Format notes:
- Version headings follow release tags (for example `v0.1.0b4`).
- Each version includes a `Services` section so users can see capability coverage quickly.

## [v0.1.0b7.post1] - 2026-05-15

### Services
- `Docs`: PyPI project description corrected.

### Changed
- README rewritten for PyPI: absolute URLs, all 9 adapters documented, stale content removed.

## [v0.1.0b7] - 2026-05-15

### Services
- `Data`: Nine benchmark adapters — TPC-H, TPC-DS, YCSB, DSB, TPC-C, TPC-C Skew, JOB, pgbench, BenchBase. All generate real data files without requiring external tools (except TPC-H generate mode).

### Added
- **TPC-C adapter** (`driftbench.data.tpcc`): Full 9-table OLTP schema with synthetic CSV generation. All 5 transaction types as SQL templates (new_order, payment, order_status, delivery, stock_level). Scale factor = number of warehouses.
- **TPC-C Skew adapter** (`driftbench.data.tpcc_skew`): Extends TPC-C with configurable Zipf warehouse access distribution. Generates `warehouse_access_weights.csv` for driver-side hot-warehouse simulation. Parameters: `hot_warehouse_fraction` (default 0.2) and `skew_factor` (Zipf α, default 0.99).
- **JOB adapter** (`driftbench.data.job`): Join Order Benchmark (Leis et al., VLDB 2015). 8-table synth subset of the IMDB schema with 20 representative SQL query templates covering 2–8-table join depths.
- **pgbench adapter** (`driftbench.data.pgbench`): TPC-B-like schema (branches, tellers, accounts, history). Synth CSV generation with correct row counts (branches=sf, tellers=10×sf, accounts=100K×sf). Three workload templates: `tpcb`, `simple_update`, `select_only`.
- **BenchBase adapter** (`driftbench.data.benchbase`): XML config generator for 10 BenchBase benchmarks (TPC-C, TPC-H, YCSB, SEATS, AuctionMark, Smallbank, Epinions, Wikipedia, Twitter, Voter). Generates separate load config + `load.sh` and execute config + `execute.sh`. Transaction weights and types are pre-configured per benchmark.
- **`GenerationResult.as_csv()`**: converts pipe-delimited `.tbl` (TPC-H) and `.dat` (TPC-DS) files to standard CSV. Both the original and the new `.csv` files are kept on disk. Method chains off the existing result — no re-generation.
- **TPC-H auto-build dbgen**: `mode="generate"` now auto-clones and builds `tpch-dbgen` on first use (cached at `~/.driftbench/cache/tpch-dbgen/`). No manual dbgen installation required.
- **`docs/benchmark_testing_guide.html`**: interactive HTML testing guide with syntax-highlighted code, sticky sidebar navigation, and verify snippets for all 9 adapters.
- **`docs/benchmark_reference.md`**: complete reference for all 9 adapters including row counts, query categories, join complexity index, scale guidance, and selection guide.
- **`AGENTS.md`**: 4-agent team definition for multi-agent DriftBench development workflows.
- 74 tests total, all pass.

### Changed
- **TPC-DS adapter** now generates synthetic pipe-delimited `.dat` files (5 tables: date_dim, store, item, customer, store_sales) with no external tool dependency. Previously required `dsdgen` (which needed bison/Xcode on macOS).
- **YCSB adapter** now generates `usertable.csv` with 10 field columns (1,000 rows per SF). Previously generated only a properties file and shell script.
- **DSB adapter** now generates 3 CSV tables (date_dim, customer, lineorder) plus DDL SQL. Previously generated only a schema blueprint and seed plan YAML.
- **Plan mode removed** from all adapters (TPC-H, TPC-C, TPC-C Skew, JOB, pgbench). Every adapter now generates actual data files directly instead of producing setup scripts.
- **Lazy generate / skip-if-exists**: all adapters check for an existing manifest before generating. Pass `force=True` to regenerate unconditionally.
- **`output_dir` is now optional**: when omitted, defaults to `~/.driftbench/data/` (overridable via `DRIFTBENCH_DATA_DIR` env var).
- `OutputDirRequiredError` is kept for backwards compatibility but is no longer raised.

## [v0.1.0b5] - 2026-05-11

### Services
- `CLI`: multi-target orchestration and dataset bootstrap flows are now part of the release line.
- `MCP`: public spec catalog now includes version/metadata support for share/import workflows.
- `CI/Release`: stronger release governance with dev-gate checks and reproducible-run workflow.
- `Packaging/Docs`: simpler user-facing PyPI description with clearer audience coverage.

### Added
- `driftbench-db orchestrate` MVP for multi-target benchmark execution planning.
- Adapter demo target configs for TPC-H and trace-style integration.
- `driftbench-db bootstrap dataset` with checksum verification and schema extraction.
- `Reproducible Drift Runs` workflow for evidence artifact generation.
- `Prepare Release Branch` workflow to block release creation unless required dev checks are green.

### Changed
- Public spec catalog upgraded to metadata-aware, versioned format.
- Bootstrap preset dataset resolution made CI-safe (tracked `data/` first, package fallback second).
- Schema-spec validation workflow now skips `driftspec/examples/adapters/*` (non-DriftSpec YAMLs).
- PyPI short description rewritten to be clearer for researcher/industry/newcomer audiences.

## [v0.1.0b4] - 2026-05-10

### Services
- `CLI`: production-facing command surface, including `init-agent`.
- `Python API`: benchmark adapter objects for data/query generation.
- `MCP`: conversational usage guidance and tooling improvements.
- `CI/Release`: release branch automation, content safety, and guarded publish policy.

### Added
- `driftbench-db init-agent` to scaffold agent-ready DriftBench usage docs/examples.
- Benchmark adapter API for common suites (TPC-H/TPC-DS/YCSB/DSB style object wrappers).
- Case-based MCP usage demos and prompt guidance for data/workload/temporal workflows.
- Release automation workflows:
  - release CI
  - content safety checks
  - release-branch web notification
  - guarded publish flow (main/release ancestry check)

### Changed
- README refocused on practical usage and conversational MCP integration.
- Temporal drift documentation clarified as an overlay on data/workload drift.

## [v0.1.0b3] - 2026-05-10

### Services
- `Packaging/Distribution`: tutorial-aware PyPI package metadata and dedicated release branch process.

### Added
- Tutorials section bundled into package-facing docs path.
- Dedicated release branch/tag workflow for clearer release provenance.

## [v0.1.0b2] - 2026-05-09

### Services
- `MCP`: schema extraction and spec construction tools for coding-agent workflows.

### Added
- MCP `extract_schema` tool.
- MCP `build_spec` tool.

### Changed
- Relaxed MCP path policy so users can work with paths outside repo root safely.

## [v0.1.0b1] - 2026-05-09

### Services
- `CLI`: first pip-installable DriftBench command-line release.
- `MCP/Service`: baseline packaged server entry points.

### Added
- Initial `driftbench-db` beta distribution (`0.1.0b1`).
- First stable package layout and console script registration.
