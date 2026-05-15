# Changelog

All notable user-facing changes to DriftBench are documented in this file.

Format notes:
- Version headings follow release tags (for example `v0.1.0b4`).
- Each version includes a `Services` section so users can see capability coverage quickly.

## [Unreleased]

### Services
- `Data`: Three new benchmark adapters — TPC-C, TPC-C Skew, and JOB (Join Order Benchmark).

### Added
- **TPC-C adapter** (`driftbench.data.tpcc`): Full 9-table OLTP schema with synthetic CSV generation and plan mode. All 5 transaction types as SQL templates (new_order, payment, order_status, delivery, stock_level). Scale factor = number of warehouses.
- **TPC-C Skew adapter** (`driftbench.data.tpcc_skew`): Extends TPC-C with configurable Zipf warehouse access distribution. Generates `warehouse_access_weights.csv` for driver-side hot-warehouse simulation. Parameters: `hot_warehouse_fraction` (default 0.2) and `skew_factor` (Zipf α, default 0.99).
- **JOB adapter** (`driftbench.data.job`): Join Order Benchmark (Leis et al., VLDB 2015). 8-table synth subset of the IMDB schema with 20 representative SQL query templates covering 2–8-table join depths. Plan mode provides a download script for the full 21-table IMDB snapshot.
- **`docs/benchmark_reference.md`**: Complete reference for all 7 adapters including row counts, query categories, join complexity index, scale guidance, and selection guide.
- **`.claude/agents/benchmark-advisor.md`**: AI sub-agent for benchmark selection questions and code generation.
- 9 new tests covering all new adapters: filesystem contract, row count scaling, plan mode output, weight manifest correctness.

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
