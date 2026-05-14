# Changelog

All notable user-facing changes to DriftBench are documented in this file.

Format notes:
- Version headings follow release tags (for example `v0.1.0b4`).
- Each version includes a `Services` section so users can see capability coverage quickly.

## [Unreleased]

### Services
- (No unreleased service changes recorded yet.)

## [v0.1.0b6] - 2026-05-14

### Services
- `CLI`: all user-facing commands normalized to `driftbench-db` across docs and examples.
- `Adapters`: TPCH and TPCDS benchmark adapters hardened with explicit mode contracts and row-count test coverage.
- `UX`: onboarding friction reduced — clearer PyPI description, persona quick-paths, troubleshooting table, MCP conversation template.

### Added
- `docs/ux_flow_contract_b5.md`: canonical command order, `init-agent` first-run flow, output location contract, minimal MCP conversation pattern with expected artifact outputs, cross-doc consistency rules.
- Troubleshooting table in README covering top 6 first-run failure modes.
- Persona quick-paths (Researcher / Vendor / New User) in README — max 3 commands to first success each.
- TPCH adapter: `mode="download"` manifest now explicitly documents the 4-table limit and alternatives.
- Test: `test_tpch_synth_row_counts_scale_with_sf` — asserts all 8 tables present and scaled tables grow with `sf`.
- Test: `test_tpcds_synth_row_counts_scale_with_sf` — asserts all 4 tables present and row counts grow with `scale_factor`.
- Test: `test_tpch_download_mode_covers_four_sample_tables_only` — locks 4-table output as an explicit product contract.

### Changed
- README opening rewritten: explains what drift means in plain language, states input → output expectation before commands.
- PyPI description rewritten: names supported benchmarks (TPC-H, TPC-DS, YCSB, DSB) and key constraint (no external tools required).
- Fixed network-dependent `test_tpch_data_auto_mode_fallback_generates_synthetic_tbls`: now mocks `urlopen` to force synth fallback deterministically.
- `driftbench-db` normalized across all docs: `p0_integration_quickstart.md`, `p0_mcp_command_matrix.md`, `p0_mcp_examples.sh`, `driftbench_indexing_guide.md`.

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
