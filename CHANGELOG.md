# Changelog

All notable user-facing changes to DriftBench are documented in this file.

Format notes:
- Version headings follow release tags (for example `v0.1.0b4`).
- Each version includes a `Services` section so users can see capability coverage quickly.

## [Unreleased]

### Services
- `CLI`: adds multi-target orchestration (`orchestrate`) and dataset bootstrapping (`bootstrap dataset`).
- `MCP`: adds public spec catalog metadata/versioning for share/import flows.
- `CI/Release`: adds reproducible drift-run workflow, dev-branch CI coverage, and release branch gating checks.

### Added
- Multi-target benchmark orchestration MVP via `driftbench-db orchestrate`.
- Adapter demo target configs for TPC-H and trace-style integration.
- Dataset bootstrap command with checksum and schema extraction.
- Reproducible drift-run workflow artifact pipeline.
- Release branch preparation workflow that blocks release creation unless required dev checks are green.

### Changed
- Public spec catalog upgraded to metadata-aware versioned format.
- Bootstrap preset dataset resolution made CI-safe (tracked `data/` first, package fallback second).
- Schema-spec validation workflow now skips `driftspec/examples/adapters/*` (non-DriftSpec files).

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
