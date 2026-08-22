# Changelog

All notable user-facing changes to DriftBench are documented in this file.

Format notes:
- Version headings follow release tags (for example `v0.1.0b4`).
- Each version includes a `Services` section so users can see capability coverage quickly.

## [v0.1.0b10] - 2026-08-22

### Services
- `Data`: Parameter- and content-aware cache manifests and corrected benchmark metadata.
- `CLI`: Fail-closed orchestration and pgbench regression-gate commands.
- `DriftSpec`: Deterministic query-mix execution and side-effect-free deep readiness validation.
- `Visualization`: Reproducible drift evidence for eight benchmark adapters.
- `CI/Release`: Required real PostgreSQL 16 benchmark regression evidence.

### Added
- Versioned `BenchmarkRunResult` v1 JSON schema with TPS, mean/p50/p95/p99 latency in milliseconds, transaction/error counts, complete run configuration, and PostgreSQL/pgbench provenance.
- Paired pgbench runner with per-round warmup, three AB/BA/AB measurement repetitions, raw transaction logs, strict metrics validation, and version-controlled TPS/p95/error thresholds.
- Required `benchmark-regression-pgbench` PostgreSQL 16 check, wired into development, main, release, tag-publish, and daily-publish paths; all evidence is uploaded even when the gate fails.
- Self-contained pgbench evidence bundles snapshot the canonical policy and exact executed candidate bytes, hash all input/environment artifacts, capture DriftBench source/version plus Python, OS/CPU, PostgreSQL/pgbench, server-setting, and inferred-scale provenance, and fail closed when environment capture is incomplete.
- Public offline bundle verification via `driftbench benchmark verify --bundle DIR --json`, including descriptor/path safety checks and exact reconstruction of raw metrics, execution order, and policy decisions.
- Strict Windows CP1252 adapter CI coverage.
- Public query-template mix drift with strict weight validation, stable semantic hashes, isolated random streams, and executable DriftSpec handler support.
- Opt-in `validate-spec --deep` preflight covering handler contracts, local inputs/outputs, collisions, adapter availability, deterministic issue codes, redaction, and single-document JSON output without executing the spec.
- A versioned visualization package with 40 spec/manifest/PNG evidence triples across eight adapters, effect-policy checks, provenance hashes, a traceable Gallery, and wheel/sdist content tests.
- Executable paper examples for cardinality, outlier, skew, FK-safe deletion, and TPC-H query-workload drift, with explicit reproducibility limits.

### Fixed
- Benchmark cache v2 now reuses artifacts only when normalized generation parameters and every managed file's path, byte count, and SHA-256 match. Legacy manifests rebuild once; `force=True` remains an unconditional rebuild.
- `orchestrate` now reports `ok: false` and exits `4` for any failed target while preserving the complete manifest. Configuration validation remains exit `3`.
- BenchBase JDBC/user/password values are XML-safe, and DSB/JOB manifest row counts match generated CSV files.
- Console messages no longer crash under a strict CP1252 stream.
- Measurement TPS is independently recomputed from successful transactions and runner elapsed time, then required to agree with pgbench-reported TPS within an inclusive 5%; inconsistent evidence is an execution/integrity failure rather than a threshold regression.
- Benchmark provenance now requires a clean DriftBench runtime-source checkout at both the start and end of a run, with a full 40-character HEAD and fail-closed dirty/change evidence.

### Changed
- Benchmark documentation consistently describes nine adapters, five synthetic TPC-DS tables, three DSB tables, eleven JOB CSV tables, and BenchBase as a ten-benchmark configuration generator.
- Orchestration failure changing from exit `0` to exit `4` is an intentional compatibility correction for automation and release gates.
- Regression policy `pgbench-ci-v1` is fixed at PostgreSQL/pgbench 16, scale 1, two clients, 3-second warmups, 5-second measurements, and three repetitions. Policy changes require explicit PM approval and a changelog entry.
- Release-branch preparation pins all required workflows and metadata checks to one immutable source SHA, rechecks the source ref immediately before creation, and pushes only that verified commit.
- CI, release, daily, and tag-publish paths install visualization dependencies, execute its dedicated test suite, and verify that built distributions contain all evidence triples while excluding runtime data/cache.

## [v0.1.0b9] - 2026-05-20

### Services
- `Data`: Benchmark results now expose a one-call summary helper for logging, dashboards, and quick assertions.

### Added
- **`GenerationResult.summary()`**: returns a lightweight, JSON-serializable dict with `benchmark`, `artifact_type`, `output_dir`, `file_count`, and the sorted list of unique table stems. Designed for log lines, the driftbench-web UI, and one-line notebook assertions without reaching into dataclass internals.
- 2 new tests in `test/test_generation_result_summary.py` covering key/value correctness and JSON serializability.

## [v0.1.0b8] - 2026-05-17

### Services
- `Data`: Benchmark adapters now expose a one-call drift API — apply data drift directly to any benchmark-generated table without manual wiring.

### Added
- **`GenerationResult.drift(table, drift_type, **params)`**: single-table drift on any benchmark CSV. Auto-extracts schema via `CSVSchemaExtractor` and delegates to `SingleTableDriftGenerator`. Supports all 7 existing drift types and writes a fresh drift manifest for the new result.
- **`GenerationResult.drift_multi(steps, relationships=None)`**: multi-table drift across all loaded benchmark tables. FK relationships for `tpch` and `job` are wired automatically; pass `relationships=[]` or a custom list to override. `tpcc`/`tpcc_skew` currently require explicit relationships because their joins use composite keys.
- **`_known_relationships(benchmark)`** helper: hard-coded FK maps for TPC-H (7 rels) and JOB (7 rels). Returns `[]` for other benchmarks.
- **5 example DriftSpec YAMLs** (`driftspec/examples/`): `tpch_lineitem_drift.yaml`, `tpcc_drift.yaml`, `job_drift.yaml`, `ycsb_drift.yaml`, `pgbench_drift.yaml`. Each uses `kind: csv` pointing at default adapter output paths with 2–3 drift variants per file.
- 10 new tests in `DriftAPITests` covering `.drift()` (TPC-H outlier injection, YCSB cardinality, fresh manifests), `.drift_multi()` (built-in relationship propagation for TPC-H and JOB, fresh manifests, skew + row-count preservation), output isolation, and error handling.
- 4 new tests in `CsvHeaderAndDriftFixTests`: TPC-H/TPC-DS header injection (with row-count preservation), end-to-end `generate().as_csv().drift()`, and the `.tbl`-without-`.as_csv()` guard rail.
- 4 new tests in `SpecPythonParityTests`: the DriftSpec YAML path runs against an `as_csv()`-produced file; spec and `.drift()` produce **byte-identical** output at the default seed and at a non-default seed (`seed=7`); and `.drift()`'s emitted hidden YAML re-runs to byte-identical output.
- **`.drift()` now emits a reproducible DriftSpec YAML** as a hidden side artifact (`<output_stem>.driftspec.yaml`, next to the drifted CSV, recorded under the manifest's `driftspec` key, kept out of `result.files`). Running it through `driftbench.spec.core.run_all` regenerates byte-identical output — so Python-generated drift is automatically shareable/automatable as a spec.

### Fixed
- **`GenerationResult.as_csv()` now writes a column header row** for known schemas: all 8 TPC-H tables (canonical spec order, verified against dbgen output) and all 5 synthetic TPC-DS tables. Previously the converted CSV was headerless, so a follow-up `.drift(..., column="l_quantity")` — via either the Python API or a `kind: csv` DriftSpec — had no named columns to target. Tables without a known schema still convert headerless (unchanged behaviour).
- **`GenerationResult.drift()` now raises a clear, actionable error** when the target table is still in `.tbl`/`.dat` form: `Table 'X' is in .tbl format; call .as_csv() before .drift().` — instead of a generic "No CSV file" message.
- **Spec engine: single-table drift now honors the spec's `seed:`.** `handle_data_single_table` previously built `SingleTableDriftGenerator` without a seed, silently using the default `42` regardless of the YAML's `seed:` (only the multi-table handler honored it). The Python `.drift()` path and the spec path now produce identical output for any seed. Backward compatible (default remains `42`).

### Changed
- `GenerationResult.drift()` rejects the reserved keywords `table`, `drift_type`, `seed`, `output_path` if they appear in `**params`, raising a `TypeError` that explains to pass them as dedicated arguments (defensive guard against signature-shadowing).

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
- **JOB adapter** (`driftbench.data.job`): Join Order Benchmark (Leis et al., VLDB 2015). 11-table synthetic IMDB snapshot (8 primary tables plus 3 lookup tables) with 20 representative SQL query templates covering 2–8-table join depths.
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
