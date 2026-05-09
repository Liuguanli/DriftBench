# P0 Foundation TODO (Release-Ready, Verified, No Major Defects)

## 0. Scope and Quality Bar

P0 target:
- DriftBench is usable as an importable Python project.
- Core workflows are executable via CLI and MCP-style automation surfaces.
- Release candidate is blocked unless all critical verification gates pass.

Definition of major defect (release-blocking):
- Wrong or missing data/workload outputs for a valid spec.
- CLI command crashes on valid inputs.
- Non-deterministic behavior when seed and inputs are fixed.
- Broken path safety (writing outside intended project scope).
- Unclear failure mode (silent failure, no actionable error).

Release rule:
- If any major defect is open, **do not release**.

## 1. P0 Deliverables (What Must Exist)

- [x] Public Python API surface is explicitly defined and documented.
- [x] CLI command set covers operational core:
  - [x] `run-yaml`
  - [x] `trace-to-spec`
  - [x] spec validation/dry-run capability
  - [x] outputs inspection/listing capability
- [x] MCP command matrix is documented (intent -> API/CLI mapping).
- [x] One integration guide: "How to embed DriftBench into another Python project."
- [x] Smoke test suite covers end-to-end path:
  - [x] schema extract
  - [x] spec generation/build
  - [x] spec execution
  - [x] output existence + shape assertions

## 2. Verification Matrix (Mandatory)

### 2.1 Unit Verification
- [x] CLI argument parsing tests for each public command.
- [x] Spec loading/validation tests (happy path + invalid path).
- [x] Determinism tests for fixed seed scenarios.
- [x] Error mapping tests (invalid input -> actionable message).

### 2.2 Integration Verification
- [x] Single-table drift spec runs end-to-end.
- [x] Multi-table drift spec runs end-to-end (minimum one representative case).
- [x] Trace-to-spec generated YAML is executable without manual edits.
- [x] Outputs are written to expected paths and schemas.

### 2.3 Service/Automation Verification
- [x] CLI workflow and service workflow produce equivalent artifacts for the same spec.
- [x] Job status/log behavior is stable (queued/running/completed/failed/interrupted).
- [x] Restart behavior preserves job history (already implemented; must be regression-tested).

### 2.4 Non-Functional Verification
- [x] Basic performance sanity check on reference demo specs (no obvious regressions).
- [x] Safety checks for file/path handling.
- [ ] Dependency/environment setup reproducibility from clean environment.
  - Close with: `./scripts/verify_p0_clean_env.sh`

## 3. Release Gates (Go/No-Go)

Gate A: Functional completeness
- [x] All P0 deliverables checked.

Gate B: Test quality
- [x] Unit + integration + smoke suites all pass.
- [x] No flaky critical tests across repeated runs.

Gate C: Defect triage
- [x] No open major defects.
- [x] All medium defects either fixed or explicitly accepted with mitigation notes.

Gate D: Reproducibility
- [ ] Fresh setup can run at least two official demo specs successfully.
- [x] Command transcript and expected outputs are documented.

Gate E: Documentation
- [x] README/CLI help is consistent with actual behavior.
- [x] Integration guide and MCP command matrix are complete.

## 4. Execution Backlog (Do-When-Time-Available)

Order is strict; each item should finish with verification evidence.

1. API boundary freeze
- [x] Identify public modules/classes/functions.
- [x] Mark internal-only modules and avoid exposing unstable internals.
- Evidence: `docs/p0_api_boundary_freeze.md` + top-level exports in `driftbench/__init__.py` and `driftbench/api.py`.

2. CLI hardening
- [x] Add missing operational commands (validation/dry-run/output listing).
- [x] Standardize exit codes and error messages.
- Evidence: `driftbench/cli.py` + `python3 -m unittest -v test.test_cli_commands` (5/5 pass).

3. Test harness completion
- [x] Add/upgrade unit + integration + smoke tests.
- [x] Add deterministic fixture specs for single-table and multi-table.
- Evidence: `python3 -m unittest discover -s test -p 'test_*.py' -v` -> 17 tests passed (5 intentional legacy skips).

4. MCP mapping contract
- [x] Define command matrix: workflow intent -> API endpoint -> CLI fallback.
- [x] Add examples for automation usage.
- Evidence: `docs/p0_mcp_command_matrix.md` + `docs/p0_mcp_examples.sh`.

5. Release candidate hardening
- [ ] Run full verification matrix on clean environment.
- [x] Triage/fix all release-blocking issues.
- Evidence: `docs/p0_test_report.md`, `docs/p0_known_issues.md`, `docs/p0_go_no_go.md`.

## 5. Artifacts Required Before P0 Sign-Off

- [x] Test report bundle (unit/integration/smoke results).
- [x] Known issues list (must exclude major defects).
- [x] CLI/MCP command matrix doc.
- [x] Integration quickstart doc.
- [x] Final Go/No-Go decision record.

## 6. Immediate Next Step

Run remaining clean-environment verification for Gate D, then either:
- promote from conditional GO to full GO, or
- keep conditional GO with explicit mitigation notes for release packaging.
