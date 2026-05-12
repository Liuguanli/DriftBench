# DriftBench B5 UX Audit Report

Status: Completed (Phase 1 baseline)  
Branch: `dev/ux-validation-checklist-b5`  
Date: 2026-05-12

## Scope And Method

This audit covers:
- install/first-run UX signals available in this environment
- command discoverability (`--help`)
- error-message quality for common failures
- README + CHANGELOG first-screen clarity

Validation executed from repo root with `python3 -m driftbench.cli ...`.

## Findings (Ranked)

### Critical

1. Invalid YAML shape can surface internal exception instead of user guidance.
- Repro:
  - create `/tmp/invalid_spec_b5.yaml` with:
    - `type: not_a_real_type`
    - `variables: {}`
  - run `python3 -m driftbench.cli validate-spec /tmp/invalid_spec_b5.yaml --json`
- Observed:
  - exit code `4`
  - error: `[ERROR] 'str' object has no attribute 'get'`
- UX impact:
  - newcomer and industry users do not get corrective action.
  - looks like product crash, not validation feedback.

### High

1. Global SciPy/NumPy compatibility warning appears on almost every CLI command.
- Observed on:
  - `driftbench-db --help`
  - `driftbench-db init-agent --help`
  - `driftbench-db validate-spec --help`
  - validation/list-output commands
- Message:
  - SciPy expects NumPy `<1.25.0`, detected `1.26.4`.
- UX impact:
  - noisy first impression.
  - weakens trust in command output even when command succeeds.

2. Error responses lack next-step hints.
- Repro:
  - `python3 -m driftbench.cli validate-spec /tmp/does_not_exist.yaml --json`
- Observed:
  - `[VALIDATION ERROR] [Errno 2] No such file or directory: '/tmp/does_not_exist.yaml'`
- UX impact:
  - technically correct, but does not tell user what to run next (example path, `ls`, or template spec).

### Medium

1. Clean-machine install path was blocked in this environment.
- `scripts/verify_p0_clean_env.sh` failed because system package `python3.10-venv` is missing.
- This is environment-related, but still indicates first-run friction risk for contributor setup docs.

2. PyPI page flow could not be fully audited from this environment.
- Network-limited session prevented direct PyPI page verification.
- README/CHANGELOG flow was audited locally instead.

### Low

1. `list-outputs` on missing root returns `ok: true` with note.
- Repro:
  - `python3 -m driftbench.cli list-outputs --root /tmp/not_found_output_dir --glob "**/*" --limit 5 --json`
- Observed:
  - returns `ok: true`, `count: 0`, note: `"root path does not exist"`.
- UX note:
  - behavior is non-breaking and machine-friendly.
  - optional improvement: mark as warning explicitly for newcomers.

## Evidence Snapshot

## Command discoverability

- `driftbench-db --help` exposes key flow commands:
  - `validate-spec`, `dry-run`, `run-yaml`, `list-outputs`, `init-agent`, `orchestrate`, `bootstrap`
- `init-agent --help` and `validate-spec --help` are concise and understandable.

## Persona first-success timing (local environment)

Measured command chain:
1. `validate-spec`
2. `dry-run`
3. `run-yaml`
4. `list-outputs`

Results:
- researcher: `3.59s`
- industry_engineer: `3.50s`
- newcomer: `3.53s`

Note: this is **not** a clean-machine timing; dependencies were already present.

## Documentation first-screen audit

- README opening clearly states audience and usage intent.
- README includes canonical command order in CLI quickstart.
- CHANGELOG `v0.1.0b5` section clearly maps user-facing changes by service.
- Remaining gap: no explicit "if this fails, do X" block near quickstart.

## Recommended Fix Queue (Top 5)

1. Convert internal parser/type errors into structured `[VALIDATION ERROR]` messages with remediation hints.
2. Resolve or suppress SciPy/NumPy mismatch warning in supported install matrix.
3. Add next-step guidance to file/path validation errors.
4. Add quick troubleshooting section directly under README quickstart.
5. Add a clean-environment prerequisite note for contributor verification script (`python3.10-venv`).

## Acceptance Readiness (Phase 1)

- Baseline UX evidence collected: **Yes**
- Blocking UX defects found: **Yes** (critical validation-error path)
- Ready to move to Phase 2 (description optimization): **Yes**, but critical defect should be fixed before release confidence sign-off.
