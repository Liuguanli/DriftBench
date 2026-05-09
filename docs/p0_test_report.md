# P0 Test Report

Date: 2026-05-08

## 1. Scope

This report covers P0 release hardening verification for:
- CLI workflows,
- spec unit/integration/smoke pipelines,
- service/automation behavior,
- safety and reproducibility checks.

## 2. Command Evidence

### 2.1 Focused P0 suites

Command:

```bash
python3 -m unittest -v \
  test.test_cli_commands \
  test.test_spec_core_unit \
  test.test_spec_execution_integration \
  test.test_smoke_pipeline
```

Result:
- 12 tests run
- 12 passed
- 0 failed

### 2.2 Full discovery sanity

Command:

```bash
python3 -m unittest discover -s test -p 'test_*.py' -v
```

Result:
- 17 tests run
- 17 passed
- 5 intentionally skipped legacy placeholders
- 0 failures

### 2.3 Flakiness check (3 consecutive runs)

Command:

```bash
python3 -m unittest -v \
  test.test_cli_commands \
  test.test_spec_core_unit \
  test.test_spec_execution_integration \
  test.test_smoke_pipeline
```

Repeated run summary:
- Run 1: pass
- Run 2: pass
- Run 3: pass

## 3. Service/Automation Verification

Executed verification:
- local service job run via `/api/run`,
- CLI `run-yaml` on equivalent spec,
- byte-level output comparison,
- service restart and job history persistence check.

Observed evidence:
- service job status: `completed`
- service vs CLI artifact hash: equal
- job history persisted across restart: true

## 4. Safety and Non-Functional Checks

### 4.1 Path safety (service)

Checks:
- `/api/spec/build` with outside-repo output path => rejected
- `/api/run` with outside-repo spec path => rejected

Observed:
- both return HTTP 400 with explicit error messages.

### 4.2 Performance sanity (reference specs)

Measured via CLI:
- `driftspec/examples/demo_data_single.yaml`: ~2.08s
- `driftspec/examples/demo_data_census_timestamp.yaml`: ~1.25s

No obvious performance regression observed for these baseline specs.

### 4.3 Reproducibility checks

Two official specs were executed and expected outputs confirmed:
- `output/data/cardinality/scale/census_original_cardinality_1.csv`
- `output/data/time_demo/census_original_with_timestamp.csv`

## 5. Summary

- P0 critical verification suites are passing.
- No release-blocking defect was observed in this verification round.
- Remaining medium-risk items are tracked in `docs/p0_known_issues.md`.
