# Cardinality Estimation Tasks

This directory contains a CLI-only runner for CE experiments on Census data.
All logic lives in `tasks/cardinality_estimation/ce_runner.py`. The shell
wrapper only forwards arguments.

## Prereqs
- Python env with driftbench and lecarb dependencies.
- Postgres running and `DATABASE_URL` set.
- Base table `census13_original` already loaded in Postgres with `row_id`
  (required for UPDATE-based drift).

The runner reads `tasks/cardinality_estimation/.env` by default.

## Key Paths
- Runner: `tasks/cardinality_estimation/ce_runner.py`
- Shell wrapper: `tasks/cardinality_estimation/run_ce_timeline_census.sh`
- Update SQL plans: `tasks/cardinality_estimation/ce_timeline_census/log/ce_timeline/sql_plan/`
- Logs: `tasks/cardinality_estimation/ce_timeline_census/log/ce_timeline/`

## Commands

### 1) Generate skew SQL plans
Generates skewed CSVs from the baseline spec, then writes UPDATE plans.
```bash
bash tasks/cardinality_estimation/run_ce_timeline_census.sh generate-skew-sql
```

### 1b) Prepare dataset artifacts (CSV -> PKL -> Table)
These are the `just` commands from AreCELearnedYet, scoped to the CE data dir.
```bash
just csv2pkl tasks/cardinality_estimation/data/census13/original.csv
just pkl2table census13 original
```

### 2) Baseline workloads (static)
Runs workloads on the base version and trains Naru/MSCN once. The trained
model names are pinned in `log/ce_timeline/model_lock.json` and reused by later steps.
```bash
bash tasks/cardinality_estimation/run_ce_timeline_census.sh apply-baseline-workload
```

### 2b) Train models only
Trains Naru/MSCN once and writes `log/ce_timeline/model_lock.json`.
```bash
bash tasks/cardinality_estimation/run_ce_timeline_census.sh train-models
```

### 3) Apply skew drift and run workloads
Applies skew updates in order (skew 2..11), and runs workloads after each update.
This step requires `log/ce_timeline/model_lock.json` from the baseline run.
```bash
bash tasks/cardinality_estimation/run_ce_timeline_census.sh apply-drift-and-workload-skew
```

### 3b) Apply cardinality drift and run workloads
Grows table cardinality over multiple steps and runs workloads after each step.
Defaults: 10 steps, `scale-step` = 0.2, `growth-mode` = `base`.
```bash
bash tasks/cardinality_estimation/run_ce_timeline_census.sh apply-drift-and-workload-cardinality
```
You can override the defaults via env vars:
- `CARDINALITY_SCALES` (comma-separated scale factors, e.g. `1.2,1.4,1.6`)
- `CARDINALITY_SCALE_STEP`
- `CARDINALITY_SCALE_STEPS`
- `CARDINALITY_GROWTH_MODE` (base|incremental)

### 4) Manual steps (optional)
Apply drift only:
```bash
bash tasks/cardinality_estimation/run_ce_timeline_census.sh apply-data-drift
```

Apply workloads only (version required):
```bash
bash tasks/cardinality_estimation/run_ce_timeline_census.sh apply-workload original
```

## What Is Hardcoded
To keep the CLI simple, `apply-drift-and-workload-skew` and
`apply-baseline-workload` use hardcoded lists in `ce_runner.py`:
- datasets (default: `census13`)
- base version (default: `original`)
- workloads (6 census workloads)
- estimators (default: `postgres`, `naru`, `mscn`)
- skew levels (2..11)

Edit `apply_drift_and_workload_skew()` and `apply_baseline_workload()` to
change these lists.

## Logs
All steps write to:
`tasks/cardinality_estimation/ce_timeline_census/log/ce_timeline/`

Useful files:
- `run_steps.log` (high-level timeline)
- `ce_prepare_data.log` (data drift generation)
- `ce_generate_sql_plan.log` (SQL plan generation)
- `apply_*.log` (per-update SQL execution)
- `<version>_<workload>_postgres.log` (workload execution)

## Troubleshooting
- `column "row_id" does not exist`: reload the base table with `row_id`
  and ensure UPDATE SQL targets `row_id`.
- `Model lock missing`: run `apply-baseline-workload` once to train and pin
  models, or set `NARU_MODEL` / `MSCN_MODEL` and create the lock manually.
