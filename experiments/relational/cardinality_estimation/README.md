# TPC-H-derived cardinality-estimation experiment

**Repository status:** Existing / verification pending
**Scope status:** Offline TPC-H plan exists; runtime verification pending
**Runtime status:** Review-only; no database or estimator execution is enabled

The active experiment is `rel-ce-tpch`. It uses deterministic SPJ cardinality
probes derived from pinned TPC-H query templates at SF1. This is a
**TPC-H-derived CE study**, not an official TPC-H run, and it must not report a
TPC-H score.

The previous Census13/Naru/MSCN workflow is preserved as inactive legacy
material. It is not the active dataset or execution interface.

## Safe offline commands

These commands read local YAML only:

```powershell
python experiments/relational/cardinality_estimation/ce_runner.py validate
python experiments/relational/cardinality_estimation/ce_runner.py plan
```

`run` deliberately returns exit code 78. A non-integrated estimator such as
PRICE produces an `ESTIMATOR_NOT_RUNNABLE` explanation before that return:

```powershell
python experiments/relational/cardinality_estimation/ce_runner.py run --estimators price
```

No command in this slice builds dbgen, generates SF1, starts PostgreSQL, trains
a model, or executes a query.

## Active contracts

- [`tpch_plan.yaml`](tpch_plan.yaml) fixes SF1, seed 42, probe identities,
  template/subquery provenance, drift phases, q-error zero handling, and the
  static-versus-adaptive reporting policy.
- [`baselines.yaml`](baselines.yaml) is the machine-readable estimator registry.
  It records immutable public-code revisions where found, literature provenance,
  license status, native datasets, hardware expectations, TPC-H compatibility,
  and blockers.
- `ce_runner.py` validates and emits those contracts as stable, sorted JSON.

Ground truth will eventually be collected with `COUNT(*)` versions of the exact
ordered probe manifest against a content-addressed dataset snapshot. Final
aggregate result cardinalities from the standard TPC-H queries are not used as
a substitute for intermediate SPJ cardinality probes.

## Baseline policy

| Group | Methods | Meaning in this repository |
|---|---|---|
| Planned anchors | PostgreSQL default statistics; PostgreSQL extended statistics | Native TPC-H-capable controls, still blocked from execution here |
| Established candidates | MSCN; DeepDB; NeuroCard; BayesCard | Important comparison families; TPC-H adapters unverified |
| Recent priority | FactorJoin (PACMMOD 2023); ALECE (PVLDB 2023); PRICE (PVLDB 2024) | Highest-priority adapter research, not reproduced results |
| Emerging watchlist | CardOOD (2026); DistJoin (2025); ZeroCard (2025) | Literature watchlist pending code/provenance and compatibility review |

Naru is intentionally excluded from the active multi-table set because the
preserved DriftBench/AreCELearnedYet integration is a single-table Census path.
Registry membership never means that a paper result was reproduced.

## Fair comparison contract

Every future estimator must receive the same dataset snapshot, ordered probes,
parameter bindings, and ground truth. Reports must include symmetric q-error
with the documented zero rule; p50/p90/p95/p99/max; inference latency;
train/build/update time; model or statistics size; peak RSS; and CPU/accelerator
details. Frozen-model and adaptive-update results are separate experiment arms.

## Legacy Census material

The following remain only to preserve prior work and provenance:

- `legacy_census_runner.py`
- `ce_timeline_census.py`
- `run_ce_timeline_census.sh` and `run_skew_and_cardinality.sh`
- `specs/census_*`
- `ce_timeline_census/notebooks/`

The legacy shell commands are not part of the active interface and have not
been container-verified. They must not be used to claim `rel-ce-tpch` results.

## Current blockers

- TPC tool/data terms and immutable generated-table hashes are pending.
- The SPJ probe extractor and ground-truth collector do not yet exist.
- No academic estimator has a verified DriftBench TPC-H adapter.
- Docker lifecycle commands remain disabled by the experiment-lab gate.
