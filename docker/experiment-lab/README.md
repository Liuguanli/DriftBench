# DriftBench experiment lab — review-only blueprint

> **DO NOT DEPLOY THIS CONFIGURATION YET.** Every experiment manifest has
> `review_only: true`. The launcher deliberately rejects `prepare`, `run`,
> `stop`, `destroy`, and Grafana export operations until a later, separately
> reviewed implementation slice replaces the guards.

This directory is an isolated Docker and observability plan for the eleven
components tracked in `experiments/README.md`. It does not change those
experiments, download datasets, build images, start containers, or claim a
performance result. The experiment tracker remains authoritative for product
readiness; `catalog.yaml` is authoritative only for this Docker plan.

Start with `EXPERIMENT_MATRIX.md` for the human-readable benchmark, dataset,
workload, stack, and resource choices. The YAML manifests are the matching
machine-readable plans. `LIFECYCLE_DESIGN.md` explains future locking, cleanup,
and devbox migration without enabling those operations now.

## Why one Compose file

`compose.yaml` is the only Docker configuration. A future guarded launcher will
select exactly one experiment profile and one Compose project at a time. "One
Docker experiment" therefore means **one active Compose stack**, not one
container:

| Profile | Planned containers | Laptop status |
|---|---:|---|
| `rel-ce-tpch` | PostgreSQL + CE runner (2) | review-only TPC-H-derived CE plan; verification pending |
| `rel-indexes-sosd` | native runner (1) | review-only; verification pending |
| `observability` | Pushgateway + Prometheus + Grafana + renderer (4) | optional, devbox recommended |
| `grafana-review` | the same review services (4), after an experiment stops | optional, runtime-unverified |

The other nine components have manifests but no runnable Compose profile. A
stub is not promoted to an implementation merely by assigning it a future
database or runner.

## Read-first workflow

1. Read `REVIEW_CHECKLIST.md`.
2. Read `catalog.yaml` and the selected file under `manifests/`.
3. Run the offline audit only:

   ```powershell
   python docker/experiment-lab/scripts/lab.py audit
   python docker/experiment-lab/scripts/lab.py plan rel-ce-tpch
   ```

4. Do not run any command containing `docker pull`, `build`, `create`, `run`,
   `up`, or `start` in this review slice.

`docker compose config` is a parser-only check. It still needs temporary,
external path variables, as shown in the test documentation. It must never be
replaced with `docker compose up` during review.

## Runtime state contract

After a future approval, the operator must set `DRIFTBENCH_LAB_STATE` to a
dedicated directory outside this repository. All persistent files use:

```text
${DRIFTBENCH_LAB_STATE}/
  cache/<dataset>/
  runs/<experiment>/<run-id>/
    logs/
    results/
    metrics/
    figures/
    run-manifest.json
    status.json
    events.ndjson
```

`down --remove-orphans` releases container CPU and RAM. Cache, database files,
logs, results, and Docker Desktop itself can still consume disk or host
resources. `stop` will preserve state; a future `destroy` operation will require
the exact run ID and a second confirmation.

See `RUN_ARTIFACT_CONTRACT.md` for the log/result format.

## Local progress page

The lightweight dashboard uses only Python's standard library, binds to
`127.0.0.1`, and reads run artifacts without modifying them:

```powershell
python docker/experiment-lab/scripts/lab.py dashboard `
  --state-root $env:DRIFTBENCH_LAB_STATE
```

It shows current status, progress, recent events, and links to saved results and
figures. This source has unit tests, but the server has not been left running.

## Grafana plan

The optional provisioning under `observability/` uses Prometheus as the
authoritative datasource and the Grafana renderer for PNG export. Grafana is
localhost-only. It is off by default, resource-limited, and marked
`runtime-unverified`. The runner protocol must emit the metrics described in
`RUN_ARTIFACT_CONTRACT.md`; the existing runners do not yet satisfy that
protocol. PNGs will eventually be written under the selected run's `figures/`
directory. Raw metrics, logs, and `run-manifest.json` remain the evidence of
record; a dashboard image is presentation output, not benchmark evidence.

## Important limits

- GitHub HEAD commits were recorded as review candidates on 2026-08-23; they
  are immutable commits, not an instruction to track HEAD.
- Source archive SHA-256 and dataset SHA-256 values are intentionally pending
  because no source archive or dataset was downloaded. The audit requires an
  explanation for every pending checksum.
- Several upstream repositories or datasets have no detected SPDX license.
  Those experiments are blocked on human license review.
- Image digests are registry manifest-list digests. A future prepare step must
  also record the resolved platform digest in `run-manifest.json`.
- No database execution, generated workload execution, correctness check,
  benchmark conformance check, or performance measurement has been performed.
