# Lifecycle design (not implemented for deployment)

The current `scripts/lab.py` intentionally blocks every Docker lifecycle
operation. This document is the review target for a future implementation.

## Single active stack

The future launcher accepts exactly one catalog experiment ID. It rejects comma
lists, repeated IDs, multiple positional IDs, and ambient `COMPOSE_PROFILES`.
Before any Docker mutation it must:

1. acquire `${DRIFTBENCH_LAB_STATE}/experiment-lab.lock` with an OS-level,
   process-owned lock;
2. query containers carrying `com.driftbench.experiment-lab=active` and refuse
   if any other run is active;
3. verify the catalog/config canonical hash against a separately reviewed
   approval record;
4. verify the exact source, dataset, platform-image, and runner-image hashes;
5. create only the selected run's external directories and secret files.

The experiment ID maps to a complete Compose profile. Optional observability is
an attribute of that same selected stack, never a second experiment selection.

## Foreground execution and cleanup

Runs stay in the foreground—no detached mode—and use Compose's
abort-on-container-exit behavior with the one-shot runner as the exit-code
source. A single `try/finally` owns setup, run-manifest finalization, and:

```text
docker compose down --remove-orphans
```

The cleanup path is identical for success, runner failure, database failure,
Ctrl-C, and dashboard/export failure. It never removes volumes or cache. All
services use `restart: "no"`, so no experiment container should remain after
cleanup. The launcher must verify that the project label has no remaining
container and report cleanup failure loudly.

This releases container CPU and RAM. It does not stop Docker Desktop itself and
does not delete disk state.

## Stop versus destroy

- Future `stop <experiment> <run-id>` performs only project-scoped
  `down --remove-orphans` and preserves cache/run files.
- Future `destroy <experiment> <run-id>` first stops the stack, resolves the
  exact run directory, proves it is a non-symlink child of
  `${DRIFTBENCH_LAB_STATE}/runs/<experiment>/`, prints its size, then requires
  the operator to repeat the exact run ID. Cache destruction is a different
  command and confirmation.
- Neither operation may accept globs, broad roots, `..`, environment-derived
  empty paths, or multiple IDs.

## Prepare and offline runs

Network access belongs only to an explicit future `prepare` operation. It will
download one immutable archive at a time to a temporary external path, verify
SHA-256 before extraction, and write a source/data lock. Image preparation will
record both the manifest-list digest and resolved platform digest.

Measured runs use `pull_policy: never`; missing inputs or images fail before a
container starts. Source mounts are read-only. The Docker socket, privileged
mode, host networking, and host cache-dropping are prohibited.

## Local laptop versus devbox

`local-lite` is capped at 2 CPU and 4 GiB RAM for the experiment services.
Live Prometheus/Grafana/renderer adds resources and is therefore off by default;
the host-side Python page is the laptop progress view. A stopped run can be
reviewed later with the bounded `grafana-review` profile.

Devbox migration changes only `DRIFTBENCH_LAB_STATE` and the separately reviewed
resource tier. Manifests, run IDs, source/data hashes, config hash, and relative
artifact paths stay unchanged. JOB and multi-node Qdrant partitioning are
already marked devbox-only.

## Future commands are forbidden now

The intended eventual interface is `prepare`, `run`, `stop`, `destroy`, and
`grafana-export`, but every such command currently exits with `REVIEW_ONLY`
before importing or invoking a Docker subprocess. A new PM slice plus
architecture, test/repro, and final-integration gates is required to enable it.
