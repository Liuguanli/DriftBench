# Run artifact contract (planned, runtime-unverified)

Each future run owns exactly one external directory:

```text
runs/<experiment-id>/<run-id>/
  run-manifest.json       immutable provenance completed at shutdown
  status.json             current atomic status snapshot
  events.ndjson           append-only progress events
  logs/                   stdout/stderr and tool-specific logs
  results/                CSV/JSON summaries and private database state
  metrics/                Prometheus state and portable metric exports
  figures/                Grafana-rendered PNGs and reviewed paper figures
```

All paths inside `run-manifest.json` are relative to the run directory. Absolute
machine paths, DSNs, passwords, and tokens are prohibited.

## Status

Writers replace `status.json` atomically. The local dashboard treats malformed
or partial files as unavailable and never repairs them.

```json
{
  "experiment_id": "rel-ce-tpch",
  "run_id": "20260823T120000Z-seed42",
  "state": "running",
  "phase": "skew-4",
  "completed": 3,
  "total": 11,
  "updated_at": "2026-08-23T12:04:00Z",
  "message": "Applying skew step 4"
}
```

Allowed states are `planned`, `preparing`, `running`, `succeeded`, `failed`, and
`cancelled`. `completed / total` is the canonical progress fraction.

## Events and logs

Each `events.ndjson` line is one JSON object with `timestamp`, `level`, `phase`,
and `message`. Tool output goes to bounded files under `logs/`; Compose's Docker
driver is independently limited to three 10 MiB files per service. Secrets and
connection strings must be redacted before either sink.

## Prometheus names

Runners may push only the generic progress metrics plus the experiment-specific
result metrics listed in its manifest. Every sample must carry
`experiment_id`, `run_id`, and `phase` labels.

- `driftbench_progress_ratio` — gauge in `[0, 1]`
- `driftbench_phase_index` — gauge
- `driftbench_*_completed_total` — monotonic counter
- experiment result gauges/histograms named in `observability.result_metrics`

Prometheus is an optional transport. Portable CSV/JSON results and the completed
run manifest remain required even when observability is disabled.

## Shutdown and result manifest

The future launcher must finalize `run-manifest.json` for success, failure, and
Ctrl-C, validate it against `schema/run-manifest.schema.json`, then execute
`docker compose down --remove-orphans` in a `finally` block. The manifest records
the canonical config hash, immutable source commits, resolved platform image
digests, dataset SHA-256, seed, resource budget, timestamps, exit state, and
relative artifact paths.

Stopping preserves the run directory and cache. Destroying a run is a separate,
future operation that requires the exact experiment ID and run ID, path
containment checks, and a second confirmation. Docker volumes are never removed
implicitly.
