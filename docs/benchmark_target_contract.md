# benchmark_target.yaml Contract (MVP)

`benchmark_target.yaml` is the orchestration contract for running one DriftSpec across multiple benchmark targets.

Related docs: [README and quickstart](../README.md),
[complete adapter reference](benchmark_reference.md), and
[hands-on testing guide](benchmark_testing_guide.html).

## Top-level schema

```yaml
targets:
  - name: <string, unique, required>
    workdir: <string path, required>
    repo_url: <string, optional>
    ref: <string, optional>
    setup_command: <string, optional>
    run_command: <string, required>
    output_globs: <list[string], optional, default []>
    env: <mapping[string, string], optional, default {}>
```

## Field semantics

- `name`: human-readable target identifier, unique in the file.
- `workdir`: local working directory for this target; relative paths are resolved from the YAML file directory.
- `repo_url`: metadata only in MVP, used for manifest traceability.
- `ref`: metadata only in MVP, used for manifest traceability.
- `setup_command`: optional shell command executed before `run_command`.
- `run_command`: shell command that runs benchmark logic for this target.
- `output_globs`: glob patterns (relative to `workdir`) to collect artifacts into the manifest.
- `env`: environment variable patch for setup/run commands.

## CLI usage

Plan-only orchestration:

```bash
driftbench-db orchestrate \
  --spec driftspec/examples/demo_data_single.yaml \
  --targets driftspec/examples/adapters/benchmark_targets_mvp.yaml \
  --manifest-out output/orchestrate_manifest.json \
  --json
```

## Adapter demos included in this repo

- Self-contained local example (works from this checkout):
  - `driftspec/examples/adapters/benchmark_target_local_example.yaml`
- TPC-H adapter demo target:
  - `driftspec/examples/adapters/benchmark_target_tpch_demo.yaml`
- Trace adapter demo target:
  - `driftspec/examples/adapters/benchmark_target_trace_demo.yaml`

Execute setup/run commands:

```bash
driftbench-db orchestrate \
  --spec driftspec/examples/demo_data_single.yaml \
  --targets driftspec/examples/adapters/benchmark_targets_mvp.yaml \
  --manifest-out output/orchestrate_manifest.json \
  --execute \
  --json
```

## Outcomes and exit codes

The manifest is always written after a valid target configuration is loaded. Every target
records its status, workdir, commit SHA (when the workdir is a Git checkout), and matched
artifacts. Each command that actually runs also records its rendered command, return code,
stdout, and stderr; skipped and plan-only commands remain `null`.

| Condition | Manifest `outcome` | CLI exit |
|---|---|---:|
| Plan-only and all workdirs valid | `planned` | 0 |
| Execute and every target succeeds | `completed` | 0 |
| Some targets fail, including an invalid workdir | `partial_failure` | 4 |
| Every target fails | `failed` | 4 |
| Invalid target YAML/schema | no execution manifest | 3 |

Target failures are never represented as `"ok": true`. A setup failure skips that target's
run command but does not prevent other targets from running.

For example, one failed target among two produces a single JSON document like this and
exits 4:

```json
{
  "ok": false,
  "outcome": "partial_failure",
  "command": "orchestrate",
  "spec_path": "/work/spec.yaml",
  "targets_file": "/work/benchmark_target.yaml",
  "manifest_path": "/work/orchestrate_manifest.json",
  "execute": true,
  "summary": {
    "total_targets": 2,
    "completed": 1,
    "failed": 1,
    "planned": 0,
    "duration_seconds": 1.25
  }
}
```

This reports target execution and artifact collection only. It does not validate a
pgbench result bundle or turn arbitrary target stdout into benchmark metrics.

## Metrics and real-database evidence

`benchmark_target.yaml` is runner-neutral and does not invent latency/TPS from arbitrary
target stdout. For the supported real PostgreSQL gate, use `driftbench benchmark pgbench`.
It writes versioned `baseline.json` and `candidate.json` metrics, `decision.json`,
`execution_order.json`, and hashed raw logs. The result contract includes TPS,
mean/p50/p95/p99 latency (R-7), error count/rate/types, warmups, repetitions, PostgreSQL and
pgbench versions, and the exact policy configuration. Baseline and candidate results are
compatible only when both full PostgreSQL/pgbench version strings and `git_sha` match.

Only measurement TPS is gated for dual-source consistency. Authoritative TPS is successful
transactions divided by runner-measured elapsed measurement seconds; its unrounded value
must satisfy `abs(computed - reported) / reported <= 0.05` against TPS parsed from the
hashed pgbench stdout. A mismatch invalidates the run before aggregation.

Producer runs require clean DriftBench runtime source at both preflight and the end of the
last measurement, with the same full 40-character Git HEAD. The dedicated SHA environment
variable is an assertion against HEAD and cannot bypass dirty source.

The benchmark command exits 0 when thresholds pass, 3 for configuration errors, 4 for
execution/parser failures, and 5 for a valid run that breaches a threshold. Its output
directory must be new or empty so evidence from different runs cannot be mixed.

Use the separate public verifier to recheck a persisted or moved pgbench bundle without
database, network, or Git access:

```bash
driftbench benchmark verify --bundle benchmark-artifacts/results --json
```

Invalid evidence is a `verification_error`, is not threshold-evaluable, and exits 4:

```json
{
  "verified": false,
  "ok": false,
  "outcome": "verification_error",
  "command": "benchmark verify",
  "bundle": "/work/benchmark-artifacts/results",
  "error": "descriptor SHA-256 mismatch for 'environment.json'"
}
```

A complete, internally consistent bundle that genuinely breaches the captured policy is
instead `threshold_failed`, remains verified, and exits 5:

```json
{
  "verified": true,
  "ok": false,
  "outcome": "threshold_failed",
  "command": "benchmark verify",
  "bundle": "/work/benchmark-artifacts/results",
  "decision": "/work/benchmark-artifacts/results/decision.json",
  "reasons": ["candidate median TPS is below the policy threshold"]
}
```

Thus `orchestrate` outcomes (`partial_failure`/`failed`) describe target commands, while
`benchmark verify` outcomes (`verification_error`/`threshold_failed`) describe pgbench
evidence integrity and policy evaluation. Verification proves internal bundle consistency
with the captured policy and bytes, not external origin authenticity.

Custom pgbench policies are bounded before an execution plan is allocated: at most 1,024
clients, 256 jobs (never more jobs than clients), 3,600 seconds per warmup or measurement,
20 repetitions, and 86,400 seconds of total planned phase time across both roles.
