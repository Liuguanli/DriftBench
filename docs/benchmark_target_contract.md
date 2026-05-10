# benchmark_target.yaml Contract (MVP)

`benchmark_target.yaml` is the orchestration contract for running one DriftSpec across multiple benchmark targets.

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

Execute setup/run commands:

```bash
driftbench-db orchestrate \
  --spec driftspec/examples/demo_data_single.yaml \
  --targets driftspec/examples/adapters/benchmark_targets_mvp.yaml \
  --manifest-out output/orchestrate_manifest.json \
  --execute \
  --json
```

