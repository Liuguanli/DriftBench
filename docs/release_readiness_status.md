# Release Readiness Status (2026-05-11)

This file records the current release-readiness evidence for `dev/v0.1.0b5-agent-collab`.

## Completed Evidence

1. Local run of two official demo specs succeeded:
   - `python3 -m driftbench.cli run-yaml driftspec/examples/demo_data_single.yaml`
   - `python3 -m driftbench.cli run-yaml driftspec/examples/workload_census.yaml`
2. Unit/integration suites passed locally:
   - `python3 -m unittest -v test.test_mcp_server_tools test.test_public_specs_service`
   - `python3 -m unittest -v test.test_cli_commands test.test_spec_core_unit test.test_orchestrate_targets_examples`
3. CI reproducibility workflow added:
   - `.github/workflows/reproducible-drift-runs.yml`
   - Runs validate -> dry-run -> execute for two specs and uploads `output/**` artifacts.

## Outstanding Blocker

The clean-environment verification script is present:

- `./scripts/verify_p0_clean_env.sh`

but in the current sandbox execution on 2026-05-11 it failed during dependency install because DNS to `pypi.org` is unavailable:

- `NameResolutionError ... Failed to resolve 'pypi.org'`
- `No matching distribution found for numpy==1.25.2`

This is an environment/network constraint, not a DriftBench functional failure. Re-run the same script in a network-enabled machine/runner and attach logs to close Release Readiness section A.
