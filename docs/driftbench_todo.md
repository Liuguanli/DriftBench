# DriftBench Unified TODO

This is the single source of truth for pending DriftBench work.

## A. Release Readiness (must close before full P0 sign-off)

- [ ] Reproducible dependency/environment setup from a clean machine.
  - Close with: `./scripts/verify_p0_clean_env.sh`
- [ ] Fresh setup can run at least two official demo specs successfully.
- [ ] Run the full verification matrix in a clean environment and attach evidence.

## B. Product Upgrade Backlog (post-demo-paper execution)

### B0. Standardized Multi-Repo Benchmarking
- [ ] Finalize `benchmark_target.yaml` schema.
- [ ] Implement `driftbench orchestrate` MVP for 2 targets.

### B1. Adapter Layer
- [ ] Add one TPC-H adapter demo.
- [ ] Add one trace adapter demo.

### B2. Auto Data/Workload Bootstrapping
- [ ] Implement dataset bootstrap (download + checksum + schema extract).

### B3. DriftSpec Catalog and Versioning
- [ ] Extend catalog metadata.
- [ ] Add DriftSpec versioning and compatibility fields.

### B4. CI/CD Reproducibility Guardrails
- [ ] Publish CI template for reproducible drift runs.
