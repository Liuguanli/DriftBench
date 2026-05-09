# P0 Integration Quickstart

## Goal

Embed DriftBench into another Python project with stable P0 interfaces.

## 1) Python API Integration

Use the frozen public API only:

```python
from driftbench import (
    run_spec,
    trace_to_spec,
    load_and_validate_spec,
    get_schema_extractor,
)
```

### Minimal flow

```python
spec, type_info = load_and_validate_spec("driftspec/examples/demo_data_single.yaml")
print(type_info)
run_spec("driftspec/examples/demo_data_single.yaml")
```

### Trace onboarding flow

```python
trace_to_spec(
    "driftspec/trace_inputs/trace_data_mock.csv",
    "driftspec/generated/trace_data_from_integration.yaml",
    trace_type="data",
)
run_spec("driftspec/generated/trace_data_from_integration.yaml")
```

## 2) CLI Integration

Validation + planning:

```bash
python -m driftbench.cli validate-spec driftspec/examples/demo_data_single.yaml --json
python -m driftbench.cli dry-run driftspec/examples/demo_data_single.yaml --json
```

Execution:

```bash
python -m driftbench.cli run-yaml driftspec/examples/demo_data_single.yaml
```

Trace conversion:

```bash
python -m driftbench.cli trace-to-spec \
  driftspec/trace_inputs/trace_data_mock.csv \
  driftspec/generated/trace_data_cli.yaml \
  --trace-type data
```

Output inspection:

```bash
python -m driftbench.cli list-outputs --root output --glob "**/*.csv" --limit 20 --json
```

## 3) Service (API) Integration

Start service:

```bash
python driftbench_service/server.py --port 8000
```

Health:

```bash
curl -s http://127.0.0.1:8000/api/health
```

Run spec (async job):

```bash
curl -s -X POST http://127.0.0.1:8000/api/run \
  -H 'Content-Type: application/json' \
  -d '{"spec_path":"driftspec/examples/demo_data_single.yaml"}'
```

Track jobs:

```bash
curl -s http://127.0.0.1:8000/api/jobs
curl -s http://127.0.0.1:8000/api/jobs/<job_id>
```

## 4) Integration Rules (P0)

- Prefer `driftbench` or `driftbench.api` imports only.
- Avoid depending on `driftbench.core.*` internals in downstream projects.
- For automation, use service endpoints when available and CLI fallback otherwise.
- Keep all service file paths inside repository root.

See:
- `docs/p0_api_boundary_freeze.md`
- `docs/p0_mcp_command_matrix.md`

## 5) Clean-Environment Reproducibility (Close the Final GO Gap)

Python version note:
- P0 lock currently supports Python `3.10/3.11/3.12`.
- If your default `python3` is `3.13`, set `P0_PYTHON` explicitly.

Run on a fresh machine or clean workspace:

```bash
./scripts/verify_p0_clean_env.sh
```

If needed, force Python 3.10:

```bash
P0_PYTHON=/Users/guanlil1/anaconda3/bin/python3.10 ./scripts/verify_p0_clean_env.sh
```

Manual two-step equivalent:

```bash
./scripts/bootstrap_p0_env.sh
source .venv-p0/bin/activate
python -m unittest -v test.test_cli_commands test.test_spec_core_unit test.test_spec_execution_integration test.test_smoke_pipeline
python -m unittest discover -s test -p 'test_*.py' -v
```
