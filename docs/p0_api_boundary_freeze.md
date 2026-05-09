# P0 API Boundary Freeze

## Goal

Define a stable Python integration surface so downstream projects do not depend
on deep internal module paths.

## Public API (Frozen for P0)

Import path:
- `driftbench`
- `driftbench.api`

Public callables:
- `run_spec(spec_path: str) -> None`
- `run_spec_and_return_summary(spec_path: str) -> dict`
- `trace_to_spec(trace_path: str, output_path: str, trace_type: str | None = None, mapping_path: str | None = None) -> dict`
- `load_spec(path: str) -> dict`
- `validate_spec(spec: dict) -> None`
- `load_and_validate_spec(spec_path: str) -> tuple[dict, dict]`
- `get_schema_extractor(source_type: str, **kwargs) -> object`
- `register_filter(name: str) -> callable`
- `get_filter(name: str) -> callable`

## Internal (Not Stable for External Integrations)

The following are implementation modules and may change without compatibility guarantees:
- `driftbench.core.*`
- `driftbench.spec.types.*`
- `driftbench.spec.registry`
- `driftbench.spec.core` (except symbols re-exported by `driftbench` / `driftbench.api`)
- `driftbench_service.*`

## Current Integration Guidance

Do:
- import from `driftbench` or `driftbench.api`
- execute generation workflows through `run_spec` and `trace_to_spec`

Avoid:
- importing generator internals directly from `driftbench.core.*` in downstream projects
- relying on handler registration internals

## Known Gaps (Next P0 Steps)

- Add contract tests that verify public API behavior and backward compatibility.
- Add service-vs-CLI equivalence checks for key workflows.
