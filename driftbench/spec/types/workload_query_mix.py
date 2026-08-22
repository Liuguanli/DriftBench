"""DriftSpec handler for deterministic query-template mix drift."""

from __future__ import annotations

from typing import Any, Mapping

from ...query_drift import QueryWorkloadMixResult, execute_query_template_mix_spec
from ..registry import register


@register(family="workload", category="drift", subtype="template_mix")
def handle_query_template_mix(
    spec: Mapping[str, Any],
    *,
    runtime_inputs: Mapping[str, Any] | None = None,
) -> QueryWorkloadMixResult:
    return execute_query_template_mix_spec(spec, runtime_inputs=runtime_inputs)


__all__ = ["handle_query_template_mix"]
