"""Observed effect gates that prevent weak canonical drift figures."""

from __future__ import annotations

import math
from typing import Any, Mapping


class EffectAssertionError(RuntimeError):
    pass


def evaluate_effect(
    policy: Mapping[str, Any],
    statistics: Mapping[str, Any],
    *,
    integrity: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    metrics = statistics.get("comparison_metrics")
    if not isinstance(metrics, Mapping):
        raise EffectAssertionError("statistics do not expose comparison_metrics")
    integrity = integrity or {}
    observed = _observed_metrics(metrics, integrity)
    results: list[dict[str, Any]] = []
    for assertion in policy["assertions"]:
        metric = str(assertion["metric"])
        operator = str(assertion["operator"])
        threshold = float(assertion["threshold"])
        value = observed.get(metric)
        passed = _compare(value, operator, threshold)
        results.append(
            {
                "metric": metric,
                "operator": operator,
                "threshold": threshold,
                "observed": value,
                "passed": passed,
            }
        )
    mode = str(policy["mode"])
    passed = all(item["passed"] for item in results) if mode == "all" else any(
        item["passed"] for item in results
    )
    result = {
        "status": "supported",
        "basis": "observed_seeded_sample_and_full_frame_counts",
        "mode": mode,
        "assertions": results,
        "passed": passed,
        "verdict": "PASS" if passed else "REJECTED",
    }
    if not passed:
        evidence = ", ".join(
            f"{item['metric']}={item['observed']} {item['operator']} {item['threshold']}"
            for item in results
        )
        raise EffectAssertionError(f"canonical drift effect gate failed: {evidence}")
    return result


def effect_label(effect: Mapping[str, Any]) -> str:
    passing = [item for item in effect["assertions"] if item["passed"]]
    evidence = passing if effect["mode"] == "any" else effect["assertions"]
    parts = [
        f"{_short_metric(str(item['metric']))} {_format_value(item['observed'])} "
        f"{_operator_symbol(str(item['operator']))} {_format_value(item['threshold'])}"
        for item in evidence
    ]
    return f"Effect gate {effect['verdict']} · " + " · ".join(parts)


def _observed_metrics(
    metrics: Mapping[str, Any], integrity: Mapping[str, Any]
) -> dict[str, float | int | None]:
    row_rate = _number(metrics.get("row_rate"))
    mover = metrics.get("max_mover")
    mover_pp = None
    if isinstance(mover, Mapping):
        mover_pp = _number(mover.get("delta_percentage_points"))
    return {
        "absolute_row_rate": None if row_rate is None else abs(row_rate),
        "row_reduction_rate": None if row_rate is None else max(0.0, -row_rate),
        "row_growth_rate": None if row_rate is None else max(0.0, row_rate),
        "ks_distance": _number(metrics.get("ks_distance")),
        "normalized_wasserstein_p95_p05": _number(
            metrics.get("normalized_wasserstein_p95_p05")
        ),
        "tail_gain_over_baseline_p99": _number(
            metrics.get("tail_gain_over_baseline_p99")
        ),
        "jensen_shannon_divergence_bits": _number(
            metrics.get("jensen_shannon_divergence_bits")
        ),
        "total_variation_distance": _number(
            metrics.get("total_variation_distance")
        ),
        "max_mover_absolute_pp": None if mover_pp is None else abs(mover_pp),
        "target_stratum_share_shift_pp": _number(
            integrity.get("target_stratum_share_shift_pp")
        ),
        "target_stratum_share_reduction_pp": _number(
            integrity.get("target_stratum_share_reduction_pp")
        ),
        "orphan_count": _number(integrity.get("orphan_count")),
    }


def _number(value: Any) -> float | int | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    number = float(value)
    if not math.isfinite(number):
        return None
    return int(value) if isinstance(value, int) else number


def _compare(value: float | int | None, operator: str, threshold: float) -> bool:
    if value is None:
        return False
    if operator == "gte":
        return float(value) >= threshold
    if operator == "lte":
        return float(value) <= threshold
    if operator == "eq":
        return math.isclose(float(value), threshold, rel_tol=0.0, abs_tol=1e-12)
    raise EffectAssertionError(f"unsupported effect operator: {operator}")


def _short_metric(value: str) -> str:
    labels = {
        "absolute_row_rate": "|row Δ|",
        "row_reduction_rate": "row reduction",
        "row_growth_rate": "row growth",
        "ks_distance": "KS-D",
        "normalized_wasserstein_p95_p05": "robust W₁",
        "tail_gain_over_baseline_p99": "P99 tail gain",
        "jensen_shannon_divergence_bits": "JSD",
        "total_variation_distance": "TVD",
        "max_mover_absolute_pp": "max mover pp",
        "target_stratum_share_shift_pp": "stratum shift pp",
        "target_stratum_share_reduction_pp": "stratum reduction pp",
        "orphan_count": "orphans",
    }
    return labels.get(value, value)


def _format_value(value: Any) -> str:
    if value is None:
        return "n/a"
    number = float(value)
    if number.is_integer():
        return str(int(number))
    return f"{number:.3f}"


def _operator_symbol(value: str) -> str:
    return {"gte": "≥", "lte": "≤", "eq": "="}[value]


__all__ = ["EffectAssertionError", "effect_label", "evaluate_effect"]
