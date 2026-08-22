"""Deterministic, plot-ready diagnostics for data and query drift.

All comparison metrics describe the observed, seeded samples.  They are not
significance tests and they do not use configured query weights as observations.
"""

from __future__ import annotations

import hashlib
import math
import re
from collections import Counter
from types import MappingProxyType
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd
from pandas.api.types import is_numeric_dtype

from driftbench.api import QueryTemplate, QueryWorkloadMixResult

from .artifacts import semantic_hash


ANALYSIS_SCHEMA = "driftbench.visualization-analysis/v3"
ANALYSIS_CONFIG = MappingProxyType(
    {
        "numeric_bins": 24,
        "categorical_top_k": 10,
        "categorical_movers": 15,
        "query_top_n": 15,
        "query_movers": 15,
        "ecdf_max_points": 512,
        "concentration_max_points": 256,
        "quantile_levels": (0.05, 0.25, 0.50, 0.75, 0.95),
        "tail_threshold_quantile": 0.99,
        "quantile_method": "linear",
        "jsd_log_base": 2,
        "effective_count_entropy_base": 2,
    }
)

_COMPARISON_RE = re.compile(
    r"(?:<>|!=|<=|>=|(?<![:<>!])=|(?<!<)<(?![=>])|(?<!>)>(?![=])|"
    r"\bBETWEEN\b|\bLIKE\b|\bIN\s*\()",
    re.IGNORECASE,
)
_JOIN_RE = re.compile(r"\bJOIN\b", re.IGNORECASE)


def analysis_metadata() -> dict[str, Any]:
    """Return the immutable analysis contract in manifest-safe form."""

    config = {
        key: list(value) if isinstance(value, tuple) else value
        for key, value in ANALYSIS_CONFIG.items()
    }
    return {
        "schema": ANALYSIS_SCHEMA,
        "config": config,
        "config_sha256": semantic_hash(config),
        "basis": "observed_sample",
    }


def summarize_data_distribution(
    baseline: pd.DataFrame,
    drifted: pd.DataFrame,
    *,
    column: str,
    sample_size: int,
    seed: int,
    top_k: int = int(ANALYSIS_CONFIG["categorical_top_k"]),
    bins: int = int(ANALYSIS_CONFIG["numeric_bins"]),
) -> dict[str, Any]:
    """Summarize one aligned baseline/drifted data distribution."""

    if column not in baseline.columns or column not in drifted.columns:
        raise ValueError(f"comparison column missing: {column}")
    if sample_size <= 0:
        raise ValueError("sample_size must be positive")
    if bins <= 0:
        raise ValueError("bins must be positive")
    if top_k <= 0:
        raise ValueError("top_k must be positive")

    baseline_raw = baseline[column]
    drifted_raw = drifted[column]
    numeric = is_numeric_dtype(baseline_raw) and is_numeric_dtype(drifted_raw)
    common: dict[str, Any] = {
        "status": "supported",
        "basis": "observed_sample",
        "column": column,
        "requested_sample_size": int(sample_size),
        "row_count": {"baseline": int(len(baseline)), "drifted": int(len(drifted))},
        "non_null_count": {
            "baseline": int(baseline_raw.notna().sum()),
            "drifted": int(drifted_raw.notna().sum()),
        },
        "missing_count": {
            "baseline": int(baseline_raw.isna().sum()),
            "drifted": int(drifted_raw.isna().sum()),
        },
        "missing_rate": {
            "baseline": _safe_rate(int(baseline_raw.isna().sum()), len(baseline)),
            "drifted": _safe_rate(int(drifted_raw.isna().sum()), len(drifted)),
        },
        "analysis": {
            "schema": ANALYSIS_SCHEMA,
            "bins": int(bins),
            "top_k": int(top_k),
            "quantile_method": "linear",
        },
    }

    if numeric:
        baseline_values = _finite_values(baseline_raw)
        drifted_values = _finite_values(drifted_raw)
        baseline_sample = _sample_array(baseline_values, sample_size, seed, "baseline")
        drifted_sample = _sample_array(drifted_values, sample_size, seed, "drifted")
        common["finite_count"] = {
            "baseline": int(len(baseline_values)),
            "drifted": int(len(drifted_values)),
        }
        common["non_finite_count"] = {
            "baseline": int(common["non_null_count"]["baseline"] - len(baseline_values)),
            "drifted": int(common["non_null_count"]["drifted"] - len(drifted_values)),
        }
        common["sample_count"] = {
            "baseline": int(len(baseline_sample)),
            "drifted": int(len(drifted_sample)),
        }
        summary = _numeric_summary(baseline_sample, drifted_sample, bins)
        summary["comparison_metrics"].update(
            _row_count_metrics(len(baseline), len(drifted))
        )
        return {**common, **summary}

    baseline_values = baseline_raw.dropna().astype(str).to_numpy(dtype=str)
    drifted_values = drifted_raw.dropna().astype(str).to_numpy(dtype=str)
    baseline_sample = _sample_array(baseline_values, sample_size, seed, "baseline")
    drifted_sample = _sample_array(drifted_values, sample_size, seed, "drifted")
    common["sample_count"] = {
        "baseline": int(len(baseline_sample)),
        "drifted": int(len(drifted_sample)),
    }
    summary = _categorical_summary(baseline_sample, drifted_sample, top_k)
    summary["comparison_metrics"].update(
        _row_count_metrics(len(baseline), len(drifted))
    )
    return {**common, **summary}


def summarize_query_distribution(
    result: QueryWorkloadMixResult,
    *,
    capabilities: Mapping[str, str],
) -> dict[str, Any]:
    """Summarize observed query samples, including full-support mix metrics."""

    template_ids = sorted(str(template_id) for template_id in result.baseline_weights)
    baseline_counts = Counter(str(template.template_id) for template in result.baseline)
    drifted_counts = Counter(str(template.template_id) for template in result.drifted)
    baseline_total = len(result.baseline)
    drifted_total = len(result.drifted)
    baseline_frequency = _frequencies(template_ids, baseline_counts, baseline_total)
    drifted_frequency = _frequencies(template_ids, drifted_counts, drifted_total)
    delta_pp = [
        100.0 * (drifted - original)
        for original, drifted in zip(baseline_frequency, drifted_frequency)
    ]
    frequency = {
        "template_ids": template_ids,
        "baseline_count": [int(baseline_counts[value]) for value in template_ids],
        "drifted_count": [int(drifted_counts[value]) for value in template_ids],
        "baseline_frequency": baseline_frequency,
        "drifted_frequency": drifted_frequency,
        "delta_percentage_points": delta_pp,
    }

    query_metrics = _probability_comparison(
        template_ids,
        baseline_frequency,
        drifted_frequency,
        entity_name="template_id",
    )
    frequency_by_id = {
        template_id: (baseline_frequency[index], drifted_frequency[index], delta_pp[index])
        for index, template_id in enumerate(template_ids)
    }
    top_ids = sorted(
        template_ids,
        key=lambda value: (
            -max(frequency_by_id[value][0], frequency_by_id[value][1]),
            value,
        ),
    )[: int(ANALYSIS_CONFIG["query_top_n"])]
    movers = sorted(
        template_ids,
        key=lambda value: (-abs(frequency_by_id[value][2]), value),
    )[: int(ANALYSIS_CONFIG["query_movers"])]
    top_entries = [
        _frequency_entry(template_id, frequency_by_id[template_id])
        for template_id in top_ids
    ]
    top_baseline = sum(entry["baseline_frequency"] for entry in top_entries)
    top_drifted = sum(entry["drifted_frequency"] for entry in top_entries)
    query_other = {
        "label": "Other",
        "is_aggregate": True,
        "baseline_frequency": max(0.0, 1.0 - top_baseline),
        "drifted_frequency": max(0.0, 1.0 - top_drifted),
    }
    query_other["delta_percentage_points"] = 100.0 * (
        query_other["drifted_frequency"] - query_other["baseline_frequency"]
    )

    lexical_supported = capabilities.get("lexical_sql_metrics") == "supported"
    lexical: dict[str, Any]
    if lexical_supported:
        if any(template.sql is None for template in result.baseline + result.drifted):
            raise ValueError("SQL lexical metrics marked supported but SQL text is missing")
        baseline_lexical = [_lexical_metrics(template) for template in result.baseline]
        drifted_lexical = [_lexical_metrics(template) for template in result.drifted]
        baseline_summary = _metric_distribution(baseline_lexical)
        drifted_summary = _metric_distribution(drifted_lexical)
        lexical = {
            "status": "supported",
            "basis": "observed_sample",
            "labels": {
                "statement_count": "SQL statement count (lexical)",
                "comparison_count": "Comparison operator count (lexical)",
                "explicit_join_count": "Explicit JOIN keyword count (lexical)",
            },
            "baseline": baseline_summary,
            "drifted": drifted_summary,
            "plot_ready": _lexical_plot_ready(baseline_lexical, drifted_lexical),
        }
    else:
        lexical = {
            "status": "unsupported",
            "reason": "The public adapter does not expose SQL text.",
        }

    unsupported = [
        {
            "metric": "predicate/selectivity distribution",
            "status": "unsupported",
            "reason": "No target database is executed in this phase.",
        },
        {
            "metric": "arrival-rate/inter-arrival distribution",
            "status": "unsupported",
            "reason": "The adapter does not materialize observed timestamps.",
        },
    ]

    return {
        "status": "supported",
        "basis": "observed_sample",
        "requested_sample_size": int(result.sample_size),
        "sample_count": {"baseline": baseline_total, "drifted": drifted_total},
        "analysis": {
            "schema": ANALYSIS_SCHEMA,
            "query_top_n": int(ANALYSIS_CONFIG["query_top_n"]),
            "query_movers": int(ANALYSIS_CONFIG["query_movers"]),
        },
        "template_frequency": frequency,
        "top_templates": top_entries,
        "template_display_frequency": {
            "labels": [
                *[_non_aggregate_label(entry["template_id"]) for entry in top_entries],
                "Other",
            ],
            "is_aggregate": [False] * len(top_entries) + [True],
            "baseline": [
                *[entry["baseline_frequency"] for entry in top_entries],
                query_other["baseline_frequency"],
            ],
            "drifted": [
                *[entry["drifted_frequency"] for entry in top_entries],
                query_other["drifted_frequency"],
            ],
            "other": query_other,
        },
        "top_movers": [
            _frequency_entry(template_id, frequency_by_id[template_id])
            for template_id in movers
        ],
        "concentration_curve": _paired_concentration_curve(
            baseline_frequency, drifted_frequency
        ),
        "comparison_metrics": query_metrics,
        "lexical_metrics": lexical,
        "unsupported": unsupported,
    }


def ks_distance(baseline: Sequence[float], drifted: Sequence[float]) -> float | None:
    """Exact two-sample empirical KS-D over pooled finite support."""

    first = _finite_array(baseline)
    second = _finite_array(drifted)
    if not len(first) or not len(second):
        return None
    support = np.unique(np.concatenate((first, second)))
    first_sorted = np.sort(first)
    second_sorted = np.sort(second)
    first_cdf = np.searchsorted(first_sorted, support, side="right") / len(first_sorted)
    second_cdf = np.searchsorted(second_sorted, support, side="right") / len(second_sorted)
    return float(np.max(np.abs(first_cdf - second_cdf)))


def wasserstein_distance_1d(
    baseline: Sequence[float], drifted: Sequence[float]
) -> float | None:
    """Exact one-dimensional W1 as the integral of |F-G| over x."""

    first = _finite_array(baseline)
    second = _finite_array(drifted)
    if not len(first) or not len(second):
        return None
    support = np.unique(np.concatenate((first, second)))
    if len(support) == 1:
        return 0.0
    first_sorted = np.sort(first)
    second_sorted = np.sort(second)
    first_cdf = np.searchsorted(first_sorted, support[:-1], side="right") / len(first)
    second_cdf = np.searchsorted(second_sorted, support[:-1], side="right") / len(second)
    return float(np.sum(np.abs(first_cdf - second_cdf) * np.diff(support)))


def jensen_shannon_divergence(
    baseline: Sequence[float], drifted: Sequence[float]
) -> float | None:
    """Jensen-Shannon divergence in base-2 bits, bounded to [0, 1]."""

    first, second = _normalized_probability_pair(baseline, drifted)
    if first is None or second is None:
        return None
    midpoint = 0.5 * (first + second)
    divergence = 0.5 * _kl_bits(first, midpoint) + 0.5 * _kl_bits(second, midpoint)
    return float(min(1.0, max(0.0, divergence)))


def total_variation_distance(
    baseline: Sequence[float], drifted: Sequence[float]
) -> float | None:
    """Total variation distance, one half of the aligned L1 distance."""

    first, second = _normalized_probability_pair(baseline, drifted)
    if first is None or second is None:
        return None
    return float(0.5 * np.sum(np.abs(first - second)))


def _numeric_summary(
    baseline: np.ndarray, drifted: np.ndarray, bins: int
) -> dict[str, Any]:
    row_metrics = _row_count_metrics(0, 0)
    if not len(baseline) or not len(drifted):
        return {
            "status": "insufficient_data",
            "reason": "Both baseline and drifted require at least one finite observation.",
            "distribution_type": "numeric",
            "axis_range": None,
            "bin_edges": [],
            "baseline_histogram": [],
            "drifted_histogram": [],
            "ecdf": {"baseline": {"x": [], "probability": []}, "drifted": {"x": [], "probability": []}},
            "quantile_comparison": {
                "levels": list(ANALYSIS_CONFIG["quantile_levels"]),
                "labels": ["P05", "P25", "P50", "P75", "P95"],
                "baseline": [],
                "drifted": [],
                "shift": [],
            },
            "baseline_summary": None,
            "drifted_summary": None,
            "comparison_metrics": {
                **row_metrics,
                "status": "insufficient_data",
                "ks_distance": None,
                "wasserstein_distance": None,
                "pooled_range": None,
                "normalized_wasserstein_range": None,
                "pooled_p95_p05_span": None,
                "normalized_wasserstein_p95_p05": None,
                "normalized_wasserstein_p95_p05_status": {
                    "status": "not_defined",
                    "reason": "one_or_both_finite_samples_are_empty",
                },
                "baseline_p99": None,
                "baseline_tail_rate": None,
                "drifted_tail_rate": None,
                "tail_gain_over_baseline_p99": None,
            },
        }

    low = float(min(baseline.min(), drifted.min()))
    high = float(max(baseline.max(), drifted.max()))
    histogram_low, histogram_high = low, high
    if math.isclose(histogram_low, histogram_high):
        padding = max(abs(histogram_low) * 0.05, 0.5)
        histogram_low -= padding
        histogram_high += padding
    edges = np.linspace(histogram_low, histogram_high, bins + 1)
    baseline_hist, _ = np.histogram(baseline, bins=edges)
    drifted_hist, _ = np.histogram(drifted, bins=edges)
    levels = np.asarray(ANALYSIS_CONFIG["quantile_levels"], dtype=float)
    baseline_quantiles = np.quantile(baseline, levels, method="linear")
    drifted_quantiles = np.quantile(drifted, levels, method="linear")
    pooled = np.concatenate((baseline, drifted))
    raw_wasserstein = wasserstein_distance_1d(baseline, drifted)
    assert raw_wasserstein is not None
    pooled_range = float(np.max(pooled) - np.min(pooled))
    pooled_p05, pooled_p95 = np.quantile(pooled, [0.05, 0.95], method="linear")
    pooled_p95_p05 = float(pooled_p95 - pooled_p05)
    baseline_p99 = float(
        np.quantile(
            baseline,
            float(ANALYSIS_CONFIG["tail_threshold_quantile"]),
            method="linear",
        )
    )
    baseline_tail_rate = float(np.mean(baseline > baseline_p99))
    drifted_tail_rate = float(np.mean(drifted > baseline_p99))

    return {
        "distribution_type": "numeric",
        "axis_range": [histogram_low, histogram_high],
        "observed_range": [low, high],
        "bin_edges": [float(value) for value in edges],
        "baseline_histogram": _normalized_counts(baseline_hist),
        "drifted_histogram": _normalized_counts(drifted_hist),
        "ecdf": {
            "baseline": _bounded_ecdf(baseline),
            "drifted": _bounded_ecdf(drifted),
        },
        "quantile_comparison": {
            "levels": [float(value) for value in levels],
            "labels": ["P05", "P25", "P50", "P75", "P95"],
            "baseline": [float(value) for value in baseline_quantiles],
            "drifted": [float(value) for value in drifted_quantiles],
            "shift": [
                float(value)
                for value in (drifted_quantiles - baseline_quantiles)
            ],
            "method": "linear",
        },
        "baseline_summary": _numeric_describe(baseline),
        "drifted_summary": _numeric_describe(drifted),
        "comparison_metrics": {
            "status": "supported",
            "basis": "observed_sample",
            "ks_distance": ks_distance(baseline, drifted),
            "wasserstein_distance": raw_wasserstein,
            "wasserstein_unit": "comparison column units",
            "pooled_range": pooled_range,
            "normalized_wasserstein_range": _safe_normalized_distance(
                raw_wasserstein, pooled_range, zero_scale_result=0.0
            ),
            "pooled_p95_p05_span": pooled_p95_p05,
            "normalized_wasserstein_p95_p05": _safe_normalized_distance(
                raw_wasserstein, pooled_p95_p05, zero_scale_result=None
            ),
            "normalized_wasserstein_p95_p05_status": (
                {"status": "supported", "reason": None}
                if pooled_p95_p05 > 0
                else {
                    "status": "not_defined",
                    "reason": "pooled_p95_p05_is_zero",
                }
            ),
            "baseline_p99": baseline_p99,
            "baseline_tail_rate": baseline_tail_rate,
            "drifted_tail_rate": drifted_tail_rate,
            "tail_gain_over_baseline_p99": drifted_tail_rate
            - baseline_tail_rate,
        },
    }


def _categorical_summary(
    baseline: np.ndarray, drifted: np.ndarray, top_k: int
) -> dict[str, Any]:
    baseline_values = [str(value) for value in baseline]
    drifted_values = [str(value) for value in drifted]
    baseline_counts = Counter(baseline_values)
    drifted_counts = Counter(drifted_values)
    support = sorted(set(baseline_counts) | set(drifted_counts))
    baseline_total = len(baseline_values)
    drifted_total = len(drifted_values)
    baseline_full = _frequencies(support, baseline_counts, baseline_total)
    drifted_full = _frequencies(support, drifted_counts, drifted_total)
    full_by_category = {
        category: (baseline_full[index], drifted_full[index])
        for index, category in enumerate(support)
    }
    ranked = sorted(
        support,
        key=lambda value: (-max(full_by_category[value]), value),
    )
    categories = ranked[:top_k]
    baseline_top = _frequencies(categories, baseline_counts, baseline_total)
    drifted_top = _frequencies(categories, drifted_counts, drifted_total)
    baseline_other = max(0.0, 1.0 - sum(baseline_top)) if baseline_total else 0.0
    drifted_other = max(0.0, 1.0 - sum(drifted_top)) if drifted_total else 0.0
    other_label = "Other"
    status = "supported" if baseline_total and drifted_total else "insufficient_data"
    metrics = _probability_comparison(
        support,
        baseline_full,
        drifted_full,
        entity_name="category",
    )
    metrics.update(_row_count_metrics(0, 0))

    display_baseline = [*baseline_top, baseline_other]
    display_drifted = [*drifted_top, drifted_other]
    category_deltas = {
        category: 100.0
        * (full_by_category[category][1] - full_by_category[category][0])
        for category in support
    }
    mover_categories = sorted(
        support,
        key=lambda value: (-abs(category_deltas[value]), value),
    )[: int(ANALYSIS_CONFIG["categorical_movers"])]
    return {
        "status": status,
        "reason": None
        if status == "supported"
        else "Both baseline and drifted require at least one non-null category.",
        "distribution_type": "categorical",
        "top_k": int(top_k),
        "categories": categories,
        "baseline_frequency": baseline_top,
        "drifted_frequency": drifted_top,
        "other": {
            "label": other_label,
            "is_aggregate": True,
            "baseline_frequency": baseline_other,
            "drifted_frequency": drifted_other,
        },
        "display_frequency": {
            "labels": [*[_non_aggregate_label(value) for value in categories], other_label],
            "is_aggregate": [False] * len(categories) + [True],
            "baseline": display_baseline,
            "drifted": display_drifted,
            "delta_percentage_points": [
                100.0 * (drifted_value - baseline_value)
                for baseline_value, drifted_value in zip(
                    display_baseline, display_drifted
                )
            ],
        },
        "baseline_unique": int(len(baseline_counts)),
        "drifted_unique": int(len(drifted_counts)),
        "top_movers": [
            {
                "category": category,
                "baseline_frequency": full_by_category[category][0],
                "drifted_frequency": full_by_category[category][1],
                "delta_percentage_points": category_deltas[category],
            }
            for category in mover_categories
        ],
        "concentration_curve": _paired_concentration_curve(
            baseline_full, drifted_full
        ),
        "comparison_metrics": metrics,
    }


def _probability_comparison(
    support: Sequence[str],
    baseline: Sequence[float],
    drifted: Sequence[float],
    *,
    entity_name: str,
) -> dict[str, Any]:
    first, second = _normalized_probability_pair(baseline, drifted)
    if first is None or second is None:
        return {
            "status": "not_defined",
            "reason": "one_or_both_probability_masses_are_zero",
            "basis": "observed_sample",
            "jensen_shannon_divergence_bits": None,
            "total_variation_distance": None,
            "entropy_bits": {"baseline": None, "drifted": None},
            "effective_count": {"baseline": None, "drifted": None},
            "top3_share": {"baseline": None, "drifted": None},
            "max_mover": None,
        }
    first_entropy = _entropy_bits(first)
    second_entropy = _entropy_bits(second)
    deltas = second - first
    max_mover = None
    if len(support):
        max_index = sorted(
            range(len(support)),
            key=lambda index: (-abs(float(deltas[index])), str(support[index])),
        )[0]
        max_mover = {
            entity_name: str(support[max_index]),
            "baseline_frequency": float(first[max_index]),
            "drifted_frequency": float(second[max_index]),
            "delta_percentage_points": float(100.0 * deltas[max_index]),
        }
    return {
        "status": "supported",
        "reason": None,
        "basis": "observed_sample",
        "jensen_shannon_divergence_bits": jensen_shannon_divergence(first, second),
        "total_variation_distance": total_variation_distance(first, second),
        "entropy_bits": {
            "baseline": first_entropy,
            "drifted": second_entropy,
        },
        "effective_count": {
            "baseline": float(2.0**first_entropy),
            "drifted": float(2.0**second_entropy),
        },
        "top3_share": {
            "baseline": float(np.sum(np.sort(first)[::-1][:3])),
            "drifted": float(np.sum(np.sort(second)[::-1][:3])),
        },
        "max_mover": max_mover,
    }


def _paired_concentration_curve(
    baseline: Sequence[float], drifted: Sequence[float]
) -> dict[str, Any]:
    length = max(len(baseline), len(drifted))
    if length == 0:
        return {"rank": [], "baseline_cumulative": [], "drifted_cumulative": []}
    first = np.sort(np.asarray(baseline, dtype=float))[::-1]
    second = np.sort(np.asarray(drifted, dtype=float))[::-1]
    if len(first) < length:
        first = np.pad(first, (0, length - len(first)))
    if len(second) < length:
        second = np.pad(second, (0, length - len(second)))
    indices = _bounded_indices(length, int(ANALYSIS_CONFIG["concentration_max_points"]))
    first_cumulative = np.cumsum(first)
    second_cumulative = np.cumsum(second)
    return {
        "rank": [int(index + 1) for index in indices],
        "baseline_cumulative": [float(first_cumulative[index]) for index in indices],
        "drifted_cumulative": [float(second_cumulative[index]) for index in indices],
    }


def _bounded_ecdf(values: np.ndarray) -> dict[str, list[float]]:
    unique, counts = np.unique(np.sort(values), return_counts=True)
    probabilities = np.cumsum(counts) / len(values)
    indices = _bounded_indices(len(unique), int(ANALYSIS_CONFIG["ecdf_max_points"]))
    return {
        "x": [float(unique[index]) for index in indices],
        "probability": [float(probabilities[index]) for index in indices],
    }


def _bounded_indices(length: int, maximum: int) -> np.ndarray:
    if length <= 0:
        return np.asarray([], dtype=int)
    if length <= maximum:
        return np.arange(length, dtype=int)
    indices = np.rint(np.linspace(0, length - 1, maximum)).astype(int)
    return np.unique(np.concatenate(([0], indices, [length - 1])))


def _numeric_describe(values: np.ndarray) -> dict[str, float]:
    quantiles = np.quantile(values, [0.05, 0.25, 0.50, 0.75, 0.95], method="linear")
    return {
        "min": float(np.min(values)),
        "max": float(np.max(values)),
        "mean": float(np.mean(values)),
        "median": float(np.median(values)),
        "stddev": float(np.std(values)),
        "p05": float(quantiles[0]),
        "p25": float(quantiles[1]),
        "p50": float(quantiles[2]),
        "p75": float(quantiles[3]),
        "p95": float(quantiles[4]),
    }


def _row_count_metrics(baseline: int, drifted: int) -> dict[str, Any]:
    delta = int(drifted - baseline)
    rate_status = (
        {"status": "supported", "reason": None}
        if baseline > 0
        else {
            "status": "not_defined",
            "reason": "baseline_row_count_is_zero",
        }
    )
    return {
        "row_delta": delta,
        "row_rate": None if baseline == 0 else float(delta / baseline),
        "row_rate_status": rate_status,
        "row_count": {
            "baseline": int(baseline),
            "drifted": int(drifted),
            "delta": delta,
            "change_rate": None if baseline == 0 else float(delta / baseline),
            "change_rate_status": rate_status,
        }
    }


def _sample_array(
    values: np.ndarray, sample_size: int, seed: int, stream: str
) -> np.ndarray:
    if len(values) <= sample_size:
        return np.asarray(values).copy()
    derived = hashlib.sha256(
        f"visualization-sample/v2|{seed}|{stream}".encode("utf-8")
    ).digest()
    generator = np.random.default_rng(int.from_bytes(derived[:8], "big"))
    indices = np.sort(generator.choice(len(values), size=sample_size, replace=False))
    return np.asarray(values)[indices]


def _finite_values(series: pd.Series) -> np.ndarray:
    values = pd.to_numeric(series, errors="coerce").to_numpy(dtype=float)
    return values[np.isfinite(values)]


def _finite_array(values: Sequence[float]) -> np.ndarray:
    array = np.asarray(values, dtype=float).reshape(-1)
    return array[np.isfinite(array)]


def _normalized_counts(counts: np.ndarray) -> list[float]:
    total = int(counts.sum())
    if total <= 0:
        return [0.0 for _ in counts]
    return [float(value / total) for value in counts]


def _frequencies(
    support: Sequence[str], counts: Counter[str], total: int
) -> list[float]:
    if total <= 0:
        return [0.0 for _ in support]
    return [float(counts[value] / total) for value in support]


def _normalized_probability_pair(
    baseline: Sequence[float], drifted: Sequence[float]
) -> tuple[np.ndarray | None, np.ndarray | None]:
    first = np.asarray(baseline, dtype=float)
    second = np.asarray(drifted, dtype=float)
    if first.shape != second.shape or first.ndim != 1:
        raise ValueError("probability vectors must be aligned one-dimensional arrays")
    if np.any(~np.isfinite(first)) or np.any(~np.isfinite(second)):
        raise ValueError("probability vectors must be finite")
    if np.any(first < 0) or np.any(second < 0):
        raise ValueError("probability vectors must be non-negative")
    first_total = float(np.sum(first))
    second_total = float(np.sum(second))
    if first_total <= 0 or second_total <= 0:
        return None, None
    return first / first_total, second / second_total


def _kl_bits(probability: np.ndarray, reference: np.ndarray) -> float:
    mask = probability > 0
    return float(
        np.sum(probability[mask] * np.log2(probability[mask] / reference[mask]))
    )


def _entropy_bits(probability: np.ndarray) -> float:
    positive = probability[probability > 0]
    return float(-np.sum(positive * np.log2(positive)))


def _safe_normalized_distance(
    distance: float, scale: float, *, zero_scale_result: float | None
) -> float | None:
    if scale > 0:
        return float(distance / scale)
    return zero_scale_result


def _safe_rate(numerator: int, denominator: int) -> float | None:
    return None if denominator == 0 else float(numerator / denominator)


def _non_aggregate_label(value: str) -> str:
    """Disambiguate a real value named Other from the aggregate display bucket."""

    return '"Other" (value)' if value == "Other" else value


def _frequency_entry(
    template_id: str, values: tuple[float, float, float]
) -> dict[str, Any]:
    return {
        "template_id": template_id,
        "baseline_frequency": float(values[0]),
        "drifted_frequency": float(values[1]),
        "delta_percentage_points": float(values[2]),
    }


def _lexical_metrics(template: QueryTemplate) -> dict[str, int]:
    assert template.sql is not None
    sql_without_comments = "\n".join(
        line for line in template.sql.splitlines() if not line.lstrip().startswith("--")
    )
    sql_without_meta = "\n".join(
        line for line in sql_without_comments.splitlines() if not line.lstrip().startswith("\\")
    )
    statements = [part for part in sql_without_meta.split(";") if part.strip()]
    return {
        "statement_count": max(1, len(statements)),
        "comparison_count": len(_COMPARISON_RE.findall(sql_without_meta)),
        "explicit_join_count": len(_JOIN_RE.findall(sql_without_meta)),
    }


def _metric_distribution(metrics: list[dict[str, int]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for name in ("statement_count", "comparison_count", "explicit_join_count"):
        values = np.asarray([entry[name] for entry in metrics], dtype=float)
        result[name] = {
            "mean": float(np.mean(values)),
            "median": float(np.median(values)),
            "p95": float(np.quantile(values, 0.95, method="linear")),
            "min": int(np.min(values)),
            "max": int(np.max(values)),
        }
    return result


def _lexical_plot_ready(
    baseline: list[dict[str, int]], drifted: list[dict[str, int]]
) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for name in ("statement_count", "comparison_count", "explicit_join_count"):
        baseline_counts = Counter(entry[name] for entry in baseline)
        drifted_counts = Counter(entry[name] for entry in drifted)
        values = sorted(set(baseline_counts) | set(drifted_counts))
        result[name] = {
            "values": [int(value) for value in values],
            "baseline_frequency": [
                float(baseline_counts[value] / len(baseline)) for value in values
            ],
            "drifted_frequency": [
                float(drifted_counts[value] / len(drifted)) for value in values
            ],
        }
    return result


__all__ = [
    "ANALYSIS_CONFIG",
    "ANALYSIS_SCHEMA",
    "analysis_metadata",
    "jensen_shannon_divergence",
    "ks_distance",
    "summarize_data_distribution",
    "summarize_query_distribution",
    "total_variation_distance",
    "wasserstein_distance_1d",
]
