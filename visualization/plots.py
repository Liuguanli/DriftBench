"""Deterministic Matplotlib diagnostic dashboards with lazy imports."""

from __future__ import annotations

import importlib.metadata
import importlib.util
import os
import tempfile
from pathlib import Path
from types import MappingProxyType
from typing import Any, Mapping, Sequence

import numpy as np

from .artifacts import semantic_hash


RENDERER_SCHEMA = "driftbench.visualization-renderer/v3"
PLOT_CONFIG = MappingProxyType(
    {
        "width_inches": 16,
        "height_inches": 10,
        "dpi": 100,
        "style_revision": 1,
        "font_family": "DejaVu Sans",
        "categorical_label_max_chars": 18,
        "categorical_shift_labels": "rank",
        "query_label_max_chars": 18,
        "data_layout": (0.115, 0.965, 0.865, 0.105, 0.28, 0.38),
        "query_layout": (0.115, 0.965, 0.865, 0.105, 0.30, 0.38),
        "background": "#FFFFFF",
        "baseline_color": "#0072B2",
        "drifted_color": "#D55E00",
        "positive_color": "#D55E00",
        "negative_color": "#0072B2",
        "accent_color": "#009E73",
        "neutral_color": "#5B6573",
        "grid_color": "#D9DEE7",
        "text_color": "#17202A",
        "muted_color": "#5B6573",
    }
)


class MissingOptionalDependency(RuntimeError):
    pass


def require_matplotlib() -> None:
    if importlib.util.find_spec("matplotlib") is None:
        raise MissingOptionalDependency(
            'Visualization generation requires matplotlib; install pip install -e ".[visualization]"'
        )


def renderer_metadata() -> dict[str, Any]:
    """Return renderer identity without importing pyplot or Matplotlib modules."""

    require_matplotlib()
    config = dict(PLOT_CONFIG)
    width = int(config["width_inches"] * config["dpi"])
    height = int(config["height_inches"] * config["dpi"])
    palette = {
        "baseline": str(config["baseline_color"]),
        "drifted": str(config["drifted_color"]),
        "positive": str(config["positive_color"]),
        "negative": str(config["negative_color"]),
        "accent": str(config["accent_color"]),
        "neutral": str(config["neutral_color"]),
    }
    return {
        "schema": RENDERER_SCHEMA,
        "matplotlib_version": importlib.metadata.version("matplotlib"),
        "numpy_version": np.__version__,
        "size_inches": [config["width_inches"], config["height_inches"]],
        "pixel_size": [width, height],
        "dpi": config["dpi"],
        "font_family": config["font_family"],
        "background": config["background"],
        "palette": palette,
        "plot_config": config,
        "plot_config_sha256": semantic_hash(config),
    }


def plot_data_comparison(
    statistics: Mapping[str, Any],
    *,
    benchmark: str,
    benchmark_title: str,
    scenario: str,
    seed: int,
    output_path: Path,
    drift_type: str | None = None,
    effect_label: str | None = None,
) -> None:
    """Render a 2x2 numeric or categorical data-drift diagnostic."""

    plt = _pyplot()
    with plt.rc_context(_rc_parameters()):
        figure, axes = plt.subplots(
            2,
            2,
            figsize=(PLOT_CONFIG["width_inches"], PLOT_CONFIG["height_inches"]),
            dpi=PLOT_CONFIG["dpi"],
        )
        figure.set_facecolor(PLOT_CONFIG["background"])
        for axis in axes.flat:
            _style_axis(axis)

        if statistics.get("status") == "insufficient_data":
            for axis in axes.flat:
                _insufficient_panel(axis, str(statistics.get("reason") or "No comparable observations."))
        elif statistics["distribution_type"] == "numeric":
            drift_identity = (drift_type or scenario).lower()
            _plot_numeric_data_dashboard(
                axes,
                statistics,
                show_tail="outlier" in drift_identity,
            )
        else:
            _plot_categorical_data_dashboard(axes, statistics)

        sample = statistics.get("sample_count", {})
        title_drift = drift_type or scenario
        _dashboard_header(
            figure,
            title=f"{benchmark_title} · Data drift diagnostic",
            subtitle=(
                f"{benchmark}  |  {title_drift}  |  scenario {scenario}  |  seed {seed}  |  "
                f"observed sample n={sample.get('baseline', 0):,}/{sample.get('drifted', 0):,}"
            ),
            verdict=effect_label,
        )
        figure.text(
            0.06,
            0.025,
            "Descriptive sample diagnostics only · identical scales are used for Baseline and Drifted",
            color=PLOT_CONFIG["muted_color"],
            fontsize=9,
            ha="left",
        )
        _apply_layout(figure, PLOT_CONFIG["data_layout"])
        _save_figure(figure, output_path, plt)


def plot_query_comparison(
    statistics: Mapping[str, Any],
    *,
    benchmark: str,
    benchmark_title: str,
    scenario: str,
    seed: int,
    output_path: Path,
    drift_type: str | None = None,
    effect_label: str | None = None,
) -> None:
    """Render a 2x2 observed query-workload diagnostic."""

    plt = _pyplot()
    with plt.rc_context(_rc_parameters()):
        figure, axes = plt.subplots(
            2,
            2,
            figsize=(PLOT_CONFIG["width_inches"], PLOT_CONFIG["height_inches"]),
            dpi=PLOT_CONFIG["dpi"],
        )
        figure.set_facecolor(PLOT_CONFIG["background"])
        for axis in axes.flat:
            _style_axis(axis)

        _plot_query_frequency(axes[0, 0], statistics)
        _plot_query_movers(axes[0, 1], statistics)
        _plot_query_concentration(axes[1, 0], statistics)
        _plot_query_lexical(axes[1, 1], statistics)

        sample = statistics.get("sample_count", {})
        title_drift = drift_type or scenario
        _dashboard_header(
            figure,
            title=f"{benchmark_title} · Query drift diagnostic",
            subtitle=(
                f"{benchmark}  |  {title_drift}  |  scenario {scenario}  |  seed {seed}  |  "
                f"observed sample n={sample.get('baseline', 0):,}/{sample.get('drifted', 0):,}"
            ),
            verdict=effect_label,
        )
        unsupported_names = [
            str(item["metric"]).replace(" distribution", "")
            for item in statistics.get("unsupported", [])
        ]
        unsupported = " · ".join(unsupported_names)
        figure.text(
            0.06,
            0.025,
            f"Unsupported in this phase: {unsupported} · No database execution or performance inference",
            color=PLOT_CONFIG["muted_color"],
            fontsize=9,
            ha="left",
        )
        _apply_layout(figure, PLOT_CONFIG["query_layout"])
        _save_figure(figure, output_path, plt)


def _plot_numeric_data_dashboard(
    axes, statistics: Mapping[str, Any], *, show_tail: bool
) -> None:
    histogram_axis, ecdf_axis = axes[0]
    quantile_axis, overview_axis = axes[1]
    edges = np.asarray(statistics["bin_edges"], dtype=float)
    baseline_histogram = np.asarray(statistics["baseline_histogram"], dtype=float)
    drifted_histogram = np.asarray(statistics["drifted_histogram"], dtype=float)

    histogram_axis.stairs(
        baseline_histogram,
        edges,
        fill=True,
        alpha=0.22,
        linewidth=2.0,
        color=PLOT_CONFIG["baseline_color"],
        label="Baseline",
    )
    histogram_axis.stairs(
        drifted_histogram,
        edges,
        fill=True,
        alpha=0.18,
        linewidth=2.0,
        color=PLOT_CONFIG["drifted_color"],
        label="Drifted",
    )
    histogram_axis.set_xlim(*statistics["axis_range"])
    histogram_axis.set_ylim(bottom=0)
    histogram_axis.set_xlabel(str(statistics["column"]))
    histogram_axis.set_ylabel("Share of sampled rows")
    histogram_axis.set_title("01  Distribution shape · shared bins + range", loc="left")
    histogram_axis.legend(loc="upper right", frameon=False, ncols=2)

    metrics = statistics["comparison_metrics"]
    if show_tail:
        _plot_tail_ccdf(ecdf_axis, statistics)
    else:
        for key, label, color, marker in (
            ("baseline", "Baseline", PLOT_CONFIG["baseline_color"], "o"),
            ("drifted", "Drifted", PLOT_CONFIG["drifted_color"], "^"),
        ):
            values = statistics["ecdf"][key]
            ecdf_axis.step(
                values["x"],
                values["probability"],
                where="post",
                linewidth=2.2,
                color=color,
                label=label,
            )
            if values["x"]:
                ecdf_axis.scatter(
                    [values["x"][-1]],
                    [values["probability"][-1]],
                    color=color,
                    marker=marker,
                    s=32,
                    zorder=3,
                )
        ecdf_axis.set_xlim(*statistics["axis_range"])
        ecdf_axis.set_ylim(0, 1.03)
        ecdf_axis.set_xlabel(str(statistics["column"]))
        ecdf_axis.set_ylabel("Cumulative probability")
        ecdf_axis.set_title("02  Empirical CDF · location + tail shift", loc="left")

    baseline_p99 = metrics.get("baseline_p99")
    if baseline_p99 is not None:
        for axis in (histogram_axis, ecdf_axis):
            axis.axvline(
                float(baseline_p99),
                color=PLOT_CONFIG["neutral_color"],
                linewidth=1.2,
                linestyle="--",
                alpha=0.9,
            )
        histogram_axis.text(
            float(baseline_p99),
            histogram_axis.get_ylim()[1] * 0.96,
            " Baseline P99",
            ha="left",
            va="top",
            fontsize=8.5,
            color=PLOT_CONFIG["neutral_color"],
        )

    quantiles = statistics["quantile_comparison"]
    positions = np.arange(len(quantiles["labels"]))
    for index, (baseline, drifted) in enumerate(
        zip(quantiles["baseline"], quantiles["drifted"])
    ):
        quantile_axis.plot(
            [baseline, drifted],
            [index, index],
            color=PLOT_CONFIG["grid_color"],
            linewidth=3,
            solid_capstyle="round",
            zorder=1,
        )
    quantile_axis.scatter(
        quantiles["baseline"],
        positions,
        s=64,
        color=PLOT_CONFIG["baseline_color"],
        marker="o",
        label="Baseline",
        zorder=3,
    )
    quantile_axis.scatter(
        quantiles["drifted"],
        positions,
        s=70,
        color=PLOT_CONFIG["drifted_color"],
        marker="^",
        label="Drifted",
        zorder=3,
    )
    quantile_axis.set_yticks(positions, quantiles["labels"])
    quantile_axis.invert_yaxis()
    quantile_axis.set_xlabel(f"{statistics['column']} · absolute units")
    quantile_axis.set_ylabel("Observed quantile")
    quantile_axis.set_title("03  Quantile shift · P05 to P95", loc="left")

    _plot_data_overview(overview_axis, statistics)


def _plot_categorical_data_dashboard(axes, statistics: Mapping[str, Any]) -> None:
    frequency_axis, shift_axis = axes[0]
    concentration_axis, overview_axis = axes[1]
    display = statistics["display_frequency"]
    labels = [
        _short_label(value, int(PLOT_CONFIG["categorical_label_max_chars"]))
        for value in display["labels"]
    ]
    positions = np.arange(len(labels))

    for index, (baseline, drifted) in enumerate(zip(display["baseline"], display["drifted"])):
        frequency_axis.plot(
            [baseline, drifted],
            [index, index],
            color=PLOT_CONFIG["grid_color"],
            linewidth=3,
            solid_capstyle="round",
            zorder=1,
        )
    frequency_axis.scatter(
        display["baseline"], positions, color=PLOT_CONFIG["baseline_color"], marker="o", s=55, label="Baseline", zorder=3
    )
    frequency_axis.scatter(
        display["drifted"], positions, color=PLOT_CONFIG["drifted_color"], marker="^", s=62, label="Drifted", zorder=3
    )
    frequency_axis.set_yticks(positions, labels)
    frequency_axis.invert_yaxis()
    frequency_axis.set_xlabel("Observed sample share")
    frequency_axis.set_xlim(left=0)
    frequency_axis.set_ylabel("Category · aggregate Other is separate")
    frequency_axis.set_title("01  Top categories + Other · frequency", loc="left")
    frequency_axis.legend(
        loc="lower right",
        bbox_to_anchor=(1.0, 1.015),
        frameon=False,
        ncols=2,
        borderaxespad=0,
    )

    deltas = np.asarray(display["delta_percentage_points"], dtype=float)
    colors = [
        PLOT_CONFIG["positive_color"] if value >= 0 else PLOT_CONFIG["negative_color"]
        for value in deltas
    ]
    shift_axis.barh(positions, deltas, color=colors, alpha=0.82, height=0.62)
    shift_axis.axvline(0, color=PLOT_CONFIG["neutral_color"], linewidth=1)
    shift_labels = [
        "Other" if is_aggregate else f"#{index + 1}"
        for index, is_aggregate in enumerate(display["is_aggregate"])
    ]
    shift_axis.set_yticks(positions, shift_labels)
    shift_axis.invert_yaxis()
    shift_axis.set_xlabel("Drifted − Baseline · percentage points")
    shift_axis.set_ylabel("Category rank from panel 01")
    shift_axis.set_title("02  Frequency movement · same ranked categories", loc="left")
    _set_symmetric_delta_axis(shift_axis, deltas)

    _plot_concentration_curve(concentration_axis, statistics["concentration_curve"])
    concentration_axis.set_title("03  Concentration profile · full support", loc="left")
    concentration_axis.set_xlabel("Category rank by observed frequency")
    concentration_axis.set_ylabel("Cumulative sample share")
    _plot_data_overview(overview_axis, statistics)


def _plot_data_overview(axis, statistics: Mapping[str, Any]) -> None:
    counts = statistics["row_count"]
    values = [counts["baseline"], counts["drifted"]]
    bars = axis.bar(
        [0, 1],
        values,
        width=0.58,
        color=[PLOT_CONFIG["baseline_color"], PLOT_CONFIG["drifted_color"]],
        alpha=0.88,
    )
    axis.set_xticks([0, 1], ["Baseline", "Drifted"])
    axis.set_ylabel("Input rows")
    axis.set_title("04  Scale + diagnostic distances", loc="left")
    axis.set_ylim(bottom=0)
    axis.bar_label(bars, labels=[f"{value:,}" for value in values], padding=4, fontsize=9)
    axis.set_xlim(-0.65, 3.45)
    metrics = statistics["comparison_metrics"]
    row_delta = counts["drifted"] - counts["baseline"]
    row_rate = None if counts["baseline"] == 0 else row_delta / counts["baseline"]
    if statistics["distribution_type"] == "numeric":
        text = "\n".join(
            (
                f"Row Δ   {row_delta:+,}  ({_format_percent(row_rate)})",
                f"KS-D    {_format_decimal(metrics.get('ks_distance'), 3)}",
                f"W₁      {_format_number(metrics.get('wasserstein_distance'))} column units",
                f"W₁ / pooled P95–P05   {_format_decimal(metrics.get('normalized_wasserstein_p95_p05'), 3)}",
                f"W₁ / pooled range      {_format_decimal(metrics.get('normalized_wasserstein_range'), 3)}",
            )
        )
    else:
        effective = metrics.get("effective_count", {})
        top3 = metrics.get("top3_share", {})
        text = "\n".join(
            (
                f"Row Δ   {row_delta:+,}  ({_format_percent(row_rate)})",
                f"JSD     {_format_decimal(metrics.get('jensen_shannon_divergence_bits'), 3)} bits",
                f"TVD     {_format_decimal(metrics.get('total_variation_distance'), 3)}",
                f"Effective categories   {_format_decimal(effective.get('baseline'), 1)} → {_format_decimal(effective.get('drifted'), 1)}",
                f"Top-3 share             {_format_share(top3.get('baseline'))} → {_format_share(top3.get('drifted'))}",
            )
        )
    axis.text(
        1.55,
        max(values) * 0.92 if max(values) else 0.92,
        text,
        ha="left",
        va="top",
        fontsize=10,
        color=PLOT_CONFIG["text_color"],
        linespacing=1.65,
    )


def _plot_query_frequency(axis, statistics: Mapping[str, Any]) -> None:
    entries = statistics["top_templates"]
    display = statistics["template_display_frequency"]
    labels = [
        _short_label(value, int(PLOT_CONFIG["query_label_max_chars"]))
        for value in display["labels"]
    ]
    positions = np.arange(len(labels))
    baseline = display["baseline"]
    drifted = display["drifted"]
    for index, (original, changed) in enumerate(zip(baseline, drifted)):
        axis.plot([original, changed], [index, index], color=PLOT_CONFIG["grid_color"], linewidth=3, solid_capstyle="round")
    axis.scatter(baseline, positions, color=PLOT_CONFIG["baseline_color"], marker="o", s=55, label="Baseline", zorder=3)
    axis.scatter(drifted, positions, color=PLOT_CONFIG["drifted_color"], marker="^", s=62, label="Drifted", zorder=3)
    axis.set_yticks(positions, labels)
    axis.invert_yaxis()
    axis.set_xlabel("Observed sample frequency")
    axis.set_xlim(left=0)
    axis.set_ylabel("Template / operation")
    axis.set_title(f"01  Workload mix · top {len(entries)} + Other", loc="left")
    axis.legend(
        loc="lower right",
        bbox_to_anchor=(1.0, 1.015),
        frameon=False,
        ncols=2,
        borderaxespad=0,
    )


def _plot_query_movers(axis, statistics: Mapping[str, Any]) -> None:
    entries = statistics["top_movers"]
    labels = [
        _short_label(entry["template_id"], int(PLOT_CONFIG["query_label_max_chars"]))
        for entry in entries
    ]
    positions = np.arange(len(entries))
    deltas = np.asarray([entry["delta_percentage_points"] for entry in entries])
    colors = [
        PLOT_CONFIG["positive_color"] if value >= 0 else PLOT_CONFIG["negative_color"]
        for value in deltas
    ]
    axis.barh(positions, deltas, color=colors, alpha=0.84, height=0.62)
    axis.axvline(0, color=PLOT_CONFIG["neutral_color"], linewidth=1)
    axis.set_yticks(positions, labels)
    axis.invert_yaxis()
    axis.set_xlabel("Drifted − Baseline · percentage points")
    axis.set_ylabel("Template / operation")
    axis.set_title(f"02  Largest mix movements · top {len(entries)}", loc="left")
    _set_symmetric_delta_axis(axis, deltas)


def _plot_query_concentration(axis, statistics: Mapping[str, Any]) -> None:
    _plot_concentration_curve(axis, statistics["concentration_curve"])
    axis.set_xlabel("Template rank by observed frequency")
    axis.set_ylabel("Cumulative workload share")
    axis.set_title("03  Mix concentration · all templates", loc="left")
    metrics = statistics["comparison_metrics"]
    effective = metrics["effective_count"]
    top3 = metrics["top3_share"]
    mover = metrics.get("max_mover") or {}
    text = (
        f"JSD {_format_decimal(metrics.get('jensen_shannon_divergence_bits'), 3)} bits   ·   "
        f"TVD {_format_decimal(metrics.get('total_variation_distance'), 3)}\n"
        f"Effective templates {_format_decimal(effective.get('baseline'), 1)} → {_format_decimal(effective.get('drifted'), 1)}   ·   "
        f"Top-3 {_format_share(top3.get('baseline'))} → {_format_share(top3.get('drifted'))}\n"
        f"Largest mover: {_short_label(str(mover.get('template_id', 'n/a')), 28)}  "
        f"{_format_signed_pp(mover.get('delta_percentage_points'))}"
    )
    axis.text(
        0.98,
        0.08,
        text,
        transform=axis.transAxes,
        ha="right",
        va="bottom",
        fontsize=9.5,
        color=PLOT_CONFIG["text_color"],
        linespacing=1.45,
    )


def _plot_query_lexical(axis, statistics: Mapping[str, Any]) -> None:
    lexical = statistics["lexical_metrics"]
    if lexical["status"] != "supported":
        axis.axis("off")
        axis.text(
            0.05,
            0.92,
            "04  SQL complexity · Unsupported",
            transform=axis.transAxes,
            ha="left",
            va="top",
            fontsize=12,
            color=PLOT_CONFIG["text_color"],
        )
        axis.text(
            0.5,
            0.56,
            "SQL lexical complexity is unavailable",
            transform=axis.transAxes,
            ha="center",
            va="center",
            fontsize=15,
            color=PLOT_CONFIG["text_color"],
        )
        axis.text(
            0.5,
            0.43,
            str(lexical["reason"]),
            transform=axis.transAxes,
            ha="center",
            va="center",
            fontsize=10,
            color=PLOT_CONFIG["muted_color"],
            wrap=True,
        )
        axis.text(
            0.5,
            0.27,
            "Template / operation mix diagnostics remain supported.",
            transform=axis.transAxes,
            ha="center",
            va="center",
            fontsize=9.5,
            color=PLOT_CONFIG["muted_color"],
        )
        return

    keys = ["statement_count", "comparison_count", "explicit_join_count"]
    labels = ["Statements", "Comparisons", "Explicit JOINs"]
    positions = np.arange(len(keys))
    for index, key in enumerate(keys):
        for offset, series, color, marker in (
            (-0.15, "baseline", PLOT_CONFIG["baseline_color"], "o"),
            (0.15, "drifted", PLOT_CONFIG["drifted_color"], "^"),
        ):
            summary = lexical[series][key]
            values = [summary["median"], summary["mean"], summary["p95"]]
            y = index + offset
            axis.plot(
                [min(values), max(values)],
                [y, y],
                color=color,
                linewidth=2.5,
                alpha=0.72,
                solid_capstyle="round",
            )
            axis.scatter(summary["median"], y, color=color, marker=marker, s=52, zorder=3)
            axis.scatter(summary["mean"], y, facecolor="white", edgecolor=color, marker="D", s=48, linewidth=1.5, zorder=3)
            axis.scatter(summary["p95"], y, color=color, marker=">", s=55, zorder=3)
    axis.set_yticks(positions, labels)
    axis.invert_yaxis()
    axis.set_xlabel("Count per sampled query")
    axis.set_ylabel("Lexical metric")
    axis.set_title("04  SQL complexity (lexical) · ○/△ median  ◇ mean  ▷ P95", loc="left")


def _plot_concentration_curve(axis, curve: Mapping[str, Sequence[float]]) -> None:
    ranks = curve["rank"]
    baseline = curve["baseline_cumulative"]
    drifted = curve["drifted_cumulative"]
    if not ranks:
        _insufficient_panel(axis, "No observed categories or templates.")
        return
    axis.plot(ranks, baseline, color=PLOT_CONFIG["baseline_color"], linewidth=2.2, marker="o", markevery=[-1], label="Baseline")
    axis.plot(ranks, drifted, color=PLOT_CONFIG["drifted_color"], linewidth=2.2, marker="^", markevery=[-1], label="Drifted")
    axis.set_ylim(0, 1.03)
    axis.set_xlim(1, max(1, int(ranks[-1])))


def _dashboard_header(
    figure, *, title: str, subtitle: str, verdict: str | None = None
) -> None:
    figure.suptitle(
        title,
        x=0.06,
        y=0.972,
        ha="left",
        fontsize=20,
        color=PLOT_CONFIG["text_color"],
        weight="bold",
    )
    figure.text(
        0.06,
        0.925,
        subtitle,
        ha="left",
        va="center",
        fontsize=10.5,
        color=PLOT_CONFIG["muted_color"],
    )
    if verdict:
        figure.text(
            0.06,
            0.895,
            verdict,
            ha="left",
            va="center",
            fontsize=10.5,
            color=PLOT_CONFIG["accent_color"],
            weight="bold",
        )


def _plot_tail_ccdf(axis, statistics: Mapping[str, Any]) -> None:
    for key, label, color in (
        ("baseline", "Baseline", PLOT_CONFIG["baseline_color"]),
        ("drifted", "Drifted", PLOT_CONFIG["drifted_color"]),
    ):
        values = statistics["ecdf"][key]
        probabilities = np.asarray(values["probability"], dtype=float)
        if not len(probabilities):
            continue
        floor = 1.0 / max(1, int(statistics["sample_count"][key]))
        survival = np.maximum(floor, 1.0 - probabilities + floor)
        axis.step(
            values["x"],
            survival,
            where="post",
            linewidth=2.2,
            color=color,
            label=label,
        )
    axis.set_xlim(*statistics["axis_range"])
    axis.set_yscale("log")
    axis.set_ylim(bottom=1.0 / max(statistics["sample_count"].values()), top=1.05)
    axis.set_xlabel(str(statistics["column"]))
    axis.set_ylabel("Tail probability · log scale")
    axis.set_title("02  Tail CCDF · outlier separation", loc="left")


def _set_symmetric_delta_axis(axis, values: Sequence[float]) -> None:
    maximum = max((abs(float(value)) for value in values), default=0.0)
    limit = max(1.0, maximum * 1.12)
    axis.set_xlim(-limit, limit)


def _apply_layout(figure, layout: Sequence[float]) -> None:
    left, right, top, bottom, wspace, hspace = layout
    figure.subplots_adjust(
        left=left,
        right=right,
        top=top,
        bottom=bottom,
        wspace=wspace,
        hspace=hspace,
    )


def _style_axis(axis) -> None:
    axis.set_facecolor(PLOT_CONFIG["background"])
    axis.spines["top"].set_visible(False)
    axis.spines["right"].set_visible(False)
    axis.spines["left"].set_color(PLOT_CONFIG["grid_color"])
    axis.spines["bottom"].set_color(PLOT_CONFIG["grid_color"])
    axis.tick_params(colors=PLOT_CONFIG["muted_color"], labelsize=9)
    axis.grid(axis="both", color=PLOT_CONFIG["grid_color"], linewidth=0.7, alpha=0.65)
    axis.set_axisbelow(True)


def _insufficient_panel(axis, reason: str) -> None:
    axis.axis("off")
    axis.text(0.5, 0.58, "Insufficient data", transform=axis.transAxes, ha="center", va="center", fontsize=14, color=PLOT_CONFIG["text_color"])
    axis.text(0.5, 0.43, reason, transform=axis.transAxes, ha="center", va="center", fontsize=9.5, color=PLOT_CONFIG["muted_color"], wrap=True)


def _rc_parameters() -> dict[str, Any]:
    return {
        "font.family": PLOT_CONFIG["font_family"],
        "font.size": 10,
        "axes.titlesize": 12,
        "axes.titleweight": "bold",
        "axes.labelsize": 9.5,
        "axes.labelcolor": PLOT_CONFIG["text_color"],
        "text.color": PLOT_CONFIG["text_color"],
        "legend.fontsize": 9,
        "figure.facecolor": PLOT_CONFIG["background"],
        "savefig.facecolor": PLOT_CONFIG["background"],
        "axes.formatter.useoffset": False,
    }


def _pyplot():
    require_matplotlib()
    try:
        import matplotlib

        matplotlib.use("Agg", force=True)
        import matplotlib.pyplot as plt
    except Exception as exc:  # pragma: no cover - corrupt optional installs
        raise MissingOptionalDependency(f"matplotlib could not be loaded: {exc}") from exc
    return plt


def _save_figure(figure, output_path: Path, plt) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(
        prefix=f".{output_path.stem}.", suffix=".png", dir=output_path.parent
    )
    os.close(fd)
    temp_path = Path(temp_name)
    try:
        figure.savefig(
            temp_path,
            format="png",
            dpi=PLOT_CONFIG["dpi"],
            facecolor=PLOT_CONFIG["background"],
            metadata={
                "Software": "DriftBench Visualization",
                "Renderer": RENDERER_SCHEMA,
            },
        )
        os.replace(temp_path, output_path)
    finally:
        plt.close(figure)
        if temp_path.exists():
            temp_path.unlink()


def _format_number(value: Any) -> str:
    if value is None:
        return "n/a"
    number = float(value)
    if number == 0:
        return "0"
    if abs(number) >= 10_000 or abs(number) < 0.01:
        return f"{number:.2e}"
    return f"{number:,.2f}"


def _format_decimal(value: Any, digits: int) -> str:
    return "n/a" if value is None else f"{float(value):.{digits}f}"


def _format_percent(value: Any) -> str:
    return "n/a" if value is None else f"{100.0 * float(value):+.1f}%"


def _format_share(value: Any) -> str:
    return "n/a" if value is None else f"{100.0 * float(value):.1f}%"


def _format_signed_pp(value: Any) -> str:
    return "n/a" if value is None else f"{float(value):+.1f} pp"


def _short_label(value: str, length: int) -> str:
    text = str(value)
    return text if len(text) <= length else f"{text[: length - 1]}…"


__all__ = [
    "MissingOptionalDependency",
    "PLOT_CONFIG",
    "RENDERER_SCHEMA",
    "plot_data_comparison",
    "plot_query_comparison",
    "renderer_metadata",
    "require_matplotlib",
]
