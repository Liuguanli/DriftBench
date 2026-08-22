from __future__ import annotations

import math
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from driftbench.api import (
    QueryTemplate,
    QueryWorkloadMixResult,
    apply_query_workload_mix_drift,
)
from visualization.benchmarks import (
    BENCHMARK_ORDER,
    PreparedQueries,
    benchmark_config,
    registry,
    resolve_query_weights,
    scenario_config,
)
from visualization.distributions import (
    jensen_shannon_divergence,
    ks_distance,
    summarize_data_distribution,
    summarize_query_distribution,
    total_variation_distance,
    wasserstein_distance_1d,
)
from visualization.specs import load_canonical_spec


EXPECTED_BENCHMARKS = (
    "tpch",
    "tpcds",
    "tpcc",
    "tpcc_skew",
    "job",
    "ycsb",
    "dsb",
    "pgbench",
)

TEMPLATE_IDS = {
    "tpch": tuple(f"q{index}" for index in range(1, 23)),
    "tpcds": tuple(f"query{index:02d}" for index in range(1, 100)),
    "tpcc": ("new_order", "payment", "order_status", "delivery", "stock_level"),
    "tpcc_skew": (
        "new_order",
        "payment",
        "order_status",
        "delivery",
        "stock_level",
    ),
    "job": (
        "1a_keyword_filter",
        "2a_company_movies",
        "3a_movie_info_filter",
        "4a_cast_keyword",
        "5a_company_country_cast",
        "6a_info_company",
        "7a_keyword_count",
        "8a_actor_productivity",
        "9a_multi_keyword_movie",
        "10a_full_join",
        "11a_company_keyword_year",
        "12a_cast_info_selective",
        "13a_movie_info_aggregate",
        "14a_company_info_cast",
        "15a_keyword_year_range",
        "16a_actor_company",
        "17a_selective_cast_keyword",
        "18a_movie_info_keyword",
        "19a_company_output_volume",
        "20a_full_eight_table",
    ),
    "ycsb": (
        "DeleteRecord",
        "InsertRecord",
        "ReadModifyWriteRecord",
        "ReadRecord",
        "ScanRecord",
        "UpdateRecord",
    ),
    "dsb": (
        "q1_revenue_by_year",
        "q2_revenue_by_region",
        "q3_margin_trend",
    ),
    "pgbench": ("tpcb", "simple_update", "select_only"),
}

YCSB_PROFILES = {
    "A": {
        "DeleteRecord": 0,
        "InsertRecord": 0,
        "ReadModifyWriteRecord": 0,
        "ReadRecord": 50,
        "ScanRecord": 0,
        "UpdateRecord": 50,
    },
    "E": {
        "DeleteRecord": 0,
        "InsertRecord": 5,
        "ReadModifyWriteRecord": 0,
        "ReadRecord": 0,
        "ScanRecord": 95,
        "UpdateRecord": 0,
    },
    "C": {
        "DeleteRecord": 0,
        "InsertRecord": 0,
        "ReadModifyWriteRecord": 0,
        "ReadRecord": 100,
        "ScanRecord": 0,
        "UpdateRecord": 0,
    },
}

EXPECTED_SCENARIOS = {
    "data": {
        "tpch": ("price_outliers", "price_skew", "lineitem_cardinality_reduction"),
        "tpcds": ("item_cardinality_reduction", "price_skew", "price_outliers"),
        "tpcc": ("discount_skew", "customer_cardinality_reduction", "order_amount_outliers"),
        "tpcc_skew": ("stock_quantity_skew", "stock_quantity_outliers", "stock_cardinality_reduction"),
        "job": ("pre_1980_title_deletion", "production_year_skew", "post_2000_title_deletion"),
        "ycsb": ("field0_hot_value_skew", "record_cardinality_reduction", "record_cardinality_growth"),
        "dsb": ("revenue_outliers", "revenue_skew", "lineorder_cardinality_reduction"),
        "pgbench": ("balance_skew", "balance_outliers", "account_cardinality_reduction"),
    },
    "query": {
        "tpch": ("hotset_concentration", "complexity_mix_shift"),
        "tpcds": ("early_id_hotset", "late_id_hotset"),
        "tpcc": ("new_order_hotset", "complexity_mix_shift"),
        "tpcc_skew": ("new_order_hotset", "complexity_mix_shift"),
        "job": ("hotset_concentration", "complexity_mix_shift"),
        "ycsb": ("scan_heavy_profile", "read_only_profile"),
        "dsb": ("region_hotset", "margin_hotset"),
        "pgbench": ("select_only_hotset", "complexity_mix_shift"),
    },
}


def _query_result(templates: tuple[QueryTemplate, ...]):
    weights = {template.template_id: 1.0 for template in templates}
    target = dict(reversed(tuple(weights.items())))
    return apply_query_workload_mix_drift(
        templates,
        baseline_weights=weights,
        target_weights=target,
        sample_size=20,
        seed=42,
    )


class RegistryConfigurationTests(unittest.TestCase):
    def test_registry_contains_exactly_the_eight_supported_benchmarks_in_order(self) -> None:
        definitions = registry()
        self.assertEqual(BENCHMARK_ORDER, EXPECTED_BENCHMARKS)
        self.assertEqual(tuple(definitions), EXPECTED_BENCHMARKS)
        self.assertEqual(set(definitions), set(EXPECTED_BENCHMARKS))
        self.assertNotIn("benchbase", definitions)

    def test_yaml_schema_versions_and_stable_scenario_ids(self) -> None:
        benchmarks = benchmark_config()
        scenarios = scenario_config()
        self.assertEqual(benchmarks["schema_version"], 1)
        self.assertEqual(scenarios["schema_version"], 3)
        self.assertEqual(tuple(benchmarks["order"]), EXPECTED_BENCHMARKS)
        self.assertEqual(tuple(benchmarks["benchmarks"]), EXPECTED_BENCHMARKS)
        self.assertEqual(tuple(scenarios["data"]), EXPECTED_BENCHMARKS)
        self.assertEqual(tuple(scenarios["query"]), EXPECTED_BENCHMARKS)

        for kind in ("data", "query"):
            for benchmark in EXPECTED_BENCHMARKS:
                with self.subTest(benchmark=benchmark, kind=kind):
                    self.assertEqual(
                        tuple(scenarios[kind][benchmark]),
                        EXPECTED_SCENARIOS[kind][benchmark],
                    )

    def test_every_query_scenario_resolves_to_complete_strict_weights(self) -> None:
        definitions = registry()
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            for benchmark in EXPECTED_BENCHMARKS:
                ids = TEMPLATE_IDS[benchmark]
                templates = tuple(QueryTemplate(template_id, None) for template_id in ids)
                profiles = YCSB_PROFILES if benchmark == "ycsb" else {}
                prepared = PreparedQueries(
                    benchmark=definitions[benchmark],
                    templates=templates,
                    input_files=(),
                    profile_weights=profiles,
                )
                for scenario in EXPECTED_SCENARIOS["query"][benchmark]:
                    with self.subTest(benchmark=benchmark, scenario=scenario):
                        spec = load_canonical_spec(
                            root,
                            kind="query",
                            benchmark=benchmark,
                            scenario=scenario,
                        )
                        variables = spec.payload["variables"]
                        baseline, target = resolve_query_weights(prepared, variables)
                        self.assertEqual(tuple(baseline), ids)
                        self.assertEqual(tuple(target), ids)
                        for weights in (baseline, target):
                            self.assertTrue(
                                all(math.isfinite(value) for value in weights.values())
                            )
                            self.assertTrue(all(value >= 0 for value in weights.values()))
                            self.assertGreater(sum(weights.values()), 0)

                        result = apply_query_workload_mix_drift(
                            templates,
                            baseline_weights=baseline,
                            target_weights=target,
                            sample_size=5,
                            seed=42,
                        )
                        self.assertAlmostEqual(sum(result.baseline_weights.values()), 1.0)
                        self.assertAlmostEqual(sum(result.target_weights.values()), 1.0)


class DataDistributionTests(unittest.TestCase):
    def test_numeric_comparison_uses_one_range_and_one_set_of_bins(self) -> None:
        baseline = pd.DataFrame({"metric": [0.0, 1.0, 2.0, 3.0, 4.0]})
        drifted = pd.DataFrame({"metric": [2.0, 4.0, 6.0, 8.0, 10.0]})
        summary = summarize_data_distribution(
            baseline,
            drifted,
            column="metric",
            sample_size=100,
            seed=42,
            bins=5,
        )

        self.assertEqual(summary["distribution_type"], "numeric")
        self.assertEqual(summary["axis_range"], [0.0, 10.0])
        self.assertEqual(summary["bin_edges"][0], summary["axis_range"][0])
        self.assertEqual(summary["bin_edges"][-1], summary["axis_range"][1])
        self.assertEqual(len(summary["baseline_histogram"]), 5)
        self.assertEqual(len(summary["drifted_histogram"]), 5)
        self.assertAlmostEqual(sum(summary["baseline_histogram"]), 1.0)
        self.assertAlmostEqual(sum(summary["drifted_histogram"]), 1.0)

    def test_seeded_sampling_is_reproducible(self) -> None:
        baseline = pd.DataFrame({"metric": range(200)})
        drifted = pd.DataFrame({"metric": range(100, 300)})
        kwargs = {
            "column": "metric",
            "sample_size": 25,
            "seed": 987,
            "bins": 7,
        }
        self.assertEqual(
            summarize_data_distribution(baseline, drifted, **kwargs),
            summarize_data_distribution(baseline, drifted, **kwargs),
        )

    def test_categorical_top_k_is_joint_deterministic_and_aligned(self) -> None:
        baseline = pd.DataFrame({"category": ["b", "a", "a", "d"]})
        drifted = pd.DataFrame({"category": ["b", "c", "c", "d"]})
        summary = summarize_data_distribution(
            baseline,
            drifted,
            column="category",
            sample_size=100,
            seed=42,
            top_k=3,
        )

        self.assertEqual(summary["distribution_type"], "categorical")
        self.assertEqual(summary["categories"], ["a", "c", "b"])
        self.assertEqual(summary["baseline_frequency"], [0.5, 0.0, 0.25])
        self.assertEqual(summary["drifted_frequency"], [0.0, 0.5, 0.25])
        self.assertEqual(
            len(summary["categories"]), len(summary["baseline_frequency"])
        )
        self.assertEqual(
            len(summary["categories"]), len(summary["drifted_frequency"])
        )


class QueryDistributionCapabilityTests(unittest.TestCase):
    def test_non_sql_benchmarks_mark_lexical_metrics_unsupported(self) -> None:
        definitions = registry()
        result = _query_result(
            (QueryTemplate("read", None), QueryTemplate("scan", None))
        )
        for benchmark in ("tpcds", "ycsb"):
            with self.subTest(benchmark=benchmark):
                summary = summarize_query_distribution(
                    result,
                    capabilities=definitions[benchmark].query_capabilities,
                )
                self.assertEqual(summary["lexical_metrics"]["status"], "unsupported")

    def test_sql_benchmarks_label_complexity_as_lexical(self) -> None:
        definitions = registry()
        templates = (
            QueryTemplate("lookup", "SELECT * FROM t WHERE id = 1;"),
            QueryTemplate(
                "join",
                "SELECT * FROM a JOIN b ON a.id = b.id WHERE a.value >= 2;",
            ),
        )
        result = _query_result(templates)
        sql_benchmarks = tuple(
            benchmark
            for benchmark in EXPECTED_BENCHMARKS
            if benchmark not in {"tpcds", "ycsb"}
        )
        for benchmark in sql_benchmarks:
            with self.subTest(benchmark=benchmark):
                summary = summarize_query_distribution(
                    result,
                    capabilities=definitions[benchmark].query_capabilities,
                )
                lexical = summary["lexical_metrics"]
                self.assertEqual(lexical["status"], "supported")
                self.assertTrue(lexical["labels"])
                for label in lexical["labels"].values():
                    self.assertIn("(lexical)", label)

    def test_selectivity_and_temporal_metrics_are_explicitly_unsupported(self) -> None:
        definitions = registry()
        sql_result = _query_result((QueryTemplate("q1", "SELECT 1;"),))
        non_sql_result = _query_result((QueryTemplate("read", None),))
        expected_metrics = {
            "predicate/selectivity distribution",
            "arrival-rate/inter-arrival distribution",
        }

        for benchmark in EXPECTED_BENCHMARKS:
            result = non_sql_result if benchmark in {"tpcds", "ycsb"} else sql_result
            with self.subTest(benchmark=benchmark):
                summary = summarize_query_distribution(
                    result,
                    capabilities=definitions[benchmark].query_capabilities,
                )
                unsupported = {
                    entry["metric"]: entry for entry in summary["unsupported"]
                }
                self.assertEqual(set(unsupported), expected_metrics)
                self.assertTrue(
                    all(entry["status"] == "unsupported" for entry in unsupported.values())
                )

    def test_lexical_supported_rejects_missing_sql_text(self) -> None:
        capabilities = registry()["tpch"].query_capabilities
        result = _query_result(
            (QueryTemplate("q1", "SELECT 1;"), QueryTemplate("q2", None))
        )
        with self.assertRaisesRegex(ValueError, "SQL text is missing"):
            summarize_query_distribution(result, capabilities=capabilities)


class AdvancedMetricKnownAnswerTests(unittest.TestCase):
    def test_numeric_known_answers_quantiles_histogram_ks_and_wasserstein(self) -> None:
        summary = summarize_data_distribution(
            pd.DataFrame({"metric": [0, 1, 2, 3, 4]}),
            pd.DataFrame({"metric": [1, 2, 3, 4, 5]}),
            column="metric",
            sample_size=100,
            seed=42,
            bins=2,
        )
        metrics = summary["comparison_metrics"]
        self.assertAlmostEqual(metrics["ks_distance"], 0.2)
        self.assertAlmostEqual(metrics["wasserstein_distance"], 1.0)
        self.assertAlmostEqual(metrics["pooled_p95_p05_span"], 4.1)
        self.assertAlmostEqual(metrics["normalized_wasserstein_p95_p05"], 10 / 41)
        self.assertEqual(summary["quantile_comparison"]["shift"], [1.0] * 5)
        self.assertEqual(summary["baseline_histogram"], [0.6, 0.4])
        self.assertEqual(summary["drifted_histogram"], [0.4, 0.6])
        self.assertEqual(metrics["basis"], "observed_sample")

        self.assertAlmostEqual(ks_distance([0, 0, 4], [2, 2]), 2 / 3)
        self.assertAlmostEqual(wasserstein_distance_1d([0, 0, 4], [2, 2]), 2.0)

    def test_constants_and_non_finite_values_have_explicit_status(self) -> None:
        identical = summarize_data_distribution(
            pd.DataFrame({"metric": [7.0, 7.0]}),
            pd.DataFrame({"metric": [7.0]}),
            column="metric",
            sample_size=10,
            seed=42,
        )
        metrics = identical["comparison_metrics"]
        self.assertEqual(metrics["ks_distance"], 0.0)
        self.assertEqual(metrics["wasserstein_distance"], 0.0)
        self.assertIsNone(metrics["normalized_wasserstein_p95_p05"])
        self.assertEqual(
            metrics["normalized_wasserstein_p95_p05_status"]["reason"],
            "pooled_p95_p05_is_zero",
        )

        filtered = summarize_data_distribution(
            pd.DataFrame({"metric": [1.0, float("nan"), float("inf")]}),
            pd.DataFrame({"metric": [2.0, float("-inf")]}),
            column="metric",
            sample_size=10,
            seed=42,
        )
        self.assertEqual(filtered["non_finite_count"], {"baseline": 1, "drifted": 1})
        self.assertEqual(filtered["missing_count"], {"baseline": 1, "drifted": 0})

        empty = summarize_data_distribution(
            pd.DataFrame({"metric": pd.Series(dtype=float)}),
            pd.DataFrame({"metric": [float("nan")]}),
            column="metric",
            sample_size=10,
            seed=42,
        )
        self.assertEqual(empty["status"], "insufficient_data")
        self.assertIsNone(empty["comparison_metrics"]["row_rate"])
        self.assertEqual(
            empty["comparison_metrics"]["row_rate_status"]["reason"],
            "baseline_row_count_is_zero",
        )

    def test_probability_metrics_use_full_union_base2_and_deterministic_ties(self) -> None:
        self.assertAlmostEqual(
            jensen_shannon_divergence([0.5, 0.5, 0], [0.5, 0, 0.5]), 0.5
        )
        self.assertAlmostEqual(
            total_variation_distance([0.5, 0.5, 0], [0.5, 0, 0.5]), 0.5
        )
        summary = summarize_data_distribution(
            pd.DataFrame({"category": ["a"] * 6 + ["b"] * 2 + ["c"] * 2}),
            pd.DataFrame({"category": ["a"] * 6 + ["d"] * 2 + ["e"] * 2}),
            column="category",
            sample_size=100,
            seed=42,
            top_k=1,
        )
        self.assertEqual(summary["display_frequency"]["labels"], ["a", "Other"])
        self.assertEqual(summary["display_frequency"]["baseline"], [0.6, 0.4])
        self.assertEqual(summary["display_frequency"]["drifted"], [0.6, 0.4])
        metrics = summary["comparison_metrics"]
        self.assertAlmostEqual(metrics["jensen_shannon_divergence_bits"], 0.4)
        self.assertAlmostEqual(metrics["total_variation_distance"], 0.4)
        self.assertEqual(metrics["max_mover"]["category"], "b")

    def test_entropy_effective_count_top3_and_other_name_collision(self) -> None:
        values = ["a", "a", "b", "c"]
        summary = summarize_data_distribution(
            pd.DataFrame({"category": values}),
            pd.DataFrame({"category": values}),
            column="category",
            sample_size=10,
            seed=42,
            top_k=3,
        )
        metrics = summary["comparison_metrics"]
        self.assertAlmostEqual(metrics["entropy_bits"]["baseline"], 1.5)
        self.assertAlmostEqual(metrics["effective_count"]["baseline"], math.sqrt(8))
        self.assertEqual(metrics["top3_share"]["baseline"], 1.0)

        collision = summarize_data_distribution(
            pd.DataFrame({"category": ["Other", "a"]}),
            pd.DataFrame({"category": ["Other", "a"]}),
            column="category",
            sample_size=10,
            seed=42,
            top_k=2,
        )
        labels = collision["display_frequency"]["labels"]
        self.assertEqual(labels[-1], "Other")
        self.assertIn('"Other" (value)', labels[:-1])

    def test_query_top15_other_movers_and_mapping_order_are_deterministic(self) -> None:
        templates = tuple(QueryTemplate(f"q{index:02d}", None) for index in range(99))
        baseline = templates * 2
        drifted = tuple(reversed(templates)) * 2
        weights = {template.template_id: 1 / 99 for template in templates}
        reverse_weights = dict(reversed(tuple(weights.items())))
        result = QueryWorkloadMixResult(
            baseline=baseline,
            drifted=drifted,
            baseline_weights=reverse_weights,
            target_weights=weights,
            sample_size=len(baseline),
            seed=42,
            semantic_hash="0" * 64,
        )
        summary = summarize_query_distribution(
            result,
            capabilities={
                "lexical_sql_metrics": "unsupported",
                "predicate_selectivity": "unsupported",
                "temporal": "unsupported",
            },
        )
        self.assertEqual(len(summary["top_templates"]), 15)
        self.assertEqual(len(summary["top_movers"]), 15)
        self.assertEqual(summary["template_display_frequency"]["labels"][-1], "Other")
        self.assertNotIn("Other", [entry["template_id"] for entry in summary["top_movers"]])
        self.assertEqual(summary["template_frequency"]["template_ids"], sorted(weights))


if __name__ == "__main__":
    unittest.main()
