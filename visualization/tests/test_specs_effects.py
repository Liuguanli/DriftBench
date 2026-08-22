from __future__ import annotations

import math
import tempfile
import unittest
from pathlib import Path, PurePosixPath

import pandas as pd

from visualization.artifacts import semantic_hash
from visualization.benchmarks import registry, scenario_config
from visualization.distributions import summarize_data_distribution
from visualization.effects import EffectAssertionError, evaluate_effect
from visualization.specs import expected_artifact_keys, load_canonical_spec


EXPECTED_ARTIFACT_KEYS = (
    ("data", "tpch", "price_outliers"),
    ("data", "tpch", "price_skew"),
    ("data", "tpch", "lineitem_cardinality_reduction"),
    ("query", "tpch", "hotset_concentration"),
    ("query", "tpch", "complexity_mix_shift"),
    ("data", "tpcds", "item_cardinality_reduction"),
    ("data", "tpcds", "price_skew"),
    ("data", "tpcds", "price_outliers"),
    ("query", "tpcds", "early_id_hotset"),
    ("query", "tpcds", "late_id_hotset"),
    ("data", "tpcc", "discount_skew"),
    ("data", "tpcc", "customer_cardinality_reduction"),
    ("data", "tpcc", "order_amount_outliers"),
    ("query", "tpcc", "new_order_hotset"),
    ("query", "tpcc", "complexity_mix_shift"),
    ("data", "tpcc_skew", "stock_quantity_skew"),
    ("data", "tpcc_skew", "stock_quantity_outliers"),
    ("data", "tpcc_skew", "stock_cardinality_reduction"),
    ("query", "tpcc_skew", "new_order_hotset"),
    ("query", "tpcc_skew", "complexity_mix_shift"),
    ("data", "job", "pre_1980_title_deletion"),
    ("data", "job", "production_year_skew"),
    ("data", "job", "post_2000_title_deletion"),
    ("query", "job", "hotset_concentration"),
    ("query", "job", "complexity_mix_shift"),
    ("data", "ycsb", "field0_hot_value_skew"),
    ("data", "ycsb", "record_cardinality_reduction"),
    ("data", "ycsb", "record_cardinality_growth"),
    ("query", "ycsb", "scan_heavy_profile"),
    ("query", "ycsb", "read_only_profile"),
    ("data", "dsb", "revenue_outliers"),
    ("data", "dsb", "revenue_skew"),
    ("data", "dsb", "lineorder_cardinality_reduction"),
    ("query", "dsb", "region_hotset"),
    ("query", "dsb", "margin_hotset"),
    ("data", "pgbench", "balance_skew"),
    ("data", "pgbench", "balance_outliers"),
    ("data", "pgbench", "account_cardinality_reduction"),
    ("query", "pgbench", "select_only_hotset"),
    ("query", "pgbench", "complexity_mix_shift"),
)


class CanonicalSpecContractTests(unittest.TestCase):
    def test_exact_ordered_matrix_and_artifact_paths_are_bijective(self) -> None:
        self.assertEqual(expected_artifact_keys(), EXPECTED_ARTIFACT_KEYS)
        self.assertEqual(len(EXPECTED_ARTIFACT_KEYS), 40)
        self.assertEqual(len(set(EXPECTED_ARTIFACT_KEYS)), 40)

        config = scenario_config()
        specs: set[str] = set()
        figures: set[str] = set()
        manifests: set[str] = set()
        for kind, benchmark, scenario in EXPECTED_ARTIFACT_KEYS:
            entry = config[kind][benchmark][scenario]
            spec = f"specs/{kind}/{benchmark}/{scenario}.yaml"
            figure = f"figures/{kind}/{benchmark}/{scenario}.png"
            manifest = f"manifests/{kind}/{benchmark}/{scenario}.json"
            self.assertEqual(entry["spec"], spec)
            for relative in (spec, figure, manifest):
                path = PurePosixPath(relative)
                self.assertFalse(path.is_absolute())
                self.assertNotIn("..", path.parts)
                self.assertNotIn("\\", relative)
            specs.add(spec)
            figures.add(figure)
            manifests.add(manifest)

        self.assertEqual(len(specs), 40)
        self.assertEqual(len(figures), 40)
        self.assertEqual(len(manifests), 40)

    def test_all_canonical_specs_parse_validate_and_have_portable_identity(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            hashes: set[str] = set()
            descriptor_paths: set[str] = set()
            for kind, benchmark, scenario in EXPECTED_ARTIFACT_KEYS:
                with self.subTest(kind=kind, benchmark=benchmark, scenario=scenario):
                    spec = load_canonical_spec(
                        root,
                        kind=kind,
                        benchmark=benchmark,
                        scenario=scenario,
                    )
                    expected = f"specs/{kind}/{benchmark}/{scenario}.yaml"
                    self.assertEqual(spec.descriptor["path"], expected)
                    self.assertEqual(spec.semantic_sha256, semantic_hash(spec.payload))
                    self.assertEqual(
                        spec.payload["metadata"],
                        {
                            **spec.payload["metadata"],
                            "benchmark": benchmark,
                            "kind": kind,
                            "scenario": scenario,
                        },
                    )
                    self.assertTrue(spec.path.resolve().is_relative_to(root))
                    self.assertEqual(spec.payload["seed"], 42)
                    hashes.add(spec.semantic_sha256)
                    descriptor_paths.add(str(spec.descriptor["path"]))

            self.assertEqual(len(hashes), 40)
            self.assertEqual(len(descriptor_paths), 40)

    def test_row_direction_effect_metrics_do_not_accept_the_opposite_direction(self) -> None:
        policies = {
            "growth": {"metric": "row_growth_rate", "threshold": 0.45},
            "reduction": {"metric": "row_reduction_rate", "threshold": 0.40},
        }
        for name, assertion in policies.items():
            with self.subTest(direction=name):
                matching_rate = 0.50 if name == "growth" else -0.45
                result = evaluate_effect(
                    {
                        "mode": "all",
                        "assertions": [
                            {
                                "metric": assertion["metric"],
                                "operator": "gte",
                                "threshold": assertion["threshold"],
                            }
                        ],
                    },
                    {"comparison_metrics": {"row_rate": matching_rate}},
                )
                self.assertTrue(result["passed"])
                with self.assertRaises(EffectAssertionError):
                    evaluate_effect(
                        {
                            "mode": "all",
                            "assertions": [
                                {
                                    "metric": assertion["metric"],
                                    "operator": "gte",
                                    "threshold": assertion["threshold"],
                                }
                            ],
                        },
                        {"comparison_metrics": {"row_rate": -matching_rate}},
                    )


class TailAndEffectPolicyTests(unittest.TestCase):
    def test_numeric_p99_tail_gain_uses_baseline_threshold(self) -> None:
        baseline = list(range(100))
        drifted = list(range(100)) + [100] * 100
        summary = summarize_data_distribution(
            pd.DataFrame({"metric": baseline}),
            pd.DataFrame({"metric": drifted}),
            column="metric",
            sample_size=1000,
            seed=42,
        )
        metrics = summary["comparison_metrics"]
        self.assertAlmostEqual(metrics["baseline_p99"], 98.01)
        self.assertAlmostEqual(metrics["baseline_tail_rate"], 0.01)
        self.assertAlmostEqual(metrics["drifted_tail_rate"], 0.505)
        self.assertAlmostEqual(metrics["tail_gain_over_baseline_p99"], 0.495)

    def test_effect_thresholds_are_inclusive_for_all_supported_operators(self) -> None:
        statistics = {
            "comparison_metrics": {
                "row_rate": -0.20,
                "ks_distance": 0.15,
                "normalized_wasserstein_p95_p05": 0.10,
                "tail_gain_over_baseline_p99": 0.04,
                "jensen_shannon_divergence_bits": 0.20,
                "total_variation_distance": 0.30,
                "max_mover": {"delta_percentage_points": -15.0},
            }
        }
        assertions = (
            ("absolute_row_rate", "gte", 0.20),
            ("row_reduction_rate", "gte", 0.20),
            ("ks_distance", "gte", 0.15),
            ("normalized_wasserstein_p95_p05", "gte", 0.10),
            ("tail_gain_over_baseline_p99", "gte", 0.04),
            ("jensen_shannon_divergence_bits", "gte", 0.20),
            ("total_variation_distance", "gte", 0.30),
            ("max_mover_absolute_pp", "gte", 15.0),
            ("orphan_count", "lte", 0.0),
            ("orphan_count", "eq", 0.0),
        )
        for metric, operator, threshold in assertions:
            with self.subTest(metric=metric, operator=operator):
                result = evaluate_effect(
                    {
                        "mode": "all",
                        "assertions": [
                            {
                                "metric": metric,
                                "operator": operator,
                                "threshold": threshold,
                            }
                        ],
                    },
                    statistics,
                    integrity={"orphan_count": 0},
                )
                self.assertTrue(result["passed"])
                self.assertEqual(result["verdict"], "PASS")
                self.assertEqual(result["assertions"][0]["observed"], threshold)

    def test_effect_policy_rejects_below_threshold_or_missing_metric(self) -> None:
        statistics = {"comparison_metrics": {"ks_distance": math.nextafter(0.15, 0.0)}}
        for metric in ("ks_distance", "tail_gain_over_baseline_p99"):
            with self.subTest(metric=metric):
                with self.assertRaisesRegex(EffectAssertionError, "effect gate failed"):
                    evaluate_effect(
                        {
                            "mode": "all",
                            "assertions": [
                                {"metric": metric, "operator": "gte", "threshold": 0.15}
                            ],
                        },
                        statistics,
                    )


class QueryCapabilityContractTests(unittest.TestCase):
    def test_query_specs_do_not_upgrade_unsupported_adapter_capabilities(self) -> None:
        definitions = registry()
        for benchmark, definition in definitions.items():
            with self.subTest(benchmark=benchmark):
                capabilities = definition.query_capabilities
                self.assertEqual(capabilities["template_frequency"], "supported")
                self.assertEqual(capabilities["predicate_selectivity"], "unsupported")
                self.assertEqual(capabilities["temporal"], "unsupported")
                expected_lexical = (
                    "unsupported" if benchmark in {"tpcds", "ycsb"} else "supported"
                )
                self.assertEqual(capabilities["lexical_sql_metrics"], expected_lexical)


if __name__ == "__main__":
    unittest.main()
