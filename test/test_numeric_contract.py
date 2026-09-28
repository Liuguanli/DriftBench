from __future__ import annotations

import math
from decimal import Decimal
import unittest

from driftbench.numeric_contract import (
    MAX_INTEGER,
    format_number,
    present_probability_map,
    present_real,
    present_value,
    require_integer,
    require_probability,
    require_real,
)
from driftbench.query_drift import QueryTemplate, apply_query_workload_mix_drift


class NumericValidationTests(unittest.TestCase):
    def test_integer_contract_rejects_bool_float_and_out_of_range_values(self):
        self.assertEqual(require_integer(0, "count", minimum=0), 0)
        self.assertEqual(require_integer(MAX_INTEGER, "count"), MAX_INTEGER)
        for value in (True, 1.0, "1", None):
            with self.subTest(value=value), self.assertRaises(TypeError):
                require_integer(value, "count", minimum=0)
        for value in (-1, MAX_INTEGER + 1):
            with self.subTest(value=value), self.assertRaises(ValueError):
                require_integer(value, "count", minimum=0)

    def test_real_contract_is_finite_and_respects_semantic_bounds(self):
        self.assertEqual(require_real(1, "duration", minimum=0), 1.0)
        self.assertEqual(require_probability(1.0, "rate"), 1.0)
        for value in (True, "0.5", None):
            with self.subTest(value=value), self.assertRaises(TypeError):
                require_real(value, "value")
        for value in (math.nan, math.inf, -math.inf):
            with self.subTest(value=value), self.assertRaises(ValueError):
                require_real(value, "value")
        for value in (-0.01, 1.01):
            with self.subTest(value=value), self.assertRaises(ValueError):
                require_probability(value, "rate")


class NumericPresentationTests(unittest.TestCase):
    def test_bounded_display_handles_recurring_tiny_and_negative_zero_values(self):
        self.assertEqual(present_real(0.06666666666666667), 0.066667)
        self.assertEqual(format_number(0.06666666666666667), "0.066667")
        self.assertEqual(format_number(-0.0), "0")
        self.assertEqual(format_number(1.230000), "1.23")
        self.assertEqual(format_number(0.000000012345678), "1.23457e-08")
        self.assertIsInstance(present_real(1.0), float)

    def test_probability_maps_preserve_types_bounds_and_exact_decimal_mass(self):
        displayed = present_probability_map(
            {
                "q1-1": 0.8,
                "q1-2": 0.2 / 3,
                "q6-1": 0.2 / 3,
                "q6-2": 0.2 / 3,
            },
            field="weights",
        )
        self.assertEqual(
            displayed,
            {
                "q1-1": 0.8,
                "q1-2": 0.066667,
                "q6-1": 0.066667,
                "q6-2": 0.066666,
            },
        )
        self.assertTrue(all(type(value) is float for value in displayed.values()))
        self.assertTrue(all(0.0 <= value <= 1.0 for value in displayed.values()))
        self.assertEqual(sum(Decimal(str(value)) for value in displayed.values()), Decimal("1"))

    def test_probability_map_errors_are_actionable(self):
        cases = (
            ({}, "non-empty"),
            ({"a": 0.4, "b": 0.4}, "sum to 1"),
            ({"a": -0.1, "b": 1.1}, ">= 0.0"),
            ({"a": math.nan, "b": 1.0}, "finite"),
        )
        for value, message in cases:
            with self.subTest(value=value), self.assertRaisesRegex(ValueError, message):
                present_probability_map(value)

    def test_recursive_presentation_uses_map_semantics_without_retyping_counts(self):
        displayed = present_value(
            {
                "baseline_weights": {"a": 1 / 3, "b": 1 / 3, "c": 1 / 3},
                "target_weights": {"a": 0.8, "b": 0.1, "c": 0.1},
                "counts": {"a": 2, "b": 1},
                "latency": 1.0,
            }
        )
        self.assertEqual(
            sum(
                Decimal(str(value))
                for value in displayed["baseline_weights"].values()
            ),
            Decimal("1"),
        )
        self.assertEqual(
            sum(
                Decimal(str(value))
                for value in displayed["target_weights"].values()
            ),
            Decimal("1"),
        )
        self.assertTrue(all(type(value) is int for value in displayed["counts"].values()))
        self.assertIsInstance(displayed["latency"], float)

        raw_weights = present_value({"weights": {"a": 2, "b": 3}})
        self.assertEqual(raw_weights, {"weights": {"a": 2, "b": 3}})

    def test_presentation_does_not_change_query_semantics_or_samples(self):
        templates = tuple(QueryTemplate(value) for value in ("a", "b", "c", "d"))
        result = apply_query_workload_mix_drift(
            templates,
            baseline_weights={value: 1 for value in "abcd"},
            target_weights={"a": 0.8, "b": 0.2 / 3, "c": 0.2 / 3, "d": 0.2 / 3},
            sample_size=12,
            seed=42,
        )
        self.assertEqual(
            result.semantic_hash,
            "866e625dd3635dffaaa34cee01119c567b5dc6338150c2127c32dbe5b4fd0983",
        )
        self.assertEqual(
            [item.template_id for item in result.drifted],
            ["a", "a", "a", "c", "a", "a", "b", "c", "a", "a", "a", "a"],
        )
        self.assertEqual(result.target_weights["b"], 0.06666666666666667)
        self.assertEqual(
            present_probability_map(result.target_weights)["b"], 0.066667
        )

if __name__ == "__main__":
    unittest.main()
