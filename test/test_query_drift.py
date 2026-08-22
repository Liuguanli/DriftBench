from __future__ import annotations

import random
import unittest

import numpy as np

import driftbench.api as public_api
from driftbench.api import (
    QueryTemplate,
    QueryWorkloadMixResult,
    apply_query_workload_mix_drift,
)


def _templates() -> tuple[QueryTemplate, ...]:
    return (
        QueryTemplate("a", "SELECT 1", {"family": "read", "ordinal": 1}),
        QueryTemplate("b", "SELECT 2", {"family": "read", "ordinal": 2}),
        QueryTemplate("c", "SELECT 3", {"family": "write", "ordinal": 3}),
    )


def _sample_ids(values: tuple[QueryTemplate, ...]) -> tuple[str, ...]:
    return tuple(value.template_id for value in values)


class QueryDriftPublicApiTests(unittest.TestCase):
    def test_public_api_exports_are_usable(self) -> None:
        self.assertIs(public_api.QueryTemplate, QueryTemplate)
        self.assertIs(
            public_api.apply_query_workload_mix_drift,
            apply_query_workload_mix_drift,
        )
        self.assertIn("QueryTemplate", public_api.__all__)
        self.assertIn("QueryWorkloadMixResult", public_api.__all__)
        self.assertIn("apply_query_workload_mix_drift", public_api.__all__)

        result = apply_query_workload_mix_drift(
            (QueryTemplate("read", None), QueryTemplate("write", "SELECT 1")),
            target_weights={"read": 3, "write": 1},
            sample_size=8,
            seed=7,
        )
        self.assertIsInstance(result, QueryWorkloadMixResult)
        self.assertEqual(result.sample_size, 8)
        self.assertEqual(len(result.baseline), 8)
        self.assertEqual(len(result.drifted), 8)

    def test_none_sql_is_a_supported_template_value(self) -> None:
        template = QueryTemplate(
            "ReadRecord", None, {"artifact": "operation", "profile": "A"}
        )
        result = apply_query_workload_mix_drift(
            (template,),
            baseline_weights={"ReadRecord": 1},
            target_weights={"ReadRecord": 1},
            sample_size=4,
            seed=42,
        )
        self.assertIsNone(result.baseline[0].sql)
        self.assertEqual(_sample_ids(result.drifted), ("ReadRecord",) * 4)


class QueryDriftValidationTests(unittest.TestCase):
    def test_weight_maps_are_strict_and_finite(self) -> None:
        valid = {"a": 1.0, "b": 2.0, "c": 3.0}
        invalid_maps = {
            "missing ID": {"a": 1.0, "b": 2.0},
            "unknown ID": {"a": 1.0, "b": 2.0, "c": 3.0, "d": 4.0},
            "negative": {"a": -1.0, "b": 2.0, "c": 3.0},
            "NaN": {"a": float("nan"), "b": 2.0, "c": 3.0},
            "positive infinity": {"a": float("inf"), "b": 2.0, "c": 3.0},
            "negative infinity": {"a": float("-inf"), "b": 2.0, "c": 3.0},
            "all zero": {"a": 0.0, "b": 0.0, "c": 0.0},
        }

        for map_name in ("baseline_weights", "target_weights"):
            for case, invalid in invalid_maps.items():
                kwargs = {
                    "baseline_weights": valid,
                    "target_weights": valid,
                    "sample_size": 5,
                    "seed": 1,
                }
                kwargs[map_name] = invalid
                with self.subTest(map_name=map_name, case=case):
                    with self.assertRaises((TypeError, ValueError)):
                        apply_query_workload_mix_drift(_templates(), **kwargs)

    def test_empty_and_duplicate_templates_are_rejected(self) -> None:
        with self.assertRaises(ValueError):
            apply_query_workload_mix_drift((), target_weights={})

        duplicates = (QueryTemplate("same", "SELECT 1"), QueryTemplate(" same "))
        with self.assertRaises(ValueError):
            apply_query_workload_mix_drift(
                duplicates,
                target_weights={"same": 1.0},
            )

    def test_invalid_sample_size_and_seed_types_are_rejected(self) -> None:
        templates = _templates()
        weights = {"a": 1.0, "b": 1.0, "c": 1.0}

        for value in (0, -1, 1.5, True, None, "10"):
            with self.subTest(sample_size=value):
                with self.assertRaises((TypeError, ValueError)):
                    apply_query_workload_mix_drift(
                        templates,
                        target_weights=weights,
                        sample_size=value,  # type: ignore[arg-type]
                    )

        for value in (True, 1.5, None, "42"):
            with self.subTest(seed=value):
                with self.assertRaises((TypeError, ValueError)):
                    apply_query_workload_mix_drift(
                        templates,
                        target_weights=weights,
                        seed=value,  # type: ignore[arg-type]
                    )


class QueryDriftReproducibilityTests(unittest.TestCase):
    def test_same_seed_produces_an_identical_result(self) -> None:
        kwargs = {
            "baseline_weights": {"a": 2, "b": 3, "c": 5},
            "target_weights": {"a": 7, "b": 2, "c": 1},
            "sample_size": 250,
            "seed": 12345,
        }
        first = apply_query_workload_mix_drift(_templates(), **kwargs)
        second = apply_query_workload_mix_drift(_templates(), **kwargs)
        self.assertEqual(first, second)

    def test_baseline_and_drifted_samples_use_independent_streams(self) -> None:
        templates = _templates()
        uniform = {"a": 1, "b": 1, "c": 1}
        all_a = {"a": 1, "b": 0, "c": 0}
        all_b = {"a": 0, "b": 1, "c": 0}
        all_c = {"a": 0, "b": 0, "c": 1}

        reference = apply_query_workload_mix_drift(
            templates,
            baseline_weights=uniform,
            target_weights=all_a,
            sample_size=128,
            seed=91,
        )
        changed_target = apply_query_workload_mix_drift(
            templates,
            baseline_weights=uniform,
            target_weights=all_c,
            sample_size=128,
            seed=91,
        )
        changed_baseline = apply_query_workload_mix_drift(
            templates,
            baseline_weights=all_b,
            target_weights=all_a,
            sample_size=128,
            seed=91,
        )

        self.assertEqual(
            _sample_ids(reference.baseline),
            _sample_ids(changed_target.baseline),
        )
        self.assertEqual(
            _sample_ids(reference.drifted),
            _sample_ids(changed_baseline.drifted),
        )
        self.assertNotEqual(
            _sample_ids(reference.drifted),
            _sample_ids(changed_target.drifted),
        )
        self.assertNotEqual(
            _sample_ids(reference.baseline),
            _sample_ids(changed_baseline.baseline),
        )

    def test_python_and_numpy_global_rng_states_are_unchanged(self) -> None:
        original_python_state = random.getstate()
        original_numpy_state = np.random.get_state()
        try:
            random.seed(8675309)
            np.random.seed(24680)
            python_before = random.getstate()
            numpy_before = np.random.get_state()

            apply_query_workload_mix_drift(
                _templates(),
                baseline_weights={"a": 1, "b": 2, "c": 3},
                target_weights={"a": 3, "b": 2, "c": 1},
                sample_size=50,
                seed=42,
            )

            python_after = random.getstate()
            numpy_after = np.random.get_state()
            self.assertEqual(python_before, python_after)
            self.assertEqual(numpy_before[0], numpy_after[0])
            np.testing.assert_array_equal(numpy_before[1], numpy_after[1])
            self.assertEqual(numpy_before[2:], numpy_after[2:])
        finally:
            random.setstate(original_python_state)
            np.random.set_state(original_numpy_state)


class QueryDriftSemanticHashTests(unittest.TestCase):
    def _result(
        self,
        *,
        templates: tuple[QueryTemplate, ...] | None = None,
        baseline_weights: dict[str, float] | None = None,
        target_weights: dict[str, float] | None = None,
        sample_size: int = 30,
        seed: int = 12,
    ):
        return apply_query_workload_mix_drift(
            templates or _templates(),
            baseline_weights=baseline_weights or {"a": 1, "b": 2, "c": 3},
            target_weights=target_weights or {"a": 3, "b": 2, "c": 1},
            sample_size=sample_size,
            seed=seed,
        )

    def test_hash_is_stable_and_weight_mapping_order_is_irrelevant(self) -> None:
        first = self._result(
            baseline_weights={"a": 1, "b": 2, "c": 3},
            target_weights={"a": 3, "b": 2, "c": 1},
        )
        reordered = self._result(
            baseline_weights={"c": 3, "a": 1, "b": 2},
            target_weights={"b": 2, "c": 1, "a": 3},
        )
        repeated = self._result()
        self.assertEqual(first.semantic_hash, reordered.semantic_hash)
        self.assertEqual(first.semantic_hash, repeated.semantic_hash)

    def test_hash_tracks_every_semantic_input(self) -> None:
        reference = self._result().semantic_hash
        variants = {
            "seed": self._result(seed=13),
            "sample size": self._result(sample_size=31),
            "template SQL": self._result(
                templates=(
                    QueryTemplate("a", "SELECT changed", {"family": "read", "ordinal": 1}),
                    _templates()[1],
                    _templates()[2],
                )
            ),
            "template metadata": self._result(
                templates=(
                    QueryTemplate("a", "SELECT 1", {"family": "changed", "ordinal": 1}),
                    _templates()[1],
                    _templates()[2],
                )
            ),
            "target weights": self._result(
                target_weights={"a": 1, "b": 1, "c": 8}
            ),
        }
        for name, variant in variants.items():
            with self.subTest(name=name):
                self.assertNotEqual(reference, variant.semantic_hash)


if __name__ == "__main__":
    unittest.main()
