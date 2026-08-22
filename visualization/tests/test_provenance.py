from __future__ import annotations

import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from unittest import mock

from visualization.benchmarks import get_benchmark, get_scenario_entry
from visualization.provenance import (
    configuration_hash_for_spec,
    manifest_semantic_hash,
)
from visualization.specs import load_canonical_spec


class ProvenanceContractTests(unittest.TestCase):
    def test_title_rationale_and_limitations_each_invalidate_configuration(self) -> None:
        analysis = {"schema": "analysis-fixture", "config": {"sample": 1000}}
        render = {"schema": "render-fixture", "style": 1}
        definition = get_benchmark("tpch")
        with tempfile.TemporaryDirectory() as temporary:
            spec = load_canonical_spec(
                Path(temporary),
                kind="data",
                benchmark="tpch",
                scenario="price_outliers",
            )
            kwargs = {"definition": definition, "analysis": analysis, "render": render}
            baseline = configuration_hash_for_spec(spec, **kwargs)

            changed_title = configuration_hash_for_spec(
                spec,
                definition=replace(definition, title=definition.title + " revised"),
                analysis=analysis,
                render=render,
            )
            changed_limitations = configuration_hash_for_spec(
                spec,
                definition=replace(
                    definition, limitations=definition.limitations + " Revised."
                ),
                analysis=analysis,
                render=render,
            )
            entry = dict(get_scenario_entry("data", "tpch", "price_outliers"))
            entry["rationale"] = str(entry["rationale"]) + " Revised."
            with mock.patch(
                "visualization.provenance.get_scenario_entry", return_value=entry
            ):
                changed_rationale = configuration_hash_for_spec(spec, **kwargs)

        self.assertEqual(
            len({baseline, changed_title, changed_limitations, changed_rationale}), 4
        )

    def test_manifest_semantic_hash_excludes_only_timestamp_and_self_hash(self) -> None:
        first = {
            "benchmark": "tpch",
            "generated_at": "2026-08-19T00:00:00Z",
            "semantic_sha256": "0" * 64,
            "effect": {"passed": True},
        }
        second = {
            **first,
            "generated_at": "2026-08-19T00:00:01Z",
            "semantic_sha256": "1" * 64,
        }
        self.assertEqual(manifest_semantic_hash(first), manifest_semantic_hash(second))
        second["effect"] = {"passed": False}
        self.assertNotEqual(manifest_semantic_hash(first), manifest_semantic_hash(second))


if __name__ == "__main__":
    unittest.main()
