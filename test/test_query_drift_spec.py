from __future__ import annotations

import copy
import json
import tempfile
import unittest
from pathlib import Path

import yaml

import driftbench.api as public_api
from driftbench.api import (
    QueryTemplate,
    QueryTemplateMixSpec,
    QueryWorkloadMixResult,
    execute_query_template_mix_spec,
    parse_query_template_mix_spec,
    run_spec,
)


def _spec(output_path="${OUTPUT_PATH}") -> dict:
    return {
        "pattern_id": "query-template-mix-test",
        "seed": 42,
        "type": {
            "family": "workload",
            "category": "drift",
            "subtype": "template_mix",
        },
        "variables": {
            "sample_size": 64,
            "template_ids": ["a", "b", "c"],
            "baseline": {"mode": "uniform"},
            "target": {
                "focus": {"a": 0.8},
                "remaining_total": 0.2,
            },
            "output_path": output_path,
        },
    }


def _templates() -> tuple[QueryTemplate, ...]:
    return (
        QueryTemplate("a", "SELECT 1", {"ordinal": 1}),
        QueryTemplate("b", "SELECT 2", {"ordinal": 2}),
        QueryTemplate("c", None, {"artifact": "operation"}),
    )


class QueryDriftSpecExecutionTests(unittest.TestCase):
    def test_public_run_spec_executes_handler_and_writes_bound_json(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            spec_path = root / "mix.driftspec.yaml"
            spec_path.write_text(yaml.safe_dump(_spec(), sort_keys=False), encoding="utf-8")
            output_path = root / "mix.json"

            result = run_spec(
                spec_path,
                bindings={"OUTPUT_PATH": output_path},
                runtime_inputs={"query_templates": tuple(reversed(_templates()))},
            )
            self.assertIsInstance(result, QueryWorkloadMixResult)
            self.assertEqual(result.seed, 42)
            self.assertEqual(result.sample_size, 64)
            self.assertAlmostEqual(result.target_weights["a"], 0.8)
            self.assertAlmostEqual(result.target_weights["b"], 0.1)
            self.assertAlmostEqual(result.target_weights["c"], 0.1)

            payload = json.loads(output_path.read_text(encoding="utf-8"))
            self.assertEqual(payload["schema"], "driftbench.query-template-mix-result/v1")
            self.assertEqual(payload["semantic_hash"], result.semantic_hash)
            self.assertEqual(
                [item["template_id"] for item in payload["templates"]],
                ["a", "b", "c"],
            )
            self.assertEqual(payload["templates"][0]["sql"], "SELECT 1")
            self.assertEqual(len(payload["baseline"]), 64)
            self.assertEqual(len(payload["drifted"]), 64)

            first_bytes = output_path.read_bytes()
            repeated = run_spec(
                spec_path,
                bindings={"OUTPUT_PATH": output_path},
                runtime_inputs={"query_templates": _templates()},
            )
            self.assertEqual(repeated, result)
            self.assertEqual(output_path.read_bytes(), first_bytes)

    def test_id_only_spec_is_executable_without_runtime_templates(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            spec = _spec(Path(tmp) / "id-only.json")
            result = execute_query_template_mix_spec(spec)
            self.assertTrue(all(template.sql is None for template in result.baseline))
            self.assertTrue((Path(tmp) / "id-only.json").is_file())

    def test_parser_supports_explicit_weight_maps(self) -> None:
        spec = _spec("result.json")
        spec["variables"]["baseline"] = {
            "weights": {"a": 2, "b": 3, "c": 5}
        }
        spec["variables"]["target"] = {
            "weights": {"a": 6, "b": 3, "c": 1}
        }
        parsed = parse_query_template_mix_spec(spec)
        self.assertIsInstance(parsed, QueryTemplateMixSpec)
        self.assertEqual(parsed.baseline_weights, {"a": 0.2, "b": 0.3, "c": 0.5})
        self.assertEqual(parsed.target_weights, {"a": 0.6, "b": 0.3, "c": 0.1})

    def test_parser_requires_spec_owned_seed_and_sample_size(self) -> None:
        cases = {}
        missing_seed = _spec("result.json")
        del missing_seed["seed"]
        cases["seed"] = missing_seed
        missing_sample = _spec("result.json")
        del missing_sample["variables"]["sample_size"]
        cases["sample_size"] = missing_sample

        for field, spec in cases.items():
            with self.subTest(field=field):
                with self.assertRaisesRegex(ValueError, field):
                    parse_query_template_mix_spec(spec)

    def test_runtime_templates_and_runtime_keys_are_strict(self) -> None:
        spec = _spec("result.json")
        with self.assertRaisesRegex(ValueError, "unused runtime inputs.*seed"):
            parse_query_template_mix_spec(spec, runtime_inputs={"seed": 9})
        with self.assertRaisesRegex(ValueError, "missing IDs.*c"):
            parse_query_template_mix_spec(
                spec,
                runtime_inputs={"query_templates": _templates()[:2]},
            )

    def test_weight_config_forms_are_unambiguous(self) -> None:
        spec = _spec("result.json")
        invalid = copy.deepcopy(spec)
        invalid["variables"]["target"] = {
            "mode": "uniform",
            "weights": {"a": 1, "b": 1, "c": 1},
        }
        with self.assertRaisesRegex(ValueError, "exactly one"):
            parse_query_template_mix_spec(invalid)

        missing_remaining = copy.deepcopy(spec)
        missing_remaining["variables"]["target"] = {"focus": {"a": 1}}
        with self.assertRaisesRegex(ValueError, "remaining_total"):
            parse_query_template_mix_spec(missing_remaining)

    def test_public_api_exports_query_spec_surface(self) -> None:
        for name in (
            "QueryTemplateMixSpec",
            "execute_query_template_mix_spec",
            "parse_query_template_mix_spec",
            "run_spec",
        ):
            self.assertIn(name, public_api.__all__)


if __name__ == "__main__":
    unittest.main()
