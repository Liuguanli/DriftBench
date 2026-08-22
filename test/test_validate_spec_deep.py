import json
import os
import tempfile
import unittest
from pathlib import Path

import yaml

from driftbench.spec.preflight import deep_validate_spec_file


def _codes(report):
    return {issue.code for issue in report.issues}


class DeepSpecValidationTests(unittest.TestCase):
    def _write_spec(self, root: Path, spec: dict, name: str = "spec.yaml") -> Path:
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(yaml.safe_dump(spec, sort_keys=False), encoding="utf-8")
        return path

    @staticmethod
    def _base_spec(family: str, category: str, subtype: str) -> dict:
        return {
            "spec_version": 1,
            "pattern_id": "deep-test",
            "seed": 42,
            "type": {
                "family": family,
                "category": category,
                "subtype": subtype,
            },
        }

    def test_all_registered_handler_types_have_pure_preflight_contracts(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            (root / "input.csv").write_text("id,value\n1,10\n", encoding="utf-8")
            (root / "schema.json").write_text(
                json.dumps({"tables": {"items": {"columns": {"id": {}}}}}),
                encoding="utf-8",
            )
            templates = root / "templates"
            templates.mkdir()
            (templates / "1.sql").write_text("select :1;\n", encoding="utf-8")

            single = self._base_spec("data", "drift", "single_table")
            single.update(
                {
                    "data_source": {"kind": "csv", "path": "input.csv"},
                    "variables": {
                        "base_table": "items",
                        "drifts": [
                            {
                                "drift_type": "vary_cardinality",
                                "scale": 2,
                                "output_path": "single-out.csv",
                            }
                        ],
                    },
                }
            )

            multi = self._base_spec("data", "drift", "multi_table")
            multi.update(
                {
                    "data_source": {"kind": "multi_table"},
                    "variables": {
                        "tables": [
                            {
                                "name": "items",
                                "path": "input.csv",
                                "key_column": "id",
                                "output_path": "multi-out.csv",
                            }
                        ],
                        "relationships": [],
                        "drift_steps": [
                            {
                                "op": "delete_keys",
                                "target": "items",
                                "key_column": "id",
                                "count": 1,
                            }
                        ],
                    },
                }
            )

            selection = self._base_spec(
                "workload", "templates", "selection_payload"
            )
            selection.update(
                {
                    "data_source": {"kind": "csv", "path": "input.csv"},
                    "variables": {
                        "base_table": "items",
                        "schema_path": "schema.json",
                        "runs": [
                            {"name": "base", "output_path": "templates-out.json"}
                        ],
                    },
                }
            )

            tpch = self._base_spec("workload", "sql_templates", "tpch")
            tpch.update(
                {
                    "variables": {
                        "template_dir": "templates",
                        "query_ids": ["1"],
                        "params": {
                            "1": [{"type": "int_range", "min": 1, "max": 10}]
                        },
                        "query_runs": [
                            {
                                "param_mode": "custom",
                                "queries_per_template": 1,
                                "output_path": "tpch-out.sql",
                            }
                        ],
                    }
                }
            )

            keylist = self._base_spec("workload", "keylist", "single_table")
            keylist.update(
                {
                    "data_source": {"kind": "csv", "path": "input.csv"},
                    "variables": {
                        "key_column": "id",
                        "query_runs": [
                            {
                                "distribution": "uniform",
                                "count": 10,
                                "output_path": "keys.bin",
                            }
                        ],
                    },
                }
            )

            mix = self._base_spec("workload", "drift", "template_mix")
            mix.update(
                {
                    "data_source": {
                        "kind": "benchmark_adapter",
                        "benchmark": "tpch",
                    },
                    "metadata": {"benchmark": "tpch"},
                    "variables": {
                        "template_ids": ["q1", "q2"],
                        "baseline": {"mode": "uniform"},
                        "target": {"weights": {"q1": 3, "q2": 1}},
                        "sample_size": 10,
                        "output_path": "mix.json",
                    },
                }
            )

            specs = [single, multi, selection, tpch, keylist, mix]
            for index, spec in enumerate(specs):
                with self.subTest(type=spec["type"]):
                    path = self._write_spec(root, spec, f"spec-{index}.yaml")
                    report = deep_validate_spec_file(path, working_dir=root)
                    self.assertTrue(report.valid, msg=report.as_dict())
                    self.assertTrue(report.locally_ready)
                    self.assertNotIn("preflight_not_supported", _codes(report))

    def test_missing_required_path_and_missing_literal_input_are_distinct(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            spec = self._base_spec("data", "drift", "single_table")
            spec.update(
                {
                    "data_source": {"kind": "csv"},
                    "variables": {
                        "base_table": "items",
                        "drifts": [
                            {
                                "drift_type": "vary_cardinality",
                                "scale": 2,
                                "output_path": "out.csv",
                            }
                        ],
                    },
                }
            )
            report = deep_validate_spec_file(
                self._write_spec(root, spec), working_dir=root
            )
            self.assertIn("required_field_missing", _codes(report))
            self.assertTrue(
                any(issue.field == "data_source.path" for issue in report.issues)
            )

            spec["data_source"]["path"] = "missing.csv"
            report = deep_validate_spec_file(
                self._write_spec(root, spec), working_dir=root
            )
            self.assertIn("input_not_found", _codes(report))
            self.assertNotIn("required_field_missing", _codes(report))

    def test_unknown_handler_and_benchmark_have_stable_codes(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            unknown_handler = self._base_spec("data", "drift", "unknown")
            unknown_handler["variables"] = {}
            report = deep_validate_spec_file(
                self._write_spec(root, unknown_handler), working_dir=root
            )
            self.assertEqual(_codes(report), {"handler_not_registered"})

            unknown_benchmark = self._base_spec(
                "workload", "drift", "template_mix"
            )
            unknown_benchmark.update(
                {
                    "data_source": {
                        "kind": "benchmark_adapter",
                        "benchmark": "not-a-benchmark",
                    },
                    "variables": {
                        "template_ids": ["q1"],
                        "baseline": {"mode": "uniform"},
                        "target": {"mode": "uniform"},
                        "sample_size": 2,
                        "output_path": "out.json",
                    },
                }
            )
            report = deep_validate_spec_file(
                self._write_spec(root, unknown_benchmark), working_dir=root
            )
            self.assertIn("benchmark_unsupported", _codes(report))

    def test_benchmark_mismatch_duplicate_output_and_input_collision_fail(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            (root / "input.csv").write_text("id\n1\n", encoding="utf-8")
            spec = self._base_spec("data", "drift", "single_table")
            spec.update(
                {
                    "metadata": {"benchmark": "tpch"},
                    "data_source": {
                        "kind": "csv",
                        "path": "input.csv",
                        "benchmark": "job",
                    },
                    "variables": {
                        "base_table": "items",
                        "drifts": [
                            {
                                "drift_type": "vary_cardinality",
                                "scale": 2,
                                "output_path": "input.csv",
                            },
                            {
                                "drift_type": "vary_cardinality",
                                "scale": 3,
                                "output_path": "input.csv",
                            },
                        ],
                    },
                }
            )
            report = deep_validate_spec_file(
                self._write_spec(root, spec), working_dir=root
            )
            self.assertFalse(report.valid)
            self.assertTrue(
                {"benchmark_mismatch", "duplicate_output", "input_output_collision"}
                <= _codes(report)
            )

    def test_exact_and_partial_placeholders_are_never_treated_as_paths(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            spec = self._base_spec("data", "drift", "single_table")
            spec.update(
                {
                    "data_source": {"kind": "csv", "path": "${INPUT}"},
                    "variables": {
                        "base_table": "items",
                        "drifts": [
                            {
                                "drift_type": "vary_cardinality",
                                "scale": 2,
                                "output_path": "results/${OUTPUT}.csv",
                            }
                        ],
                    },
                }
            )
            report = deep_validate_spec_file(
                self._write_spec(root, spec), working_dir=root
            )
            unresolved = [
                issue for issue in report.issues if issue.code == "unresolved_placeholder"
            ]
            self.assertEqual(len(unresolved), 2)
            self.assertFalse(report.locally_ready)
            self.assertNotIn("input_not_found", _codes(report))

    def test_relative_resources_resolve_from_working_directory_not_spec_directory(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            (root / "input.csv").write_text("id\n1\n", encoding="utf-8")
            spec = self._base_spec("data", "drift", "single_table")
            spec.update(
                {
                    "data_source": {"kind": "csv", "path": "input.csv"},
                    "variables": {
                        "base_table": "items",
                        "drifts": [
                            {
                                "drift_type": "vary_cardinality",
                                "scale": 2,
                                "output_path": "out.csv",
                            }
                        ],
                    },
                }
            )
            spec_path = self._write_spec(root, spec, "nested/spec.yaml")
            report = deep_validate_spec_file(spec_path, working_dir=root)
            self.assertTrue(report.valid, msg=report.as_dict())

    def test_existing_output_is_warning_and_issue_order_is_deterministic(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            (root / "input.csv").write_text("id\n1\n", encoding="utf-8")
            (root / "out.csv").write_text("old\n", encoding="utf-8")
            spec = self._base_spec("data", "drift", "single_table")
            spec.update(
                {
                    "data_source": {"kind": "csv", "path": "input.csv"},
                    "variables": {
                        "base_table": "items",
                        "drifts": [
                            {
                                "drift_type": "vary_cardinality",
                                "scale": 2,
                                "output_path": "out.csv",
                            }
                        ],
                    },
                }
            )
            path = self._write_spec(root, spec)
            first = deep_validate_spec_file(path, working_dir=root)
            second = deep_validate_spec_file(path, working_dir=root)
            self.assertTrue(first.valid)
            self.assertIn("would_overwrite", _codes(first))
            self.assertEqual(first.as_dict(), second.as_dict())

    def test_outlier_extreme_scale_must_be_positive(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            (root / "input.csv").write_text("id\n1\n", encoding="utf-8")
            spec = self._base_spec("data", "drift", "single_table")
            spec.update(
                {
                    "data_source": {"kind": "csv", "path": "input.csv"},
                    "variables": {
                        "base_table": "items",
                        "drifts": [
                            {
                                "drift_type": "outlier_injection",
                                "column": "id",
                                "extreme_scale": -1,
                                "output_path": "out.csv",
                            }
                        ],
                    },
                }
            )
            report = deep_validate_spec_file(
                self._write_spec(root, spec), working_dir=root
            )
            self.assertFalse(report.valid)
            self.assertTrue(
                any(
                    issue.code == "field_value_invalid"
                    and issue.field.endswith(".extreme_scale")
                    for issue in report.issues
                )
            )

    def test_selection_controls_use_effective_defaults_and_run_overrides(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            (root / "schema.json").write_text(
                json.dumps({"tables": {"items": {"columns": {"id": {}}}}}),
                encoding="utf-8",
            )
            controls = (
                "num_templates",
                "max_predicates",
                "max_payload_columns",
                "join_count",
            )
            for control in controls:
                with self.subTest(control=control):
                    spec = self._base_spec(
                        "workload", "templates", "selection_payload"
                    )
                    spec.update(
                        {
                            "data_source": {"kind": "csv"},
                            "variables": {
                                "base_table": "items",
                                "schema_path": "schema.json",
                                "defaults": {control: -1},
                                "runs": [
                                    {
                                        "name": "base",
                                        "output_path": f"{control}.json",
                                    }
                                ],
                            },
                        }
                    )
                    report = deep_validate_spec_file(
                        self._write_spec(root, spec), working_dir=root
                    )
                    self.assertFalse(report.valid)
                    self.assertTrue(
                        any(
                            issue.field == f"variables.defaults.{control}"
                            for issue in report.issues
                        ),
                        msg=report.as_dict(),
                    )

                    spec["variables"]["runs"][0][control] = 1
                    report = deep_validate_spec_file(
                        self._write_spec(root, spec), working_dir=root
                    )
                    self.assertTrue(report.valid, msg=report.as_dict())

    def test_schema_json_requires_one_of_the_runtime_supported_shapes(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            spec = self._base_spec("workload", "templates", "selection_payload")
            spec.update(
                {
                    "data_source": {"kind": "csv"},
                    "variables": {
                        "base_table": "items",
                        "schema_path": "schema.json",
                        "runs": [{"name": "base", "output_path": "out.json"}],
                    },
                }
            )
            path = self._write_spec(root, spec)

            shapes = (
                {"tables": {"items": {"columns": {"id": {}}}}},
                {"source": {"columns": {"id": {}}}},
                {"table": "items", "columns": {"id": {}}},
            )
            for shape in shapes:
                with self.subTest(shape=shape):
                    (root / "schema.json").write_text(
                        json.dumps(shape), encoding="utf-8"
                    )
                    report = deep_validate_spec_file(path, working_dir=root)
                    self.assertTrue(report.valid, msg=report.as_dict())

            (root / "schema.json").write_text("{}", encoding="utf-8")
            report = deep_validate_spec_file(path, working_dir=root)
            self.assertFalse(report.valid)
            self.assertTrue(
                any(
                    issue.code == "config_invalid"
                    and issue.field == "variables.schema_path"
                    for issue in report.issues
                )
            )

    def test_handler_components_match_runtime_verbatim(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            (root / "input.csv").write_text("id\n1\n", encoding="utf-8")
            spec = self._base_spec(" data ", "drift", "single_table")
            spec.update(
                {
                    "data_source": {"kind": "csv", "path": "input.csv"},
                    "variables": {
                        "base_table": "items",
                        "drifts": [
                            {
                                "drift_type": "vary_cardinality",
                                "scale": 2,
                                "output_path": "out.csv",
                            }
                        ],
                    },
                }
            )
            report = deep_validate_spec_file(
                self._write_spec(root, spec), working_dir=root
            )
            self.assertFalse(report.valid)
            self.assertIn("handler_not_registered", _codes(report))

    def test_hard_link_input_output_alias_is_a_collision(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source = root / "input.csv"
            alias = root / "alias.csv"
            source.write_text("id\n1\n", encoding="utf-8")
            try:
                os.link(source, alias)
            except OSError as exc:
                self.skipTest(f"hard links unavailable: {exc}")
            spec = self._base_spec("data", "drift", "single_table")
            spec.update(
                {
                    "data_source": {"kind": "csv", "path": "input.csv"},
                    "variables": {
                        "base_table": "items",
                        "drifts": [
                            {
                                "drift_type": "vary_cardinality",
                                "scale": 2,
                                "output_path": "alias.csv",
                            }
                        ],
                    },
                }
            )
            report = deep_validate_spec_file(
                self._write_spec(root, spec), working_dir=root
            )
            self.assertFalse(report.valid)
            self.assertIn("input_output_collision", _codes(report))

    def test_template_mix_errors_do_not_echo_spec_values(self) -> None:
        secret = "super-secret-template-id"
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            spec = self._base_spec("workload", "drift", "template_mix")
            spec.update(
                {
                    "variables": {
                        "template_ids": ["q1"],
                        "baseline": {"mode": "uniform"},
                        "target": {"weights": {secret: 1}},
                        "sample_size": 2,
                        "output_path": "mix.json",
                    }
                }
            )
            report = deep_validate_spec_file(
                self._write_spec(root, spec), working_dir=root
            )
            self.assertFalse(report.valid)
            self.assertNotIn(secret, json.dumps(report.as_dict()))

    def test_single_table_drift_rejects_unknown_runtime_parameters(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            (root / "input.csv").write_text("id\n1\n", encoding="utf-8")
            spec = self._base_spec("data", "drift", "single_table")
            spec.update(
                {
                    "data_source": {"kind": "csv", "path": "input.csv"},
                    "variables": {
                        "base_table": "items",
                        "drifts": [
                            {
                                "drift_type": "vary_cardinality",
                                "scale": 2,
                                "misspelled_parameter": 1,
                                "output_path": "out.csv",
                            }
                        ],
                    },
                }
            )
            report = deep_validate_spec_file(
                self._write_spec(root, spec), working_dir=root
            )
            self.assertFalse(report.valid)
            self.assertIn("parameter_unsupported", _codes(report))

    def test_external_runtime_checks_are_warnings_not_readiness_claims(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            spec = self._base_spec("workload", "drift", "template_mix")
            spec.update(
                {
                    "data_source": {
                        "kind": "benchmark_adapter",
                        "benchmark": "pgbench",
                    },
                    "metadata": {"benchmark": "pgbench"},
                    "variables": {
                        "template_ids": ["q1"],
                        "baseline": {"mode": "uniform"},
                        "target": {"mode": "uniform"},
                        "sample_size": 2,
                        "output_path": "mix.json",
                    },
                }
            )
            report = deep_validate_spec_file(
                self._write_spec(root, spec), working_dir=root
            )
            self.assertTrue(report.valid)
            warnings = [
                issue
                for issue in report.issues
                if issue.code == "external_not_checked"
            ]
            self.assertEqual(len(warnings), 1)
            self.assertEqual(warnings[0].severity, "warning")


if __name__ == "__main__":
    unittest.main()
