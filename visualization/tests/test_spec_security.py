from __future__ import annotations

import copy
import math
import unittest
from importlib import resources
from typing import Any

import yaml

from visualization.benchmarks import get_scenario_entry
from visualization.specs import expected_artifact_keys, validate_canonical_spec


def _payload(kind: str, benchmark: str, scenario: str) -> dict[str, Any]:
    relative = str(get_scenario_entry(kind, benchmark, scenario)["spec"])
    source = resources.files("visualization").joinpath(*relative.split("/"))
    payload = yaml.safe_load(source.read_text(encoding="utf-8"))
    assert isinstance(payload, dict)
    return payload


def _node(payload: Any, path: tuple[str | int, ...]) -> Any:
    current = payload
    for part in path:
        current = current[part]
    return current


class CanonicalSpecSecurityTests(unittest.TestCase):
    def assert_rejected(
        self,
        payload: dict[str, Any],
        *,
        kind: str = "data",
        benchmark: str = "tpch",
        scenario: str = "price_outliers",
    ) -> None:
        with self.assertRaises(ValueError):
            validate_canonical_spec(
                payload,
                kind=kind,
                benchmark=benchmark,
                scenario=scenario,
            )

    def test_all_registered_specs_pass_the_strict_validator(self) -> None:
        keys = expected_artifact_keys()
        self.assertEqual(len(keys), 40)
        for kind, benchmark, scenario in keys:
            with self.subTest(kind=kind, benchmark=benchmark, scenario=scenario):
                result = validate_canonical_spec(
                    _payload(kind, benchmark, scenario),
                    kind=kind,
                    benchmark=benchmark,
                    scenario=scenario,
                )
                expected = (
                    ("workload", "drift", "template_mix")
                    if kind == "query"
                    else (
                        "data",
                        "drift",
                        "multi_table"
                        if benchmark == "job" and "deletion" in scenario
                        else "single_table",
                    )
                )
                self.assertEqual(result, expected)

    def test_extra_and_missing_keys_fail_at_every_single_table_level(self) -> None:
        base = _payload("data", "tpch", "price_outliers")
        nodes = (
            ((), "seed"),
            (("type",), "family"),
            (("metadata",), "benchmark"),
            (("metadata", "comparison"), "column"),
            (("effect_policy",), "mode"),
            (("effect_policy", "assertions", 0), "metric"),
            (("data_source",), "kind"),
            (("data_source", "schema_extractor"), "source_type"),
            (("variables",), "base_table"),
            (("variables", "drifts", 0), "column"),
        )
        for path, required_key in nodes:
            with self.subTest(path=path, mutation="missing"):
                candidate = copy.deepcopy(base)
                del _node(candidate, path)[required_key]
                self.assert_rejected(candidate)
            with self.subTest(path=path, mutation="extra"):
                candidate = copy.deepcopy(base)
                _node(candidate, path)["unexpected"] = "value"
                self.assert_rejected(candidate)

    def test_extra_and_missing_keys_fail_at_query_and_job_levels(self) -> None:
        cases = (
            (
                "query",
                "tpch",
                "hotset_concentration",
                (
                    (("metadata",), "scenario"),
                    (("data_source",), "benchmark"),
                    (("variables",), "sample_size"),
                    (("variables", "baseline"), "mode"),
                    (("variables", "target"), "remaining_total"),
                ),
            ),
            (
                "data",
                "job",
                "pre_1980_title_deletion",
                (
                    (("metadata", "comparison"), "stratum"),
                    (("metadata", "comparison", "stratum"), "max"),
                    (("data_source",), "kind"),
                    (("variables",), "validate_integrity"),
                    (("variables", "tables", 0), "format"),
                    (("variables", "relationships", 0), "fk"),
                    (("variables", "drift_steps", 0), "fraction"),
                    (("variables", "drift_steps", 0, "filter"), "max"),
                    (
                        ("variables", "drift_steps", 0, "propagate", 0),
                        "policy",
                    ),
                ),
            ),
        )
        for kind, benchmark, scenario, nodes in cases:
            base = _payload(kind, benchmark, scenario)
            for path, required_key in nodes:
                with self.subTest(
                    kind=kind, benchmark=benchmark, path=path, mutation="missing"
                ):
                    candidate = copy.deepcopy(base)
                    del _node(candidate, path)[required_key]
                    self.assert_rejected(
                        candidate,
                        kind=kind,
                        benchmark=benchmark,
                        scenario=scenario,
                    )
                with self.subTest(
                    kind=kind, benchmark=benchmark, path=path, mutation="extra"
                ):
                    candidate = copy.deepcopy(base)
                    _node(candidate, path)["unexpected"] = "value"
                    self.assert_rejected(
                        candidate,
                        kind=kind,
                        benchmark=benchmark,
                        scenario=scenario,
                    )

    def test_identity_type_operation_and_target_tampering_fails(self) -> None:
        base = _payload("data", "tpch", "price_outliers")
        mutations = []

        candidate = copy.deepcopy(base)
        candidate["pattern_id"] = "visualization-tpch-not-canonical"
        mutations.append(candidate)
        candidate = copy.deepcopy(base)
        candidate["seed"] = 42.0
        mutations.append(candidate)
        candidate = copy.deepcopy(base)
        candidate["type"]["subtype"] = "multi_table"
        mutations.append(candidate)
        candidate = copy.deepcopy(base)
        candidate["metadata"]["benchmark"] = "tpcds"
        mutations.append(candidate)
        candidate = copy.deepcopy(base)
        candidate["metadata"]["comparison"]["table"] = "orders"
        mutations.append(candidate)
        candidate = copy.deepcopy(base)
        candidate["metadata"]["comparison"]["column"] = "l_quantity"
        mutations.append(candidate)
        candidate = copy.deepcopy(base)
        candidate["variables"]["base_table"] = "orders"
        mutations.append(candidate)
        candidate = copy.deepcopy(base)
        candidate["variables"]["drifts"][0]["name"] = "different"
        mutations.append(candidate)
        candidate = copy.deepcopy(base)
        candidate["variables"]["drifts"][0]["column"] = "l_quantity"
        mutations.append(candidate)
        candidate = copy.deepcopy(base)
        candidate["variables"]["drifts"][0]["drift_type"] = "value_skew"
        mutations.append(candidate)

        for index, candidate in enumerate(mutations):
            with self.subTest(index=index):
                self.assert_rejected(candidate)

    def test_runtime_placeholders_and_dynamic_inputs_are_closed(self) -> None:
        base = _payload("data", "tpch", "price_outliers")
        mutations: list[dict[str, Any]] = []
        for path, value in (
            (("data_source", "path"), "prefix-${DRIFTBENCH_INPUT}"),
            (("data_source", "path"), "relative/input.csv"),
            (("variables", "drifts", 0, "output_path"), "${DRIFTBENCH_OUTPUT_X}"),
            (
                ("data_source", "schema_extractor", "schema_output_path"),
                "${HOME}",
            ),
        ):
            candidate = copy.deepcopy(base)
            parent = _node(candidate, path[:-1])
            parent[path[-1]] = value
            mutations.append(candidate)

        for field, value in (
            ("filter_registry_modules", ["evil.module"]),
            ("callable", "os.system"),
            ("db_config", {"host": "localhost"}),
            ("uri", "postgresql://localhost/db"),
        ):
            candidate = copy.deepcopy(base)
            candidate["variables"]["drifts"][0][field] = value
            mutations.append(candidate)
        candidate = copy.deepcopy(base)
        candidate["data_source"]["kind"] = "postgres"
        mutations.append(candidate)
        candidate = copy.deepcopy(base)
        candidate["data_source"]["path"] = "C:\\private\\input.csv"
        mutations.append(candidate)

        for index, candidate in enumerate(mutations):
            with self.subTest(index=index):
                self.assert_rejected(candidate)

    def test_selective_deletion_and_invalid_operation_parameters_fail(self) -> None:
        base = _payload("data", "tpch", "price_outliers")
        invalid_drifts = (
            {
                "name": "price_outliers",
                "drift_type": "selective_deletion",
                "output_path": "${DRIFTBENCH_OUTPUT}",
            },
            {**base["variables"]["drifts"][0], "n_ratio": math.nan},
            {**base["variables"]["drifts"][0], "n_ratio": -0.1},
            {**base["variables"]["drifts"][0], "extreme_scale": 1},
            {**base["variables"]["drifts"][0], "extreme_direction": "both"},
        )
        for drift in invalid_drifts:
            with self.subTest(drift=drift):
                candidate = copy.deepcopy(base)
                candidate["variables"]["drifts"] = [drift]
                self.assert_rejected(candidate)

    def test_effect_policy_metrics_operators_and_thresholds_are_strict(self) -> None:
        data = _payload("data", "tpch", "price_outliers")
        query = _payload("query", "tpch", "hotset_concentration")
        cases = []
        candidate = copy.deepcopy(data)
        candidate["effect_policy"]["assertions"][0]["metric"] = (
            "max_mover_absolute_pp"
        )
        cases.append((candidate, "data", "tpch", "price_outliers"))
        candidate = copy.deepcopy(query)
        candidate["effect_policy"]["assertions"][0]["metric"] = "row_growth_rate"
        cases.append((candidate, "query", "tpch", "hotset_concentration"))
        for value in (True, math.nan, math.inf, "0.2"):
            candidate = copy.deepcopy(data)
            candidate["effect_policy"]["assertions"][0]["threshold"] = value
            cases.append((candidate, "data", "tpch", "price_outliers"))
        candidate = copy.deepcopy(data)
        candidate["effect_policy"]["assertions"][0]["operator"] = "gt"
        cases.append((candidate, "data", "tpch", "price_outliers"))

        for candidate, kind, benchmark, scenario in cases:
            with self.subTest(kind=kind, value=candidate["effect_policy"]):
                self.assert_rejected(
                    candidate,
                    kind=kind,
                    benchmark=benchmark,
                    scenario=scenario,
                )

    def test_query_weight_forms_are_strict_and_describe_real_drift(self) -> None:
        base = _payload("query", "tpch", "hotset_concentration")
        mutations = []
        candidate = copy.deepcopy(base)
        candidate["variables"]["baseline"] = {"adapter_profile": "A"}
        mutations.append(candidate)
        candidate = copy.deepcopy(base)
        candidate["variables"]["baseline"] = {"mode": "weighted"}
        mutations.append(candidate)
        candidate = copy.deepcopy(base)
        candidate["variables"]["target"]["focus"]["unknown"] = 0.1
        mutations.append(candidate)
        candidate = copy.deepcopy(base)
        candidate["variables"]["target"]["remaining_total"] = 0.2
        mutations.append(candidate)
        candidate = copy.deepcopy(base)
        candidate["variables"]["template_ids"].append("q1")
        mutations.append(candidate)
        candidate = copy.deepcopy(base)
        candidate["variables"]["sample_size"] = True
        mutations.append(candidate)
        candidate = copy.deepcopy(base)
        candidate["variables"]["output_path"] = "${DRIFTBENCH_OUTPUT_QUERY}"
        mutations.append(candidate)
        candidate = copy.deepcopy(base)
        candidate["variables"]["baseline"] = copy.deepcopy(
            candidate["variables"]["target"]
        )
        mutations.append(candidate)

        weighted = _payload("query", "tpcc", "new_order_hotset")
        candidate = copy.deepcopy(weighted)
        del candidate["variables"]["target"]["weights"]["payment"]
        mutations.append((candidate, "tpcc", "new_order_hotset"))
        candidate = copy.deepcopy(weighted)
        candidate["variables"]["target"]["weights"]["payment"] = -0.1
        mutations.append((candidate, "tpcc", "new_order_hotset"))
        candidate = copy.deepcopy(weighted)
        candidate["variables"]["target"]["weights"]["payment"] = math.inf
        mutations.append((candidate, "tpcc", "new_order_hotset"))

        for index, item in enumerate(mutations):
            if isinstance(item, tuple):
                candidate, benchmark, scenario = item
            else:
                candidate, benchmark, scenario = item, "tpch", "hotset_concentration"
            with self.subTest(index=index, benchmark=benchmark):
                self.assert_rejected(
                    candidate,
                    kind="query",
                    benchmark=benchmark,
                    scenario=scenario,
                )

    def test_job_filter_graph_integrity_and_propagation_are_exact(self) -> None:
        base = _payload("data", "job", "pre_1980_title_deletion")
        mutations = []
        candidate = copy.deepcopy(base)
        candidate["variables"]["validate_integrity"] = False
        mutations.append(candidate)
        candidate = copy.deepcopy(base)
        candidate["metadata"]["comparison"]["stratum"]["max"] = 1979
        mutations.append(candidate)
        candidate = copy.deepcopy(base)
        candidate["metadata"]["comparison"]["stratum"]["max"] = 1979
        candidate["variables"]["drift_steps"][0]["filter"]["max"] = 1979
        mutations.append(candidate)
        candidate = copy.deepcopy(base)
        candidate["variables"]["drift_steps"][0]["fraction"] = 0.39
        mutations.append(candidate)
        candidate = copy.deepcopy(base)
        candidate["variables"]["tables"][0]["path"] = (
            "${DRIFTBENCH_INPUT_TITLE}"
        )
        mutations.append(candidate)
        candidate = copy.deepcopy(base)
        candidate["variables"]["tables"].pop()
        mutations.append(candidate)
        candidate = copy.deepcopy(base)
        candidate["variables"]["relationships"][0]["fk"] = "person_id"
        mutations.append(candidate)
        candidate = copy.deepcopy(base)
        candidate["variables"]["relationships"].pop()
        mutations.append(candidate)
        candidate = copy.deepcopy(base)
        candidate["variables"]["drift_steps"][0]["target"] = "name"
        mutations.append(candidate)
        candidate = copy.deepcopy(base)
        candidate["variables"]["drift_steps"][0]["propagate"].pop()
        mutations.append(candidate)
        candidate = copy.deepcopy(base)
        candidate["variables"]["drift_steps"][0]["propagate"][0]["policy"] = (
            "null"
        )
        mutations.append(candidate)
        candidate = copy.deepcopy(base)
        candidate["variables"]["drift_steps"][0]["propagate"][0] = copy.deepcopy(
            candidate["variables"]["drift_steps"][0]["propagate"][1]
        )
        mutations.append(candidate)

        for index, candidate in enumerate(mutations):
            with self.subTest(index=index):
                self.assert_rejected(
                    candidate,
                    benchmark="job",
                    scenario="pre_1980_title_deletion",
                )

        post = _payload("data", "job", "post_2000_title_deletion")
        post["metadata"]["comparison"]["stratum"]["min"] = 2000
        post["variables"]["drift_steps"][0]["filter"]["min"] = 2000
        self.assert_rejected(
            post, benchmark="job", scenario="post_2000_title_deletion"
        )


if __name__ == "__main__":
    unittest.main()
