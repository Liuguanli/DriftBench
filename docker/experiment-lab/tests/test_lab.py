from __future__ import annotations

import copy
import json
import shutil
import sys
from pathlib import Path

import jsonschema
import pytest
import yaml

LAB_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(LAB_ROOT / "scripts"))

import audit  # noqa: E402
import lab  # noqa: E402


def _sample_run_manifest() -> dict[str, object]:
    return {
        "schema_version": 1,
        "experiment_id": "rel-ce-tpch",
        "run_id": "20260823T120000Z-seed42",
        "config_sha256": "a" * 64,
        "upstream_commits": {"tpch-kit": "852ad0a5ee31ebefeed884cea4188781dd9613a3"},
        "image_digests": {"postgres": "sha256:" + "c" * 64},
        "dataset_sha256": "d" * 64,
        "seed": 42,
        "resource_budget": {"cpus": 2.0, "memory_mib": 3840},
        "started_at": "2026-08-23T12:00:00Z",
        "finished_at": "2026-08-23T12:10:00Z",
        "exit_status": "succeeded",
        "artifacts": {
            "logs": "logs/runner.log",
            "results": "results/summary.csv",
            "metrics": "metrics/portable.csv",
            "figures": "figures/grafana-overview.png",
        },
    }


def test_complete_offline_audit_passes() -> None:
    errors, digest, manifests = audit.audit(LAB_ROOT)
    assert errors == []
    assert len(manifests) == 11
    assert len(digest) == 64


def test_catalog_has_exactly_the_tracker_components() -> None:
    catalog = yaml.safe_load((LAB_ROOT / "catalog.yaml").read_text(encoding="utf-8"))
    entries = catalog["experiments"]
    assert {entry["id"] for entry in entries} == audit.EXPECTED_IDS
    assert len(entries) == len(audit.EXPECTED_IDS)
    for entry in entries:
        manifest = yaml.safe_load((LAB_ROOT / entry["manifest"]).read_text(encoding="utf-8"))
        assert manifest["review_only"] is True
        assert (audit.REPOSITORY_ROOT / manifest["tracker"]["path"]).is_file()


def test_review_only_change_is_rejected() -> None:
    catalog = lab._catalog()
    manifest = copy.deepcopy(lab._manifest("rel-ce-tpch"))
    manifest["review_only"] = False
    errors = audit.manifest_semantic_errors(manifest, catalog)
    assert any("review_only" in error for error in errors)


def test_local_resource_overflow_is_rejected() -> None:
    catalog = lab._catalog()
    manifest = copy.deepcopy(lab._manifest("rel-ce-tpch"))
    manifest["resources"]["cpus"] = 2.01
    manifest["resources"]["memory_mib"] = 4097
    errors = audit.manifest_semantic_errors(manifest, catalog)
    assert any("CPU budget" in error for error in errors)
    assert any("memory budget" in error for error in errors)


def test_runtime_path_escape_is_rejected() -> None:
    catalog = lab._catalog()
    manifest = copy.deepcopy(lab._manifest("rel-ce-tpch"))
    manifest["artifacts"]["logs"] = "${DRIFTBENCH_LAB_STATE}/runs/../escape/"
    errors = audit.manifest_semantic_errors(manifest, catalog)
    assert any("artifact path logs" in error or "traversal" in error for error in errors)


def test_machine_absolute_manifest_path_is_rejected() -> None:
    catalog = lab._catalog()
    manifest = copy.deepcopy(lab._manifest("rel-ce-tpch"))
    manifest["blockers"].append("/etc/driftbench/private.conf")
    errors = audit.manifest_semantic_errors(manifest, catalog)
    assert any("machine absolute path" in error for error in errors)


@pytest.mark.parametrize(
    "source",
    [
        "${DRIFTBENCH_LAB_STATE_EVIL}/runs/rel-ce-tpch/run-1",
        "${DRIFTBENCH_LAB_STATE}/runs/../escape",
        "${DRIFTBENCH_LAB_STATE:-C:/tmp}/runs/rel-ce-tpch/run-1",
        "../../../outside",
        "../../driftbench/../README.md",
        "../../driftbench-neighbor",
        "./observability/../compose.yaml",
        "${DRIFTBENCH_LAB_STATE}/runs",
        "${DRIFTBENCH_LAB_STATE}/cache",
    ],
)
def test_bind_source_prefix_and_containment_bypasses_are_rejected(source: str) -> None:
    compose = audit.load_yaml(LAB_ROOT / "compose.yaml")
    catalog = lab._catalog()
    compose["services"]["ce-runner"]["volumes"][0]["source"] = source
    errors = audit.compose_errors(compose, catalog)
    assert any("unsupported bind source" in error for error in errors)


def test_prometheus_scrape_timeout_is_explicit_and_bounded() -> None:
    config = audit.load_yaml(LAB_ROOT / "observability/prometheus.yml")
    assert audit.prometheus_errors(config) == []

    missing = copy.deepcopy(config)
    missing["global"].pop("scrape_timeout")
    assert any("scrape_timeout" in error for error in audit.prometheus_errors(missing))

    too_slow = copy.deepcopy(config)
    too_slow["global"]["scrape_timeout"] = "6s"
    errors = audit.prometheus_errors(too_slow)
    assert any("at most 5 seconds" in error for error in errors)
    assert any("must not exceed scrape_interval" in error for error in errors)


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("cpus", True, "cpus must be a positive number"),
        ("cpus", "1.0", "cpus must be a positive number"),
        ("cpus", 0, "cpus must be a positive number"),
        ("cpus", 2.01, "cpus must be a positive number"),
        ("mem_limit", 512, "mem_limit must be a positive MiB value"),
        ("mem_limit", "0m", "mem_limit must be a positive MiB value"),
        ("mem_limit", "4097m", "mem_limit must be a positive MiB value"),
        ("pids_limit", True, "pids_limit must be a positive integer"),
        ("pids_limit", "128", "pids_limit must be a positive integer"),
        ("pids_limit", 0, "pids_limit must be a positive integer"),
        ("pids_limit", 513, "pids_limit must be a positive integer"),
    ],
)
def test_compose_resources_are_strongly_typed_positive_and_capped(field: str, value: object, message: str) -> None:
    compose = audit.load_yaml(LAB_ROOT / "compose.yaml")
    compose["services"]["postgres"][field] = value
    errors = audit.compose_errors(compose, lab._catalog())
    assert any(message in error for error in errors)


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("cpus", 1.0, "CPU total"),
        ("mem_limit", "1600m", "memory total"),
    ],
)
def test_local_lite_profile_resource_aggregates_are_bounded(field: str, value: object, message: str) -> None:
    compose = audit.load_yaml(LAB_ROOT / "compose.yaml")
    compose["services"]["ce-runner"][field] = value
    errors = audit.compose_errors(compose, lab._catalog())
    assert any("rel-ce-tpch" in error and message in error for error in errors)


def test_bind_target_and_selected_state_subtrees_are_exactly_allowlisted() -> None:
    catalog = lab._catalog()
    compose = audit.load_yaml(LAB_ROOT / "compose.yaml")
    volume = compose["services"]["ce-runner"]["volumes"][3]
    volume["source"] = "${DRIFTBENCH_LAB_STATE}/runs"
    volume["target"] = "/"
    errors = audit.compose_errors(compose, catalog)
    assert any("not allowlisted" in error for error in errors)


@pytest.mark.parametrize("bad_file", ["/tmp/password.txt", "${DRIFTBENCH_LAB_STATE}/runs/../password.txt"])
def test_secret_files_reject_absolute_and_traversing_paths(bad_file: str) -> None:
    compose = audit.load_yaml(LAB_ROOT / "compose.yaml")
    compose["secrets"]["postgres_password"]["file"] = bad_file
    errors = audit.compose_errors(compose, lab._catalog())
    assert any("postgres_password" in error and "results/secrets" in error for error in errors)


def test_secret_definitions_and_service_references_are_a_closed_set() -> None:
    catalog = lab._catalog()
    missing = audit.load_yaml(LAB_ROOT / "compose.yaml")
    missing["secrets"].pop("postgres_password")
    errors = audit.compose_errors(missing, catalog)
    assert any("exactly the reviewed secret definitions" in error for error in errors)

    extra = audit.load_yaml(LAB_ROOT / "compose.yaml")
    extra["secrets"]["extra"] = {"file": "/tmp/extra"}
    errors = audit.compose_errors(extra, catalog)
    assert any("exactly the reviewed secret definitions" in error for error in errors)

    missing_reference = audit.load_yaml(LAB_ROOT / "compose.yaml")
    missing_reference["services"]["ce-runner"]["secrets"] = []
    errors = audit.compose_errors(missing_reference, catalog)
    assert any("postgres_password service references" in error for error in errors)


@pytest.mark.parametrize("selection", ["", "rel-ce-tpch,rel-indexes-sosd", "rel-ce-tpch rel-indexes-sosd", " rel-ce-tpch"])
def test_multiple_or_ambiguous_experiment_selection_is_rejected(selection: str) -> None:
    with pytest.raises(ValueError):
        lab.validate_experiment_selection(selection, audit.EXPECTED_IDS)


def test_lifecycle_commands_are_blocked_without_docker(monkeypatch: pytest.MonkeyPatch) -> None:
    called = False

    def forbidden(*_args: object, **_kwargs: object) -> None:
        nonlocal called
        called = True
        raise AssertionError("subprocess must not be called")

    monkeypatch.setattr("subprocess.run", forbidden)
    assert lab.main(["run", "rel-ce-tpch"]) == 78
    assert lab.main(["prepare", "rel-indexes-sosd"]) == 78
    assert called is False


def test_run_manifest_schema_accepts_portable_relative_artifacts() -> None:
    schema = json.loads((LAB_ROOT / "schema/run-manifest.schema.json").read_text(encoding="utf-8"))
    jsonschema.Draft202012Validator(schema, format_checker=jsonschema.FormatChecker()).validate(_sample_run_manifest())


@pytest.mark.parametrize(
    "malicious_path",
    [
        "logs/../../outside.txt",
        "logs/../outside.txt",
        "logs/C:/outside.txt",
        "logs\\..\\outside.txt",
        "/logs/outside.txt",
        "logs/payload\x00.txt",
        "logs/NUL.txt",
        "logs/report.",
        "logs/report ",
        "logs/file\n",
    ],
)
def test_run_manifest_schema_rejects_nonportable_or_escaping_artifacts(malicious_path: str) -> None:
    schema = json.loads((LAB_ROOT / "schema/run-manifest.schema.json").read_text(encoding="utf-8"))
    sample = _sample_run_manifest()
    sample["artifacts"]["logs"] = malicious_path  # type: ignore[index]
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.Draft202012Validator(schema).validate(sample)
    assert audit.run_manifest_semantic_errors(sample)


@pytest.mark.parametrize("run_id", ["CON", "nul.txt", "run-42.", "run-42 ", "run\x01", "run-42\n"])
def test_run_manifest_schema_and_semantics_reject_noncanonical_windows_ids(run_id: str) -> None:
    schema = json.loads((LAB_ROOT / "schema/run-manifest.schema.json").read_text(encoding="utf-8"))
    sample = _sample_run_manifest()
    sample["run_id"] = run_id
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.Draft202012Validator(schema).validate(sample)
    assert any("run_id" in error for error in audit.run_manifest_semantic_errors(sample))


def test_run_manifest_schema_rejects_all_control_characters_in_portable_fields() -> None:
    schema = json.loads((LAB_ROOT / "schema/run-manifest.schema.json").read_text(encoding="utf-8"))
    validator = jsonschema.Draft202012Validator(schema)
    controls = [*(chr(codepoint) for codepoint in range(32)), chr(127)]
    for control in controls:
        samples = []
        for field, value in (
            ("experiment_id", f"rel-ce-tpch{control}"),
            ("run_id", f"run-42{control}"),
        ):
            sample = _sample_run_manifest()
            sample[field] = value
            samples.append((field, sample))
        artifact_sample = _sample_run_manifest()
        artifact_sample["artifacts"]["logs"] = f"logs/file{control}"  # type: ignore[index]
        samples.append(("artifacts.logs", artifact_sample))
        for field, sample in samples:
            assert list(validator.iter_errors(sample)), f"schema accepted control U+{ord(control):04X} in {field}"


def test_canonical_digest_covers_all_audited_runtime_configuration(tmp_path: Path) -> None:
    entries = lab._catalog()["experiments"]
    manifest_paths = [LAB_ROOT / entry["manifest"] for entry in entries]
    observed = {
        audit._canonical_config_key(path, LAB_ROOT, audit.REPOSITORY_ROOT)
        for path in audit._canonical_config_paths(LAB_ROOT, manifest_paths)
    }
    expected = {
        "catalog.yaml",
        "compose.yaml",
        "schema/experiment-manifest.schema.json",
        "schema/run-manifest.schema.json",
        "observability/prometheus.yml",
        "observability/grafana/provisioning/datasources/prometheus.yaml",
        "observability/grafana/provisioning/dashboards/provider.yaml",
        "observability/grafana/dashboards/driftbench-experiment.json",
        "experiments/relational/cardinality_estimation/tpch_plan.yaml",
        "experiments/relational/cardinality_estimation/baselines.yaml",
        *(entry["manifest"] for entry in entries),
    }
    assert expected <= observed

    copied = tmp_path / "experiment-lab"
    shutil.copytree(LAB_ROOT, copied, ignore=shutil.ignore_patterns("__pycache__", ".pytest_cache"))
    _, baseline, _ = audit.audit(copied)
    prometheus_path = copied / "observability/prometheus.yml"
    prometheus = audit.load_yaml(prometheus_path)
    prometheus["global"]["external_labels"]["digest_probe"] = "prometheus"
    prometheus_path.write_text(yaml.safe_dump(prometheus, sort_keys=False), encoding="utf-8")
    _, prometheus_digest, _ = audit.audit(copied)
    assert prometheus_digest != baseline

    run_schema_path = copied / "schema/run-manifest.schema.json"
    run_schema = json.loads(run_schema_path.read_text(encoding="utf-8"))
    run_schema["$comment"] = "digest probe"
    run_schema_path.write_text(json.dumps(run_schema), encoding="utf-8")
    _, schema_digest, _ = audit.audit(copied)
    assert schema_digest != prometheus_digest

    copied_repository = tmp_path / "repository"
    copied_lab = copied_repository / "docker/experiment-lab"
    shutil.copytree(LAB_ROOT, copied_lab, ignore=shutil.ignore_patterns("__pycache__", ".pytest_cache"))
    copied_ce = copied_repository / "experiments/relational/cardinality_estimation"
    copied_ce.mkdir(parents=True)
    for filename in ("tpch_plan.yaml", "baselines.yaml"):
        shutil.copy2(audit.REPOSITORY_ROOT / "experiments/relational/cardinality_estimation" / filename, copied_ce)
    _, contract_baseline, _ = audit.audit(copied_lab, copied_repository)
    registry_path = copied_ce / "baselines.yaml"
    registry = audit.load_yaml(registry_path)
    registry["groups"]["recent-priority-candidate"]["purpose"] += " Digest probe."
    registry_path.write_text(yaml.safe_dump(registry, sort_keys=False), encoding="utf-8")
    _, contract_changed, _ = audit.audit(copied_lab, copied_repository)
    assert contract_changed != contract_baseline


@pytest.mark.parametrize("mutation", ["missing", "duplicate", "inconsistent"])
def test_ce_manifest_baseline_group_references_are_closed(mutation: str) -> None:
    manifest = copy.deepcopy(lab._manifest("rel-ce-tpch"))
    groups = manifest["workload"]["baseline_groups"]
    if mutation == "missing":
        manifest["workload"].pop("baseline_groups")
    elif mutation == "duplicate":
        groups.append(groups[0])
    else:
        groups[-1] = "undefined-group"
    errors = audit.ce_contract_errors(manifest)
    assert any("baseline group IDs" in error for error in errors)


def test_compose_has_no_default_or_unbounded_service() -> None:
    compose = yaml.safe_load((LAB_ROOT / "compose.yaml").read_text(encoding="utf-8"))
    for service in compose["services"].values():
        assert service["profiles"]
        assert service["restart"] == "no"
        assert service["pull_policy"] == "never"
        assert service["cpus"]
        assert service["mem_limit"]
        assert service["pids_limit"]
        assert service["logging"]["options"] == {"max-size": "10m", "max-file": "3"}
    catalog = audit.load_yaml(LAB_ROOT / "catalog.yaml")
    assert catalog["service_resource_caps"] == {"cpus": 2.0, "memory_mib": 4096, "pids": 512}
    assert catalog["local_lite_budget"]["pids"] == 1024


def test_runtime_failsafe_ignore_exists() -> None:
    rules = (LAB_ROOT / ".gitignore").read_text(encoding="utf-8")
    assert "review-state/" in rules
    assert "__pycache__" in rules
