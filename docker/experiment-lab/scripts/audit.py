"""Offline policy audit for the review-only experiment lab.

This module reads local configuration only. It does not invoke Docker, Git,
network clients, benchmark code, or databases.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
from pathlib import Path, PurePosixPath, PureWindowsPath
from typing import Any, Iterable

import jsonschema
import yaml

LAB_ROOT = Path(__file__).resolve().parents[1]
REPOSITORY_ROOT = LAB_ROOT.parents[1]
EXPECTED_IDS = {
    "rel-ce-tpch",
    "rel-optimizer-job",
    "rel-indexes-sosd",
    "rel-execution-tpch",
    "rel-buffer-sysbench",
    "rel-transactions-tpcc",
    "vec-ann-sift",
    "vec-quantization-sift",
    "vec-reranking-scifact",
    "vec-ingestion-qdrant",
    "vec-partition-qdrant",
}
PINNED_IMAGE = re.compile(r"^[^@\s]+:[^@\s]+@sha256:[0-9a-f]{64}$")
COMMIT = re.compile(r"^[0-9a-f]{40}$")
SHA256 = re.compile(r"^[0-9a-f]{64}$")
STATE_BIND_SOURCE = re.compile(
    r"^\$\{DRIFTBENCH_LAB_STATE(?::\?[^{}\x00\r\n]+)?\}(?P<suffix>/.*)?$"
)
PROMETHEUS_DURATION = re.compile(r"^(?P<value>[0-9]+(?:\.[0-9]+)?)(?P<unit>ms|s|m|h)$")
MAX_CANONICAL_FILES = 256
MAX_CANONICAL_BYTES = 2 * 1024 * 1024
WINDOWS_RESERVED_NAMES = {
    "con",
    "prn",
    "aux",
    "nul",
    *(f"com{number}" for number in range(1, 10)),
    *(f"lpt{number}" for number in range(1, 10)),
}
MEMORY_LIMIT = re.compile(r"^(?P<mib>[1-9][0-9]*)m$")
CE_CONTRACT_FIELDS = ("plan_contract", "baseline_registry")

EXPECTED_BINDS: dict[str, set[tuple[str, str, bool]]] = {
    "postgres": {
        (
            "${DRIFTBENCH_LAB_STATE:?Set an external state root}/runs/rel-ce-tpch/${LAB_RUN_ID}/results/postgres-data",
            "/var/lib/postgresql/data",
            False,
        )
    },
    "ce-runner": {
        ("../../experiments/relational/cardinality_estimation", "/workspace/experiment", True),
        ("../../driftbench", "/workspace/driftbench", True),
        ("${DRIFTBENCH_LAB_STATE}/cache/tpch-sf1", "/cache/tpch-sf1", True),
        ("${DRIFTBENCH_LAB_STATE}/runs/rel-ce-tpch/${LAB_RUN_ID}", "/run-artifacts", False),
    },
    "indexes-runner": {
        ("../../experiments/relational/indexes", "/workspace/experiment", True),
        ("${DRIFTBENCH_LAB_STATE}/cache/sosd-osm-keys", "/cache/sosd-osm-keys", True),
        ("${DRIFTBENCH_LAB_STATE}/runs/rel-indexes-sosd/${LAB_RUN_ID}", "/run-artifacts", False),
    },
    "pushgateway": {
        (
            "${DRIFTBENCH_LAB_STATE}/runs/${LAB_EXPERIMENT_ID}/${LAB_RUN_ID}/metrics/pushgateway",
            "/data",
            False,
        )
    },
    "prometheus": {
        ("./observability/prometheus.yml", "/etc/prometheus/prometheus.yml", True),
        (
            "${DRIFTBENCH_LAB_STATE}/runs/${LAB_EXPERIMENT_ID}/${LAB_RUN_ID}/metrics/prometheus",
            "/prometheus",
            False,
        ),
    },
    "grafana": {
        ("./observability/grafana/provisioning", "/etc/grafana/provisioning", True),
        ("./observability/grafana/dashboards", "/var/lib/grafana/dashboards", True),
        (
            "${DRIFTBENCH_LAB_STATE}/runs/${LAB_EXPERIMENT_ID}/${LAB_RUN_ID}/metrics/grafana",
            "/var/lib/grafana",
            False,
        ),
        (
            "${DRIFTBENCH_LAB_STATE}/runs/${LAB_EXPERIMENT_ID}/${LAB_RUN_ID}/figures",
            "/var/lib/grafana/figures",
            False,
        ),
    },
    "renderer": set(),
}
EXPECTED_SECRETS: dict[str, tuple[str, set[str]]] = {
    "postgres_password": (
        "${DRIFTBENCH_LAB_STATE}/runs/rel-ce-tpch/${LAB_RUN_ID}/results/secrets/postgres_password.txt",
        {"postgres", "ce-runner"},
    ),
    "grafana_admin_password": (
        "${DRIFTBENCH_LAB_STATE}/runs/${LAB_EXPERIMENT_ID}/${LAB_RUN_ID}/results/secrets/grafana_admin_password.txt",
        {"grafana"},
    ),
}


def load_yaml(path: Path) -> Any:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def _inside(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
        return True
    except ValueError:
        return False


def _is_link_like(path: Path) -> bool:
    """Recognize symlinks and Windows reparse-point directories (junctions)."""
    try:
        if path.is_symlink():
            return True
        is_junction = getattr(path, "is_junction", None)
        if is_junction is not None and is_junction():
            return True
        attributes = getattr(path.lstat(), "st_file_attributes", 0)
    except (OSError, ValueError):
        return False
    return bool(attributes & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0))


def _machine_absolute_path(value: str) -> bool:
    if value.startswith("${") or re.match(r"^[A-Za-z][A-Za-z0-9+.-]*://", value):
        return False
    return PurePosixPath(value).is_absolute() or PureWindowsPath(value).is_absolute()


def _state_bind_source(source: str) -> bool:
    match = STATE_BIND_SOURCE.fullmatch(source)
    if match is None:
        return False
    suffix = match.group("suffix")
    if suffix is None:
        return True
    if "\\" in suffix:
        return False
    parts = suffix[1:].split("/")
    return len(parts) >= 2 and parts[0] in {"cache", "runs"} and all(
        part not in {"", ".", ".."} for part in parts
    )


def _portable_windows_segment(value: str) -> bool:
    if not value or value[-1] in {".", " "} or "\\" in value or ":" in value:
        return False
    if any(ord(character) < 32 or ord(character) == 127 for character in value):
        return False
    if re.fullmatch(r"[A-Za-z0-9_][A-Za-z0-9._-]*", value) is None:
        return False
    return value.split(".", 1)[0].casefold() not in WINDOWS_RESERVED_NAMES


def _portable_artifact_parts(value: Any, expected_category: str) -> tuple[str, ...] | None:
    if not isinstance(value, str) or value.startswith("/"):
        return None
    parts = tuple(value.split("/"))
    if len(parts) < 2 or parts[0] != expected_category:
        return None
    return parts if all(_portable_windows_segment(part) for part in parts) else None


def run_manifest_semantic_errors(manifest: dict[str, Any]) -> list[str]:
    """Enforce the portable path rules that complement run-manifest JSON Schema."""
    errors: list[str] = []
    for identity in ("experiment_id", "run_id"):
        value = manifest.get(identity)
        if not isinstance(value, str) or not _portable_windows_segment(value):
            errors.append(f"run manifest: {identity} is not a portable Windows path segment")
    artifacts = manifest.get("artifacts")
    if not isinstance(artifacts, dict):
        return [*errors, "run manifest: artifacts must be a mapping"]
    expected = {"logs", "results", "metrics", "figures"}
    if set(artifacts) != expected:
        errors.append("run manifest: artifacts must contain exactly logs, results, metrics, and figures")
    for category in sorted(expected):
        if _portable_artifact_parts(artifacts.get(category), category) is None:
            errors.append(f"run manifest: {category} path is not a portable confined {category} path")
    return errors


def _repository_bind_source(source: str, lab_root: Path, repository_root: Path) -> bool:
    normalized = source.replace("\\", "/")
    if not normalized.startswith("../../"):
        return False
    if PurePosixPath(normalized).is_absolute() or PureWindowsPath(source).is_absolute():
        return False
    try:
        candidate = (lab_root / Path(source)).resolve()
        allowed_roots = [(repository_root / name).resolve() for name in ("driftbench", "experiments")]
    except (OSError, ValueError):
        return False
    return any(_inside(candidate, allowed) for allowed in allowed_roots)


def _observability_bind_source(source: str, lab_root: Path) -> bool:
    normalized = source.replace("\\", "/")
    if not normalized.startswith("./observability/"):
        return False
    try:
        candidate = (lab_root / Path(source)).resolve()
        allowed = (lab_root / "observability").resolve()
    except (OSError, ValueError):
        return False
    return _inside(candidate, allowed)


def _walk(value: Any) -> Iterable[Any]:
    yield value
    if isinstance(value, dict):
        for child in value.values():
            yield from _walk(child)
    elif isinstance(value, list):
        for child in value:
            yield from _walk(child)


def manifest_semantic_errors(manifest: dict[str, Any], catalog: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    prefix = str(manifest.get("id", "<missing-id>"))
    if manifest.get("review_only") is not True:
        errors.append(f"{prefix}: review_only must remain true")

    budget = catalog["local_lite_budget"]
    resources = manifest.get("resources", {})
    if resources.get("tier") == "local-lite":
        if float(resources.get("cpus", 0)) > float(budget["cpus"]):
            errors.append(f"{prefix}: local-lite CPU budget exceeded")
        if int(resources.get("memory_mib", 0)) > int(budget["memory_mib"]):
            errors.append(f"{prefix}: local-lite memory budget exceeded")

    expected_base = f"${{DRIFTBENCH_LAB_STATE}}/runs/{prefix}/${{RUN_ID}}/"
    artifacts = manifest.get("artifacts", {})
    for key in ("logs", "results", "metrics", "figures"):
        expected = f"{expected_base}{key}/"
        if artifacts.get(key) != expected:
            errors.append(f"{prefix}: artifact path {key} must equal {expected}")
    expected_manifest = f"{expected_base}run-manifest.json"
    if artifacts.get("run_manifest") != expected_manifest:
        errors.append(f"{prefix}: run_manifest must equal {expected_manifest}")

    cache_path = manifest.get("dataset", {}).get("cache_path", "")
    if not isinstance(cache_path, str) or not cache_path.startswith("${DRIFTBENCH_LAB_STATE}/cache/"):
        errors.append(f"{prefix}: dataset cache must be rooted at DRIFTBENCH_LAB_STATE/cache")

    for source in manifest.get("sources", []):
        commit = source.get("commit", "")
        if not COMMIT.fullmatch(str(commit)):
            errors.append(f"{prefix}: source commit must be a 40-character SHA")
        if str(commit) not in str(source.get("archive_url", "")):
            errors.append(f"{prefix}: archive URL must contain the pinned commit")
        checksum = source.get("archive_sha256", {})
        if checksum.get("status") == "verified" and not SHA256.fullmatch(str(checksum.get("value", ""))):
            errors.append(f"{prefix}: verified source archive needs a SHA-256 value")
        if checksum.get("status") == "pending-not-downloaded" and checksum.get("value") is not None:
            errors.append(f"{prefix}: pending source checksum value must be null")

    dataset_checksum = manifest.get("dataset", {}).get("checksum", {})
    if dataset_checksum.get("status") == "verified" and not SHA256.fullmatch(str(dataset_checksum.get("value", ""))):
        errors.append(f"{prefix}: verified dataset needs a SHA-256 value")
    if dataset_checksum.get("status") == "pending-not-downloaded" and dataset_checksum.get("value") is not None:
        errors.append(f"{prefix}: pending dataset checksum value must be null")

    for image in manifest.get("images", []):
        reference = str(image.get("reference", ""))
        if not PINNED_IMAGE.fullmatch(reference):
            errors.append(f"{prefix}: image is not tag-and-digest pinned: {reference}")
        if ":latest" in reference:
            errors.append(f"{prefix}: latest image tags are prohibited")

    for value in _walk(manifest):
        if not isinstance(value, str):
            continue
        if _machine_absolute_path(value):
            errors.append(f"{prefix}: machine absolute path is prohibited: {value}")
        if value.startswith("${DRIFTBENCH_LAB_STATE}") and ".." in PurePosixPath(value.replace("\\", "/")).parts:
            errors.append(f"{prefix}: runtime path traversal is prohibited: {value}")

    status = manifest.get("tracker", {}).get("repository_status")
    readiness = manifest.get("readiness")
    if status == "Stub" and readiness == "existing-verification-pending":
        errors.append(f"{prefix}: a Stub cannot claim existing-verification-pending")
    if status == "Existing / verification pending" and readiness not in {"existing-verification-pending", "devbox-only"}:
        errors.append(f"{prefix}: existing experiment readiness does not match tracker status")
    return errors


def compose_errors(
    compose: dict[str, Any],
    catalog: dict[str, Any],
    lab_root: Path = LAB_ROOT,
    repository_root: Path = REPOSITORY_ROOT,
) -> list[str]:
    errors: list[str] = []
    services = compose.get("services", {})
    if not isinstance(services, dict) or not services:
        return ["compose: services must be a non-empty mapping"]
    all_profiles: set[str] = set()
    raw = json.dumps(compose, sort_keys=True)
    if "docker.sock" in raw:
        errors.append("compose: Docker socket mounts are prohibited")
    caps = catalog["service_resource_caps"]
    resources_by_service: dict[str, tuple[float, int, int]] = {}
    for name, service in services.items():
        label = f"compose service {name}"
        profiles = service.get("profiles") or []
        if not profiles:
            errors.append(f"{label}: every service must be behind an explicit profile")
        all_profiles.update(profiles)
        if service.get("restart") != "no":
            errors.append(f"{label}: restart must be quoted 'no'")
        if service.get("pull_policy") != "never":
            errors.append(f"{label}: pull_policy must be never in the review blueprint")
        cpus = service.get("cpus")
        memory_match = MEMORY_LIMIT.fullmatch(service.get("mem_limit")) if isinstance(service.get("mem_limit"), str) else None
        pids = service.get("pids_limit")
        valid_cpus = isinstance(cpus, (int, float)) and not isinstance(cpus, bool) and 0 < cpus <= caps["cpus"]
        valid_memory = memory_match is not None and 0 < int(memory_match.group("mib")) <= caps["memory_mib"]
        valid_pids = isinstance(pids, int) and not isinstance(pids, bool) and 0 < pids <= caps["pids"]
        if not valid_cpus:
            errors.append(f"{label}: cpus must be a positive number no greater than {caps['cpus']}")
        if not valid_memory:
            errors.append(f"{label}: mem_limit must be a positive MiB value no greater than {caps['memory_mib']}m")
        if not valid_pids:
            errors.append(f"{label}: pids_limit must be a positive integer no greater than {caps['pids']}")
        if valid_cpus and valid_memory and valid_pids:
            resources_by_service[name] = (float(cpus), int(memory_match.group("mib")), int(pids))
        logging = service.get("logging", {})
        options = logging.get("options", {}) if isinstance(logging, dict) else {}
        if options.get("max-size") != "10m" or str(options.get("max-file")) != "3":
            errors.append(f"{label}: Docker log rotation must be 3 x 10m")
        if service.get("privileged") is True or service.get("network_mode") in {"host", "service:docker"}:
            errors.append(f"{label}: privileged/host networking is prohibited")
        for port in service.get("ports", []) or []:
            if not str(port).startswith("127.0.0.1:"):
                errors.append(f"{label}: host ports must bind to 127.0.0.1")
        image = str(service.get("image", ""))
        if not PINNED_IMAGE.fullmatch(image):
            errors.append(f"{label}: image is not tag-and-digest pinned")
        expected_binds = EXPECTED_BINDS.get(name, set())
        observed_binds: set[tuple[str, str, bool]] = set()
        for volume in service.get("volumes", []) or []:
            if not isinstance(volume, dict):
                errors.append(f"{label}: use long-form volumes for auditable access modes")
                continue
            if volume.get("type") != "bind":
                errors.append(f"{label}: only explicit bind volumes are supported")
                continue
            source = str(volume.get("source", ""))
            target = str(volume.get("target", ""))
            bind = (source, target, volume.get("read_only") is True)
            observed_binds.add(bind)
            if bind not in expected_binds:
                errors.append(f"{label}: bind source/target/access mode is not allowlisted: {source} -> {target}")
            if _repository_bind_source(source, lab_root, repository_root) or _observability_bind_source(
                source, lab_root
            ):
                if volume.get("read_only") is not True:
                    errors.append(f"{label}: repository source/config mounts must be read-only")
            elif _state_bind_source(source):
                pass
            else:
                errors.append(f"{label}: unsupported bind source {source}")
            target_path = PurePosixPath(target)
            if not target_path.is_absolute() or ".." in target_path.parts or "\x00" in target:
                errors.append(f"{label}: bind target must be a confined absolute container path: {target}")
        missing_binds = expected_binds - observed_binds
        if missing_binds:
            errors.append(f"{label}: missing {len(missing_binds)} required allowlisted bind(s)")
        if len(service.get("volumes", []) or []) != len(observed_binds):
            errors.append(f"{label}: duplicate bind definitions are prohibited")

    expected_profiles = set(catalog["profiles"]) | set(catalog["special_profiles"])
    if all_profiles != expected_profiles:
        errors.append(f"compose: profiles {sorted(all_profiles)} != catalog {sorted(expected_profiles)}")
    for profile, details in catalog["profiles"].items():
        observed = sorted(name for name, service in services.items() if profile in (service.get("profiles") or []))
        if observed != sorted(details["services"]):
            errors.append(f"compose: profile {profile} services {observed} != catalog {sorted(details['services'])}")
    for profile, details in catalog["special_profiles"].items():
        observed = sorted(name for name, service in services.items() if profile in (service.get("profiles") or []))
        if observed != sorted(details["services"]):
            errors.append(f"compose: special profile {profile} services do not match catalog")

    local_profiles = dict(catalog["profiles"])
    local_profiles.update(
        (profile, details)
        for profile, details in catalog["special_profiles"].items()
        if details.get("resource_tier") == "local-lite"
    )
    budget = catalog["local_lite_budget"]
    for profile, details in local_profiles.items():
        profile_resources = [resources_by_service.get(service) for service in details["services"]]
        if any(resource is None for resource in profile_resources):
            continue
        cpus_total = sum(resource[0] for resource in profile_resources if resource is not None)
        memory_total = sum(resource[1] for resource in profile_resources if resource is not None)
        pids_total = sum(resource[2] for resource in profile_resources if resource is not None)
        if cpus_total > budget["cpus"]:
            errors.append(f"compose: local-lite profile {profile} CPU total exceeds {budget['cpus']}")
        if memory_total > budget["memory_mib"]:
            errors.append(f"compose: local-lite profile {profile} memory total exceeds {budget['memory_mib']} MiB")
        if pids_total > budget["pids"]:
            errors.append(f"compose: local-lite profile {profile} PID total exceeds {budget['pids']}")

    secret_definitions = compose.get("secrets")
    if not isinstance(secret_definitions, dict):
        errors.append("compose: top-level secrets must be a mapping")
        secret_definitions = {}
    if set(secret_definitions) != set(EXPECTED_SECRETS):
        errors.append("compose: top-level secrets must contain exactly the reviewed secret definitions")
    references: dict[str, set[str]] = {secret: set() for secret in EXPECTED_SECRETS}
    for service_name, service in services.items():
        service_secrets = service.get("secrets", [])
        if not isinstance(service_secrets, list) or any(not isinstance(secret, str) for secret in service_secrets):
            errors.append(f"compose service {service_name}: secrets must be a list of reviewed secret names")
            continue
        if len(service_secrets) != len(set(service_secrets)):
            errors.append(f"compose service {service_name}: duplicate secret references are prohibited")
        for secret in service_secrets:
            if secret not in references:
                errors.append(f"compose service {service_name}: references unknown secret {secret}")
                continue
            references[secret].add(service_name)
    for secret, (expected_file, expected_services) in EXPECTED_SECRETS.items():
        definition = secret_definitions.get(secret)
        if not isinstance(definition, dict) or set(definition) != {"file"} or definition.get("file") != expected_file:
            errors.append(f"compose: secret {secret} must use its selected-run results/secrets file")
        if references[secret] != expected_services:
            errors.append(f"compose: secret {secret} service references do not match the reviewed closure")
    return errors


def _duration_seconds(value: Any) -> float | None:
    match = PROMETHEUS_DURATION.fullmatch(str(value))
    if match is None:
        return None
    multiplier = {"ms": 0.001, "s": 1.0, "m": 60.0, "h": 3600.0}[match.group("unit")]
    return float(match.group("value")) * multiplier


def prometheus_errors(config: dict[str, Any]) -> list[str]:
    global_config = config.get("global", {})
    if not isinstance(global_config, dict):
        return ["prometheus: global configuration must be a mapping"]
    interval = _duration_seconds(global_config.get("scrape_interval"))
    timeout = _duration_seconds(global_config.get("scrape_timeout"))
    errors: list[str] = []
    if interval is None or interval <= 0:
        errors.append("prometheus: scrape_interval must be an explicit positive duration")
    if timeout is None or timeout <= 0:
        errors.append("prometheus: scrape_timeout must be an explicit positive duration")
    elif timeout > 5:
        errors.append("prometheus: scrape_timeout must be at most 5 seconds for the local review profile")
    if interval is not None and timeout is not None and timeout > interval:
        errors.append("prometheus: scrape_timeout must not exceed scrape_interval")
    return errors


def _ce_contract_path(repository_root: Path, value: Any, label: str) -> Path:
    if not isinstance(value, str) or not value or _machine_absolute_path(value):
        raise ValueError(f"{label} must be a repository-relative path")
    candidate = (repository_root / Path(value)).resolve()
    allowed = (repository_root / "experiments/relational/cardinality_estimation").resolve()
    if (
        not _inside(candidate, allowed)
        or not candidate.is_file()
        or _is_link_like(candidate)
        or candidate.suffix not in {".yaml", ".yml"}
    ):
        raise ValueError(f"{label} escapes, is linked, or is missing: {value}")
    return candidate


def ce_contract_errors(manifest: dict[str, Any], repository_root: Path = REPOSITORY_ROOT) -> list[str]:
    """Validate the CE plan/registry references without running experiment code."""
    if manifest.get("id") != "rel-ce-tpch":
        return []
    workload = manifest.get("workload", {})
    try:
        plan_path = _ce_contract_path(repository_root, workload.get("plan_contract"), "CE plan contract")
        registry_path = _ce_contract_path(
            repository_root, workload.get("baseline_registry"), "CE baseline registry"
        )
        plan = load_yaml(plan_path)
        registry = load_yaml(registry_path)
    except (OSError, UnicodeError, ValueError, yaml.YAMLError) as exc:
        return [f"rel-ce-tpch: {exc}"]

    errors: list[str] = []
    groups = registry.get("groups")
    if not isinstance(groups, dict) or not groups or not all(isinstance(key, str) and key for key in groups):
        return ["rel-ce-tpch: baseline registry groups must be a non-empty ID mapping"]
    registry_group_ids = list(groups)

    manifest_group_ids = workload.get("baseline_groups")
    if not isinstance(manifest_group_ids, list) or not manifest_group_ids:
        errors.append("rel-ce-tpch: manifest baseline group IDs are missing")
    elif len(manifest_group_ids) != len(set(manifest_group_ids)):
        errors.append("rel-ce-tpch: manifest baseline group IDs must be unique")
    elif manifest_group_ids != registry_group_ids:
        errors.append("rel-ce-tpch: manifest baseline group IDs do not match the registry")

    plan_registry = plan.get("baseline_registry", {})
    plan_group_ids = plan_registry.get("group_ids") if isinstance(plan_registry, dict) else None
    if not isinstance(plan_group_ids, list) or not plan_group_ids:
        errors.append("rel-ce-tpch: plan baseline group IDs are missing")
    elif len(plan_group_ids) != len(set(plan_group_ids)):
        errors.append("rel-ce-tpch: plan baseline group IDs must be unique")
    elif plan_group_ids != registry_group_ids:
        errors.append("rel-ce-tpch: plan baseline group IDs do not match the registry")

    plan_registry_value = plan_registry.get("path") if isinstance(plan_registry, dict) else None
    try:
        plan_registry_path = (plan_path.parent / Path(str(plan_registry_value))).resolve()
    except (OSError, ValueError):
        plan_registry_path = None
    if plan_registry_path != registry_path:
        errors.append("rel-ce-tpch: plan does not reference the manifest's baseline registry")

    estimators = registry.get("estimators")
    if not isinstance(estimators, list) or not estimators:
        errors.append("rel-ce-tpch: baseline registry estimators are missing")
    else:
        observed_groups = [item.get("group") for item in estimators if isinstance(item, dict)]
        if len(observed_groups) != len(estimators) or any(group not in groups for group in observed_groups):
            errors.append("rel-ce-tpch: estimator references an undefined baseline group")
        elif set(observed_groups) != set(registry_group_ids):
            errors.append("rel-ce-tpch: one or more registry baseline groups have no estimator")
    return errors


def _canonical_config_paths(
    root: Path,
    manifest_paths: Iterable[Path],
    repository_root: Path = REPOSITORY_ROOT,
) -> list[Path]:
    """Return every semantic runtime input included in the reproducibility digest."""
    paths = {
        (root / "catalog.yaml").resolve(),
        (root / "compose.yaml").resolve(),
        (root / "schema/experiment-manifest.schema.json").resolve(),
        (root / "schema/run-manifest.schema.json").resolve(),
        (root / "observability/prometheus.yml").resolve(),
    }
    paths.update(path.resolve() for path in manifest_paths)
    for manifest_path in manifest_paths:
        manifest = load_yaml(manifest_path)
        workload = manifest.get("workload", {})
        for field in CE_CONTRACT_FIELDS:
            if field in workload:
                paths.add(_ce_contract_path(repository_root, workload[field], f"{manifest.get('id')}: {field}"))
    for relative_root in (
        "observability/grafana/provisioning",
        "observability/grafana/dashboards",
    ):
        tree_root = (root / relative_root).resolve()
        for current, directories, files in os.walk(tree_root, followlinks=False):
            current_path = Path(current)
            safe_directories: list[str] = []
            for directory in directories:
                candidate = current_path / directory
                if _is_link_like(candidate):
                    raise ValueError(f"canonical configuration may not contain a link: {candidate}")
                safe_directories.append(directory)
            directories[:] = safe_directories
            for filename in files:
                candidate = current_path / filename
                if _is_link_like(candidate):
                    raise ValueError(f"canonical configuration may not contain a link: {candidate}")
                paths.add(candidate.resolve())
                if len(paths) > MAX_CANONICAL_FILES:
                    raise ValueError("canonical configuration file limit exceeded")
    for path in paths:
        if (
            not (_inside(path, root) or _inside(path, repository_root))
            or not path.is_file()
            or _is_link_like(path)
        ):
            raise ValueError(f"canonical configuration escapes, is linked, or is missing: {path}")
    return sorted(paths, key=lambda path: _canonical_config_key(path, root, repository_root))


def _canonical_config_key(path: Path, root: Path, repository_root: Path) -> str:
    if _inside(path, root):
        return path.relative_to(root).as_posix()
    return path.relative_to(repository_root).as_posix()


def _canonical_value(path: Path) -> Any:
    size = path.stat().st_size
    if size > MAX_CANONICAL_BYTES:
        raise ValueError(f"canonical configuration exceeds {MAX_CANONICAL_BYTES} bytes: {path}")
    if path.suffix == ".json":
        return json.loads(path.read_text(encoding="utf-8"))
    if path.suffix in {".yaml", ".yml"}:
        return load_yaml(path)
    return {"sha256": hashlib.sha256(path.read_bytes()).hexdigest()}


def provisioning_errors(root: Path) -> list[str]:
    errors: list[str] = []
    datasource_path = root / "observability/grafana/provisioning/datasources/prometheus.yaml"
    provider_path = root / "observability/grafana/provisioning/dashboards/provider.yaml"
    dashboard_path = root / "observability/grafana/dashboards/driftbench-experiment.json"
    datasource = load_yaml(datasource_path)
    provider = load_yaml(provider_path)
    dashboard = json.loads(dashboard_path.read_text(encoding="utf-8"))
    if datasource.get("apiVersion") != 1 or provider.get("apiVersion") != 1:
        errors.append("grafana: provisioning apiVersion must be 1")
    datasources = datasource.get("datasources", [])
    if len([item for item in datasources if item.get("isDefault") is True]) != 1:
        errors.append("grafana: exactly one default datasource is required")
    uids = {item.get("uid") for item in datasources}
    if "driftbench-prometheus" not in uids:
        errors.append("grafana: driftbench-prometheus datasource UID is missing")
    for item in datasources:
        if item.get("url") == "http://localhost:9090" or "127.0.0.1" in str(item.get("url", "")):
            errors.append("grafana: datasource must use Compose service DNS, not localhost")
    providers = provider.get("providers", [])
    if len(providers) != 1 or providers[0].get("options", {}).get("path") != "/var/lib/grafana/dashboards":
        errors.append("grafana: dashboard provider path is not the mounted dashboard path")
    if providers and (providers[0].get("allowUiUpdates") is not False or providers[0].get("disableDeletion") is not True):
        errors.append("grafana: provisioned dashboards must be immutable in the UI")
    for value in _walk(dashboard):
        if isinstance(value, dict) and "uid" in value and value.get("type") == "prometheus":
            if value.get("uid") not in uids:
                errors.append(f"grafana: dashboard references unknown datasource UID {value.get('uid')}")
    if dashboard.get("uid") != "driftbench-experiment":
        errors.append("grafana: stable dashboard UID is required")
    return errors


def audit(
    root: Path = LAB_ROOT,
    repository_root: Path = REPOSITORY_ROOT,
) -> tuple[list[str], str, list[dict[str, Any]]]:
    root = root.resolve()
    repository_root = repository_root.resolve()
    errors: list[str] = []
    catalog = load_yaml(root / "catalog.yaml")
    schema = json.loads((root / "schema/experiment-manifest.schema.json").read_text(encoding="utf-8"))
    json.loads((root / "schema/run-manifest.schema.json").read_text(encoding="utf-8"))

    entries = catalog.get("experiments", [])
    ids = [entry.get("id") for entry in entries]
    if len(ids) != len(set(ids)):
        errors.append("catalog: experiment IDs must be unique")
    if set(ids) != EXPECTED_IDS or len(ids) != 11:
        errors.append(f"catalog: expected exactly 11 tracker IDs, observed {sorted(ids)}")

    manifests: list[dict[str, Any]] = []
    manifest_paths: list[Path] = []
    for entry in entries:
        try:
            relative = Path(str(entry.get("manifest", "")))
            manifest_path = (root / relative).resolve()
        except (OSError, ValueError):
            errors.append(f"catalog: malformed manifest path: {entry.get('manifest', '')}")
            continue
        if not _inside(manifest_path, root) or not manifest_path.is_file():
            errors.append(f"catalog: manifest path escapes or is missing: {relative}")
            continue
        manifest = load_yaml(manifest_path)
        manifests.append(manifest)
        manifest_paths.append(manifest_path)
        if manifest.get("id") != entry.get("id") or manifest.get("component") != entry.get("component"):
            errors.append(f"catalog: identity mismatch for {relative}")
        if manifest.get("target_stack", {}).get("compose_profile") != entry.get("compose_profile"):
            errors.append(f"catalog: compose profile mismatch for {entry.get('id')}")
        try:
            jsonschema.Draft202012Validator(schema).validate(manifest)
        except jsonschema.ValidationError as exc:
            location = "/".join(str(part) for part in exc.absolute_path)
            errors.append(f"{entry.get('id')}: schema {location or '<root>'}: {exc.message}")
        errors.extend(manifest_semantic_errors(manifest, catalog))
        errors.extend(ce_contract_errors(manifest, repository_root))

        tracker = (repository_root / manifest.get("tracker", {}).get("path", "")).resolve()
        if not _inside(tracker, repository_root) or not tracker.is_file():
            errors.append(f"{entry.get('id')}: tracker README is missing or escapes repository")
        else:
            expected = f"**Repository status:** {manifest['tracker']['repository_status']}"
            if expected not in tracker.read_text(encoding="utf-8"):
                errors.append(f"{entry.get('id')}: tracker status does not match {expected}")

    compose = load_yaml(root / "compose.yaml")
    errors.extend(compose_errors(compose, catalog, root, repository_root))
    prometheus = load_yaml(root / "observability/prometheus.yml")
    errors.extend(prometheus_errors(prometheus))
    errors.extend(provisioning_errors(root))

    canonical_files: dict[str, Any] = {}
    try:
        for path in _canonical_config_paths(root, manifest_paths, repository_root):
            canonical_files[_canonical_config_key(path, root, repository_root)] = _canonical_value(path)
    except (OSError, UnicodeError, ValueError, json.JSONDecodeError, yaml.YAMLError) as exc:
        errors.append(f"canonical configuration: {exc}")
    digest_input = json.dumps(canonical_files, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
    digest = hashlib.sha256(digest_input).hexdigest()
    return sorted(set(errors)), digest, manifests


def main() -> int:
    errors, digest, manifests = audit()
    if errors:
        print(f"BLOCKED: {len(errors)} offline experiment-lab policy violation(s)")
        for error in errors:
            print(f"- {error}")
        return 1
    print(f"PASS: {len(manifests)} review-only manifests and one Compose blueprint")
    print(f"Canonical configuration SHA-256: {digest}")
    print("No Docker command, network call, database, benchmark, or workload was used by this audit.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
