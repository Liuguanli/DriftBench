"""Read-only, AUD Azure cost estimates; never authorize or perform mutations."""

from __future__ import annotations

import argparse
import calendar
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_FLOOR
import json
from pathlib import Path
import shutil
import subprocess
import sys
from typing import Any
from urllib.parse import urlencode, urlsplit
from urllib.request import urlopen


REPO = Path(__file__).resolve().parents[1]
GIB = Decimal(1024**3)
EGRESS_GB = Decimal(1_000_000_000)
CHUNK = 4 * 1024 * 1024
SCHEMA = "driftbench.azure-budget-report/v1"
MAX_OBJECTS = 100_000


class BudgetBlocked(ValueError):
    """Essential evidence is unavailable, unsupported, or inconsistent."""


def require(condition: bool, message: str) -> None:
    if not condition:
        raise BudgetBlocked(message)


def money(value: Any, label: str) -> Decimal:
    require(type(value) in (int, float, str, Decimal), f"{label} must be numeric")
    try:
        result = Decimal(str(value))
    except InvalidOperation as exc:
        raise BudgetBlocked(f"{label} must be numeric") from exc
    require(result.is_finite() and 0 <= result <= Decimal("1e12"),
            f"{label} must be finite and between zero and 1e12")
    return result


def count(value: Any, label: str) -> int:
    require(type(value) is int and 0 <= value <= 2**63 - 1,
            f"{label} must be a non-negative signed-64-bit integer")
    return value


def text(value: Decimal) -> str:
    return format(value, "f")


def load_budget(path: Path) -> dict[str, Any]:
    import yaml

    require(path.is_file() and not path.is_symlink(), "budget configuration must be a regular file")
    require(path.stat().st_size <= 16 * 1024, "budget configuration exceeds 16 KiB")

    class UniqueLoader(yaml.SafeLoader):
        pass

    def mapping(loader, node, deep=False):
        result = {}
        for key_node, value_node in node.value:
            key = loader.construct_object(key_node, deep=deep)
            require(isinstance(key, str) and key not in result, "budget configuration has invalid or duplicate keys")
            result[key] = loader.construct_object(value_node, deep=deep)
        return result

    UniqueLoader.add_constructor(yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, mapping)
    try:
        value = yaml.load(path.read_text(encoding="utf-8"), Loader=UniqueLoader)
    except (yaml.YAMLError, UnicodeError) as exc:
        raise BudgetBlocked("budget configuration is not valid UTF-8 YAML") from exc
    require(isinstance(value, dict) and set(value) == {
        "schema", "monthly_budget_aud", "reserved_aud", "azure_cache_config",
    }, "budget configuration fields are invalid")
    require(value["schema"] == "driftbench.azure-budget/v1", "unsupported budget configuration schema")
    money(value["monthly_budget_aud"], "monthly budget")
    money(value["reserved_aud"], "explicit reserve")
    require(isinstance(value["azure_cache_config"], str) and value["azure_cache_config"],
            "azure_cache_config must identify the existing non-secret Azure configuration")
    return value


def remaining_month_fraction(now: datetime) -> Decimal:
    require(now.tzinfo is not None and now.utcoffset() is not None, "observation time must be timezone-aware")
    now = now.astimezone(timezone.utc)
    days = calendar.monthrange(now.year, now.month)[1]
    start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    total_seconds = Decimal(days * 86400)
    elapsed = Decimal(str((now - start).total_seconds()))
    return (total_seconds - elapsed) / total_seconds


def read_units(sizes: list[int]) -> int:
    return sum((count(size, "file size") + CHUNK - 1) // CHUNK + 2 for size in sizes)


def download_cost(sizes: list[int], prices: dict[str, Any]) -> Decimal:
    return (
        Decimal(sum(sizes)) / EGRESS_GB * money(prices["egress"], "egress rate")
        + Decimal(read_units(sizes)) / 10_000 * money(prices["read"], "read rate")
    )


def estimate(
    budget: dict[str, Any],
    evidence: dict[str, Any],
    *,
    now: datetime,
    mode: str = "report",
    planned_bytes: int = 0,
    planned_files: int = 0,
    verification_downloads: int = 2,
) -> dict[str, Any]:
    require(mode in {"report", "pre-upload"}, "unsupported report mode")
    limit = money(budget["monthly_budget_aud"], "monthly budget")
    reserve = money(budget["reserved_aud"], "explicit reserve")
    require(evidence["currency"] == "AUD", "billing/pricing currency must be AUD; no exchange rate is assumed")
    spent = money(evidence["subscription_mtd_aud"], "subscription MTD cost")
    stored = count(evidence["live_bytes"], "live inventory bytes")
    count(planned_bytes, "planned bytes")
    count(planned_files, "planned files")
    count(verification_downloads, "verification downloads")
    if mode == "report":
        require(planned_bytes == planned_files == 0, "planned uploads require pre-upload mode")
    else:
        require(planned_bytes > 0 and planned_files > 0,
                "pre-upload requires positive planned bytes and file count, including metadata")
    rates = {name: money(evidence["prices"][name], name + " price")
             for name in ("storage", "read", "write", "namespace", "egress")}
    require(all(rate > 0 for rate in rates.values()), "required paid rates must be positive")
    fraction = remaining_month_fraction(now)
    carrying = Decimal(stored) / GIB * rates["storage"]
    carrying_after = Decimal(stored + planned_bytes) / GIB * rates["storage"]
    remaining_storage = carrying_after * fraction
    transfer = Decimal(planned_bytes * verification_downloads) / EGRESS_GB * rates["egress"]
    operations = Decimal(0)
    if planned_files:
        # Aggregate rounding plus per-file overhead conservatively bounds ordinary chunk requests.
        units = (planned_bytes + CHUNK - 1) // CHUNK + 3 * planned_files
        operations = (
            Decimal(units) / 10_000 * rates["write"]
            + Decimal(units * verification_downloads) / 10_000 * rates["read"]
            + Decimal(planned_files + 4) / 100 * rates["namespace"]
        )
    modeled_total = spent + reserve + remaining_storage + transfer + operations
    headroom = max(Decimal(0), limit - modeled_total)
    scenarios = []
    corpus: dict[str, int] = {}
    for entry in evidence["datasets"]:
        sizes = [count(item["bytes"], "dataset file size") for item in entry["files"]]
        require(bool(sizes), "dataset must contain payload files")
        cost = download_cost(sizes, rates)
        require(cost > 0, "full-download cost could not be estimated")
        scenarios.append({
            "id": entry["id"], "benchmark": entry["benchmark"],
            "artifact_type": entry["artifact_type"], "parameters": entry["parameters"],
            "payload_bytes": sum(sizes), "files": len(sizes),
            "estimated_aud_per_full_download": text(cost),
            "conditional_full_downloads": int((headroom / cost).to_integral_value(rounding=ROUND_FLOOR)),
        })
        for item in entry["files"]:
            full = entry["path"] + "/" + item["path"]
            require(full not in corpus or corpus[full] == item["bytes"], "corpus file sizes disagree")
            corpus[full] = item["bytes"]
    corpus_cost = download_cost(list(corpus.values()), rates) if corpus else None
    return {
        "schema": SCHEMA,
        "status": "BLOCKED" if modeled_total > limit else "ESTIMATE_READY",
        "mode": mode,
        "authorizes_azure_mutations": False,
        "observed_at": now.astimezone(timezone.utc).isoformat(),
        "billing_month_utc": now.astimezone(timezone.utc).strftime("%Y-%m"),
        "currency": "AUD",
        "budget_scope": "entire linked Azure subscription",
        "inventory_scope": evidence["inventory_scope"],
        "monthly_budget_aud": text(limit),
        "subscription_mtd_aud": text(spent),
        "explicit_reserve_aud": text(reserve),
        "live_bytes": stored,
        "monthly_live_storage_aud": text(carrying),
        "monthly_live_storage_after_upload_aud": text(carrying_after),
        "remaining_month_fraction": text(fraction),
        "remaining_month_storage_obligation_aud": text(remaining_storage),
        "planned_upload": {
            "bytes": planned_bytes, "files": planned_files,
            "full_verification_downloads": verification_downloads if planned_files else 0,
            "upload_bandwidth_aud": "0",
            "verification_egress_aud": text(transfer),
            "operations_allowance_aud": text(operations),
        },
        "modeled_month_total_aud": text(modeled_total),
        "conditional_headroom_aud": text(headroom),
        "capacity_basis": "upper bound after known costs and explicit reserve; unknown future subscription charges reduce it",
        "download_definition": "one complete Internet download of payload files, not a SQL query or catalog browse",
        "capacity_scenarios_share_one_budget": True,
        "free_egress_credit_applied_gb": "0",
        "datasets": scenarios,
        "whole_catalog": {
            "payload_bytes": sum(corpus.values()),
            "estimated_aud_per_full_download": text(corpus_cost) if corpus_cost is not None else None,
            "conditional_full_downloads": (
                int((headroom / corpus_cost).to_integral_value(rounding=ROUND_FLOOR))
                if corpus_cost is not None else None
            ),
        },
        "warnings": [
            "MTD billing may lag. Future charges for other Azure resources are unknown, not assumed absent.",
            "Reserve is an explicit configured allowance, not proof of future subscription spending.",
            "Capacities assume all conditional headroom goes to ONE scenario; they cannot be added together.",
            "Unknown remaining free egress is credited as zero; first paid tiers are used without volume discounts.",
            "Storage uses GiB. Egress uses decimal GB conservatively; this is not a claim about invoice byte units.",
            "Namespace-index capacity, taxes, retries and optional-service costs are not fully metered here.",
            "These are conditional estimates, not a bill, a spending cap, or download quotas.",
            "Native reader approval permits repeated downloads; catalog browsing itself is offline.",
        ],
        "sources": evidence["sources"],
        "access": evidence["access"],
    }


def validate_account(account: dict[str, Any]) -> None:
    require(isinstance(account, dict), "account evidence must be an object")
    require(account.get("hns") is True and account.get("sku") == "Standard_LRS"
            and account.get("kind") == "StorageV2" and account.get("tier") == "Hot",
            "initial supported profile is HNS StorageV2 Standard_LRS Hot")
    require(not account.get("sftp") and not account.get("nfs"), "SFTP/NFS extra costs require a separately priced plan")
    require(account.get("allow_public") is False, "anonymous-access policy is not explicitly disabled; review required")
    routing = account.get("routing")
    if routing is None:
        routing = {}
    require(isinstance(routing, dict), "routing evidence must be an object")
    require(routing.get("routingChoice", "MicrosoftRouting") == "MicrosoftRouting",
            "non-default routing requires its own bandwidth prices")


def inventory(container: Any, prefix: str) -> dict[str, int]:
    boundary = prefix + "/" if prefix else ""
    result: dict[str, int] = {}
    for item in container.list_blobs(name_starts_with=boundary, include=["metadata"]):
        require(len(result) < MAX_OBJECTS, "inventory exceeds its bounded object limit")
        name = getattr(item, "name", None)
        require(isinstance(name, str) and name.startswith(boundary) and name not in result,
                "inventory path is invalid, escaped scope, or duplicated")
        size = count(getattr(item, "size", None), "inventory object size")
        raw_tier = getattr(item, "blob_tier", None)
        tier = str(raw_tier) if raw_tier is not None else None
        require(tier == "Hot" or (size == 0 and tier is None),
                "mixed, non-Hot, or unknown object tiers require a separately priced plan")
        result[name] = size
    return result


def reconcile(entries: list[dict[str, Any]], objects: dict[str, int]) -> None:
    for entry in entries:
        for item in entry["files"]:
            full = entry["path"] + "/" + item["path"]
            require(full in objects and objects[full] == item["bytes"],
                    "catalog differs from live inventory; refresh the catalog before relying on download estimates")


def billing_total(payload: dict[str, Any]) -> Decimal:
    require(isinstance(payload, dict), "billing response must be an object")
    props = payload.get("properties", {})
    require(isinstance(props, dict), "billing properties must be an object")
    require(isinstance(props.get("columns"), list)
            and all(isinstance(column, dict) for column in props["columns"]),
            "billing columns are missing or invalid")
    require(not props.get("nextLink"), "unexpected paginated aggregate bill; no partial total is accepted")
    columns = [column.get("name") for column in props.get("columns", [])]
    require(len(columns) == len(set(columns)) and "PreTaxCost" in columns and "Currency" in columns,
            "billing response lacks unique cost/currency columns")
    rows = props.get("rows")
    require(isinstance(rows, list) and bool(rows), "billing returned no rows; spend is unknown, not zero")
    total = Decimal(0)
    for row in rows:
        require(isinstance(row, list) and len(row) == len(columns), "invalid billing row")
        require(row[columns.index("Currency")] == "AUD", "subscription billing is not AUD; no conversion is assumed")
        total += money(row[columns.index("PreTaxCost")], "subscription billing cost")
    return total


def _public_json(url: str) -> dict[str, Any]:
    require(isinstance(url, str), "pricing URL must be a string")
    parsed = urlsplit(url)
    require(parsed.scheme == "https" and parsed.netloc == "prices.azure.com"
            and parsed.path == "/api/retail/prices", "pricing URL escaped the official API")
    with urlopen(url, timeout=45) as response:
        final = urlsplit(response.url)
        require(final.scheme == "https" and final.netloc == "prices.azure.com"
                and final.path == "/api/retail/prices", "unexpected pricing redirect")
        encoded = response.read(8 * 1024 * 1024 + 1)
    require(len(encoded) <= 8 * 1024 * 1024, "pricing response exceeded its size limit")
    payload = json.loads(encoded)
    require(isinstance(payload, dict), "pricing API response must be a JSON object")
    return payload


def retail_items(query: str) -> list[dict[str, Any]]:
    url = "https://prices.azure.com/api/retail/prices?" + urlencode({"currencyCode": "'AUD'", "$filter": query})
    items: list[dict[str, Any]] = []
    for _ in range(10):
        payload = _public_json(url)
        require(isinstance(payload, dict), "pricing API response must be a JSON object")
        require(isinstance(payload.get("Items"), list), "pricing API did not return items")
        items.extend(payload["Items"])
        url = payload.get("NextPageLink")
        require(url is None or isinstance(url, str), "pricing continuation must be a URL string or null")
        if not url:
            return items
    raise BudgetBlocked("pricing pagination exceeded its bound")


def select_meter(items: list[dict[str, Any]], name: str, unit: str, now: datetime, *, paid_egress=False) -> dict[str, Any]:
    matches = []
    for item in items:
        require(isinstance(item, dict), "pricing entries must be objects")
        if item.get("meterName") != name or item.get("isPrimaryMeterRegion") is not True:
            continue
        require(item.get("currencyCode") == "AUD" and item.get("unitOfMeasure") == unit,
                "pricing currency/unit mismatch")
        stamp = item.get("effectiveStartDate")
        require(isinstance(stamp, str), "pricing effective date is missing")
        try:
            started = datetime.fromisoformat(stamp.replace("Z", "+00:00"))
        except ValueError as exc:
            raise BudgetBlocked("pricing effective date is invalid") from exc
        require(started.tzinfo is not None, "pricing effective date must have a timezone")
        tier = money(item["tierMinimumUnits"], "price tier")
        price = money(item["retailPrice"], "retail price")
        if started <= now and price > 0 and (tier > 0 if paid_egress else tier == 0):
            matches.append((started, item))
    require(bool(matches), f"no current paid price for {name}")
    if paid_egress:
        first = min(money(item["tierMinimumUnits"], "price tier") for _, item in matches)
        matches = [(started, item) for started, item in matches
                   if money(item["tierMinimumUnits"], "price tier") == first]
    latest = max(started for started, _ in matches)
    current = [item for started, item in matches if started == latest]
    require(len({money(item["retailPrice"], "retail price") for item in current}) == 1,
            "ambiguous current prices")
    item = current[0]
    return {key: item[key] for key in (
        "currencyCode", "productName", "skuName", "meterName", "unitOfMeasure",
        "retailPrice", "tierMinimumUnits", "effectiveStartDate",
    )}


def collect(budget: dict[str, Any], now: datetime) -> dict[str, Any]:
    try:
        from azure.core.exceptions import AzureError
    except ImportError as exc:
        raise BudgetBlocked("live preflight needs the existing optional driftbench-db[azure] dependencies") from exc
    try:
        return _collect_live(budget, now)
    except AzureError as exc:
        raise BudgetBlocked("Azure authentication or scoped metadata reading failed; no cost or balance is assumed") from exc


def _collect_live(budget: dict[str, Any], now: datetime) -> dict[str, Any]:
    from driftbench import catalog
    from driftbench.cache.models import RemoteCacheMode
    from driftbench.cache.errors import CacheConfigurationError
    from driftbench.cache.requests import load_azure_cache_config
    try:
        import requests
        from azure.identity import AzureCliCredential
        from azure.storage.blob import BlobServiceClient
    except ImportError as exc:
        raise BudgetBlocked("live preflight needs the existing optional driftbench-db[azure] dependencies") from exc

    try:
        cache = load_azure_cache_config(REPO / budget["azure_cache_config"], mode=RemoteCacheMode.READ, credential_env_file=None)
    except CacheConfigurationError as exc:
        raise BudgetBlocked("Azure cache configuration is invalid; no credentials are read from it") from exc
    source = {"account_url": cache.account_url, "file_system": cache.file_system, "prefix": cache.prefix}
    require(catalog.info()["source"] == source, "catalog/config storage scopes differ")
    name = urlsplit(cache.account_url).hostname.split(".")[0]
    az = shutil.which("az")
    require(az is not None, "Azure CLI is missing; no account/billing evidence available")
    query = (
        "{id:id,region:location,sku:sku.name,kind:kind,tier:accessTier,hns:isHnsEnabled,"
        "sftp:isSftpEnabled,nfs:isNfsV3Enabled,allow_public:allowBlobPublicAccess,"
        "shared_key:allowSharedKeyAccess,routing:routingPreference}"
    )
    completed = subprocess.run(
        [az, "storage", "account", "show", "--name", name, "--query", query, "--output", "json", "--only-show-errors"],
        capture_output=True, text=True, check=False, timeout=90,
    )
    require(completed.returncode == 0, "Azure account discovery failed; check login and read permissions")
    account = json.loads(completed.stdout)
    validate_account(account)
    require(isinstance(account.get("id"), str), "account resource ID is missing or invalid")
    pieces = account["id"].split("/")
    require(len(pieces) >= 3 and pieces[1] == "subscriptions", "account subscription scope is invalid")
    from uuid import UUID
    subscription = str(UUID(pieces[2]))
    region = account["region"]
    require(isinstance(region, str) and region.isalnum(), "unsupported Azure region identifier")
    base_filter = f"armRegionName eq '{region}' and priceType eq 'Consumption'"
    storage_filter = (
        base_filter + " and productName eq 'Azure Data Lake Storage Gen2 Hierarchical Namespace'"
        " and skuName eq 'Hot LRS'"
    )
    bandwidth_filter = (
        base_filter + " and serviceName eq 'Bandwidth' and productName eq 'Rtn Preference: MGN'"
        " and skuName eq 'Standard'"
    )
    def scoped_prices(query: str, product: str, sku: str) -> list[dict[str, Any]]:
        items = retail_items(query)
        require(all(isinstance(item, dict) for item in items), "pricing entries must be objects")
        return [item for item in items if item.get("armRegionName") == region
                and item.get("productName") == product and item.get("skuName") == sku
                and item.get("type") == "Consumption"]

    storage = scoped_prices(storage_filter, "Azure Data Lake Storage Gen2 Hierarchical Namespace", "Hot LRS")
    bandwidth = scoped_prices(bandwidth_filter, "Rtn Preference: MGN", "Standard")
    meters = {
        "storage": select_meter(storage, "Hot LRS Data Stored", "1 GB/Month", now),
        "read": select_meter(storage, "Hot Read Operations", "10K", now),
        "write": select_meter(storage, "Hot Write Operations", "10K", now),
        "namespace": select_meter(storage, "Hot Iterative Write Operations", "100", now),
        "egress": select_meter(bandwidth, "Standard Data Transfer Out", "1 GB", now, paid_egress=True),
    }
    entries = catalog.list()
    with AzureCliCredential(process_timeout=30) as credential, BlobServiceClient(
        cache.account_url.replace(".dfs.", ".blob."), credential=credential,
        connection_timeout=15, read_timeout=45, retry_total=2,
    ) as service:
        container = service.get_container_client(cache.file_system)
        require(getattr(container.get_container_properties(), "public_access", "unknown") is None,
                "container privacy is not established")
        objects = inventory(container, cache.prefix)
        reconcile(entries, objects)
        token = credential.get_token("https://management.azure.com/.default").token
        url = f"https://management.azure.com/subscriptions/{subscription}/providers/Microsoft.CostManagement/query?api-version=2025-03-01"
        body = {"type": "ActualCost", "timeframe": "MonthToDate",
                "dataset": {"aggregation": {"totalCost": {"name": "PreTaxCost", "function": "Sum"}}}}
        with requests.Session() as session:
            session.trust_env = False
            response = session.post(url, headers={"Authorization": "Bearer " + token},
                                    json=body, timeout=(15, 60), allow_redirects=False)
            require(response.status_code == 200, f"subscription cost query failed (HTTP {response.status_code}); spend is unknown")
            require(len(response.content) <= 1024 * 1024, "billing aggregate response exceeded its limit")
            spent = billing_total(response.json())
    return {
        "currency": "AUD", "subscription_mtd_aud": spent,
        "live_bytes": sum(objects.values()), "datasets": entries,
        "inventory_scope": {**source, "listed_paths": len(objects), "region": region},
        "prices": {key: value["retailPrice"] for key, value in meters.items()},
        "sources": {
            "pricing": {"api": "https://prices.azure.com/api/retail/prices", "observed_at": now.isoformat(), "meters": meters},
            "billing": {"scope": "subscription linked to the configured storage account",
                        "subscription_id": subscription, "timeframe": "MonthToDate",
                        "api_version": "2025-03-01", "observed_at": now.isoformat()},
            "inventory": {"live": True, "includes_uncatalogued_objects": True,
                          "payload_bytes_downloaded": 0, "content_hashes_reverified": False},
        },
        "access": {
            "anonymous_account_access": False, "container_public_access": None,
            "shared_key_allowed": account.get("shared_key") is not False,
            "only_owner_has_access": "not established; existing roles, ACLs, keys and administrators require review",
            "grants_performed": 0,
        },
    }


def write_private_report(path: Path, report: dict[str, Any]) -> None:
    private = REPO / ".private"
    require(private.is_dir() and not private.is_symlink()
            and not getattr(private, "is_junction", lambda: False)(), "private report directory is unavailable")
    require(private.resolve() == REPO.resolve() / ".private", "private report directory must not redirect elsewhere")
    resolved = path.resolve()
    require(resolved.is_relative_to(private.resolve()) and not path.is_symlink(),
            "financial reports must remain under the ignored project .private directory")
    require(path.parent.is_dir(), "report parent directory must already exist")
    for parent in path.parents:
        if parent == private:
            break
        require(not parent.is_symlink() and not getattr(parent, "is_junction", lambda: False)(),
                "financial report path must not traverse a link")
        require(parent.resolve() == parent.absolute(), "financial report parent must not redirect elsewhere")
    if path.exists():
        require(path.is_file() and path.stat().st_size <= 4 * 1024 * 1024, "report target is not a bounded regular file")
        previous = json.loads(path.read_text(encoding="utf-8"))
        require(isinstance(previous, dict) and previous.get("schema") == SCHEMA,
                "refusing to overwrite an unrelated private file")
    import os
    import tempfile
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=path.parent, prefix=".azure-budget-", delete=False) as stream:
            temporary = Path(stream.name)
            json.dump(report, stream, ensure_ascii=True, indent=2)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        temporary.replace(path)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=REPO / ".private" / "azure-budget.yaml")
    parser.add_argument("--mode", choices=("report", "pre-upload"), default="report")
    parser.add_argument("--planned-upload-bytes", type=int, default=0)
    parser.add_argument("--planned-file-count", type=int, default=0)
    parser.add_argument("--verification-downloads", type=int, default=2)
    parser.add_argument("--output", type=Path, default=REPO / ".private" / "azure-budget-report.json")
    args = parser.parse_args(argv)
    try:
        budget = load_budget(args.config)
        # Validate requested work before credential discovery or network access.
        count(args.planned_upload_bytes, "planned bytes")
        count(args.planned_file_count, "planned files")
        count(args.verification_downloads, "verification downloads")
        require((args.mode == "report" and args.planned_upload_bytes == args.planned_file_count == 0)
                or (args.mode == "pre-upload" and args.planned_upload_bytes > 0 and args.planned_file_count > 0),
                "pre-upload requires planned bytes and file count; report mode must not carry an upload plan")
        now = datetime.now(timezone.utc)
        evidence = collect(budget, now)
        finished = datetime.now(timezone.utc)
        require(now.strftime("%Y-%m") == finished.strftime("%Y-%m"),
                "billing month changed during discovery; rerun with fresh evidence")
        report = estimate(budget, evidence, now=finished, mode=args.mode,
                          planned_bytes=args.planned_upload_bytes, planned_files=args.planned_file_count,
                          verification_downloads=args.verification_downloads)
    except (BudgetBlocked, OSError, ValueError, KeyError, TypeError, subprocess.SubprocessError) as exc:
        reason = str(exc) if isinstance(exc, BudgetBlocked) else "configuration, pricing, billing or network evidence failed"
        report = {"schema": SCHEMA, "status": "BLOCKED", "authorizes_azure_mutations": False,
                  "reason": reason, "no_zero_cost_fallback": True,
                  "observed_at": datetime.now(timezone.utc).isoformat()}
    try:
        report["private_report_updated"] = True
        write_private_report(args.output, report)
    except (BudgetBlocked, OSError, ValueError, TypeError):
        report = {"schema": SCHEMA, "status": "BLOCKED", "authorizes_azure_mutations": False,
                  "reason": "private report could not be safely saved; discard any previous report",
                  "private_report_updated": False, "observed_at": datetime.now(timezone.utc).isoformat()}
    print(json.dumps(report, ensure_ascii=True, indent=2))
    return 2 if report["status"] == "BLOCKED" else 0


if __name__ == "__main__":
    raise SystemExit(main())
