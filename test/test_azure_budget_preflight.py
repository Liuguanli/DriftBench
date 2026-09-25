from __future__ import annotations

import copy
from datetime import datetime, timezone
from decimal import Decimal
import io
import json
from pathlib import Path
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import yaml

from scripts import azure_budget_preflight as guard


NOW = datetime(2026, 9, 16, tzinfo=timezone.utc)
BUDGET = {
    "schema": "driftbench.azure-budget/v1",
    "monthly_budget_aud": "100", "reserved_aud": "5",
    "azure_cache_config": ".private\\azure-cache.yaml",
}
ACCOUNT = {"hns": True, "sku": "Standard_LRS", "kind": "StorageV2", "tier": "Hot",
           "sftp": False, "nfs": None, "allow_public": False, "routing": None}


def evidence():
    return {
        "currency": "AUD", "subscription_mtd_aud": "10", "live_bytes": 2 * 1024**3,
        "inventory_scope": {"prefix": "team"},
        "prices": {"storage": "2", "read": "3", "write": "4", "namespace": "5", "egress": "0.2"},
        "datasets": [{
            "id": "sample", "benchmark": "tpch", "artifact_type": "data", "parameters": {"scale_factor": "1"},
            "path": "team/dataset", "files": [{"path": "table.tbl", "bytes": 1024**3}],
        }],
        "sources": {"kind": "offline fixture"}, "access": {"grants_performed": 0},
    }


def meter(name="Hot Read Operations", price="1", unit="10K", tier=0, started="2020-01-01T00:00:00Z"):
    return {"meterName": name, "retailPrice": price, "unitOfMeasure": unit,
            "tierMinimumUnits": tier, "effectiveStartDate": started, "currencyCode": "AUD",
            "isPrimaryMeterRegion": True, "productName": "fixture", "skuName": "fixture"}


class BudgetArithmeticTests(unittest.TestCase):
    def test_remaining_month_storage_does_not_double_count_mtd(self):
        report = guard.estimate(BUDGET, evidence(), now=NOW)
        self.assertEqual(report["remaining_month_fraction"], "0.5")
        self.assertEqual(Decimal(report["monthly_live_storage_aud"]), 4)
        self.assertEqual(Decimal(report["remaining_month_storage_obligation_aud"]), 2)
        self.assertEqual(Decimal(report["modeled_month_total_aud"]), 17)
        self.assertEqual(Decimal(report["conditional_headroom_aud"]), 83)
        self.assertFalse(report["authorizes_azure_mutations"])
        self.assertEqual(report["free_egress_credit_applied_gb"], "0")

    def test_capacity_is_exact_floor_and_scenarios_not_additive(self):
        data = evidence()
        data["datasets"].append({**copy.deepcopy(data["datasets"][0]), "id": "second", "path": "team/other"})
        report = guard.estimate(BUDGET, data, now=NOW)
        headroom = Decimal(report["conditional_headroom_aud"])
        for entry in report["datasets"]:
            n, cost = entry["conditional_full_downloads"], Decimal(entry["estimated_aud_per_full_download"])
            self.assertLessEqual(n * cost, headroom)
            self.assertGreater((n + 1) * cost, headroom)
        self.assertTrue(report["capacity_scenarios_share_one_budget"])
        self.assertLess(report["whole_catalog"]["conditional_full_downloads"],
                        report["datasets"][0]["conditional_full_downloads"])

    def test_upload_includes_storage_delta_operations_and_two_full_readbacks(self):
        report = guard.estimate(BUDGET, evidence(), now=NOW, mode="pre-upload",
                                planned_bytes=1024**3, planned_files=2, verification_downloads=2)
        self.assertEqual(Decimal(report["monthly_live_storage_after_upload_aud"]), 6)
        self.assertEqual(Decimal(report["remaining_month_storage_obligation_aud"]), 3)
        upload = report["planned_upload"]
        self.assertEqual(upload["full_verification_downloads"], 2)
        self.assertEqual(Decimal(upload["verification_egress_aud"]), Decimal(2 * 1024**3) / 1_000_000_000 * Decimal("0.2"))
        self.assertGreater(Decimal(upload["operations_allowance_aud"]), 0)
        self.assertEqual(upload["upload_bandwidth_aud"], "0")

    def test_over_budget_blocks_and_never_produces_negative_capacity(self):
        report = guard.estimate({**BUDGET, "monthly_budget_aud": "1"}, evidence(), now=NOW)
        self.assertEqual(report["status"], "BLOCKED")
        self.assertEqual(report["conditional_headroom_aud"], "0")
        self.assertEqual(report["datasets"][0]["conditional_full_downloads"], 0)

    def test_missing_plan_currency_prices_and_unsafe_numbers_fail_closed(self):
        for kwargs in ({"mode": "pre-upload"}, {"planned_bytes": 1},
                       {"mode": "pre-upload", "planned_bytes": -1, "planned_files": 1}):
            with self.subTest(kwargs=kwargs), self.assertRaises(guard.BudgetBlocked):
                guard.estimate(BUDGET, evidence(), now=NOW, **kwargs)
        for value in (True, "NaN", "Infinity", "-1", "1e1000000", None):
            with self.subTest(value=value), self.assertRaises(guard.BudgetBlocked):
                guard.money(value, "fixture")
        for field, value in (("currency", "USD"), ("subscription_mtd_aud", None)):
            data = evidence()
            data[field] = value
            with self.assertRaises(guard.BudgetBlocked):
                guard.estimate(BUDGET, data, now=NOW)
        data = evidence()
        data["prices"]["egress"] = 0
        with self.assertRaises(guard.BudgetBlocked):
            guard.estimate(BUDGET, data, now=NOW)

    def test_month_fraction_is_utc_and_handles_leap_months(self):
        self.assertEqual(guard.remaining_month_fraction(datetime(2024, 2, 1, tzinfo=timezone.utc)), 1)
        self.assertEqual(guard.remaining_month_fraction(datetime(2024, 2, 15, 12, tzinfo=timezone.utc)), Decimal("0.5"))
        with self.assertRaises(guard.BudgetBlocked):
            guard.remaining_month_fraction(datetime(2024, 2, 1))


class EvidenceTests(unittest.TestCase):
    def test_whole_subscription_bill_is_aggregate_aud_not_missing_zero(self):
        payload = {"properties": {"columns": [{"name": "Currency"}, {"name": "PreTaxCost"}],
                                  "rows": [["AUD", "0.01"], ["AUD", "0.02"]]}}
        self.assertEqual(guard.billing_total(payload), Decimal("0.03"))
        for change in ({"rows": []}, {"rows": [["USD", 1]]}, {"nextLink": "another page"}, {"columns": []}):
            altered = copy.deepcopy(payload)
            altered["properties"].update(change)
            with self.subTest(change=change), self.assertRaises(guard.BudgetBlocked):
                guard.billing_total(altered)

    def test_price_selection_uses_current_paid_tier_and_exact_currency_unit(self):
        items = [meter(price="2"), meter(price="3", started="2025-01-01T00:00:00Z"),
                 meter(price="1", started="2030-01-01T00:00:00Z")]
        self.assertEqual(guard.select_meter(items, "Hot Read Operations", "10K", NOW)["retailPrice"], "3")
        egress = [meter("Standard Data Transfer Out", "0", "1 GB", 0),
                  meter("Standard Data Transfer Out", "0.2", "1 GB", 100),
                  meter("Standard Data Transfer Out", "0.1", "1 GB", 10000)]
        self.assertEqual(guard.select_meter(egress, "Standard Data Transfer Out", "1 GB", NOW,
                                           paid_egress=True)["tierMinimumUnits"], 100)
        for change in ({"currencyCode": "USD"}, {"unitOfMeasure": "100"}, {"effectiveStartDate": "broken"}):
            with self.subTest(change=change), self.assertRaises(guard.BudgetBlocked):
                guard.select_meter([{**meter(), **change}], "Hot Read Operations", "10K", NOW)
        with self.assertRaises(guard.BudgetBlocked):
            guard.select_meter([meter(price="1"), meter(price="2")], "Hot Read Operations", "10K", NOW)

    def test_pricing_pagination_is_bounded_and_never_accepts_incomplete_results(self):
        page = {"Items": [meter()], "NextPageLink": "https://prices.azure.com/api/retail/prices?next=1"}
        with patch.object(guard, "_public_json", return_value=page) as fetch:
            with self.assertRaisesRegex(guard.BudgetBlocked, "pagination"):
                guard.retail_items("fixture")
            self.assertEqual(fetch.call_count, 10)
        with self.assertRaises(guard.BudgetBlocked):
            guard._public_json("https://unapproved.invalid/api/retail/prices")

    def test_price_dates_are_ordered_by_instant_not_text(self):
        older = meter(price="1", started="2026-09-15T23:30:00+02:00")
        newer = meter(price="2", started="2026-09-15T22:00:00-01:00")
        selected = guard.select_meter([older, newer], "Hot Read Operations", "10K", NOW)
        self.assertEqual(selected["retailPrice"], "2")
        same_instant = meter(price="3", started="2026-09-15T23:00:00Z")
        with self.assertRaises(guard.BudgetBlocked):
            guard.select_meter([newer, same_instant], "Hot Read Operations", "10K", NOW)

    def test_supported_profile_and_live_inventory_include_unlisted_bytes(self):
        guard.validate_account(ACCOUNT)
        for change in ({"sku": "Standard_GRS"}, {"tier": "Cool"}, {"sftp": True},
                       {"allow_public": True}, {"routing": {"routingChoice": "InternetRouting"}}):
            with self.subTest(change=change), self.assertRaises(guard.BudgetBlocked):
                guard.validate_account({**ACCOUNT, **change})
        objects = [
            SimpleNamespace(name="team/dataset/table.tbl", size=1024**3, blob_tier="Hot"),
            SimpleNamespace(name="team/staging/unlisted.bin", size=123, blob_tier="Hot"),
            SimpleNamespace(name="team/folder", size=0, blob_tier=None),
        ]
        container = SimpleNamespace(list_blobs=lambda **kwargs: iter(objects))
        listing = guard.inventory(container, "team")
        self.assertEqual(sum(listing.values()), 1024**3 + 123)
        guard.reconcile(evidence()["datasets"], listing)
        with self.assertRaises(guard.BudgetBlocked):
            guard.reconcile(evidence()["datasets"], {})
        for item in (SimpleNamespace(name="team2/outside", size=1, blob_tier="Hot"),
                     SimpleNamespace(name="team/cold", size=1, blob_tier="Cool")):
            with self.assertRaises(guard.BudgetBlocked):
                guard.inventory(SimpleNamespace(list_blobs=lambda **kwargs: iter([item])), "team")
        def interrupted(**kwargs):
            yield objects[0]
            raise OSError("partial listing")
        with self.assertRaises(OSError):
            guard.inventory(SimpleNamespace(list_blobs=interrupted), "team")

    def test_collector_uses_only_metadata_reads_and_the_subscription_query(self):
        from driftbench import catalog
        from driftbench.cache.models import AzureHNSCacheConfig, RemoteCacheMode
        from driftbench.cache import requests as cache_requests

        source = {"account_url": "https://budgettest.dfs.core.windows.net",
                  "file_system": "sample-cache", "prefix": "team"}
        config = AzureHNSCacheConfig(**source, mode=RemoteCacheMode.READ)
        calls = []

        class Credential:
            def __init__(self, **kwargs):
                pass
            def __enter__(self):
                return self
            def __exit__(self, *args):
                pass
            def get_token(self, scope):
                assert scope == "https://management.azure.com/.default"
                return SimpleNamespace(token="fake-token-not-for-reports")

        class Container:
            def get_container_properties(self):
                calls.append("container properties")
                return SimpleNamespace(public_access=None)
            def list_blobs(self, **kwargs):
                assert kwargs == {"name_starts_with": "team/", "include": ["metadata"]}
                calls.append("scoped inventory")
                return iter([SimpleNamespace(name="team/dataset/table.tbl", size=1024**3, blob_tier="Hot")])

        class Service:
            def __init__(self, endpoint, **kwargs):
                assert endpoint == "https://budgettest.blob.core.windows.net"
            def __enter__(self):
                return self
            def __exit__(self, *args):
                pass
            def get_container_client(self, name):
                assert name == "sample-cache"
                return Container()

        class Session:
            def __enter__(self):
                return self
            def __exit__(self, *args):
                pass
            def post(self, url, **kwargs):
                assert url == ("https://management.azure.com/subscriptions/00000000-0000-0000-0000-000000000000/"
                               "providers/Microsoft.CostManagement/query?api-version=2025-03-01")
                assert kwargs["json"]["type"] == "ActualCost"
                assert kwargs["json"]["timeframe"] == "MonthToDate"
                assert kwargs["allow_redirects"] is False
                calls.append("read-only cost query POST")
                return SimpleNamespace(status_code=200, content=b"{}",
                                       json=lambda: {"properties": {
                                           "columns": [{"name": "PreTaxCost"}, {"name": "Currency"}],
                                           "rows": [[1, "AUD"]],
                                       }})

        account = {**ACCOUNT, "id": "/subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/example",
                   "region": "australiaeast", "shared_key": True}
        def prices(query):
            if "Bandwidth" in query:
                values = [meter("Standard Data Transfer Out", "0.2", "1 GB", 100)]
                product, sku = "Rtn Preference: MGN", "Standard"
            else:
                values = [meter("Hot LRS Data Stored", "2", "1 GB/Month"),
                          meter("Hot Read Operations", "3"), meter("Hot Write Operations", "4"),
                          meter("Hot Iterative Write Operations", "5", "100")]
                product, sku = "Azure Data Lake Storage Gen2 Hierarchical Namespace", "Hot LRS"
            return [{**value, "armRegionName": "australiaeast", "productName": product,
                     "skuName": sku, "type": "Consumption"} for value in values]

        modules = {"azure.identity": SimpleNamespace(AzureCliCredential=Credential),
                   "azure.storage.blob": SimpleNamespace(BlobServiceClient=Service),
                   "requests": SimpleNamespace(Session=Session)}
        with patch.dict(sys.modules, modules), \
                patch.object(cache_requests, "load_azure_cache_config", return_value=config), \
                patch.object(catalog, "info", return_value={"source": source}), \
                patch.object(catalog, "list", return_value=evidence()["datasets"]), \
                patch.object(guard.shutil, "which", return_value="az"), \
                patch.object(guard.subprocess, "run", return_value=SimpleNamespace(
                    returncode=0, stdout=json.dumps(account))) as command, \
                patch.object(guard, "retail_items", side_effect=prices):
            result = guard._collect_live(BUDGET, NOW)
        self.assertEqual(calls, ["container properties", "scoped inventory", "read-only cost query POST"])
        self.assertEqual(command.call_args.args[0][1:4], ["storage", "account", "show"])
        self.assertEqual(result["subscription_mtd_aud"], 1)
        self.assertEqual(result["access"]["grants_performed"], 0)
        self.assertNotIn("fake-token-not-for-reports", json.dumps(result, default=str))


class PrivateReportTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix="azure-budget-test-")
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name).resolve()
        (self.root / ".private").mkdir()
        self.patch = patch.object(guard, "REPO", self.root)
        self.patch.start()
        self.addCleanup(self.patch.stop)
        self.config = self.root / ".private" / "budget.yaml"
        self.config.write_text(yaml.safe_dump(BUDGET), encoding="utf-8")
        self.output = self.root / ".private" / "report.json"

    def test_private_budget_yaml_rejects_unknown_and_duplicate_fields(self):
        self.assertEqual(guard.load_budget(self.config), BUDGET)
        for extra in ('monthly_budget_aud: "999"\n', 'client_secret: DO-NOT-ECHO\n'):
            self.config.write_text(yaml.safe_dump(BUDGET) + extra, encoding="utf-8")
            with self.assertRaises(guard.BudgetBlocked) as result:
                guard.load_budget(self.config)
            self.assertNotIn("DO-NOT-ECHO", str(result.exception))

    def test_live_failure_replaces_previous_success_and_never_authorizes(self):
        self.output.write_text(json.dumps({"schema": guard.SCHEMA, "status": "ESTIMATE_READY"}))
        stdout = io.StringIO()
        with patch.object(guard, "collect", side_effect=guard.BudgetBlocked("pricing unavailable")), patch("sys.stdout", stdout):
            status = guard.main(["--config", str(self.config), "--output", str(self.output)])
        self.assertEqual(status, 2)
        report = json.loads(stdout.getvalue())
        self.assertEqual(report, json.loads(self.output.read_text()))
        self.assertEqual(report["status"], "BLOCKED")
        self.assertFalse(report["authorizes_azure_mutations"])

    def test_nonobject_pricing_replaces_stale_success_with_structured_block(self):
        for payload in (None, [], {"Items": [], "NextPageLink": 123}):
            with self.subTest(payload=payload):
                self.output.write_text(json.dumps({"schema": guard.SCHEMA, "status": "ESTIMATE_READY"}))
                stdout = io.StringIO()
                with patch.object(guard, "_public_json", return_value=payload), \
                        patch.object(guard, "collect", side_effect=lambda *_: guard.retail_items("fixture")), \
                        patch("sys.stdout", stdout):
                    status = guard.main(["--config", str(self.config), "--output", str(self.output)])
                report = json.loads(stdout.getvalue())
                self.assertEqual(status, 2)
                self.assertEqual(report, json.loads(self.output.read_text()))
                self.assertEqual(report["status"], "BLOCKED")
                self.assertTrue(report["private_report_updated"])

    def test_nonobject_existing_private_report_blocks_without_overwrite(self):
        for encoded in ("null", "[]"):
            with self.subTest(encoded=encoded):
                self.output.write_text(encoded)
                stdout = io.StringIO()
                with patch.object(guard, "collect", return_value=evidence()), patch("sys.stdout", stdout):
                    status = guard.main(["--config", str(self.config), "--output", str(self.output)])
                report = json.loads(stdout.getvalue())
                self.assertEqual(status, 2)
                self.assertEqual(report["status"], "BLOCKED")
                self.assertFalse(report["private_report_updated"])
                self.assertEqual(self.output.read_text(), encoded)

    def test_invalid_upload_plan_never_discovers_credentials(self):
        with patch.object(guard, "collect") as collect, patch("sys.stdout", io.StringIO()):
            status = guard.main(["--config", str(self.config), "--output", str(self.output), "--mode", "pre-upload"])
        self.assertEqual(status, 2)
        collect.assert_not_called()

    def test_reports_cannot_write_tracked_paths_or_replace_unrelated_private_files(self):
        with self.assertRaises(guard.BudgetBlocked):
            guard.write_private_report(self.root / "public.json", {"schema": guard.SCHEMA})
        self.assertFalse((self.root / "public.json").exists())
        self.output.write_text('{"unrelated":true}')
        with self.assertRaises(guard.BudgetBlocked):
            guard.write_private_report(self.output, {"schema": guard.SCHEMA})
        self.assertEqual(self.output.read_text(), '{"unrelated":true}')


class SkillContractTests(unittest.TestCase):
    def test_project_skill_is_versionable_but_financial_settings_remain_ignored(self):
        root = Path(__file__).resolve().parents[1]
        skill = root / ".agents" / "skills" / "driftbench-azure-budget" / "SKILL.md"
        text = skill.read_text(encoding="utf-8")
        front = yaml.safe_load(text.split("---", 2)[1])
        self.assertEqual(front["name"], "driftbench-azure-budget")
        self.assertTrue(front["description"])
        self.assertTrue((skill.parent / "references" / "workflow.md").is_file())
        self.assertTrue((skill.parent / "agents" / "openai.yaml").is_file())
        private = subprocess.run(["git", "check-ignore", "--no-index", ".private\\azure-budget.yaml"],
                                 cwd=root, capture_output=True, text=True)
        self.assertEqual(private.returncode, 0)
        versioned = subprocess.run(["git", "check-ignore", "--no-index", str(skill)],
                                   cwd=root, capture_output=True, text=True)
        self.assertEqual(versioned.returncode, 1)
        self.assertIn("driftbench-azure-budget", (root / "AGENTS.md").read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
