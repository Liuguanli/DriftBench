from __future__ import annotations

import contextlib
import hashlib
import importlib
import io
import json
import os
from pathlib import Path
import random
import shutil
import socket
import subprocess
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import patch
from uuid import uuid4

import numpy as np

from driftbench import catalog
from driftbench.cache import datasets
from driftbench.cache.errors import CacheTransportError
from docs.benchmark_walkthroughs import _build_notebooks as builder
from docs.benchmark_walkthroughs import _walkthrough as walkthrough
from scripts.export_azure_catalog import build_snapshot
from test.benchmarks.ldbc.fixtures import PRIVATE_DUMMY, write_config, write_data, write_parameters
from test.benchmarks.ssb.test_adapter import write_source
from test.benchmarks.tpch import test_adapter as tpch_fixtures
from test.catalog_fixtures import CONFIG, STAMP, Inventory
from test.test_catalog_materialize import ReadBackend


ROOT = Path(__file__).resolve().parents[1]
DOCS = ROOT / "docs" / "benchmark_walkthroughs"


class WalkthroughTests(unittest.TestCase):
    def setUp(self):
        self.base = Path(os.environ.get(
            "DRIFTBENCH_WALKTHROUGH_TEST_ROOT", str(Path.home() / ".driftbench" / "walkthrough-tests")
        )).resolve()
        self.base.mkdir(parents=True, exist_ok=True)
        self.root = self.base / uuid4().hex
        self.root.mkdir()
        self.capture = contextlib.redirect_stdout(io.StringIO())
        self.capture.__enter__()

    def tearDown(self):
        self.capture.__exit__(None, None, None)
        self.assertEqual(self.root.parent, self.base)
        self.assertEqual(len(self.root.name), 32)
        shutil.rmtree(self.root)

    def create(self, name="ycsb", **options):
        return walkthrough.Walkthrough(name, output_dir=self.root / "runs",
                                       package_target="checkout", **options)

    def azure_fixture(self):
        store = Inventory()
        rows = [tpch_fixtures.TPCHAdapterTests._LINEITEM_TBL.format(ok=i, qty=10 + i) for i in range(1, 9)]
        store.dataset(table_data={"lineitem": "".join(rows).encode("utf-8")})
        snapshot = build_snapshot(CONFIG, store.listing(), store.read, observed_at=STAMP)
        return snapshot, ReadBackend(store)

    def test_azure_preparation_downloads_once_then_preserves_cached_inputs(self):
        snapshot, backend = self.azure_fixture()
        entry_id = snapshot["entries"][0]["id"]
        cache_dir = self.root / "datasets"
        with patch.object(catalog, "_load", return_value=snapshot), \
                patch.object(walkthrough, "TPCH_AZURE_DATASET_ID", entry_id):
            first = self.create("tpch")
            with patch.object(datasets, "_build_backend", return_value=backend):
                preparation = first.prepare_azure_data(entry_id, cache_dir=cache_dir)
            before = {path: Path(path).read_bytes() for path in preparation["files"]}
            with walkthrough.local_operations():
                report = first.run_all()
            self.assertEqual(report["status"], "PASS")
            self.assertTrue(report["network_or_database_execution"])
            self.assertFalse(report["database_execution"])
            self.assertEqual(report["steps"]["load_data"]["rows"], 8)
            self.assertEqual(report["steps"]["drift_data"]["actual_rows"], 4)
            self.assertFalse(report["steps"]["generate_data"]["source_scale_is_user_declared"])
            second = self.create("tpch")
            with patch.object(datasets, "_build_backend", side_effect=AssertionError("no second download")), \
                    walkthrough.local_operations():
                reused = second.prepare_azure_data(entry_id, cache_dir=cache_dir,
                                                   credential_env_file=self.root / "unread.env")
                second_report = second.run_all()
            self.assertEqual(reused["cache_outcome"], "hit")
            self.assertEqual(second_report["status"], "PASS")
            self.assertFalse(second_report["network_or_database_execution"])
            self.assertEqual(first.source_scale_factor, "0.01")
            self.assertEqual(second.inputs["source_dir"], first.inputs["source_dir"])
            self.assertEqual({path: Path(path).read_bytes() for path in before}, before)
            self.assertEqual(len(backend.reads), 10)
            self.assertNotIn("unread.env", json.dumps(second_report))

    def test_azure_preparation_rejects_other_inputs_and_reports_missing_api(self):
        snapshot, _ = self.azure_fixture()
        entry_id = snapshot["entries"][0]["id"]
        with patch.object(catalog, "_load", return_value=snapshot), \
                patch.object(walkthrough, "TPCH_AZURE_DATASET_ID", entry_id), \
                patch.object(catalog, "materialize", side_effect=AssertionError("must not download")):
            with self.assertRaisesRegex(ValueError, "only available"):
                self.create().prepare_azure_data(entry_id, cache_dir=self.root / "cache")
            with self.assertRaisesRegex(ValueError, "either"):
                self.create("tpch", source_scale_factor="10").prepare_azure_data(entry_id, cache_dir=self.root / "cache")
            demo = self.create("tpch")
            with self.assertRaisesRegex(ValueError, "does not download SF10"):
                demo.prepare_azure_data("another-entry", cache_dir=self.root / "cache")
            with self.assertRaisesRegex(ValueError, "separate"):
                demo.prepare_azure_data(entry_id, cache_dir=demo.root)
            snapshot["entries"][0]["parameters"]["scale_factor"] = "10"
            with self.assertRaisesRegex(ValueError, "pinned SF0.01"):
                demo.prepare_azure_data(entry_id, cache_dir=self.root / "cache")
        demo = self.create("tpch")
        with patch.object(demo, "_import", return_value=(SimpleNamespace(), None)):
            result = demo.prepare_azure_data(walkthrough.TPCH_AZURE_DATASET_ID, cache_dir=self.root / "cache")
        self.assertEqual(result["status"], "UNAVAILABLE")
        self.assertEqual(demo.generate_data()["status"], "UNAVAILABLE")
        self.assertFalse(demo.report()["network_or_database_execution"])

    def test_failed_azure_repreparation_cannot_reuse_stale_data_success(self):
        snapshot, backend = self.azure_fixture()
        entry_id = snapshot["entries"][0]["id"]
        cache_dir = self.root / "datasets"
        with patch.object(catalog, "_load", return_value=snapshot), \
                patch.object(walkthrough, "TPCH_AZURE_DATASET_ID", entry_id):
            with patch.object(datasets, "_build_backend", return_value=backend):
                catalog.materialize(entry_id, cache_dir=cache_dir)
            demo = self.create("tpch")
            demo.prepare_azure_data(entry_id, cache_dir=cache_dir)
            demo.generate_data()
            demo.load_data()
            demo.drift_data()
            old = {path: path.read_bytes() for path in demo.data_result.files}
            with patch.object(catalog, "materialize", side_effect=CacheTransportError("read interrupted")):
                with self.assertRaisesRegex(CacheTransportError, "interrupted"):
                    demo.prepare_azure_data(entry_id, cache_dir=cache_dir)
            self.assertIsNone(demo.data_result)
            self.assertIsNone(demo.frame)
            self.assertIsNone(demo.drifted_frame)
            self.assertNotIn("source_dir", demo.inputs)
            self.assertEqual(demo.generate_data()["status"], "FAIL")
            report = demo.report()
            self.assertEqual(report["status"], "FAIL")
            self.assertIsNone(report["network_or_database_execution"])
            self.assertEqual({path: path.read_bytes() for path in old}, old)

    def test_azure_notebook_and_cli_share_the_explicit_preparation(self):
        snapshot, backend = self.azure_fixture()
        entry_id = snapshot["entries"][0]["id"]
        notebook = builder.notebook("tpch")
        settings = next(cell for cell in notebook["cells"] if cell["id"] == "settings")
        self.assertIn("AZURE_DATASET_ID = None", settings["source"])
        original_path = list(sys.path)
        sys.path.insert(0, str(DOCS))
        try:
            standalone_helper = importlib.import_module("_walkthrough")
            with patch.object(catalog, "_load", return_value=snapshot), \
                    patch.object(walkthrough, "TPCH_AZURE_DATASET_ID", entry_id), \
                    patch.object(standalone_helper, "TPCH_AZURE_DATASET_ID", entry_id), \
                    patch.object(datasets, "_build_backend", return_value=backend):
                namespace = {"__name__": "__azure_notebook_cells_test__"}
                for cell in notebook["cells"]:
                    if cell["cell_type"] == "code":
                        exec(compile(cell["source"], f"tpch.ipynb:{cell['id']}", "exec"), namespace)
                        if "parameters" in cell["metadata"].get("tags", []):
                            namespace.update(
                                PACKAGE_TARGET="checkout", OUTPUT_DIR=self.root / "notebook",
                                AZURE_DATASET_ID=entry_id, DATASET_CACHE_DIR=self.root / "datasets",
                            )
                self.assertEqual(namespace["report"]["status"], "PASS")
                self.assertEqual(namespace["report"]["steps"]["prepare_azure_data"]["cache_outcome"], "downloaded")
                command = ["tpch.py", "--package-target", "checkout", "--output-dir", str(self.root / "cli"),
                           "--azure-catalog-id", entry_id, "--dataset-cache-dir", str(self.root / "datasets")]
                with patch.object(sys, "argv", command), \
                        patch.object(datasets, "_build_backend", side_effect=AssertionError("no second download")):
                    self.assertEqual(walkthrough.main("tpch"), 0)
                report_path = next((self.root / "cli").glob("tpch-*/*-report/report.json"))
                cli_report = json.loads(report_path.read_text(encoding="utf-8"))
                self.assertEqual(cli_report["steps"]["prepare_azure_data"]["cache_outcome"], "hit")
                with patch.object(sys, "argv", command + ["--source-scale-factor", "10"]), \
                        contextlib.redirect_stderr(io.StringIO()):
                    with self.assertRaises(SystemExit) as error:
                        walkthrough.main("tpch")
                self.assertEqual(error.exception.code, 2)
                self.assertEqual(len(backend.reads), 10)
        finally:
            sys.path[:] = original_path

    def test_twelve_pairs_are_current_unexecuted_notebooks_and_passive_imports(self):
        self.assertEqual(len(walkthrough.RECIPES), 12)
        for filename, expected in builder.generated_files():
            with self.subTest(filename=filename):
                self.assertEqual((DOCS / filename).read_text(encoding="utf-8"), expected)
                if filename.endswith(".ipynb"):
                    doc = json.loads(expected)
                    self.assertEqual((doc["nbformat"], doc["nbformat_minor"]), (4, 5))
                    ids = [cell["id"] for cell in doc["cells"]]
                    self.assertEqual(len(ids), len(set(ids)))
                    for cell in doc["cells"]:
                        if cell["cell_type"] == "code":
                            self.assertIsNone(cell["execution_count"])
                            self.assertEqual(cell["outputs"], [])
                            compile(cell["source"], filename, "exec")
        with patch.object(walkthrough.Walkthrough, "__init__", side_effect=AssertionError("active import")), \
                patch.object(walkthrough.platform, "machine", side_effect=AssertionError("import queried host")), \
                patch.object(Path, "mkdir", side_effect=AssertionError("import wrote files")), \
                patch.object(subprocess, "Popen", side_effect=AssertionError("import started a process")):
            for name in walkthrough.RECIPES:
                module = importlib.import_module(f"docs.benchmark_walkthroughs.{name}")
                self.assertTrue(callable(module.create))

    def test_all_default_public_api_runs_have_truthful_capability_results(self):
        expected_rows = {"tpcds": 10000, "tpcc": 3000, "tpcc_skew": 6000,
                         "job": 500, "ycsb": 200, "dsb": 5000, "pgbench": 10}
        for name in walkthrough.RECIPES:
            with self.subTest(benchmark=name):
                demo = self.create(name)
                result = demo.run_all()
                self.assertEqual(json.loads(Path(result["report_file"]).read_text(encoding="utf-8")), result)
                self.assertEqual(set(result["steps"]), set(walkthrough.STEPS))
                self.assertTrue(result["inputs_unchanged"])
                self.assertFalse(result["network_or_database_execution"])
                self.assertNotIn("FAIL", [row["status"] for row in result["steps"].values()])
                if name in expected_rows:
                    self.assertEqual(result["steps"]["load_data"]["rows"], expected_rows[name])
                    self.assertEqual(result["steps"]["drift_data"]["actual_rows"], expected_rows[name] // 2)
                if name in ("tpch", "ssb", "ldbc"):
                    self.assertEqual(result["steps"]["generate_data"]["status"], "NEEDS_INPUT")
                if name in ("benchbase", "sysbench"):
                    self.assertEqual(result["steps"]["load_data"]["status"], "NOT_SUPPORTED")
                    self.assertEqual(result["steps"]["generate_data"]["artifact_kind"], "prepare_plan")
                if name != "ldbc":
                    stream = result["steps"]["query_stream"]
                    self.assertEqual(stream["status"], "PASS")
                    self.assertTrue(stream["deterministic"])
                    self.assertEqual(sum(stream["baseline"]["counts"].values()), 200)
                    self.assertTrue(all(len(item["source_sha256"]) == 64 for item in demo.templates))
                if name in ("tpcds", "ycsb", "benchbase", "sysbench", "pgbench"):
                    self.assertTrue(all(item["sql"] is None for item in demo.templates))
                if name == "ycsb":
                    manifest = json.loads(demo.query_results[0].metadata.read_text(encoding="utf-8"))
                    self.assertEqual(manifest["record_count"], 200)
                    self.assertEqual(stream["baseline"]["weights"]["ReadRecord"], 0.95)

    def test_supplied_tpch_and_ssb_inputs_are_imported_without_scale_inference(self):
        tpch = self.root / "tpch-input"
        tpch.mkdir()
        # Supplied layout fixture, not evidence of a genuine TPC-H scale factor.
        fixture = tpch_fixtures.TPCHAdapterTests
        for table in fixture._TABLES:
            if table != "lineitem":
                (tpch / f"{table}.tbl").write_text("1|value|\n", encoding="utf-8")
        rows = [fixture._LINEITEM_TBL.format(ok=i, qty=10 + i) for i in range(1, 9)]
        (tpch / "lineitem.tbl").write_text("".join(rows), encoding="utf-8")
        no_scale = self.create("tpch", source_dir=tpch)
        self.assertEqual(no_scale.generate_data()["status"], "NEEDS_INPUT")
        ssb = write_source(self.root / "ssb-input")
        line = (ssb / "lineorder.tbl").read_text(encoding="utf-8")
        (ssb / "lineorder.tbl").write_text(
            "".join(str(i) + line[line.index("|"):] for i in range(1, 5)), encoding="utf-8")
        for name, source, options, expected in (
            ("tpch", tpch, {"source_scale_factor": "0.01"}, 8),
            ("ssb", ssb, {}, 4),
        ):
            with self.subTest(benchmark=name):
                before = {path: path.read_bytes() for path in source.iterdir()}
                demo = self.create(name, source_dir=source, **options)
                result = demo.run_all()
                self.assertEqual(result["status"], "PASS")
                self.assertEqual(result["steps"]["load_data"]["rows"], expected)
                self.assertEqual(result["steps"]["drift_data"]["actual_rows"], expected // 2)
                self.assertEqual({path: path.read_bytes() for path in source.iterdir()}, before)

    def test_ldbc_uses_pipe_delimiters_real_parameters_and_keeps_config_private(self):
        source = write_data(self.root / "graph")
        parameters = write_parameters(self.root / "parameters")
        config = write_config(self.root / "native" / "config.properties")
        demo = self.create("ldbc", source_dir=source, parameters_dir=parameters, driver_config=config)
        result = demo.run_all()
        self.assertEqual(result["steps"]["load_data"]["rows"], 1)
        self.assertEqual(len(result["steps"]["load_data"]["columns"]), 9)
        self.assertEqual(len(demo.templates), 14)
        self.assertTrue(all(item["sql"] is None for item in demo.templates))
        self.assertEqual(result["steps"]["drift_data"]["status"], "NOT_SUPPORTED")
        self.assertEqual(result["steps"]["query_stream"]["status"], "PASS")
        self.assertEqual(result["steps"]["change_query_parameters"]["status"], "NOT_SUPPORTED")
        self.assertNotIn(PRIVATE_DUMMY, json.dumps(result))
        parameters.joinpath("interactive_1_param.txt").write_text("personId|firstName\n1|Grace\n", encoding="utf-8")
        with self.assertRaisesRegex(AssertionError, "input changed"):
            demo.report()

    def test_stream_controls_are_reproducible_and_invalid_weights_fail_loudly(self):
        demo = self.create(sample_size=73, seed=123, record_count=40)
        demo.generate_queries()
        first = demo.templates[0]["id"]
        target = {item["id"]: int(item["id"] == first) for item in demo.templates}
        baseline = demo.query_stream()
        changed = demo.change_mix(weights=target)
        self.assertEqual(changed["drifted"]["counts"][first], 73)
        self.assertEqual(sum(changed["drifted"]["counts"].values()), 73)
        self.assertAlmostEqual(sum(changed["drifted"]["weights"].values()), 1)
        self.assertEqual(demo.query_stream()["semantic_hash"], baseline["semantic_hash"])
        with self.assertRaisesRegex(ValueError, "match template IDs"):
            demo.change_mix(weights={"unknown": 1})
        self.assertEqual(demo.report()["status"], "FAIL")
        demo.generate_queries()
        self.assertNotIn("query_stream", demo.steps)
        self.assertNotIn("change_mix", demo.steps)

    def test_tpch_presents_bounded_weights_but_keeps_canonical_precision(self):
        demo = self.create("tpch")
        self.assertEqual(demo.generate_queries()["status"], "PASS")
        self.assertEqual(demo.query_stream()["status"], "PASS")
        changed = demo.change_mix()
        self.assertEqual(changed["status"], "PASS")
        self.assertNotIn("0.06666666666666667", repr(changed))
        self.assertEqual(
            changed["drifted"]["weights"],
            {
                "q1-1": 0.8,
                "q1-2": 0.066667,
                "q6-1": 0.066667,
                "q6-2": 0.066666,
            },
        )
        canonical = Path(changed["file"]).read_text(encoding="utf-8")
        self.assertIn("0.06666666666666667", canonical)

    def test_rerunning_data_invalidates_dependent_steps_and_preserves_old_files(self):
        demo = self.create(record_count=20)
        demo.generate_data()
        old = {path: path.read_bytes() for path in demo.data_result.files}
        demo.load_data()
        demo.drift_data()
        python_state, numpy_state = random.getstate(), np.random.get_state()
        demo.drift_data(scale=0.75)
        self.assertEqual(random.getstate(), python_state)
        actual = np.random.get_state()
        self.assertTrue(np.array_equal(actual[1], numpy_state[1]))
        self.assertEqual(actual[2:], numpy_state[2:])
        demo.generate_data()
        self.assertIsNone(demo.frame)
        self.assertNotIn("load_data", demo.steps)
        self.assertNotIn("drift_data", demo.steps)
        self.assertEqual({path: path.read_bytes() for path in old}, old)
        with self.assertRaisesRegex(ValueError, "scale"):
            demo.drift_data(scale=float("nan"))
        self.assertEqual(demo.report()["status"], "FAIL")

    def test_missing_adapter_api_dependency_and_parameters_are_distinct(self):
        demo = self.create()
        with patch.object(walkthrough.importlib, "import_module",
                          side_effect=ModuleNotFoundError("No module named pandas", name="pandas")):
            module, missing = demo._import("pandas")
        self.assertIsNone(module)
        self.assertEqual(missing["kind"], "dependency_missing")
        demo.module = None
        demo.missing = {"kind": "public_module_missing", "name": "driftbench.data.ycsb"}
        self.assertEqual(demo.generate_data()["kind"], "public_module_missing")
        demo = self.create(record_count=20)
        demo.generate_data()
        with patch.object(demo, "_import", return_value=(None, {"kind": "dependency_missing", "name": "pandas"})):
            self.assertEqual(demo.load_data()["kind"], "dependency_missing")
        demo.module = SimpleNamespace(data=lambda scale_factor=1: None)
        row = demo.generate_data()
        self.assertEqual(row["status"], "UNAVAILABLE")
        self.assertEqual(row["parameters"], ["record_count"])
        demo = self.create()
        demo.generate_queries()
        with patch.object(demo, "_import", return_value=(SimpleNamespace(), None)):
            self.assertEqual(demo.query_stream()["status"], "UNAVAILABLE")

    def test_invalid_paths_targets_parameters_and_external_actions_are_rejected(self):
        with self.assertRaisesRegex(ValueError, "Installed mode rejects"):
            walkthrough.Walkthrough("ycsb", output_dir=self.root / "unused", package_target="installed")
        self.assertFalse((self.root / "unused").exists())
        installed = SimpleNamespace(version=walkthrough.metadata.version("driftbench-db"),
                                    read_text=lambda name: "{}",
                                    locate_file=lambda name: self.root / "site-packages" / name)
        with patch.object(walkthrough, "_ROOT", self.root / "copied-companions"), \
                patch.object(walkthrough.metadata, "distribution", return_value=installed):
            with self.assertRaisesRegex(ValueError, "Installed mode rejects"):
                walkthrough.Walkthrough("ycsb", output_dir=self.root / "unused", package_target="installed")
        with self.assertRaises(ValueError):
            walkthrough.Walkthrough("ycsb", output_dir=Path.home(), package_target="checkout")
        if os.name == "nt":
            with self.assertRaisesRegex(ValueError, "shorter output_dir"):
                walkthrough.Walkthrough("ycsb", output_dir=self.root / ("a" * 101), package_target="checkout")
        source = self.root / "inputs"
        source.mkdir()
        with self.assertRaisesRegex(ValueError, "separate"):
            walkthrough.Walkthrough("tpch", output_dir=source, source_dir=source, package_target="checkout")
        for options in ({"sample_size": True}, {"sample_size": 0}, {"record_count": 100001}, {"seed": False}):
            with self.subTest(options=options), self.assertRaises((ValueError, TypeError)):
                self.create(**options)
        demo = self.create()
        with self.assertRaises(ValueError):
            demo._owned(self.root / "outside.sql")
        with walkthrough.local_operations():
            with self.assertRaisesRegex(RuntimeError, "local artifacts only"):
                subprocess.run([sys.executable, "-c", "raise AssertionError('must not execute')"])
            with self.assertRaisesRegex(RuntimeError, "local artifacts only"):
                socket.create_connection(("example.invalid", 443))

    def test_unexpected_generator_failures_do_not_leave_stale_success(self):
        demo = self.create()
        demo.generate_queries()
        factory = demo.module.queries

        def broken(**options):
            adapter = factory(**options)
            adapter.generate = lambda **kwargs: (_ for _ in ()).throw(RuntimeError("fixture failure"))
            return adapter

        with patch.object(demo.module, "queries", side_effect=broken):
            with self.assertRaisesRegex(RuntimeError, "fixture failure"):
                demo.generate_queries()
        self.assertEqual(demo.report()["status"], "FAIL")
        self.assertEqual(demo.templates, [])
        self.assertNotIn("query_stream", demo.steps)

    def test_standalone_cli_from_outside_checkout_reports_the_explicit_target(self):
        cwd = self.root / "caller"
        cwd.mkdir()
        env = dict(os.environ, PYTHONPATH=str(ROOT), PYTHONDONTWRITEBYTECODE="1")
        command = [sys.executable, "-B", str(DOCS / "ycsb.py"), "--package-target", "checkout",
                   "--output-dir", str(self.root / "cli"), "--record-count", "20", "--sample-size", "30"]
        completed = subprocess.run(command, cwd=cwd, env=env, text=True, capture_output=True, timeout=120)
        self.assertEqual(completed.returncode, 0, completed.stderr)
        reports = list((self.root / "cli").glob("ycsb-*/*-report/report.json"))
        self.assertEqual(len(reports), 1)
        result = json.loads(reports[0].read_text(encoding="utf-8"))
        self.assertEqual(result["package"]["selected_target"], "checkout")
        self.assertEqual(Path(result["package"]["import_file"]), ROOT / "driftbench" / "__init__.py")
        self.assertEqual(result["steps"]["query_stream"]["sample_size"], 30)
        partial = subprocess.run(
            [sys.executable, "-B", str(DOCS / "tpch.py"), "--package-target", "checkout",
             "--output-dir", str(self.root / "partial")],
            cwd=cwd, env=env, text=True, capture_output=True, timeout=120)
        self.assertEqual(partial.returncode, 2, partial.stderr)

    def test_cold_start_bootstraps_host_metadata_before_guarded_import(self):
        script = r'''
import importlib
import json
from pathlib import Path
import platform
import subprocess
import sys
from unittest.mock import patch

with patch.object(platform, "machine", side_effect=AssertionError("active import")):
    from docs.benchmark_walkthroughs import _walkthrough as walkthrough
    from docs.benchmark_walkthroughs.tpch import create
assert "driftbench" not in sys.modules
events, architecture = [], []

def machine():
    events.append("machine")
    if not architecture:
        architecture.append(subprocess.check_output(
            [sys.executable, "-B", "-c", "import platform; print(platform.machine())"],
            text=True, timeout=30,
        ).strip())
        events.append("host_probe_completed")
    return architecture[0]

real_import = importlib.import_module

def guarded_import(name, package=None):
    if name == "driftbench":
        events.append("guarded_import")
        platform.machine()
        try:
            subprocess.check_output([sys.executable, "-B", "-c", "print('must not run')"])
        except RuntimeError as exc:
            assert "local artifacts only" in str(exc)
            events.append("process_blocked")
        else:
            raise AssertionError("Package import lost its process guard")
    return real_import(name, package)

with patch.object(platform, "machine", side_effect=machine), \
        patch.object(walkthrough.importlib, "import_module", side_effect=guarded_import):
    demo = create(output_dir=Path(sys.argv[1]), package_target="checkout")
assert events[:4] == ["machine", "host_probe_completed", "guarded_import", "machine"], events
assert events.count("host_probe_completed") == 1
assert "process_blocked" in events
assert demo.identity["host_architecture"] == architecture[0]
with walkthrough.local_operations():
    try:
        subprocess.run([sys.executable, "-B", "-c", "print('must not run')"])
    except RuntimeError as exc:
        assert "local artifacts only" in str(exc)
    else:
        raise AssertionError("Artifact operations lost their process guard")
print(json.dumps({"host_probe_count": events.count("host_probe_completed"),
                  "package_guard_active": True, "artifact_guard_active": True}))
'''
        cwd = self.root / "cold"
        cwd.mkdir()
        env = dict(os.environ, PYTHONPATH=str(ROOT), PYTHONDONTWRITEBYTECODE="1")
        completed = subprocess.run([sys.executable, "-B", "-c", script, str(self.root / "cold-out")],
                                   cwd=cwd, env=env, text=True, capture_output=True, timeout=120)
        self.assertEqual(completed.returncode, 0, completed.stderr)
        evidence = json.loads(completed.stdout.splitlines()[-1])
        self.assertEqual(evidence, {"host_probe_count": 1, "package_guard_active": True,
                                    "artifact_guard_active": True})

    def test_bootstrap_errors_propagate_and_unknown_architecture_stays_unknown(self):
        with patch.object(walkthrough.platform, "machine", side_effect=OSError("host metadata unavailable")):
            with self.assertRaisesRegex(OSError, "host metadata unavailable"):
                self.create("tpch")
        self.assertFalse((self.root / "runs").exists())
        with patch.object(walkthrough.platform, "machine", return_value=""):
            demo = self.create("tpch")
        self.assertEqual(demo.identity["host_architecture"], "")

    def test_all_notebook_python_cells_execute_with_only_explicit_target_and_output_overrides(self):
        original_path = list(sys.path)
        sys.path.insert(0, str(DOCS))
        try:
            for name in walkthrough.RECIPES:
                with self.subTest(benchmark=name):
                    path = DOCS / f"{name}.ipynb"
                    before = hashlib.sha256(path.read_bytes()).hexdigest()
                    notebook = json.loads(path.read_text(encoding="utf-8"))
                    namespace = {"__name__": "__walkthrough_notebook_test__"}
                    for cell in notebook["cells"]:
                        if cell["cell_type"] == "code":
                            exec(compile(cell["source"], f"{path.name}:{cell['id']}", "exec"), namespace)
                            if "parameters" in cell["metadata"].get("tags", []):
                                namespace.update(PACKAGE_TARGET="checkout", OUTPUT_DIR=self.root / "notebook-runs")
                    self.assertIn(namespace["report"]["status"], ("PASS", "PARTIAL"))
                    self.assertEqual(namespace["report"]["package"]["selected_target"], "checkout")
                    self.assertEqual(hashlib.sha256(path.read_bytes()).hexdigest(), before)
        finally:
            sys.path[:] = original_path


if __name__ == "__main__":
    unittest.main()
