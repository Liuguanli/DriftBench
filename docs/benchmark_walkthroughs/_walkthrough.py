"""Local, capability-aware companions. No work is performed at import time."""

from __future__ import annotations

import argparse
from collections import Counter
from contextlib import contextmanager
import csv
import hashlib
import importlib
from importlib import metadata
import inspect
import json
import math
import os
from pathlib import Path
import platform
import random
import re
import socket
import stat
import subprocess
import sys
from unittest.mock import patch
import urllib.request
from uuid import uuid4
import xml.etree.ElementTree as ET


RECIPES = {
    "tpch": {"table": "lineitem", "data": {"mode": "copy"},
             "queries": [{"query_ids": [1, 6], "queries_per_template": 2, "shuffle": False}],
             "change": {"query_ids": [3, 6], "queries_per_template": 3},
             "options": ("query_ids", "queries_per_template", "seed", "shuffle", "scale")},
    "tpcds": {"table": "store_sales", "data": {"scale_factor": 1}, "queries": [{}]},
    "tpcc": {"table": "customer", "data": {"scale_factor": 1}, "queries": [{}]},
    "tpcc_skew": {"table": "customer", "data": {"scale_factor": 2, "hot_warehouse_fraction": 0.5},
                  "queries": [{"scale_factor": 2, "hot_warehouse_fraction": 0.5, "skew_factor": 0.99}],
                  "change": {"skew_factor": 1.5},
                  "options": ("scale_factor", "hot_warehouse_fraction", "skew_factor")},
    "job": {"table": "title", "data": {"scale_factor": 1}, "queries": [{}]},
    "ycsb": {"table": "usertable", "data": {"record_count": 200},
             "queries": [{"workload": "B", "run_seconds": 1, "target_rate": 10}],
             "change": {"workload": "A", "target_rate": 20},
             "options": ("workload", "run_seconds", "target_rate")},
    "dsb": {"table": "lineorder", "data": {"scale_factor": 1}, "queries": [{}]},
    "pgbench": {"table": "pgbench_tellers", "data": {"scale_factor": 1},
                "queries": [{"workload": "select_only", "clients": 1, "duration": 1, "rate": 10},
                            {"workload": "simple_update", "clients": 1, "duration": 1, "rate": 10}],
                "change": {"workload": "tpcb", "rate": 20},
                "options": ("workload", "clients", "duration", "rate")},
    "benchbase": {"table": None, "data": {"benchmark": "tpcc", "scale_factor": 1},
                  "queries": [{"benchmark": "tpcc", "scale_factor": 1,
                               "terminals": 1, "duration": 1, "rate": 10}],
                  "change": {"terminals": 2, "rate": 20},
                  "options": ("terminals", "duration", "rate")},
    "sysbench": {"table": None, "data": {"tables": 1, "table_size": 200},
                 "queries": [{"workload": "oltp_read_only", "tables": 1, "table_size": 200,
                              "threads": 1, "duration": 1, "rate": 10},
                             {"workload": "oltp_read_write", "tables": 1, "table_size": 200,
                              "threads": 1, "duration": 1, "rate": 10}],
                 "change": {"distribution": "zipfian", "rate": 20},
                 "options": ("workload", "threads", "duration", "rate", "distribution", "seed")},
    "ssb": {"table": "lineorder", "data": {}, "queries": [{}],
            "change": {"query_ids": ["1.1", "3.2", "4.3"]}, "options": ("query_ids",)},
    "ldbc": {"table": None, "data": {}, "queries": [{"read_only": True}]},
}
STEPS = ("environment", "generate_data", "load_data", "drift_data", "generate_queries",
         "query_stream", "change_mix", "change_query_parameters")
_ROOT = Path(__file__).resolve().parents[2]
_MAX_INPUT_BYTES = 64 * 1024 * 1024
TPCH_AZURE_DATASET_ID = (
    "tpch/data/immutable-dataset-v1/"
    "72764dad47622f7d5e0387b6faf623f2a4257648533b6f08040adcc6393cba2c"
)


def _check(condition, message):
    if not condition:
        raise AssertionError(message)


def _digest(path):
    _no_links(path)
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _no_links(path):
    for part in (path, *path.parents):
        if part.exists() or part.is_symlink():
            info = part.lstat()
            if part.is_symlink() or getattr(info, "st_file_attributes", 0) & stat.FILE_ATTRIBUTE_REPARSE_POINT:
                raise ValueError(f"Linked paths are not tutorial inputs/outputs: {part}")


def _positive_int(value, name, maximum):
    if type(value) is not int or not 1 <= value <= maximum:
        raise ValueError(f"{name} must be an integer from 1 to {maximum}")
    return value


@contextmanager
def local_operations():
    """Fail closed if an artifact API attempts a network or external action."""
    def denied(*args, **kwargs):
        raise RuntimeError("Walkthroughs allow local artifacts only, not network/driver/database execution")

    from contextlib import ExitStack
    python_state = random.getstate()
    numpy = sys.modules.get("numpy")
    numpy_state = numpy.random.get_state() if numpy is not None else None
    with ExitStack() as stack:
        for owner, name in (
            (subprocess, "run"), (subprocess, "Popen"), (subprocess, "call"),
            (subprocess, "check_call"), (subprocess, "check_output"), (os, "system"),
            (socket, "create_connection"), (socket.socket, "connect"), (socket.socket, "connect_ex"),
            (urllib.request, "urlopen"),
        ):
            stack.enter_context(patch.object(owner, name, side_effect=denied))
        if "psycopg2" in sys.modules:
            stack.enter_context(patch.object(sys.modules["psycopg2"], "connect", side_effect=denied))
        try:
            yield
        finally:
            random.setstate(python_state)
            if numpy_state is not None:
                numpy.random.set_state(numpy_state)


class Walkthrough:
    """One explicit package target and one persistent, exclusively owned run."""

    def __init__(self, benchmark, *, output_dir, package_target="installed", source_dir=None,
                 parameters_dir=None, driver_config=None, record_count=200, seed=42,
                 sample_size=200, source_scale_factor=None):
        if benchmark not in RECIPES:
            raise ValueError(f"Unknown benchmark: {benchmark}")
        if package_target not in ("installed", "checkout"):
            raise ValueError("package_target must be installed or checkout")
        if type(seed) is not int:
            raise TypeError("seed must be an integer")
        self.benchmark, self.recipe = benchmark, RECIPES[benchmark]
        self.seed = seed
        self.sample_size = _positive_int(sample_size, "sample_size", 100_000)
        self.record_count = _positive_int(record_count, "record_count", 100_000)
        self.source_scale_factor = source_scale_factor
        if source_scale_factor is not None:
            if benchmark != "tpch":
                raise ValueError("source_scale_factor is only used by TPC-H copy mode")
            if isinstance(source_scale_factor, bool) or not math.isfinite(float(source_scale_factor)) or float(source_scale_factor) <= 0:
                raise ValueError("source_scale_factor must declare a positive finite TPC-H scale")
            self.source_scale_factor = str(source_scale_factor)
        self.steps, self.inputs, self._input_snapshots = {}, {}, {}
        self.data_result, self.frame, self.drifted_frame = None, None, None
        self.query_results, self.templates, self.default_weights = [], [], {}
        self.baseline_weights = None
        self._counter = 0
        self.package_target = package_target
        self._azure_source_attached = False
        self._azure_network_usage = False
        # Windows metadata may need a read-only OS probe before NumPy's cold import.
        host_architecture = platform.machine()
        with local_operations():
            self.package = importlib.import_module("driftbench")
        origin = Path(self.package.__file__).resolve()
        try:
            distribution = metadata.distribution("driftbench-db")
        except metadata.PackageNotFoundError:
            distribution = None
        direct = json.loads(distribution.read_text("direct_url.json") or "{}") if distribution else {}
        editable = direct.get("dir_info", {}).get("editable", False)
        checkout = origin == (_ROOT / "driftbench" / "__init__.py").resolve()
        installed_origin = (Path(distribution.locate_file(Path("driftbench") / "__init__.py")).resolve()
                            if distribution else None)
        if package_target == "installed" and (checkout or editable or origin != installed_origin):
            raise ValueError(
                "Installed mode rejects a checkout/editable/unregistered import. Use a separate "
                "environment outside the checkout, or explicitly select package_target='checkout'. "
                f"Actual import: {origin}"
            )
        if package_target == "checkout" and not checkout:
            raise ValueError(f"Checkout mode requires this companion's repository package; actual import: {origin}")
        self.identity = {
            "selected_target": package_target,
            "distribution_version": distribution.version if distribution else None,
            "module_version": getattr(self.package, "__version__", None),
            "import_file": str(origin), "editable_install": editable,
            "host_architecture": host_architecture,
        }
        self.module, self.missing = self._import(f"driftbench.data.{benchmark}")
        for name, value in (("source_dir", source_dir), ("parameters_dir", parameters_dir),
                            ("driver_config", driver_config)):
            if value is not None:
                if name == "source_dir" and benchmark not in ("tpch", "ssb", "ldbc"):
                    raise ValueError(f"{benchmark} does not accept {name} in this walkthrough")
                if name != "source_dir" and benchmark != "ldbc":
                    raise ValueError(f"{name} is only used by the LDBC walkthrough")
                path = Path(value).expanduser().absolute()
                _no_links(path)
                if not path.exists() or (name.endswith("_dir") and not path.is_dir()):
                    raise FileNotFoundError(f"{name} must name an existing local input: {path}")
                if name == "driver_config" and not path.is_file():
                    raise ValueError("driver_config must be a regular local file")
                self.inputs[name] = path.resolve()
        base = Path(output_dir).expanduser().absolute()
        _no_links(base)
        base = base.resolve()
        if base in (Path(base.anchor), Path.home().resolve(), _ROOT):
            raise ValueError("Choose a dedicated output directory, not a filesystem/home/repository root")
        if os.name == "nt" and len(str(base)) > 100:
            raise ValueError("Choose a shorter output_dir (at most 100 characters on Windows) to leave room for benchmark paths")
        for path in self.inputs.values():
            if base == path or base in path.parents or path in base.parents:
                raise ValueError("Input and output directories must be separate")
        base.mkdir(parents=True, exist_ok=True)
        self.root = base / f"{benchmark}-{uuid4().hex}"
        self.root.mkdir(mode=0o700)
        self._mark("environment", "PASS", "Package identity observed; no database or Azure access.",
                   **self.identity)

    def _import(self, name):
        try:
            with local_operations():
                return importlib.import_module(name), None
        except ModuleNotFoundError as exc:
            kind = "public_module_missing" if name.startswith("driftbench.") and exc.name == name else "dependency_missing"
            return None, {"kind": kind, "name": exc.name,
                          "action": "Use a package that provides this API, or install its declared dependency."}

    def _mark(self, stage, status, reason, **evidence):
        row = {"status": status, "reason": reason, **evidence}
        self.steps[stage] = row
        return row

    def _begin(self, stage):
        dependants = {
            "prepare_azure_data": ("generate_data", "load_data", "drift_data"),
            "generate_data": ("load_data", "drift_data"),
            "load_data": ("drift_data",),
            "generate_queries": ("query_stream", "change_mix", "change_query_parameters"),
            "query_stream": ("change_mix",),
        }
        for name in dependants.get(stage, ()):
            self.steps.pop(name, None)
        self._mark(stage, "FAIL", "Step did not complete; resolve the raised exception before reusing its result.")
        if stage in ("prepare_azure_data", "generate_data"):
            self.data_result = None
        if stage in ("prepare_azure_data", "generate_data", "load_data"):
            self.frame = None
        if stage in ("prepare_azure_data", "generate_data", "load_data", "drift_data"):
            self.drifted_frame = None
        if stage == "generate_queries":
            self.query_results, self.templates, self.default_weights = [], [], {}
        if stage in ("generate_queries", "query_stream"):
            self.baseline_weights = None

    def _unavailable(self, stage):
        if self.module is None:
            return self._mark(stage, "UNAVAILABLE", "Adapter import is unavailable.", **self.missing)
        return None

    def _requires(self, stage, earlier):
        row = self.steps.get(earlier)
        if row is None or row["status"] != "PASS":
            status = row["status"] if row else "NEEDS_INPUT"
            return self._mark(stage, status, f"Requires {earlier}; run or resolve that step first.",
                              prerequisite=earlier)
        return None

    def _stage(self, name):
        _no_links(self.root)
        self._counter += 1
        path = self.root / f"{self._counter:02d}-{name}"
        path.mkdir()
        return path

    def _owned(self, path):
        path = Path(path).absolute()
        _no_links(path)
        resolved = path.resolve()
        if resolved == self.root or self.root not in resolved.parents:
            raise ValueError("Returned artifact escaped the exclusively owned run directory")
        return resolved

    def _input_snapshot(self, names):
        files = []
        for name in names:
            path = self.inputs[name]
            files.extend(sorted(path.rglob("*")) if path.is_dir() else [path])
        result, total = {}, 0
        for path in files:
            _no_links(path)
            if path.is_file():
                total += path.stat().st_size
                if total > _MAX_INPUT_BYTES:
                    raise ValueError("Tutorial inputs exceed 64 MiB; supply a small local fixture, not SF10")
                result[str(path)] = _digest(path)
        return result

    def _generate(self, kind, options, stage, inputs=()):
        factory = getattr(self.module, kind, None)
        if not callable(factory):
            return None, self._mark(stage, "UNAVAILABLE", f"Public {kind} factory is absent.")
        signature = inspect.signature(factory)
        unsupported = set(options) - set(signature.parameters)
        if unsupported and not any(p.kind == p.VAR_KEYWORD for p in signature.parameters.values()):
            return None, self._mark(stage, "UNAVAILABLE", "Public factory parameters are absent.",
                                    parameters=sorted(unsupported), action="Select a package with this public API.")
        before = self._input_snapshot(inputs)
        for name in inputs:
            source = self.inputs[name]
            self._input_snapshots[name] = {path: digest for path, digest in before.items()
                                           if Path(path) == source or source in Path(path).parents}
        destination = self._stage(stage)
        with local_operations():
            result = factory(**options).generate(output_dir=destination, force=False)
        files = [self._owned(path) for path in result.files]
        manifest = self._owned(result.metadata)
        self._owned(result.output_dir)
        _check(all(path.is_file() for path in [*files, manifest]), "Generated artifacts are missing")
        _check(self._input_snapshot(inputs) == before, "Supplied input changed during artifact generation")
        _check(result.benchmark == self.benchmark and result.artifact_type == kind,
               "GenerationResult identity differs from the requested adapter")
        return result, {"files": [str(path) for path in files], "manifest": str(manifest),
                        "file_count": len(files), "inputs_unchanged": True}

    def prepare_azure_data(self, entry_id, *, cache_dir, credential_env_file=None):
        if self.benchmark != "tpch":
            raise ValueError("Azure preparation is only available for the TPC-H SF0.01 walkthrough")
        if ("source_dir" in self.inputs or self.source_scale_factor is not None) and not self._azure_source_attached:
            raise ValueError("Choose either explicit local source_dir or Azure preparation, not both")
        self._begin("prepare_azure_data")
        self.inputs.pop("source_dir", None)
        self._input_snapshots.pop("source_dir", None)
        self.source_scale_factor = None
        self._azure_source_attached = False
        if entry_id != TPCH_AZURE_DATASET_ID:
            raise ValueError("Select the documented TPC-H SF0.01 catalog ID; this tutorial does not download SF10")
        module, missing = self._import("driftbench.catalog")
        if missing:
            return self._mark("prepare_azure_data", "UNAVAILABLE", "Catalog API is unavailable.", **missing)
        materialize = getattr(module, "materialize", None)
        if not callable(materialize):
            return self._mark("prepare_azure_data", "UNAVAILABLE",
                              "This package does not provide catalog.materialize; no checkout fallback or download was attempted.")
        with local_operations():
            entry = module.get(entry_id)
        tables = {"customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"}
        if (entry["benchmark"] != "tpch" or entry["artifact_type"] != "data"
                or entry["layout"] != "immutable-dataset-v1"
                or entry["parameters"].get("scale_factor") != "0.01"
                or entry["file_count"] != 8
                or {item["path"] for item in entry["files"]} != {f"tpch/data/sf_0.01/{name}.tbl" for name in tables}):
            raise ValueError("Azure preparation requires the pinned SF0.01 dataset and all eight TPC-H tables")
        cache = Path(cache_dir).expanduser().absolute()
        _no_links(cache)
        cache = cache.resolve()
        if cache == self.root or cache in self.root.parents or self.root in cache.parents:
            raise ValueError("Dataset cache and walkthrough output directories must be separate")
        if os.name == "nt" and len(str(cache)) > 100:
            raise ValueError("Choose a dataset cache_dir of at most 100 characters on Windows")
        previous_network_usage = self._azure_network_usage
        self._azure_network_usage = True if previous_network_usage is True else None
        # Only this explicitly requested preparation may use Azure authentication/network.
        result = materialize(entry_id, cache_dir=cache, credential_env_file=credential_env_file,
                             max_bytes=_MAX_INPUT_BYTES)
        _check(result["entry_id"] == entry_id and result["cache_outcome"] in ("downloaded", "hit"),
               "Catalog materialization returned an unexpected identity or outcome")
        self._azure_network_usage = True if result["cache_outcome"] == "downloaded" else previous_network_usage
        source = Path(result["payload_dir"]).absolute()
        _no_links(source)
        source = source.resolve()
        _check(cache in source.parents and source.is_dir(), "Catalog payload directory escaped its cache")
        _check(len(result["files"]) == 8
               and {Path(path).name for path in result["files"]} == {f"{name}.tbl" for name in tables}
               and all(Path(path).parent == source for path in result["files"]),
               "Catalog materialization did not return the eight TPC-H table files")
        self.inputs["source_dir"] = source
        self.source_scale_factor = entry["parameters"]["scale_factor"]
        self._azure_source_attached = True
        self._input_snapshots["source_dir"] = self._input_snapshot(("source_dir",))
        return self._mark("prepare_azure_data", "PASS",
                          "Verified catalog data prepared; cache hits do not recheck live Azure availability.",
                          source_scale_factor=self.source_scale_factor, **result)

    def generate_data(self):
        self._begin("generate_data")
        if "prepare_azure_data" in self.steps:
            blocked = self._requires("generate_data", "prepare_azure_data")
            if blocked:
                return blocked
        unavailable = self._unavailable("generate_data")
        if unavailable:
            return unavailable
        options = dict(self.recipe["data"])
        inputs = ()
        if self.benchmark in ("tpch", "ssb", "ldbc"):
            if "source_dir" not in self.inputs:
                return self._mark("generate_data", "NEEDS_INPUT",
                                  "Supply source_dir with the benchmark's actual local input files; no downloader is run.")
            options["source_dir"] = self.inputs["source_dir"]
            inputs = ("source_dir",)
        if self.benchmark == "tpch":
            if self.source_scale_factor is None:
                return self._mark("generate_data", "NEEDS_INPUT",
                                  "Declare source_scale_factor for the supplied TPC-H files; never infer it from factory defaults.")
            options["scale_factor"] = self.source_scale_factor
        if self.benchmark == "ycsb":
            options["record_count"] = self.record_count
        result, evidence = self._generate("data", options, "generate_data", inputs)
        if result is None:
            return evidence
        self.data_result = result
        kind = "prepare_plan" if self.benchmark in ("benchbase", "sysbench") else "local_data_files"
        return self._mark("generate_data", "PASS", "Generated/imported artifacts, not database rows loaded into a server.",
                          artifact_kind=kind, source_scale_factor=self.source_scale_factor,
                          source_scale_is_user_declared=self.benchmark == "tpch" and not self._azure_source_attached,
                          **evidence)

    def load_data(self):
        self._begin("load_data")
        blocked = self._requires("load_data", "generate_data")
        if blocked:
            return blocked
        if self.benchmark in ("benchbase", "sysbench"):
            return self._mark("load_data", "NOT_SUPPORTED", "The data adapter exports a preparation plan, not table rows.")
        pandas, missing = self._import("pandas")
        if missing:
            return self._mark("load_data", "UNAVAILABLE", "Pandas inspection dependency is absent.", **missing)
        result = self.data_result
        if any(path.suffix in (".tbl", ".dat") for path in result.files):
            if not callable(getattr(result, "as_csv", None)):
                return self._mark("load_data", "UNAVAILABLE", "GenerationResult.as_csv is absent.")
            with local_operations():
                result = result.as_csv()
            self.data_result = result
        paths = [self._owned(path) for path in result.files]
        if self.benchmark == "ldbc":
            selected = [path for path in paths if re.fullmatch(r"person_\d+_\d+\.csv", path.name)]
            delimiter = "|"
        else:
            selected = [path for path in paths if path.name == f"{self.recipe['table']}.csv"]
            delimiter = ","
        _check(bool(selected), "The selected table is not present in the generated artifacts")
        self.frame = pandas.concat([pandas.read_csv(path, sep=delimiter) for path in selected], ignore_index=True)
        _check(len(self.frame.columns) > 1 and len(self.frame) > 0, "Expected a nonempty, correctly delimited table")
        self._baseline_hashes = {str(path): _digest(path) for path in paths if path.is_file()}
        return self._mark("load_data", "PASS", "Pandas inspected local files; this is not database loading.",
                          rows=len(self.frame), columns=list(self.frame.columns),
                          selected_files=[str(path) for path in selected])

    def preview(self, rows=5):
        _positive_int(rows, "rows", 100)
        return self.frame.head(rows) if self.frame is not None else self.steps.get("load_data", {"status": "NEEDS_INPUT"})

    def drift_data(self, scale=0.5):
        self._begin("drift_data")
        if isinstance(scale, bool) or not isinstance(scale, (int, float)) or not math.isfinite(scale) or not 0 < scale <= 2:
            raise ValueError("Tutorial cardinality scale must be finite and in (0, 2]")
        if self.benchmark in ("benchbase", "sysbench", "ldbc"):
            return self._mark("drift_data", "NOT_SUPPORTED",
                              "No table-row drift for preparation plans, and no graph-integrity drift claim for LDBC.")
        blocked = self._requires("drift_data", "load_data")
        if blocked:
            return blocked
        if not callable(getattr(self.data_result, "drift", None)):
            return self._mark("drift_data", "UNAVAILABLE", "GenerationResult.drift is absent.")
        expected = int(len(self.frame) * scale)
        if len(self.frame) < 2 or expected < 1:
            return self._mark("drift_data", "NEEDS_INPUT", "Use a table and scale that produce at least one output row.")
        destination = self._stage("drift_data") / "changed.csv"
        with local_operations():
            result = self.data_result.drift(self.recipe["table"], "vary_cardinality",
                                           scale=scale, seed=self.seed, output_path=destination)
        paths = [self._owned(path) for path in result.files]
        self._owned(result.metadata)
        _check(len(paths) == 1, "Expected a single drifted table")
        pandas = importlib.import_module("pandas")
        self.drifted_frame = pandas.read_csv(paths[0])
        _check(len(self.drifted_frame) == expected, "Cardinality drift did not meet int(baseline_rows * scale)")
        _check(all(_digest(Path(path)) == digest for path, digest in self._baseline_hashes.items()),
               "Cardinality drift modified the baseline artifacts")
        return self._mark("drift_data", "PASS", "Rows were resynthesized, not merely deleted; no PK/FK guarantee.",
                          baseline_rows=len(self.frame), expected_rows=expected, actual_rows=len(self.drifted_frame),
                          scale=scale, baseline_unchanged=True, files=[str(path) for path in paths])

    def _query_options(self, options):
        options = dict(options)
        if self.benchmark == "tpch":
            options.setdefault("seed", self.seed)
            _positive_int(options.get("queries_per_template", 1), "queries_per_template", 100)
        if self.benchmark == "ycsb":
            options["record_count"] = self.record_count
        if self.benchmark == "ldbc":
            options.update({name: self.inputs[name] for name in ("parameters_dir", "driver_config")})
        return options

    def generate_queries(self):
        self._begin("generate_queries")
        unavailable = self._unavailable("generate_queries")
        if unavailable:
            return unavailable
        inputs = ("parameters_dir", "driver_config") if self.benchmark == "ldbc" else ()
        if any(name not in self.inputs for name in inputs):
            return self._mark("generate_queries", "NEEDS_INPUT",
                              "Supply all 14 LDBC parameter files and an existing driver_config; no driver is executed.")
        for options in self.recipe["queries"]:
            actual = self._query_options(options)
            result, evidence = self._generate("queries", actual, "generate_queries", inputs)
            if result is None:
                return evidence
            self.query_results.append(result)
            self._templates(result, actual)
        _check(bool(self.templates), "The real query artifacts did not provide any templates or operations")
        _check(len({item["id"] for item in self.templates}) == len(self.templates), "Template IDs are not unique")
        if not self.default_weights:
            self.default_weights = {item["id"]: 1 for item in self.templates}
        return self._mark("generate_queries", "PASS", "Artifacts inspected as text only; no SQL or driver execution.",
                          templates=self.templates,
                          files=[str(path) for result in self.query_results for path in result.files])

    def _templates(self, result, options):
        files = [self._owned(path) for path in result.files]
        manifest = json.loads(self._owned(result.metadata).read_text(encoding="utf-8"))
        hashes = {path: _digest(path) for path in files}

        def remember(template_id, path, kind, sql=None):
            self.templates.append({"id": template_id, "sql": sql, "kind": kind,
                                   "source_file": path.name, "source_sha256": hashes[path]})

        if self.benchmark == "tpch":
            path = next(path for path in files if path.name == "tpch_queries.csv")
            with path.open(encoding="utf-8", newline="") as stream:
                for row in csv.DictReader(stream):
                    remember(f"q{row['query_id']}-{row['index']}", path, "sql_instance", row["sql"])
        elif self.benchmark == "tpcds":
            path = next(path for path in files if path.name == "query_ids.txt")
            for line in path.read_text(encoding="utf-8").splitlines():
                if line.strip():
                    remember(line.strip(), path, "query_id_only")
        elif self.benchmark in ("ycsb", "benchbase"):
            path = next(path for path in files if path.suffix == ".xml")
            root = ET.fromstring(path.read_text(encoding="utf-8"))
            names = [element.text for element in root.findall(".//transactiontype/name")]
            _check(all(names) and bool(names), "Workload XML has no named operations")
            for name in names:
                remember(name, path, "operation_name")
            if self.benchmark == "ycsb":
                self.default_weights = dict(manifest["weights"])
            else:
                weights = [float(value) for value in root.find(".//work/weights").text.split(",")]
                _check(len(names) == len(weights), "Workload weights do not match operation names")
                self.default_weights = dict(zip(names, weights))
        elif self.benchmark == "sysbench":
            _check(any(path.name == "command.json" for path in files), "Missing native command plan")
            path = next(path for path in files if path.name == "command.json")
            remember(options["workload"], path, "native_profile_reference")
        elif self.benchmark == "ldbc":
            for path in files:
                if re.fullmatch(r"interactive_\d+_param\.txt", path.name):
                    remember(path.stem, path, "parameter_file_reference")
        else:
            for path in files:
                if path.suffix != ".sql" or "_all_" in path.stem or path.stem == "schema" or path.stem.endswith("_schema"):
                    continue
                native = self.benchmark == "pgbench"
                remember(path.stem, path, "pgbench_script_reference" if native else "sql_template",
                         None if native else path.read_text(encoding="utf-8"))

    def _mix(self, stage, target_weights, baseline_weights):
        blocked = self._requires(stage, "generate_queries")
        if blocked:
            return blocked
        api, missing = self._import("driftbench.api")
        if missing:
            return self._mark(stage, "UNAVAILABLE", "Public query API is unavailable.", **missing)
        names = ("QueryTemplate", "execute_query_template_mix_spec", "apply_query_workload_mix_drift",
                 "present_probability_map")
        if not all(callable(getattr(api, name, None)) for name in names):
            return self._mark(stage, "UNAVAILABLE", "The selected package lacks the public query-mix API.",
                              action="Use a release with these APIs or explicitly select the development checkout.")
        templates = [api.QueryTemplate(item["id"], sql=item["sql"],
                                      metadata={key: value for key, value in item.items() if key not in ("id", "sql")})
                     for item in self.templates]
        output = self._stage(stage) / "stream.json"
        spec = {"seed": self.seed, "type": {"family": "workload", "category": "drift", "subtype": "template_mix"},
                "variables": {"template_ids": [item["id"] for item in self.templates],
                              "baseline": {"weights": baseline_weights}, "target": {"weights": target_weights},
                              "sample_size": self.sample_size, "output_path": str(output)}}
        with local_operations():
            result = api.execute_query_template_mix_spec(spec, runtime_inputs={"query_templates": templates})
            repeated = api.apply_query_workload_mix_drift(
                templates, baseline_weights=baseline_weights, target_weights=target_weights,
                sample_size=self.sample_size, seed=self.seed)
        _check(result == repeated, "Identical stream requests did not reproduce identical samples")
        payload = json.loads(self._owned(output).read_text(encoding="utf-8"))
        _check(payload["semantic_hash"] == result.semantic_hash, "Persisted stream differs from the returned result")
        observations = {}
        for name in ("baseline", "drifted"):
            values = getattr(result, name)
            counts = Counter(item.template_id for item in values)
            weights = result.baseline_weights if name == "baseline" else result.target_weights
            _check(len(values) == self.sample_size and set(counts) <= set(weights), "Invalid sampled stream shape")
            _check(all(counts[key] == 0 for key, weight in weights.items() if weight == 0),
                   "A zero-weight template was sampled")
            observations[name] = {"weights": api.present_probability_map(dict(weights), field="weights"),
                                  "counts": {key: counts[key] for key in weights},
                                  "frequencies": api.present_probability_map(
                                      {key: counts[key] / self.sample_size for key in weights},
                                      field="frequencies")}
        self.baseline_weights = dict(result.baseline_weights)
        return self._mark(stage, "PASS", "Template/operation samples, not timed replay or exact frequency quotas.",
                          sample_size=self.sample_size, seed=self.seed, semantic_hash=result.semantic_hash,
                          deterministic=True, file=str(output), **observations)

    def query_stream(self, *, weights=None):
        self._begin("query_stream")
        baseline = self.default_weights if weights is None else weights
        return self._mix("query_stream", baseline, baseline)

    def change_mix(self, *, weights=None):
        self._begin("change_mix")
        blocked = self._requires("change_mix", "query_stream")
        if blocked:
            return blocked
        ids = [item["id"] for item in self.templates]
        if len(ids) < 2:
            return self._mark("change_mix", "NOT_SUPPORTED", "One template cannot demonstrate a frequency shift.")
        target = {key: (0.8 if index == 0 else 0.2 / (len(ids) - 1)) for index, key in enumerate(ids)}
        return self._mix("change_mix", target if weights is None else weights, self.baseline_weights)

    def change_query_parameters(self, **changes):
        self._begin("change_query_parameters")
        if "change" not in self.recipe:
            if changes:
                raise ValueError(f"{self.benchmark} has no parameter-change recipe; use change_mix for frequencies")
            return self._mark("change_query_parameters", "NOT_SUPPORTED",
                              "No public parameter-change recipe for these fixed artifacts; change_mix changes frequencies, not SQL.")
        blocked = self._requires("change_query_parameters", "generate_queries")
        if blocked:
            return blocked
        if set(changes) - set(self.recipe["options"]):
            raise ValueError(f"Supported changes: {', '.join(self.recipe['options'])}")
        options = self._query_options(self.recipe["queries"][0])
        options.update(changes or self.recipe["change"])
        options = self._query_options(options)
        before = {str(path): _digest(path) for result in self.query_results for path in result.files}
        result, evidence = self._generate("queries", options, "change_query_parameters")
        if result is None:
            return evidence
        _check(all(_digest(Path(path)) == digest for path, digest in before.items()), "Original query artifacts changed")
        after = {path.name: _digest(path) for path in result.files}
        original = {path.name: _digest(path) for path in self.query_results[0].files}
        _check(after != original, "Requested query/profile change did not change any artifact")
        return self._mark("change_query_parameters", "PASS",
                          "Public parameters changed artifacts/configuration, not proven execution behavior.",
                          parameters=options, original_unchanged=True, **evidence)

    def report(self):
        _check(all(self._input_snapshot((name,)) == snapshot for name, snapshot in self._input_snapshots.items()),
               "A supplied input changed")
        names = list(STEPS)
        if "prepare_azure_data" in self.steps:
            names.insert(1, "prepare_azure_data")
        rows = {name: self.steps.get(name, {"status": "NEEDS_INPUT", "reason": "Step not run."}) for name in names}
        status = "PASS" if all(row["status"] == "PASS" for row in rows.values()) else "PARTIAL"
        if any(row["status"] == "FAIL" for row in rows.values()):
            status = "FAIL"
        report = {"schema": "driftbench.walkthrough/v1", "benchmark": self.benchmark,
                  "status": status,
                  "package": self.identity, "output_dir": str(self.root), "steps": rows,
                  "inputs_unchanged": True, "network_or_database_execution": self._azure_network_usage,
                  "input_files_checked": sum(len(snapshot) for snapshot in self._input_snapshots.values()),
                  "limits": ["No database loading/execution, Azure access, performance or conformance evidence.",
                             "Query streams are samples, not native driver replay or arrival-rate guarantees.",
                             "Cardinality drift resynthesizes rows and does not preserve PK/FK/graph integrity."]}
        if "prepare_azure_data" in self.steps:
            report["database_execution"] = False
            report["limits"][0] = "No database loading/execution, performance or conformance evidence."
            report["limits"].append("Cache validity is pinned-catalog agreement, not live freshness or current remote authorization.")
        path = self._stage("report") / "report.json"
        report["report_file"] = str(path)
        path.write_text(json.dumps(report, indent=2, ensure_ascii=True, allow_nan=False) + "\n", encoding="utf-8")
        return report

    def run_all(self):
        self.generate_data()
        self.load_data()
        self.drift_data()
        self.generate_queries()
        self.query_stream()
        self.change_mix()
        self.change_query_parameters()
        return self.report()


def main(benchmark):
    description = (f"Local {benchmark} public-API walkthrough; no Azure or database execution."
                   if benchmark != "tpch" else
                   "TPC-H local walkthrough with optional read-only Azure SF0.01 preparation; no database execution.")
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--output-dir", type=Path, required=True, help="Dedicated persistent output directory; each run owns a fresh child.")
    parser.add_argument("--package-target", choices=("installed", "checkout"), default="installed")
    parser.add_argument("--source-dir", type=Path)
    parser.add_argument("--source-scale-factor", help="Explicit scale declaration for supplied TPC-H files; not inferred.")
    parser.add_argument("--parameters-dir", type=Path)
    parser.add_argument("--driver-config", type=Path)
    parser.add_argument("--sample-size", type=int, default=200)
    parser.add_argument("--record-count", type=int, default=200)
    parser.add_argument("--seed", type=int, default=42)
    if benchmark == "tpch":
        parser.add_argument("--azure-catalog-id", help="Explicitly prepare the documented Azure TPC-H SF0.01 dataset.")
        parser.add_argument("--dataset-cache-dir", type=Path, help="Persistent verified input cache, separate from run outputs.")
        parser.add_argument("--credential-env-file", type=Path, help="Optional existing private credential file; never saved in reports.")
    options = vars(parser.parse_args())
    azure_id = options.pop("azure_catalog_id", None)
    cache_dir = options.pop("dataset_cache_dir", None)
    credential = options.pop("credential_env_file", None)
    if not azure_id and (cache_dir is not None or credential is not None):
        parser.error("Azure cache/authentication options require --azure-catalog-id")
    if azure_id and (azure_id != TPCH_AZURE_DATASET_ID or options["source_dir"] is not None
                     or options["source_scale_factor"] is not None):
        parser.error("Use the documented SF0.01 catalog ID without --source-dir or --source-scale-factor")
    demo = Walkthrough(benchmark, **options)
    print(json.dumps(demo.identity, indent=2))
    if azure_id:
        demo.prepare_azure_data(azure_id, cache_dir=cache_dir or Path.home() / "DriftBenchDatasets",
                                credential_env_file=credential)
    result = demo.run_all()
    print(json.dumps(result, indent=2, ensure_ascii=True))
    return {"PASS": 0, "PARTIAL": 2, "FAIL": 1}[result["status"]]
