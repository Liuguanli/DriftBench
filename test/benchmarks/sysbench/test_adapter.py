from __future__ import annotations

import hashlib
import json
import os
import tempfile
import unittest
from contextlib import ExitStack
from pathlib import Path
from unittest.mock import patch

from driftbench.data.sysbench import SysbenchData, SysbenchQueries, data, queries
from ..helpers import BenchmarkAdapterTestMixin


def read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def contents(result) -> dict[str, bytes]:
    return {path.name: path.read_bytes() for path in [*result.files, result.metadata]}


class SysbenchAdapterTests(BenchmarkAdapterTestMixin, unittest.TestCase):
    def test_prepare_exports_native_configuration_and_provenance_without_rows(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            result = data(tables=3, table_size=20, db_driver="pgsql", seed=99).generate(root)
            self._assert_result_is_filesystem_contract(result, root)
            self.assertEqual(result.files[0].read_text(), (
                "db-driver=pgsql\ntables=3\ntable-size=20\nthreads=1\nrand-seed=99\n"
            ))
            command = read_json(result.files[1])
            self.assertEqual(command["argv"], [
                "sysbench", "--config-file=sysbench.cnf", "oltp_read_write", "prepare",
            ])
            self.assertEqual(command["cwd"], ".")
            self.assertTrue(command["requires_local_connection_options"])
            self.assertFalse(command["executed"])
            manifest = read_json(result.metadata)
            self.assertEqual(manifest["artifact_kind"], "prepare_plan")
            self.assertEqual(manifest["data_rows_generated"], 0)
            self.assertFalse(manifest["database_executed"])
            self.assertEqual(manifest["provenance"]["native_contract_revision"],
                             "3ceba0b1e115f8c50d1d045a4574d8ed643bd497")
            self.assertIn("not a dataset", result.files[2].read_text())
            self.assertFalse(any(path.suffix in (".csv", ".tbl", ".sql") for path in result.files))

    def test_run_exports_native_duration_rate_distribution_and_selected_script(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = queries(
                workload="oltp_point_select", tables=2, table_size=150, threads=8,
                duration=12, rate=250, db_driver="mysql", distribution="zipfian", seed=7,
            ).generate(tmp)
            self._assert_result_is_filesystem_contract(result, Path(tmp))
            self.assertEqual(result.files[0].read_text(), (
                "db-driver=mysql\ntables=2\ntable-size=150\nthreads=8\ntime=12\n"
                "rate=250\nevents=0\nrand-type=zipfian\nrand-seed=7\n"
            ))
            self.assertEqual(read_json(result.files[1])["argv"][-2:], ["oltp_point_select", "run"])
            self.assertEqual(read_json(result.metadata)["artifact_kind"], "run_plan")

    def test_all_supported_workloads_and_distributions(self):
        with tempfile.TemporaryDirectory() as tmp:
            for workload in ("oltp_read_only", "oltp_read_write", "oltp_write_only", "oltp_point_select"):
                for distribution in ("uniform", "gaussian", "pareto", "zipfian"):
                    with self.subTest(workload=workload, distribution=distribution):
                        result = queries(workload=workload, distribution=distribution).generate(tmp)
                        self.assertEqual(read_json(result.files[1])["argv"][-2], workload)
                        self.assertIn(f"rand-type={distribution}\n", result.files[0].read_text())

    def test_cache_reuses_verified_files_and_force_writes_identical_artifacts(self):
        with tempfile.TemporaryDirectory() as tmp:
            for factory in (data, queries):
                with self.subTest(factory=factory):
                    first = factory().generate(tmp)
                    expected = contents(first)
                    cached = factory().generate(tmp)
                    self.assertTrue(cached.reused_local)
                    self.assertEqual(contents(cached), expected)
                    forced = factory().generate(tmp, force=True)
                    self.assertFalse(forced.reused_local)
                    self.assertEqual(contents(forced), expected)

    def test_every_input_parameter_invalidates_the_cache(self):
        changes = {
            "tables": 2, "table_size": 37, "db_driver": "pgsql", "seed": 123,
            "workload": "oltp_read_only", "threads": 8, "duration": 5,
            "rate": 10, "distribution": "pareto",
        }
        with tempfile.TemporaryDirectory() as tmp:
            for factory, names in (
                (data, ("tables", "table_size", "db_driver", "seed")),
                (queries, tuple(changes)),
            ):
                for name in names:
                    with self.subTest(factory=factory, parameter=name):
                        baseline = factory().generate(tmp)
                        fingerprint = read_json(baseline.metadata)["cache"]["fingerprint"]
                        updated = factory(**{name: changes[name]}).generate(tmp)
                        self.assertFalse(updated.reused_local)
                        self.assertNotEqual(read_json(updated.metadata)["cache"]["fingerprint"], fingerprint)
                        self.assertEqual(read_json(updated.metadata)["parameters"][name], changes[name])
                        self.assertTrue(factory(**{name: changes[name]}).generate(tmp).reused_local)

    def test_tampered_or_missing_files_and_corrupt_manifest_regenerate(self):
        with tempfile.TemporaryDirectory() as tmp:
            for factory in (data, queries):
                first = factory().generate(tmp)
                expected = contents(first)
                for path in [*first.files, first.metadata]:
                    with self.subTest(factory=factory, target=path.name):
                        path.write_text("tampered", encoding="utf-8")
                        recovered = factory().generate(tmp)
                        self.assertFalse(recovered.reused_local)
                        self.assertEqual(contents(recovered), expected)
                        path.unlink()
                        self.assertFalse(factory().generate(tmp).reused_local)
                        self.assertEqual(contents(recovered), expected)

    def test_output_is_portable_between_roots_and_keeps_unknown_files(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            for factory in (data, queries):
                first = factory().generate(root / "one")
                unrelated = first.files[0].parent / "my-notes.txt"
                unrelated.write_bytes(b"keep me")
                second = factory().generate(root / "two")
                self.assertEqual(contents(first), contents(second))
                factory(seed=11).generate(root / "one", force=True)
                self.assertEqual(unrelated.read_bytes(), b"keep me")

    def test_invalid_values_fail_before_creating_output(self):
        invalid = {
            "tables": [True, 0, -1, 2**31, 1.0, float("nan"), float("inf"), "1", None],
            "table_size": [False, 0, -1, 2**31, 1.0, float("inf"), "20"],
            "seed": [True, 0, -1, 2**31, 1.0, float("nan"), "42"],
            "db_driver": [None, True, [], "postgres", "mysql\npassword=secret"],
            "threads": [False, 0, 1025, 1.0, float("inf"), "4"],
            "duration": [True, 0, -1, 86401, 1.0, float("nan"), "60"],
            "rate": [False, -1, 2**31, 1.5, float("inf"), "0"],
            "workload": [None, [], "unknown", "../../custom", "oltp_read_write\ncleanup"],
            "distribution": [None, [], "special", "unknown", "uniform\ntime=0"],
        }
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / "must-not-exist"
            for factory, names in (
                (data, ("tables", "table_size", "db_driver", "seed")),
                (queries, tuple(invalid)),
            ):
                for name in names:
                    for value in invalid[name]:
                        with self.subTest(factory=factory, parameter=name, value=value):
                            with self.assertRaisesRegex(ValueError, name):
                                factory(**{name: value}).generate(output)
                            self.assertFalse(output.exists())
                with self.assertRaisesRegex(ValueError, "force"):
                    factory().generate(output, force="yes")
                self.assertFalse(output.exists())

    def test_unknown_factory_options_are_rejected(self):
        for factory in (data, queries):
            with self.assertRaises(TypeError):
                factory(password="must-not-be-recorded")
            with self.assertRaises(TypeError):
                factory(host="localhost")

    def test_numeric_boundaries_are_accepted_without_generating_large_data(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = queries(
                tables=2**31-1, table_size=2**31-1, threads=1024,
                duration=86400, rate=2**31-1, seed=2**31-1,
            ).generate(tmp)
            self.assertLess(sum(path.stat().st_size for path in result.files), 10000)

    def test_generate_does_not_invoke_process_network_or_database(self):
        with tempfile.TemporaryDirectory() as tmp, ExitStack() as stack:
            guards = [stack.enter_context(patch(target, side_effect=AssertionError(target))) for target in (
                "subprocess.run", "subprocess.Popen", "os.system", "socket.create_connection",
                "urllib.request.urlopen", "psycopg2.connect",
            )]
            for factory in (data, queries):
                factory().generate(tmp)
                factory().generate(tmp)
                factory().generate(tmp, force=True)
            for guard in guards:
                guard.assert_not_called()

    def test_directory_collision_preflight_leaves_every_file_untouched(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            conflict = root / "sysbench" / "queries" / "command.json"
            conflict.mkdir(parents=True)
            marker = conflict / "keep.txt"
            marker.write_bytes(b"keep")
            with self.assertRaisesRegex(ValueError, "regular"):
                queries().generate(root)
            self.assertEqual(marker.read_bytes(), b"keep")
            self.assertFalse((conflict.parent / "sysbench.cnf").exists())
            self.assertFalse((conflict.parent / "sysbench_queries_manifest.json").exists())

    def test_hardlinked_managed_target_does_not_modify_external_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "output"
            target = root / "sysbench" / "data" / "sysbench.cnf"
            target.parent.mkdir(parents=True)
            external = Path(tmp) / "external.cnf"
            external.write_bytes(b"external original")
            try:
                os.link(external, target)
            except OSError as exc:
                self.skipTest(f"hard links unavailable: {exc}")
            with self.assertRaisesRegex(ValueError, "unshared"):
                data().generate(root)
            self.assertEqual(external.read_bytes(), b"external original")
            self.assertEqual(list(target.parent.iterdir()), [target])

    def test_output_child_link_does_not_write_outside_root(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "output"
            root.mkdir()
            external = Path(tmp) / "external"
            external.mkdir()
            link = root / "sysbench"
            try:
                link.symlink_to(external, target_is_directory=True)
            except OSError as exc:
                if os.name != "nt":
                    self.skipTest(f"symbolic links unavailable: {exc}")
                # Windows directory junctions exercise the real reparse-point
                # boundary without requiring the symlink privilege.
                import _winapi
                _winapi.CreateJunction(str(external), str(link))
            with self.assertRaisesRegex(ValueError, "link"):
                queries().generate(root)
            self.assertEqual(list(external.iterdir()), [])

    def test_symlinked_manifest_is_not_read_or_overwritten(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "output"
            target = root / "sysbench" / "queries" / "sysbench_queries_manifest.json"
            target.parent.mkdir(parents=True)
            external = Path(tmp) / "external.json"
            external.write_bytes(b"external original")
            try:
                target.symlink_to(external)
            except OSError as exc:
                self.skipTest(f"symbolic links unavailable: {exc}")
            with self.assertRaisesRegex(ValueError, "link"):
                queries().generate(root)
            self.assertEqual(external.read_bytes(), b"external original")
            self.assertFalse((target.parent / "sysbench.cnf").exists())

    def test_default_output_root_and_classes_preserve_generation_contract(self):
        root = Path(self._default_data_tmpdir)
        for adapter in (SysbenchData(), SysbenchQueries()):
            self._assert_result_is_filesystem_contract(adapter.generate(), root)

    def test_manifest_hashes_match_exported_bytes(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = queries().generate(tmp)
            manifest = read_json(result.metadata)
            for path, record in zip(result.files, manifest["cache"]["artifacts"]):
                self.assertEqual(hashlib.sha256(path.read_bytes()).hexdigest(), record["sha256"])
