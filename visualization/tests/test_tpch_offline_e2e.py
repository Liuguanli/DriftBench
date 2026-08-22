from __future__ import annotations

import contextlib
import io
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from visualization import VISUALIZATION_SCHEMA_VERSION
from visualization.artifacts import reject_machine_paths, sha256_file
from visualization.cli import EXIT_OK, main
from visualization.drift_scenarios import SPEC_EXECUTOR, SPEC_EXECUTOR_VERSION
from visualization.gallery import validate_manifest


class TPCHOfflineVisualizationE2ETests(unittest.TestCase):
    def _write_lineitem_fixture(self, source: Path, rows: int = 80) -> None:
        source.mkdir(parents=True, exist_ok=True)
        fixtures = {
            "customer.tbl": "1|Customer#000000001|address|0|10-000-000-0000|0.00|BUILDING|fixture|\n",
            "nation.tbl": "0|ALGERIA|0|fixture|\n",
            "orders.tbl": "1|1|O|1000.00|1996-01-01|1-URGENT|Clerk#000000001|0|fixture|\n",
            "part.tbl": "1|part|Manufacturer#1|Brand#1|STANDARD|1|SM BOX|1.00|fixture|\n",
            "partsupp.tbl": "1|1|1|1.00|fixture|\n",
            "region.tbl": "0|AFRICA|fixture|\n",
            "supplier.tbl": "1|Supplier#000000001|address|0|10-000-000-0000|0.00|fixture|\n",
        }
        for name, content in fixtures.items():
            (source / name).write_text(content, encoding="utf-8")
        records = []
        for index in range(1, rows + 1):
            records.append(
                (
                    f"{index}|155190|7706|1|{10 + index}|{1000.0 + index * 17.25:.2f}|"
                    "0.04|0.02|N|O|1996-03-13|1996-02-12|1996-03-22|"
                    "DELIVER IN PERSON|TRUCK|offline fixture|\n"
                )
            )
        (source / "lineitem.tbl").write_text("".join(records), encoding="utf-8")

    def _run(self, root: Path, *, force: bool) -> tuple[int, str, str]:
        argv = [
            "generate",
            "--benchmark",
            "tpch",
            "--kind",
            "data",
            "--scenario",
            "price_outliers",
            "--seed",
            "42",
            "--sample-size",
            "1000",
            "--offline",
            "--output-dir",
            str(root),
        ]
        if force:
            argv.append("--force")
        stdout = io.StringIO()
        stderr = io.StringIO()
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            code = main(argv)
        return code, stdout.getvalue(), stderr.getvalue()

    def test_real_tpch_adapter_offline_generate_cache_and_force(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            sandbox = Path(temporary)
            source = sandbox / "local-tpch-source"
            output = sandbox / "visualization-output"
            self._write_lineitem_fixture(source)

            environment = {
                "DRIFTBENCH_TPCH_SOURCE_DIR": str(source),
                "MPLCONFIGDIR": str(sandbox / "matplotlib-cache"),
            }
            with (
                mock.patch.dict(os.environ, environment, clear=False),
                mock.patch(
                    "driftbench.data.tpch.TPCHData._auto_build_dbgen",
                    side_effect=AssertionError("offline fixture must not build/download dbgen"),
                ),
                mock.patch(
                    "driftbench.data.tpch.subprocess.run",
                    side_effect=AssertionError("offline fixture must not execute dbgen"),
                ),
            ):
                first_code, first_stdout, first_stderr = self._run(output, force=False)
                self.assertEqual(first_code, EXIT_OK, first_stderr)
                self.assertIn("Generated tpch/data/price_outliers", first_stdout)

                figure_path = (
                    output / "figures" / "data" / "tpch" / "price_outliers.png"
                )
                manifest_path = (
                    output
                    / "manifests"
                    / "data"
                    / "tpch"
                    / "price_outliers.json"
                )
                spec_path = (
                    output / "specs" / "data" / "tpch" / "price_outliers.yaml"
                )
                self.assertTrue(figure_path.is_file())
                self.assertTrue(manifest_path.is_file())
                self.assertTrue(spec_path.is_file())
                first_figure_hash = sha256_file(figure_path)
                first_figure_mtime = figure_path.stat().st_mtime_ns
                first_manifest_hash = sha256_file(manifest_path)
                first_manifest_mtime = manifest_path.stat().st_mtime_ns

                second_code, second_stdout, second_stderr = self._run(
                    output, force=False
                )
                self.assertEqual(second_code, EXIT_OK, second_stderr)
                self.assertIn("Reused tpch/data/price_outliers", second_stdout)
                self.assertEqual(sha256_file(figure_path), first_figure_hash)
                self.assertEqual(sha256_file(manifest_path), first_manifest_hash)
                self.assertEqual(figure_path.stat().st_mtime_ns, first_figure_mtime)
                self.assertEqual(manifest_path.stat().st_mtime_ns, first_manifest_mtime)

                forced_code, forced_stdout, forced_stderr = self._run(
                    output, force=True
                )
                self.assertEqual(forced_code, EXIT_OK, forced_stderr)
                self.assertIn("Generated tpch/data/price_outliers", forced_stdout)

            payload = json.loads(manifest_path.read_text(encoding="utf-8"))
            validate_manifest(payload, output, verify_figure=True)
            reject_machine_paths(payload)
            serialized = json.dumps(payload, sort_keys=True)
            self.assertNotIn(str(source), serialized)
            self.assertNotIn(str(output), serialized)
            self.assertEqual(payload["schema_version"], VISUALIZATION_SCHEMA_VERSION)
            self.assertEqual(payload["benchmark"], "tpch")
            self.assertEqual(payload["kind"], "data")
            self.assertEqual(payload["scenario"], "price_outliers")
            self.assertEqual(payload["seed"], 42)
            self.assertEqual(payload["sample_size"], 1000)
            self.assertIn("--offline", payload["reproduce"])
            self.assertIn("--scenario price_outliers", payload["reproduce"])
            self.assertEqual(
                payload["figure"]["path"],
                "figures/data/tpch/price_outliers.png",
            )
            self.assertEqual(payload["effect"]["verdict"], "PASS")
            self.assertIs(payload["effect"]["passed"], True)
            self.assertEqual(payload["execution"]["status"], "supported")
            self.assertEqual(payload["execution"]["engine"], SPEC_EXECUTOR)
            self.assertEqual(
                payload["execution"]["engine_version"], SPEC_EXECUTOR_VERSION
            )
            self.assertEqual(
                payload["drift_spec"]["path"],
                "specs/data/tpch/price_outliers.yaml",
            )
            self.assertEqual(
                payload["drift_spec"]["type"],
                {"family": "data", "category": "drift", "subtype": "single_table"},
            )
            self.assertEqual(payload["drift_spec"]["sha256"], sha256_file(spec_path))
            self.assertEqual(payload["drift_spec"]["bytes"], spec_path.stat().st_size)
            statistics = payload["statistics"]
            self.assertEqual(statistics["distribution_type"], "numeric")
            self.assertEqual(
                len(statistics["baseline_histogram"]),
                len(statistics["drifted_histogram"]),
            )
            self.assertEqual(
                len(statistics["bin_edges"]),
                len(statistics["baseline_histogram"]) + 1,
            )
            self.assertEqual(statistics["bin_edges"][0], statistics["axis_range"][0])
            self.assertEqual(statistics["bin_edges"][-1], statistics["axis_range"][1])
            self.assertAlmostEqual(sum(statistics["baseline_histogram"]), 1.0)
            self.assertAlmostEqual(sum(statistics["drifted_histogram"]), 1.0)


if __name__ == "__main__":
    unittest.main()
