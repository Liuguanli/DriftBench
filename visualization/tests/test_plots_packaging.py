from __future__ import annotations

import importlib.util
import hashlib
import struct
import subprocess
import sys
import tarfile
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest import mock

import numpy as np
import pandas as pd

from driftbench.api import QueryTemplate, apply_query_workload_mix_drift
from visualization.distributions import (
    summarize_data_distribution,
    summarize_query_distribution,
)
from visualization.plots import (
    MissingOptionalDependency,
    PLOT_CONFIG,
    plot_data_comparison,
    plot_query_comparison,
    renderer_metadata,
    require_matplotlib,
)
from visualization.specs import expected_artifact_keys


REPO_ROOT = Path(__file__).resolve().parents[2]


def _toml_section(text: str, name: str) -> str:
    marker = f"[{name}]"
    if marker not in text:
        raise AssertionError(f"missing TOML section: {name}")
    remainder = text.split(marker, 1)[1]
    return remainder.split("\n[", 1)[0]


def _png_text_metadata(path: Path) -> dict[str, str]:
    payload = path.read_bytes()
    if not payload.startswith(b"\x89PNG\r\n\x1a\n"):
        raise AssertionError("not a PNG file")
    offset = 8
    metadata: dict[str, str] = {}
    while offset + 12 <= len(payload):
        length = struct.unpack(">I", payload[offset : offset + 4])[0]
        chunk_type = payload[offset + 4 : offset + 8]
        chunk_data = payload[offset + 8 : offset + 8 + length]
        offset += 12 + length
        if chunk_type == b"tEXt" and b"\x00" in chunk_data:
            key, value = chunk_data.split(b"\x00", 1)
            metadata[key.decode("latin-1")] = value.decode("latin-1")
        elif chunk_type == b"iTXt" and b"\x00" in chunk_data:
            key, remainder = chunk_data.split(b"\x00", 1)
            # PNG iTXt: compression flag/method, language, translated keyword, text.
            fields = remainder.split(b"\x00", 4)
            if len(fields) == 5 and fields[0] == b"\x00":
                metadata[key.decode("latin-1")] = fields[4].decode("utf-8")
        if chunk_type == b"IEND":
            break
    return metadata


class PlotContractTests(unittest.TestCase):
    def _numeric_statistics(self) -> dict[str, object]:
        return summarize_data_distribution(
            pd.DataFrame({"amount": [0.0, 1.0, 2.0, 3.0]}),
            pd.DataFrame({"amount": [2.0, 4.0, 6.0, 8.0]}),
            column="amount",
            sample_size=100,
            seed=42,
            bins=4,
        )

    def test_numeric_plot_is_four_panel_diagnostic_with_shared_range_and_labels(self) -> None:
        captured: dict[str, object] = {}

        def capture(figure, output_path, pyplot) -> None:
            del output_path
            captured["figure"] = figure
            captured["pyplot"] = pyplot

        with mock.patch("visualization.plots._save_figure", side_effect=capture):
            plot_data_comparison(
                self._numeric_statistics(),
                benchmark="tpch",
                benchmark_title="TPC-H",
                scenario="price_outliers",
                seed=42,
                output_path=Path("unused.png"),
            )

        figure = captured["figure"]
        pyplot = captured["pyplot"]
        try:
            self.assertEqual(len(figure.axes), 4)
            distribution_axis, tail_axis, quantile_axis, overview_axis = figure.axes
            self.assertTrue(all(axis.has_data() for axis in figure.axes))
            np.testing.assert_allclose(distribution_axis.get_xlim(), (0.0, 8.0))
            self.assertEqual(distribution_axis.get_xlabel(), "amount")
            np.testing.assert_allclose(tail_axis.get_xlim(), (0.0, 8.0))
            self.assertIn("shared bins + range", distribution_axis.get_title(loc="left"))
            self.assertIn("Tail CCDF", tail_axis.get_title(loc="left"))
            self.assertEqual(tail_axis.get_yscale(), "log")
            self.assertIn("Quantile shift", quantile_axis.get_title(loc="left"))
            self.assertIn("diagnostic distances", overview_axis.get_title(loc="left"))
            self.assertEqual(
                [text.get_text() for text in distribution_axis.get_legend().texts],
                ["Baseline", "Drifted"],
            )
            title = figure._suptitle.get_text()
            self.assertIn("TPC-H", title)
            self.assertIn("Data drift diagnostic", title)
            subtitle = " ".join(text.get_text() for text in figure.texts)
            self.assertIn("tpch", subtitle)
            self.assertIn("price_outliers", subtitle)
            self.assertIn("seed 42", subtitle)
            self.assertIn("observed sample n=4/4", subtitle)
        finally:
            pyplot.close(figure)

    def test_query_plot_has_identity_seed_and_baseline_drifted_legend(self) -> None:
        templates = (
            QueryTemplate("q1", "SELECT * FROM a WHERE id = 1;"),
            QueryTemplate("q2", "SELECT * FROM a JOIN b ON a.id = b.id;"),
        )
        result = apply_query_workload_mix_drift(
            templates,
            baseline_weights={"q1": 1, "q2": 1},
            target_weights={"q1": 1, "q2": 3},
            sample_size=20,
            seed=42,
        )
        statistics = summarize_query_distribution(
            result,
            capabilities={
                "lexical_sql_metrics": "supported",
                "predicate_selectivity": "unsupported",
                "temporal": "unsupported",
            },
        )
        captured: dict[str, object] = {}

        def capture(figure, output_path, pyplot) -> None:
            del output_path
            captured["figure"] = figure
            captured["pyplot"] = pyplot

        with mock.patch("visualization.plots._save_figure", side_effect=capture):
            plot_query_comparison(
                statistics,
                benchmark="tpch",
                benchmark_title="TPC-H",
                scenario="hotset_concentration",
                seed=42,
                output_path=Path("unused.png"),
            )

        figure = captured["figure"]
        pyplot = captured["pyplot"]
        try:
            self.assertEqual(len(figure.axes), 4)
            frequency_axis = figure.axes[0]
            self.assertEqual(
                [text.get_text() for text in frequency_axis.get_legend().texts],
                ["Baseline", "Drifted"],
            )
            self.assertEqual(frequency_axis.get_xlabel(), "Observed sample frequency")
            self.assertIn("+ Other", frequency_axis.get_title(loc="left"))
            title = figure._suptitle.get_text()
            self.assertIn("TPC-H", title)
            self.assertIn("Query drift diagnostic", title)
            footer = " ".join(text.get_text() for text in figure.texts)
            self.assertIn("tpch", footer)
            self.assertIn("seed 42", footer)
            self.assertIn("predicate/selectivity", footer)
            self.assertIn("arrival-rate/inter-arrival", footer)
        finally:
            pyplot.close(figure)

    def test_categorical_plot_has_ranked_movers_other_and_four_populated_panels(self) -> None:
        statistics = summarize_data_distribution(
            pd.DataFrame({"category": ["a"] * 6 + ["b"] * 2 + ["c"] * 2}),
            pd.DataFrame({"category": ["a"] * 6 + ["d"] * 2 + ["e"] * 2}),
            column="category",
            sample_size=100,
            seed=42,
            top_k=1,
        )
        captured: dict[str, object] = {}

        def capture(figure, output_path, pyplot) -> None:
            del output_path
            captured["figure"] = figure
            captured["pyplot"] = pyplot

        with mock.patch("visualization.plots._save_figure", side_effect=capture):
            plot_data_comparison(
                statistics,
                benchmark="ycsb",
                benchmark_title="YCSB",
                scenario="field0_hot_value_skew",
                seed=42,
                output_path=Path("unused.png"),
            )
        figure = captured["figure"]
        pyplot = captured["pyplot"]
        try:
            self.assertEqual(len(figure.axes), 4)
            self.assertTrue(all(axis.has_data() for axis in figure.axes))
            self.assertIn("Other", [tick.get_text() for tick in figure.axes[0].get_yticklabels()])
            self.assertEqual(
                [tick.get_text() for tick in figure.axes[1].get_yticklabels()],
                ["#1", "Other"],
            )
            self.assertIn("Concentration profile", figure.axes[2].get_title(loc="left"))
        finally:
            pyplot.close(figure)

    def test_query_without_sql_uses_an_explicit_nonblank_unsupported_panel(self) -> None:
        templates = (QueryTemplate("ReadRecord", None), QueryTemplate("ScanRecord", None))
        result = apply_query_workload_mix_drift(
            templates,
            baseline_weights={"ReadRecord": 1, "ScanRecord": 1},
            target_weights={"ReadRecord": 1, "ScanRecord": 3},
            sample_size=20,
            seed=42,
        )
        statistics = summarize_query_distribution(
            result,
            capabilities={
                "lexical_sql_metrics": "unsupported",
                "predicate_selectivity": "unsupported",
                "temporal": "unsupported",
            },
        )
        captured: dict[str, object] = {}

        def capture(figure, output_path, pyplot) -> None:
            del output_path
            captured["figure"] = figure
            captured["pyplot"] = pyplot

        with mock.patch("visualization.plots._save_figure", side_effect=capture):
            plot_query_comparison(
                statistics,
                benchmark="ycsb",
                benchmark_title="YCSB",
                scenario="scan_heavy_profile",
                seed=42,
                output_path=Path("unused.png"),
            )
        figure = captured["figure"]
        pyplot = captured["pyplot"]
        try:
            panel_text = " ".join(text.get_text() for text in figure.axes[3].texts)
            self.assertIn("Unsupported", panel_text)
            self.assertIn("does not expose SQL text", panel_text)
            self.assertTrue(figure.axes[0].has_data())
            self.assertTrue(figure.axes[1].has_data())
            self.assertTrue(figure.axes[2].has_data())
        finally:
            pyplot.close(figure)

    def test_saved_png_contains_driftbench_software_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            output_path = Path(temporary) / "comparison.png"
            plot_data_comparison(
                self._numeric_statistics(),
                benchmark="tpch",
                benchmark_title="TPC-H",
                scenario="price_outliers",
                seed=42,
                output_path=output_path,
            )
            metadata = _png_text_metadata(output_path)
            payload = output_path.read_bytes()
            dimensions = struct.unpack(">II", payload[16:24])
        self.assertEqual(metadata.get("Software"), "DriftBench Visualization")
        self.assertEqual(metadata.get("Renderer"), "driftbench.visualization-renderer/v3")
        self.assertEqual(dimensions, (1600, 1000))
        self.assertEqual(renderer_metadata()["pixel_size"], [1600, 1000])

    def test_rendering_does_not_leak_process_global_rcparams(self) -> None:
        import matplotlib.pyplot as plt

        keys = ("font.family", "font.size", "axes.titlesize", "figure.facecolor")
        before = {key: plt.rcParams[key] for key in keys}
        with tempfile.TemporaryDirectory() as temporary:
            plot_data_comparison(
                self._numeric_statistics(),
                benchmark="tpch",
                benchmark_title="TPC-H",
                scenario="price_outliers",
                seed=42,
                output_path=Path(temporary) / "comparison.png",
            )
        after = {key: plt.rcParams[key] for key in keys}
        self.assertEqual(after, before)

    def test_same_environment_renders_identical_png_bytes(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            first = Path(temporary) / "first.png"
            second = Path(temporary) / "second.png"
            for output in (first, second):
                plot_data_comparison(
                    self._numeric_statistics(),
                    benchmark="tpch",
                    benchmark_title="TPC-H",
                    scenario="price_outliers",
                    seed=42,
                    output_path=output,
                )
            first_hash = hashlib.sha256(first.read_bytes()).hexdigest()
            second_hash = hashlib.sha256(second.read_bytes()).hexdigest()
        self.assertEqual(first_hash, second_hash)

    def test_missing_matplotlib_has_actionable_optional_dependency_error(self) -> None:
        with mock.patch("visualization.plots.importlib.util.find_spec", return_value=None):
            with self.assertRaisesRegex(
                MissingOptionalDependency, r"\.\[visualization\]"
            ):
                require_matplotlib()


class LazyImportAndPackagingTests(unittest.TestCase):
    def test_core_and_cli_help_do_not_import_matplotlib(self) -> None:
        script = r'''
import sys

class BlockMatplotlib:
    def find_spec(self, fullname, path=None, target=None):
        if fullname == "matplotlib" or fullname.startswith("matplotlib."):
            raise AssertionError(f"unexpected eager import: {fullname}")
        return None

sys.meta_path.insert(0, BlockMatplotlib())
import driftbench
import driftbench.api
import visualization
from visualization.cli import build_parser
help_text = build_parser().format_help()
assert "generate" in help_text
assert not any(name == "matplotlib" or name.startswith("matplotlib.") for name in sys.modules)
'''
        completed = subprocess.run(
            [sys.executable, "-c", script],
            cwd=REPO_ROOT,
            capture_output=True,
            check=False,
            text=True,
            timeout=60,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr or completed.stdout)

    def test_visualization_dependency_and_package_are_declared(self) -> None:
        text = (REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8")
        core_project = _toml_section(text, "project").lower()
        optional = _toml_section(text, "project.optional-dependencies").lower()
        wheel = _toml_section(text, "tool.hatch.build.targets.wheel")
        sdist = _toml_section(text, "tool.hatch.build.targets.sdist")

        self.assertNotIn("matplotlib", core_project)
        self.assertIn("visualization = [", optional)
        self.assertIn("matplotlib", optional)
        self.assertNotIn("seaborn", optional)
        self.assertIn('"visualization"', wheel)
        self.assertIn('"/visualization"', sdist)

    def test_distributions_contain_gallery_artifacts_and_exclude_runtime_data(self) -> None:
        if importlib.util.find_spec("hatchling") is None:
            self.skipTest("hatchling is not installed; offline distribution build is unavailable")

        with tempfile.TemporaryDirectory() as temporary:
            for target in ("wheel", "sdist"):
                completed = subprocess.run(
                    [
                        sys.executable,
                        "-m",
                        "hatchling",
                        "build",
                        "--target",
                        target,
                        "--directory",
                        temporary,
                    ],
                    cwd=REPO_ROOT,
                    capture_output=True,
                    check=False,
                    text=True,
                    timeout=120,
                )
                self.assertEqual(
                    completed.returncode,
                    0,
                    completed.stderr or completed.stdout,
                )
            wheels = tuple(Path(temporary).glob("*.whl"))
            sdists = tuple(Path(temporary).glob("*.tar.gz"))
            self.assertEqual(len(wheels), 1)
            self.assertEqual(len(sdists), 1)
            with zipfile.ZipFile(wheels[0]) as archive:
                wheel_names = set(archive.namelist())
            with tarfile.open(sdists[0], mode="r:gz") as archive:
                sdist_names = {
                    name.split("/", 1)[1]
                    for name in archive.getnames()
                    if "/" in name
                }

            expected_pngs = {
                f"visualization/figures/{kind}/{benchmark}/{scenario}.png"
                for kind, benchmark, scenario in expected_artifact_keys()
            }
            expected_manifests = {
                f"visualization/manifests/{kind}/{benchmark}/{scenario}.json"
                for kind, benchmark, scenario in expected_artifact_keys()
            }
            expected_specs = {
                f"visualization/specs/{kind}/{benchmark}/{scenario}.yaml"
                for kind, benchmark, scenario in expected_artifact_keys()
            }
            source_pngs = {
                path.relative_to(REPO_ROOT).as_posix()
                for path in (REPO_ROOT / "visualization" / "figures").rglob("*.png")
            }
            source_manifests = {
                path.relative_to(REPO_ROOT).as_posix()
                for path in (REPO_ROOT / "visualization" / "manifests").rglob("*.json")
            }
            source_specs = {
                path.relative_to(REPO_ROOT).as_posix()
                for path in (REPO_ROOT / "visualization" / "specs").rglob("*.yaml")
            }
            self.assertEqual(source_pngs, expected_pngs)
            self.assertEqual(source_manifests, expected_manifests)
            self.assertEqual(source_specs, expected_specs)
            self.assertEqual(len(expected_pngs), 40)
            self.assertEqual(len(expected_manifests), 40)
            self.assertEqual(len(expected_specs), 40)

            for names in (wheel_names, sdist_names):
                self.assertIn("visualization/GALLERY.md", names)
                self.assertIn("visualization/configs/benchmarks.yaml", names)
                self.assertIn("visualization/configs/drift_scenarios.yaml", names)
                self.assertEqual(
                    {name for name in names if name.startswith("visualization/figures/") and name.endswith(".png")},
                    expected_pngs,
                )
                self.assertEqual(
                    {name for name in names if name.startswith("visualization/manifests/") and name.endswith(".json")},
                    expected_manifests,
                )
                self.assertEqual(
                    {
                        name
                        for name in names
                        if name.startswith("visualization/specs/")
                        and name.endswith(".yaml")
                    },
                    expected_specs,
                )
                self.assertFalse(
                    any(
                        name.startswith(("visualization/data/", "visualization/cache/"))
                        for name in names
                    )
                )


if __name__ == "__main__":
    unittest.main()
