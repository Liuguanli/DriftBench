import tempfile
import unittest
from pathlib import Path
from unittest import mock

from driftbench.data import GenerationResult
from driftbench.data.ycsb import YCSBData
from ..helpers import BenchmarkAdapterTestMixin


class BenchmarkAdapterTests(BenchmarkAdapterTestMixin, unittest.TestCase):
    def test_output_dir_defaults_when_none(self) -> None:
        # setUp redirects DRIFTBENCH_DATA_DIR to a temp dir, so generate(output_dir=None)
        # writes there instead of ~/.driftbench/data/.
        result = YCSBData(scale_factor=1).generate(output_dir=None)
        self.assertIsInstance(result, GenerationResult)
        self.assertTrue(result.output_dir.exists())
        self.assertTrue(result.output_dir.resolve().is_relative_to(Path(self._default_data_tmpdir).resolve()))

    def test_generate_reuses_intact_data_without_force(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "out"
            adapter = YCSBData(scale_factor=1)
            r1 = adapter.generate(output_dir=out)
            with mock.patch.object(
                YCSBData,
                "_generate_synth",
                side_effect=AssertionError("intact cache should be reused"),
            ):
                r2 = adapter.generate(output_dir=out)
            self.assertEqual(r1.files, r2.files)

    def test_generate_regenerates_tampered_data_without_force(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "out"
            adapter = YCSBData(scale_factor=1)
            r1 = adapter.generate(output_dir=out)
            managed_file = r1.files[0]
            original_content = managed_file.read_bytes()
            managed_file.write_text("SENTINEL", encoding="utf-8")

            r2 = adapter.generate(output_dir=out)

            self.assertEqual(managed_file.read_bytes(), original_content)
            self.assertEqual(r1.files, r2.files)

    def test_generate_force_regenerates_existing_data(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "out"
            adapter = YCSBData(scale_factor=1)
            r1 = adapter.generate(output_dir=out)
            sentinel = r1.files[0]
            original_content = sentinel.read_text(encoding="utf-8")
            sentinel.write_text("SENTINEL", encoding="utf-8")
            # force=True should overwrite
            adapter.generate(output_dir=out, force=True)
            self.assertEqual(sentinel.read_text(encoding="utf-8"), original_content)
