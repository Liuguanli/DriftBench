import io
import os
import random
import socket
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import numpy as np
import yaml

import driftbench.data.tpch as tpch_adapter
from driftbench.spec import registry
from driftbench.spec.preflight import deep_validate_spec_file


class DeepValidationSideEffectTests(unittest.TestCase):
    def test_preflight_does_not_execute_or_mutate_process_or_filesystem_state(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            spec = {
                "spec_version": 1,
                "pattern_id": "side-effect-test",
                "seed": 42,
                "type": {
                    "family": "workload",
                    "category": "drift",
                    "subtype": "template_mix",
                },
                "metadata": {"benchmark": "tpch"},
                "data_source": {
                    "kind": "benchmark_adapter",
                    "benchmark": "tpch",
                },
                "variables": {
                    "template_ids": ["q1", "q2"],
                    "baseline": {"mode": "uniform"},
                    "target": {"weights": {"q1": 3, "q2": 1}},
                    "sample_size": 10,
                    "output_path": "result.json",
                },
            }
            spec_path = root / "spec.yaml"
            spec_path.write_text(yaml.safe_dump(spec, sort_keys=False), encoding="utf-8")

            def snapshot() -> dict[str, tuple[bytes, int]]:
                return {
                    path.relative_to(root).as_posix(): (
                        path.read_bytes(),
                        path.stat().st_mtime_ns,
                    )
                    for path in root.rglob("*")
                    if path.is_file()
                }

            before_tree = snapshot()
            before_cwd = Path.cwd()
            before_environment = dict(os.environ)
            random.seed(123456)
            np.random.seed(654321)
            before_python_rng = random.getstate()
            before_numpy_rng = np.random.get_state()

            fake_handler = mock.Mock(name="handler_that_must_not_execute")
            real_io_open = io.open

            def read_only_open(file, mode="r", *args, **kwargs):
                self.assertFalse(
                    any(marker in mode for marker in ("w", "a", "x", "+")),
                    msg=f"validation attempted write mode {mode!r}",
                )
                return real_io_open(file, mode, *args, **kwargs)

            key = ("workload", "drift", "template_mix")
            with mock.patch.dict(registry._REGISTRY, {key: fake_handler}), mock.patch(
                "io.open", side_effect=read_only_open
            ), mock.patch.object(Path, "mkdir") as mkdir, mock.patch(
                "os.makedirs"
            ) as makedirs, mock.patch("os.replace") as replace, mock.patch(
                "tempfile.mkstemp"
            ) as mkstemp, mock.patch.object(
                subprocess, "run", side_effect=AssertionError("subprocess called")
            ), mock.patch.object(
                socket,
                "create_connection",
                side_effect=AssertionError("network called"),
            ), mock.patch.object(
                tpch_adapter.TPCHData,
                "generate",
                side_effect=AssertionError("adapter data generation called"),
            ), mock.patch.object(
                tpch_adapter.TPCHQueries,
                "generate",
                side_effect=AssertionError("adapter query generation called"),
            ):
                report = deep_validate_spec_file(spec_path, working_dir=root)

            self.assertTrue(report.valid, msg=report.as_dict())
            fake_handler.assert_not_called()
            mkdir.assert_not_called()
            makedirs.assert_not_called()
            replace.assert_not_called()
            mkstemp.assert_not_called()
            self.assertEqual(Path.cwd(), before_cwd)
            self.assertEqual(dict(os.environ), before_environment)
            self.assertEqual(random.getstate(), before_python_rng)

            after_numpy_rng = np.random.get_state()
            self.assertEqual(after_numpy_rng[0], before_numpy_rng[0])
            np.testing.assert_array_equal(after_numpy_rng[1], before_numpy_rng[1])
            self.assertEqual(after_numpy_rng[2:], before_numpy_rng[2:])
            self.assertEqual(snapshot(), before_tree)


if __name__ == "__main__":
    unittest.main()
