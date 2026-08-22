from __future__ import annotations

import random
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import numpy as np
import yaml

from driftbench.spec.core import run_all, run_spec


def _numpy_state_equal(first, second) -> bool:
    return (
        first[0] == second[0]
        and np.array_equal(first[1], second[1])
        and first[2:] == second[2:]
    )


class SpecRuntimeBindingTests(unittest.TestCase):
    def _write_spec(self, root: Path, variables: dict, *, seed: int = 17) -> Path:
        path = root / "runtime.driftspec.yaml"
        path.write_text(
            yaml.safe_dump(
                {
                    "seed": seed,
                    "type": {
                        "family": "test",
                        "category": "runtime",
                        "subtype": "echo",
                    },
                    "variables": variables,
                },
                sort_keys=False,
            ),
            encoding="utf-8",
        )
        return path

    def test_exact_bindings_preserve_values_and_return_handler_result(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = self._write_spec(
                Path(tmp),
                {
                    "output": "${OUTPUT}",
                    "count": "${COUNT}",
                    "literal": "prefix-${OUTPUT}",
                    "nested": {"${KEY}": "${OUTPUT}"},
                },
            )

            def handler(spec):
                return spec["variables"]

            with patch("driftbench.spec.core.get_handler", return_value=handler):
                result = run_spec(
                    path,
                    bindings={"OUTPUT": "result.json", "COUNT": 3, "KEY": "bound"},
                )

            self.assertEqual(result["output"], "result.json")
            self.assertEqual(result["count"], 3)
            self.assertEqual(result["literal"], "prefix-${OUTPUT}")
            self.assertEqual(result["nested"], {"bound": "result.json"})

    def test_missing_and_unused_bindings_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = self._write_spec(Path(tmp), {"output": "${OUTPUT}"})
            with self.assertRaisesRegex(ValueError, "missing bindings.*OUTPUT"):
                run_spec(path)
            with self.assertRaisesRegex(ValueError, "unused bindings.*EXTRA"):
                run_spec(
                    path,
                    bindings={"OUTPUT": "result.json", "EXTRA": "not-used"},
                )

    def test_partial_interpolation_does_not_consume_a_binding(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = self._write_spec(Path(tmp), {"literal": "prefix-${NAME}"})
            with self.assertRaisesRegex(ValueError, "unused bindings.*NAME"):
                run_spec(path, bindings={"NAME": "value"})

    def test_nonempty_runtime_inputs_require_handler_support(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = self._write_spec(Path(tmp), {})
            with patch(
                "driftbench.spec.core.get_handler", return_value=lambda spec: None
            ):
                with self.assertRaisesRegex(ValueError, "does not accept runtime_inputs"):
                    run_all(path, runtime_inputs={"templates": ()})


class SpecRuntimeRngTests(unittest.TestCase):
    def _spec(self, root: Path) -> Path:
        path = root / "rng.driftspec.yaml"
        path.write_text(
            yaml.safe_dump(
                {
                    "seed": 23,
                    "type": {
                        "family": "test",
                        "category": "runtime",
                        "subtype": "rng",
                    },
                    "variables": {},
                }
            ),
            encoding="utf-8",
        )
        return path

    def test_runtime_inputs_are_forwarded_and_rng_states_restored(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = self._spec(Path(tmp))
            original_python = random.getstate()
            original_numpy = np.random.get_state()
            try:
                random.seed(8675309)
                np.random.seed(24680)
                before_python = random.getstate()
                before_numpy = np.random.get_state()

                def handler(spec, *, runtime_inputs):
                    return {
                        "python": random.random(),
                        "numpy": float(np.random.random()),
                        "runtime": runtime_inputs["value"],
                    }

                with patch("driftbench.spec.core.get_handler", return_value=handler):
                    result = run_spec(path, runtime_inputs={"value": "seen"})

                self.assertEqual(result["runtime"], "seen")
                self.assertEqual(result["python"], random.Random(23).random())
                self.assertEqual(result["numpy"], np.random.RandomState(23).random())
                self.assertEqual(random.getstate(), before_python)
                self.assertTrue(_numpy_state_equal(np.random.get_state(), before_numpy))
            finally:
                random.setstate(original_python)
                np.random.set_state(original_numpy)

    def test_rng_states_are_restored_when_handler_raises(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = self._spec(Path(tmp))
            before_python = random.getstate()
            before_numpy = np.random.get_state()

            def handler(spec):
                random.random()
                np.random.random()
                raise RuntimeError("boom")

            with patch("driftbench.spec.core.get_handler", return_value=handler):
                with self.assertRaisesRegex(RuntimeError, "boom"):
                    run_spec(path)

            self.assertEqual(random.getstate(), before_python)
            self.assertTrue(_numpy_state_equal(np.random.get_state(), before_numpy))


if __name__ == "__main__":
    unittest.main()
