import random
import unittest

import numpy as np

from driftbench.spec.core import (
    get_type_triple,
    migrate_spec,
    seed_everything,
    validate_spec,
)


class SpecCoreUnitTests(unittest.TestCase):
    def test_migrate_spec_is_idempotent(self) -> None:
        spec = {"type": {"family": "data"}, "variables": {}}
        migrated_once = migrate_spec(dict(spec))
        migrated_twice = migrate_spec(dict(migrated_once))
        self.assertEqual(migrated_once.get("spec_version"), 1)
        self.assertEqual(migrated_twice.get("spec_version"), 1)

    def test_validate_spec_requires_type_and_variables(self) -> None:
        with self.assertRaises(ValueError):
            validate_spec({})
        with self.assertRaises(ValueError):
            validate_spec({"type": {"family": "data"}})
        with self.assertRaises(ValueError):
            validate_spec({"variables": {}})
        validate_spec({"type": {"family": "data"}, "variables": {}})

    def test_get_type_triple(self) -> None:
        spec = {
            "type": {"family": "workload", "category": "templates", "subtype": "selection_payload"},
            "variables": {},
        }
        self.assertEqual(
            get_type_triple(spec),
            ("workload", "templates", "selection_payload"),
        )

    def test_seed_everything_is_deterministic(self) -> None:
        seed_everything(7)
        a_random = [random.random() for _ in range(3)]
        a_np = np.random.rand(3).tolist()

        seed_everything(7)
        b_random = [random.random() for _ in range(3)]
        b_np = np.random.rand(3).tolist()

        self.assertEqual(a_random, b_random)
        self.assertEqual(a_np, b_np)


if __name__ == "__main__":
    unittest.main()

