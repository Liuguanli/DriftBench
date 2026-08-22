#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

python3 - <<'PY'
import unittest

suite = unittest.defaultTestLoader.discover(
    "test/benchmarks",
    pattern="test_adapter*.py",
    top_level_dir=".",
)
count = suite.countTestCases()
print(f"adapter tests discovered: {count}")
assert count == 47, count
PY
python3 -m unittest discover -s test/benchmarks -t . -p 'test_adapter*.py' -v
