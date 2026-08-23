import pathlib
import re
import unittest

import driftbench


class VersionConsistencyTests(unittest.TestCase):
    def test_runtime_and_project_versions_match_target(self) -> None:
        root = pathlib.Path(__file__).resolve().parents[1]
        project_text = (root / "pyproject.toml").read_text(encoding="utf-8")
        project_section = project_text.split("[project]", 1)[1].split("\n[", 1)[0]
        match = re.search(r'^version\s*=\s*"([^"]+)"', project_section, re.MULTILINE)
        self.assertIsNotNone(match)
        project_version = match.group(1)
        self.assertEqual(project_version, "0.1.0")
        self.assertEqual(driftbench.__version__, project_version)
if __name__ == "__main__":
    unittest.main()
