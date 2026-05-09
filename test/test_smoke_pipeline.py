import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

import yaml


REPO_ROOT = Path(__file__).resolve().parents[1]
TRACE_INPUT = REPO_ROOT / "driftspec/trace_inputs/trace_data_mock.csv"


def run_cli(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, "-m", "driftbench.cli", *args],
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
        check=False,
    )


class SmokePipelineTests(unittest.TestCase):
    def test_trace_to_spec_validate_dry_run_and_execute(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            spec_path = tmp_path / "trace_generated.yaml"

            p1 = run_cli("trace-to-spec", str(TRACE_INPUT), str(spec_path))
            self.assertEqual(p1.returncode, 0, msg=p1.stderr)
            self.assertTrue(spec_path.exists())

            spec_obj = yaml.safe_load(spec_path.read_text(encoding="utf-8"))
            ds = spec_obj.setdefault("data_source", {})
            se = ds.setdefault("schema_extractor", {})
            se.setdefault("source_type", "csv")
            se["schema_output_dir"] = str(tmp_path / "schemas")

            drifts = ((spec_obj.get("variables") or {}).get("drifts") or [])
            for i, drift in enumerate(drifts):
                drift["output_path"] = str(tmp_path / f"smoke_out_{i}.csv")
            spec_path.write_text(yaml.safe_dump(spec_obj, sort_keys=False), encoding="utf-8")

            p2 = run_cli("validate-spec", str(spec_path), "--json")
            self.assertEqual(p2.returncode, 0, msg=p2.stderr)

            p3 = run_cli("dry-run", str(spec_path), "--json")
            self.assertEqual(p3.returncode, 0, msg=p3.stderr)

            p4 = run_cli("run-yaml", str(spec_path))
            self.assertEqual(p4.returncode, 0, msg=p4.stderr)

            for i in range(len(drifts)):
                self.assertTrue((tmp_path / f"smoke_out_{i}.csv").exists())


if __name__ == "__main__":
    unittest.main()
