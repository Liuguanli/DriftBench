"""Reproduce the shipped own-input teaching examples against local artifacts."""

import json
from pathlib import Path
import subprocess
import sys

import pandas as pd
import pytest
import yaml

from driftbench.api import run_spec


ROOT = Path(__file__).resolve().parents[1]
EXAMPLES = ROOT / "docs/examples/run-guide"
SOURCE = EXAMPLES / "own-inputs/orders.csv"


def write_spec(tmp_path, change=None):
    spec = yaml.safe_load((EXAMPLES / "own-data-drift.yaml").read_text())
    spec["data_source"]["path"] = str(SOURCE)
    spec["data_source"]["schema_extractor"]["schema_output_path"] = str(tmp_path / "schema.json")
    spec["variables"]["drifts"][0]["output_path"] = str(tmp_path / "drifted.csv")
    if change:
        change(spec)
    path = tmp_path / "spec.yaml"
    path.write_text(yaml.safe_dump(spec), encoding="utf-8")
    return path


def test_data_example_changes_only_selected_values_and_repeats(tmp_path):
    original = SOURCE.read_bytes()
    baseline = pd.read_csv(SOURCE)
    path = write_spec(tmp_path)
    run_spec(str(path))
    output = tmp_path / "drifted.csv"
    first = output.read_bytes()
    drifted = pd.read_csv(output)
    assert list(drifted.columns) == list(baseline.columns)
    assert len(drifted) == len(baseline) == 12
    assert (drifted["amount"] != baseline["amount"]).sum() == 9
    pd.testing.assert_frame_equal(drifted[["order_id", "region"]], baseline[["order_id", "region"]])
    run_spec(str(path))
    assert output.read_bytes() == first
    assert SOURCE.read_bytes() == original


@pytest.mark.parametrize("missing", ["file", "column"])
def test_data_example_surfaces_missing_inputs_without_drift_output(tmp_path, missing):
    def change(spec):
        if missing == "file":
            spec["data_source"]["path"] = str(tmp_path / "missing-orders.csv")
        else:
            spec["variables"]["drifts"][0]["columns"] = ["missing_amount"]
    path = write_spec(tmp_path, change)
    error, text = (FileNotFoundError, "missing-orders") if missing == "file" else (KeyError, "missing_amount")
    with pytest.raises(error, match=text):
        run_spec(str(path))
    assert not (tmp_path / "drifted.csv").exists()


def test_query_script_retains_sql_counts_seed_and_existing_outputs(tmp_path):
    def generate(name):
        return subprocess.run(
            [sys.executable, str(EXAMPLES / "own_query_drift.py"), "--output-dir", str(tmp_path / name)],
            cwd=ROOT, capture_output=True, text=True, timeout=60,
        )
    for name in ("first", "second"):
        completed = generate(name)
        assert completed.returncode == 0, completed.stderr
    first = tmp_path / "first"
    metadata = json.loads((first / "mix.json").read_text())
    assert metadata["seed"] == 42
    assert metadata["sample_size"] == 100
    templates = metadata["templates"]
    assert set(templates) == {"lookup", "report"}
    for phase in ("baseline", "drifted"):
        statements = (first / f"{phase}.sql").read_text().splitlines()
        assert len(statements) == 100
        assert set(statements) == set(templates.values())
        assert metadata["observed_counts"][phase] == {
            key: statements.count(sql) for key, sql in templates.items()
        }
    assert metadata["observed_counts"]["baseline"]["lookup"] > metadata["observed_counts"]["drifted"]["lookup"]
    files = {p.name: p.read_bytes() for p in first.iterdir()}
    assert files == {p.name: p.read_bytes() for p in (tmp_path / "second").iterdir()}
    rejected = generate("first")
    assert rejected.returncode != 0
    assert "choose a new directory" in rejected.stderr
    assert files == {p.name: p.read_bytes() for p in first.iterdir()}
