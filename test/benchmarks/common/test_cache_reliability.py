from __future__ import annotations

import hashlib
import json
import unittest
from pathlib import Path
from unittest import mock

from driftbench.data.ycsb import YCSBData
from ..helpers import (
    ReliabilityTestMixin,
    csv_data_rows as _csv_data_rows,
    read_json as _read_json,
)


def _case_parameter_cache_is_strict_and_force_bypasses(tmp_path: Path) -> None:
    first = YCSBData(scale_factor=1, record_count=2).generate(output_dir=tmp_path)
    manifest = _read_json(first.metadata)
    cache = manifest["cache"]
    assert cache["schema"] == "driftbench.benchmark-cache"
    assert cache["version"] == 2
    assert cache["generator"].endswith("YCSBData/data")
    assert cache["parameters"] == {"record_count": 2, "scale_factor": 1}
    assert len(cache["fingerprint"]) == 64
    int(cache["fingerprint"], 16)

    data_file = first.files[0]
    original_bytes = data_file.read_bytes()
    assert cache["artifacts"] == [
        {
            "path": manifest["files"][0],
            "bytes": len(original_bytes),
            "sha256": hashlib.sha256(original_bytes).hexdigest(),
        }
    ]

    # Identical parameters and intact managed files reuse without regenerating.
    with mock.patch.object(
        YCSBData,
        "_generate_synth",
        side_effect=AssertionError("intact cache should be reused"),
    ):
        reused = YCSBData(scale_factor=1.0, record_count=2).generate(
            output_dir=tmp_path
        )
    assert reused.files == first.files

    # A size mismatch must invalidate the cache.
    data_file.write_bytes(original_bytes + b"x")
    repaired = YCSBData(scale_factor=1, record_count=2).generate(output_dir=tmp_path)
    assert repaired.files[0].read_bytes() == original_bytes

    # A same-size content/hash mismatch must also invalidate the cache.
    same_size_corruption = bytearray(original_bytes)
    same_size_corruption[0] ^= 0xFF
    data_file.write_bytes(same_size_corruption)
    repaired = YCSBData(scale_factor=1, record_count=2).generate(output_dir=tmp_path)
    assert repaired.files[0].read_bytes() == original_bytes

    # Simulate an interrupted writer partially overwriting the middle of a file.
    partial_overwrite = bytearray(original_bytes)
    start = len(partial_overwrite) // 2
    end = min(len(partial_overwrite), start + 32)
    partial_overwrite[start:end] = bytes(
        byte ^ 0xFF for byte in partial_overwrite[start:end]
    )
    data_file.write_bytes(partial_overwrite)
    repaired = YCSBData(scale_factor=1, record_count=2).generate(output_dir=tmp_path)
    assert repaired.files[0].read_bytes() == original_bytes

    changed = YCSBData(scale_factor=1, record_count=3).generate(output_dir=tmp_path)
    assert _csv_data_rows(changed.files[0]) == 3
    assert _read_json(changed.metadata)["cache"]["fingerprint"] != cache["fingerprint"]

    # Legacy, corrupt, identity mismatch, descriptor mismatch, file mapping
    # mismatch, and missing-file manifests all fail closed.
    cases = (
        "legacy",
        "corrupt",
        "generator",
        "fingerprint",
        "artifact-size",
        "artifact-hash",
        "artifact-path",
        "missing",
    )
    for case in cases:
        current = _read_json(changed.metadata)
        if case == "legacy":
            current.pop("cache")
            changed.metadata.write_text(json.dumps(current), encoding="utf-8")
        elif case == "corrupt":
            changed.metadata.write_text("{", encoding="utf-8")
        elif case == "generator":
            current["cache"]["generator"] = "wrong/generator"
            changed.metadata.write_text(json.dumps(current), encoding="utf-8")
        elif case == "fingerprint":
            current["cache"]["fingerprint"] = "0" * 64
            changed.metadata.write_text(json.dumps(current), encoding="utf-8")
        elif case == "artifact-size":
            current["cache"]["artifacts"][0]["bytes"] += 1
            changed.metadata.write_text(json.dumps(current), encoding="utf-8")
        elif case == "artifact-hash":
            current["cache"]["artifacts"][0]["sha256"] = "0" * 64
            changed.metadata.write_text(json.dumps(current), encoding="utf-8")
        elif case == "artifact-path":
            current["cache"]["artifacts"][0]["path"] = "different.csv"
            changed.metadata.write_text(json.dumps(current), encoding="utf-8")
        else:  # missing
            changed.files[0].unlink()

        changed = YCSBData(scale_factor=1, record_count=3).generate(output_dir=tmp_path)
        assert _csv_data_rows(changed.files[0]) == 3

    changed.files[0].write_text("sentinel\n", encoding="utf-8")
    forced = YCSBData(scale_factor=1, record_count=3).generate(
        output_dir=tmp_path, force=True
    )
    assert _csv_data_rows(forced.files[0]) == 3

    contained_root = tmp_path / "contained"
    contained = YCSBData(scale_factor=1, record_count=2).generate(
        output_dir=contained_root
    )
    outside = tmp_path / "outside-sentinel.csv"
    outside.write_text("do-not-touch\n", encoding="utf-8")
    for untrusted_path in (str(outside.resolve()), "../outside-sentinel.csv"):
        payload = _read_json(contained.metadata)
        payload["files"] = [untrusted_path]
        payload["cache"]["artifacts"] = [
            {
                "path": untrusted_path,
                "bytes": outside.stat().st_size,
                "sha256": hashlib.sha256(outside.read_bytes()).hexdigest(),
            }
        ]
        contained.metadata.write_text(json.dumps(payload), encoding="utf-8")

        contained = YCSBData(scale_factor=1, record_count=2).generate(
            output_dir=contained_root
        )
        assert _csv_data_rows(contained.files[0]) == 2
        assert outside.read_text(encoding="utf-8") == "do-not-touch\n"

class BenchmarkReliabilityTests(ReliabilityTestMixin, unittest.TestCase):
    def test_parameter_cache_is_strict_and_force_bypasses(self) -> None:
        self._run_case(_case_parameter_cache_is_strict_and_force_bypasses)
