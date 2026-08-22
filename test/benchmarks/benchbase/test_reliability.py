from __future__ import annotations

import json
import unittest
from pathlib import Path
from xml.etree import ElementTree

from driftbench.data.benchbase import (
    _BENCHBASE_BENCHMARKS,
    BenchBaseData,
    BenchBaseQueries,
)
from ..helpers import ReliabilityTestMixin, read_json as _read_json


def _case_benchbase_all_load_and_execute_xml_is_parseable(tmp_path: Path) -> None:
    url = "jdbc:postgresql://db/bench?sslmode=disable&tag=<special>"
    username = "user&<admin>"
    password = "secret&<value>"

    for benchmark, metadata in _BENCHBASE_BENCHMARKS.items():
        load = BenchBaseData(
            benchmark_name=benchmark,
            scale_factor=1.5,
            terminals=2,
            db_url=url,
            username=username,
            password=password,
        ).generate(output_dir=tmp_path)
        execute = BenchBaseQueries(
            benchmark_name=benchmark,
            scale_factor=1.5,
            terminals=3,
            duration=7,
            rate=11,
            db_url=url,
            username=username,
            password=password,
        ).generate(output_dir=tmp_path)

        for result, config_name in (
            (load, f"{benchmark}_load_config.xml"),
            (execute, f"{benchmark}_execute_config.xml"),
        ):
            xml_path = next(path for path in result.files if path.suffix == ".xml")
            script_path = next(path for path in result.files if path.suffix == ".sh")
            root = ElementTree.parse(xml_path).getroot()
            assert root.findtext("url") == url
            assert root.findtext("username") == username
            assert root.findtext("password") == password
            assert root.findtext("scalefactor") == "1.5"
            assert [node.text for node in root.findall("./transactiontypes/transactiontype/name")] == [
                transaction[0] for transaction in metadata["transactions"]
            ]
            assert config_name in script_path.read_text(encoding="utf-8")

            manifest_text = result.metadata.read_text(encoding="utf-8")
            assert password not in manifest_text
            assert url not in manifest_text
            cache_parameters = _read_json(result.metadata)["cache"]["parameters"]
            assert cache_parameters["password"] == "<redacted>"
            assert cache_parameters["db_url"] == "<redacted>"

        load_xml = ElementTree.parse(next(path for path in load.files if path.suffix == ".xml")).getroot()
        execute_xml = ElementTree.parse(next(path for path in execute.files if path.suffix == ".xml")).getroot()
        assert load_xml.findtext("terminals") == "2"
        assert load_xml.findtext("./works/work/time") == "0"
        assert load_xml.findtext("./works/work/rate") == "unlimited"
        assert execute_xml.findtext("terminals") == "3"
        assert execute_xml.findtext("./works/work/time") == "7"
        assert execute_xml.findtext("./works/work/rate") == "11"


def _case_benchbase_password_changes_fingerprint_without_manifest_leak(tmp_path: Path) -> None:
    first_url = "jdbc:postgresql://first-user:first-url-secret@localhost/benchbase"
    second_url = "jdbc:postgresql://second-user:second-url-secret@localhost/benchbase"
    first = BenchBaseQueries(db_url=first_url, password="first-secret").generate(
        output_dir=tmp_path
    )
    first_fingerprint = _read_json(first.metadata)["cache"]["fingerprint"]

    changed_url = BenchBaseQueries(db_url=second_url, password="first-secret").generate(
        output_dir=tmp_path
    )
    changed_url_fingerprint = _read_json(changed_url.metadata)["cache"]["fingerprint"]
    assert changed_url_fingerprint != first_fingerprint

    second = BenchBaseQueries(db_url=second_url, password="second-secret").generate(
        output_dir=tmp_path
    )
    manifest_text = second.metadata.read_text(encoding="utf-8")
    second_cache = json.loads(manifest_text)["cache"]
    assert second_cache["fingerprint"] != changed_url_fingerprint
    assert second_cache["parameters"]["password"] == "<redacted>"
    assert second_cache["parameters"]["db_url"] == "<redacted>"
    assert "first-secret" not in manifest_text
    assert "second-secret" not in manifest_text
    assert first_url not in manifest_text
    assert second_url not in manifest_text
    xml_path = next(path for path in second.files if path.suffix == ".xml")
    xml_root = ElementTree.parse(xml_path).getroot()
    assert xml_root.findtext("url") == second_url
    assert xml_root.findtext("password") == "second-secret"

class BenchmarkReliabilityTests(ReliabilityTestMixin, unittest.TestCase):
    def test_benchbase_all_load_and_execute_xml_is_parseable(self) -> None:
        self._run_case(_case_benchbase_all_load_and_execute_xml_is_parseable)

    def test_benchbase_password_changes_fingerprint_without_manifest_leak(self) -> None:
        self._run_case(_case_benchbase_password_changes_fingerprint_without_manifest_leak)
