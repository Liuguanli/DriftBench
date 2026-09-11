from __future__ import annotations

import hashlib
import json
import os
import shutil
import tempfile
import unittest
from contextlib import ExitStack
from pathlib import Path
from unittest.mock import patch

from driftbench.data import ldbc as ldbc_module
from driftbench.data.ldbc import LDBCData, LDBCQueries, data, queries
from ..helpers import BenchmarkAdapterTestMixin
from .fixtures import (
    HEADERS, PREFIX, PRIVATE_DUMMY, UPDATES,
    write_config, write_data, write_parameters, write_updates,
)


def snapshot(root: Path) -> dict[str, bytes]:
    return {path.relative_to(root).as_posix(): path.read_bytes() for path in root.rglob("*") if path.is_file()}


def read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def fixture_queries(parent: Path, **options):
    parameters = write_parameters(parent / "parameters")
    config = write_config(parent / "native/config.properties")
    return queries(parameters_dir=parameters, driver_config=config, **options), parameters, config


def directory_link(source: Path, target: Path) -> None:
    try:
        target.symlink_to(source, target_is_directory=True)
    except OSError:
        if os.name != "nt":
            raise
        import _winapi
        _winapi.CreateJunction(str(source), str(target))


class LDBCAdapterTests(BenchmarkAdapterTestMixin, unittest.TestCase):
    def test_data_stages_twenty_real_layout_families_and_source_hashes(self):
        with tempfile.TemporaryDirectory() as tmp:
            parent = Path(tmp)
            source = write_data(parent / "source")
            original = snapshot(source)
            root = parent / "output"
            result = data(source_dir=source).generate(root)
            self._assert_result_is_filesystem_contract(result, root)
            self.assertEqual(len(result.files), 20)
            self.assertEqual(snapshot(root / "ldbc/data/social_network"), original)
            self.assertEqual(snapshot(source), original)
            payload = read_json(result.metadata)
            self.assertEqual(payload["suite"], "snb-interactive-v1")
            self.assertEqual(payload["format"], "csv_merge_foreign")
            self.assertEqual(payload["date_format"], "epoch_millis")
            self.assertEqual(payload["partition_files"], 20)
            self.assertEqual(payload["tables"]["dynamic/person"], 1)
            self.assertEqual(set(payload["tables"]), set(HEADERS))
            self.assertEqual(payload["datagen_revision"], "37d35f40f5023fcf1afd3b6d0984f71c202f4bca")
            for name, content in original.items():
                self.assertEqual(payload["source_files"][f"ldbc/data/social_network/{name}"], {
                    "bytes": len(content), "sha256": hashlib.sha256(content).hexdigest(),
                })

    def test_data_accepts_declared_iso_dates_and_multiple_partition_files(self):
        with tempfile.TemporaryDirectory() as tmp:
            source = write_data(Path(tmp) / "source", date_format="iso8601")
            person = source / "dynamic/person_0_0.csv"
            original = person.read_bytes()
            self.assertIn(b"|1990-01-01|2012-01-01T00:00:00.000+0000|", original)
            extra = source / "dynamic/person_1_0.csv"
            extra.write_text(person.read_text().replace("1|Ada", "2|Grace"))
            result = data(source, date_format="iso8601").generate(Path(tmp) / "output")
            payload = read_json(result.metadata)
            self.assertEqual(payload["partition_files"], 21)
            self.assertEqual(payload["tables"]["dynamic/person"], 2)
            self.assertEqual(payload["date_format"], "iso8601")
            self.assertEqual((result.output_dir / "ldbc/data/social_network/dynamic/person_0_0.csv").read_bytes(), original)
            self.assertEqual((result.output_dir / "ldbc/data/social_network/dynamic/person_1_0.csv").read_bytes(), extra.read_bytes())
            self.assertEqual(person.read_bytes(), original)

    def test_read_only_plan_preserves_private_user_config_and_prioritizes_overrides(self):
        with tempfile.TemporaryDirectory() as tmp:
            parent = Path(tmp)
            adapter, parameters, config = fixture_queries(parent)
            original_config = config.read_bytes()
            original_parameters = snapshot(parameters)
            root = parent / "output"
            result = adapter.generate(root)
            self._assert_result_is_filesystem_contract(result, root)
            work = root / "ldbc/queries"
            self.assertEqual((work / "user.properties").read_bytes(), original_config)
            self.assertEqual(snapshot(work / "substitution_parameters"), original_parameters)
            self.assertEqual(config.read_bytes(), original_config)
            self.assertEqual(snapshot(parameters), original_parameters)
            self.assertNotIn(PRIVATE_DUMMY, result.metadata.read_text())
            payload = read_json(result.metadata)
            self.assertTrue(payload["read_only"])
            self.assertEqual(payload["parameter_files"], 14)
            self.assertEqual(payload["source_files"]["ldbc/queries/user.properties"]["sha256"], hashlib.sha256(original_config).hexdigest())
            self.assertEqual(payload["driver_revision"], "4cd13735f964406ad34f34ccd5bef4d6e6c284d0")
            overrides = (work / "overrides.properties").read_text()
            self.assertIn("FIRST -P", overrides)
            self.assertIn("workload=org.ldbcouncil.snb.driver.workloads.interactive.LdbcSnbInteractiveWorkload", overrides)
            for name in UPDATES:
                self.assertIn(f"{PREFIX}LdbcUpdate{name}_enable=false\n", overrides)
            self.assertIn("empty-updates", overrides)
            self.assertEqual(list((work / "empty-updates").glob("updateStream*")), [])
            command = read_json(work / "command.json")
            self.assertFalse(command["executed"])
            self.assertEqual(command["cwd"], str(config.parent))
            argv = command["argv"]
            self.assertEqual(argv[:2], ["java", "-cp"])
            self.assertEqual([argv[index + 1] for index, value in enumerate(argv[:-1]) if value == "-P"], [str(work / "overrides.properties"), str(work / "user.properties")])

    def test_mixed_plan_preserves_operation_flags_and_paired_update_stream_bytes(self):
        with tempfile.TemporaryDirectory() as tmp:
            parent = Path(tmp)
            updates = write_updates(parent / "updates")
            adapter, _, config = fixture_queries(parent, updates_dir=updates, read_only=False)
            source_updates = snapshot(updates)
            result = adapter.generate(parent / "output")
            work = result.output_dir / "ldbc/queries"
            self.assertFalse(read_json(result.metadata)["read_only"])
            self.assertEqual(snapshot(work / "updates"), source_updates)
            self.assertEqual((work / "user.properties").read_bytes(), config.read_bytes())
            overrides = (work / "overrides.properties").read_text()
            self.assertNotIn("LdbcUpdate", overrides)
            self.assertNotIn("empty-updates", overrides)
            self.assertEqual(snapshot(updates), source_updates)

    def test_cache_reuses_verified_files_and_force_is_byte_stable(self):
        with tempfile.TemporaryDirectory() as tmp:
            parent = Path(tmp)
            source = write_data(parent / "source")
            query_adapter, _, _ = fixture_queries(parent)
            root = parent / "output"
            for adapter in (data(source), query_adapter):
                first = adapter.generate(root)
                paths = [*first.files, first.metadata]
                expected = {str(path): path.read_bytes() for path in paths}
                self.assertTrue(adapter.generate(root).reused_local)
                self.assertFalse(adapter.generate(root, force=True).reused_local)
                self.assertEqual({str(path): path.read_bytes() for path in paths}, expected)
                for path in (first.files[0], first.metadata):
                    path.write_bytes(b"tampered")
                    self.assertFalse(adapter.generate(root).reused_local)
                    self.assertEqual({str(item): item.read_bytes() for item in paths}, expected)
                    path.unlink()
                    self.assertFalse(adapter.generate(root).reused_local)

    def test_changed_graph_source_invalidates_cache_and_keeps_unrelated_files(self):
        with tempfile.TemporaryDirectory() as tmp:
            parent = Path(tmp)
            source = write_data(parent / "source")
            root = parent / "output"
            first = data(source).generate(root)
            key = read_json(first.metadata)["cache"]["fingerprint"]
            note = root / "ldbc/data/notes.txt"
            note.write_bytes(b"keep")
            person = source / "dynamic/person_0_0.csv"
            person.write_text(person.read_text().replace("Ada", "Grace"))
            source_before = snapshot(source)
            second = data(source).generate(root)
            self.assertFalse(second.reused_local)
            self.assertNotEqual(read_json(second.metadata)["cache"]["fingerprint"], key)
            self.assertIn("Grace", (root / "ldbc/data/social_network/dynamic/person_0_0.csv").read_text())
            self.assertEqual(note.read_bytes(), b"keep")
            self.assertEqual(snapshot(source), source_before)

    def test_changed_parameters_and_driver_config_each_invalidate_cache(self):
        with tempfile.TemporaryDirectory() as tmp:
            parent = Path(tmp)
            adapter, parameters, config = fixture_queries(parent)
            root = parent / "output"
            first = adapter.generate(root)
            key = read_json(first.metadata)["cache"]["fingerprint"]
            path = parameters / "interactive_1_param.txt"
            path.write_text(path.read_text().replace("Ada", "Grace"))
            second = adapter.generate(root)
            self.assertFalse(second.reused_local)
            new_key = read_json(second.metadata)["cache"]["fingerprint"]
            self.assertNotEqual(new_key, key)
            self.assertEqual((root / "ldbc/queries/substitution_parameters/interactive_1_param.txt").read_bytes(), path.read_bytes())
            write_config(config, {"operation_count": "200"})
            third = adapter.generate(root)
            self.assertFalse(third.reused_local)
            self.assertNotEqual(read_json(third.metadata)["cache"]["fingerprint"], new_key)
            self.assertEqual((root / "ldbc/queries/user.properties").read_bytes(), config.read_bytes())
            self.assertNotIn(PRIVATE_DUMMY, third.metadata.read_text())

    def test_source_change_during_staging_does_not_replace_prior_outputs(self):
        with tempfile.TemporaryDirectory() as tmp:
            parent = Path(tmp)
            adapter, _, _ = fixture_queries(parent)
            root = parent / "output"
            adapter.generate(root)
            expected = snapshot(root)
            real_copy = shutil.copyfile
            changed = False

            def change_source_before_copy(source, destination, *args, **kwargs):
                nonlocal changed
                if not changed:
                    source_path = Path(source)
                    source_path.write_bytes(source_path.read_bytes() + b"\n")
                    changed = True
                return real_copy(source, destination, *args, **kwargs)

            with patch("driftbench.data.ldbc.shutil.copyfile", side_effect=change_source_before_copy):
                with self.assertRaisesRegex(ValueError, "source changed"):
                    adapter.generate(root, force=True)
            self.assertTrue(changed)
            self.assertEqual(snapshot(root), expected)

    def test_person_replaced_after_validation_is_rejected_before_publication(self):
        with tempfile.TemporaryDirectory() as tmp:
            parent = Path(tmp)
            source = write_data(parent / "source")
            person = source / "dynamic/person_0_0.csv"
            original = person.read_bytes()
            adapter = data(source)
            root = parent / "output"
            adapter.generate(root)
            expected = snapshot(root)
            real_rows = ldbc_module._rows

            def replace_after_validation(path, *args, **kwargs):
                result = real_rows(path, *args, **kwargs)
                if Path(path) == person:
                    person.write_bytes(b"invalid|header\ninvalid|row\n")
                return result

            for output in (root, parent / "fresh-output"):
                person.write_bytes(original)
                with patch.object(ldbc_module, "_rows", side_effect=replace_after_validation):
                    with self.subTest(output=output), self.assertRaisesRegex(ValueError, "source changed"):
                        adapter.generate(output)
                self.assertEqual(snapshot(root), expected)
                self.assertFalse((parent / "fresh-output").exists())

    def test_parameter_replaced_after_validation_is_rejected_before_publication(self):
        with tempfile.TemporaryDirectory() as tmp:
            parent = Path(tmp)
            adapter, parameters, _ = fixture_queries(parent)
            parameter = parameters / "interactive_14_param.txt"
            original = parameter.read_bytes()
            root = parent / "output"
            adapter.generate(root)
            expected = snapshot(root)
            real_rows = ldbc_module._rows

            def replace_after_validation(path, *args, **kwargs):
                result = real_rows(path, *args, **kwargs)
                if Path(path) == parameter:
                    parameter.write_bytes(b"person1Id|person2Id\ninvalid|invalid\n")
                return result

            for output in (root, parent / "fresh-output"):
                parameter.write_bytes(original)
                with patch.object(ldbc_module, "_rows", side_effect=replace_after_validation):
                    with self.subTest(output=output), self.assertRaisesRegex(ValueError, "source changed"):
                        adapter.generate(output)
                self.assertEqual(snapshot(root), expected)
                self.assertFalse((parent / "fresh-output").exists())

    def test_native_config_replaced_after_validation_is_rejected_before_publication(self):
        with tempfile.TemporaryDirectory() as tmp:
            parent = Path(tmp)
            adapter, _, config = fixture_queries(parent)
            original = config.read_bytes()
            root = parent / "output"
            adapter.generate(root)
            expected = snapshot(root)
            real_properties = ldbc_module._properties

            def replace_after_validation(path):
                result = real_properties(path)
                config.write_bytes(b"db=invalid class\nmode=invalid\n")
                return result

            for output in (root, parent / "fresh-output"):
                config.write_bytes(original)
                with patch.object(ldbc_module, "_properties", side_effect=replace_after_validation):
                    with self.subTest(output=output), self.assertRaisesRegex(ValueError, "source changed"):
                        adapter.generate(output)
                self.assertEqual(snapshot(root), expected)
                self.assertFalse((parent / "fresh-output").exists())

    def test_moving_output_requires_regeneration_of_native_absolute_paths(self):
        with tempfile.TemporaryDirectory() as tmp:
            parent = Path(tmp)
            adapter, _, _ = fixture_queries(parent)
            first_root, moved_root = parent / "first", parent / "moved"
            first = adapter.generate(first_root)
            key = read_json(first.metadata)["cache"]["fingerprint"]
            shutil.copytree(first_root, moved_root)
            moved = adapter.generate(moved_root)
            self.assertFalse(moved.reused_local)
            self.assertNotEqual(read_json(moved.metadata)["cache"]["fingerprint"], key)
            command = read_json(moved_root / "ldbc/queries/command.json")
            self.assertIn(str(moved_root / "ldbc/queries/overrides.properties"), command["argv"])

    def test_java_properties_escapes_and_continuations_preserve_native_config(self):
        with tempfile.TemporaryDirectory() as tmp:
            parent = Path(tmp)
            adapter, _, config = fixture_queries(parent)
            raw = config.read_bytes().replace(b"db=", b"d\\u0062=").replace(b"thread_count=2", b"thread_count=\\\r\n  2")
            config.write_bytes(raw)
            root = parent / "output space-数据"
            result = adapter.generate(root)
            self.assertEqual((root / "ldbc/queries/user.properties").read_bytes(), raw)
            overrides = (root / "ldbc/queries/overrides.properties").read_text()
            self.assertIn(r"output\ space-\u6570\u636e", overrides)
            self.assertNotIn("数据", overrides)
            self.assertNotIn(PRIVATE_DUMMY, result.metadata.read_text())

    def test_native_properties_comments_do_not_continue_but_continued_values_keep_markers(self):
        with tempfile.TemporaryDirectory() as tmp:
            parent = Path(tmp)
            adapter, _, config = fixture_queries(parent)
            original = config.read_bytes()
            for marker in ("#", "!"):
                # Java ignores an entire fresh comment, including its final
                # backslash; a marker on a continued value is literal content.
                comment = marker + " Windows directory example C:" + chr(92) + "data" + chr(92) + "\r\n"
                continued = "custom.continued=prefix" + chr(92) + "\r\n  " + marker + "literal\r\n"
                raw = comment.encode("ascii") + original + continued.encode("ascii")
                config.write_bytes(raw)
                with self.subTest(marker=marker):
                    parsed = ldbc_module._properties(config)
                    self.assertEqual(parsed["db"], "example.local.Database")
                    self.assertEqual(parsed["custom.continued"], "prefix" + marker + "literal")
                    result = adapter.generate(parent / "output")
                    self.assertEqual((result.output_dir / "ldbc/queries/user.properties").read_bytes(), raw)

    def test_missing_families_bad_headers_and_wrong_declared_dates_write_no_artifacts(self):
        with tempfile.TemporaryDirectory() as tmp:
            parent = Path(tmp)
            source = write_data(parent / "source")
            output = parent / "output"
            person = source / "dynamic/person_0_0.csv"
            original = person.read_bytes()
            for content in (None, b"wrong|header\n1|2\n", original.replace(b"1325376000000", b"not-a-date"), original.replace(b"1|Ada|", b"1|Ada|extra|")):
                if content is None:
                    person.unlink()
                else:
                    person.write_bytes(content)
                before = snapshot(source)
                with self.subTest(content=content), self.assertRaises(ValueError):
                    data(source).generate(output)
                self.assertFalse(output.exists())
                self.assertEqual(snapshot(source), before)
                person.write_bytes(original)
            with self.assertRaisesRegex(ValueError, "birthday|creationDate"):
                data(source, date_format="iso8601").generate(output)
            self.assertFalse(output.exists())

    def test_invalid_layout_options_force_and_read_only_fail_before_output(self):
        with tempfile.TemporaryDirectory() as tmp:
            parent = Path(tmp)
            source = write_data(parent / "source")
            output = parent / "output"
            for options in ({"format": "csv_basic"}, {"benchmark_version": "snb-bi"}, {"date_format": "auto"}, {"date_format": True}):
                with self.subTest(options=options), self.assertRaises(ValueError):
                    data(source, **options).generate(output)
                self.assertFalse(output.exists())
            query_adapter, parameters, config = fixture_queries(parent)
            for adapter in (LDBCData(source), query_adapter):
                with self.assertRaisesRegex(ValueError, "force"):
                    adapter.generate(output, force=1)
                self.assertFalse(output.exists())
            for value in (None, 1, "false", []):
                with self.subTest(read_only=value), self.assertRaisesRegex(ValueError, "read_only"):
                    LDBCQueries(parameters, config, read_only=value).generate(output)
                self.assertFalse(output.exists())

    def test_invalid_parameter_fields_and_missing_files_preserve_existing_output(self):
        with tempfile.TemporaryDirectory() as tmp:
            parent = Path(tmp)
            adapter, parameters, _ = fixture_queries(parent)
            root = parent / "output"
            adapter.generate(root)
            expected = snapshot(root)
            cases = [
                (1, None), (1, "personId|firstName\n"), (1, "personId|firstName\n1|\n"),
                (2, "personId|maxDate\n1|not-a-date\n"),
                (3, "personId|startDate|durationDays|countryXName|countryYName\n1|1325376000000|0|Germany|France\n"),
                (10, "personId|month\n1|13\n"), (13, f"person1Id|person2Id\n{2**63}|2\n"),
                (14, "wrong|header\n1|2\n"),
            ]
            for number, content in cases:
                path = parameters / f"interactive_{number}_param.txt"
                original = path.read_bytes()
                if content is None:
                    path.unlink()
                else:
                    path.write_text(content)
                source_before = snapshot(parameters)
                with self.subTest(number=number, content=content), self.assertRaises(ValueError):
                    adapter.generate(root)
                self.assertEqual(snapshot(root), expected)
                self.assertEqual(snapshot(parameters), source_before)
                path.write_bytes(original)

    def test_invalid_native_configuration_does_not_publish_or_leak_values(self):
        with tempfile.TemporaryDirectory() as tmp:
            parent = Path(tmp)
            adapter, _, config = fixture_queries(parent)
            root = parent / "output"
            invalid = [
                {"db": None}, {"db": "bad class"}, {"mode": "validate"},
                {"operation_count": "0"}, {"operation_count": str(2**63)},
                {"thread_count": "0"}, {"thread_count": str(2**31)},
                {"time_compression_ratio": "nan"}, {"time_compression_ratio": "inf"},
                {"time_compression_ratio": "0"}, {PREFIX + "scale_factor": "2"},
                {PREFIX + "LdbcQuery14_enable": None}, {PREFIX + "LdbcQuery1_enable": "yes"},
                {PREFIX + f"LdbcQuery{i}_enable": "false" for i in range(1, 15)},
                {PREFIX + "LdbcShortQuery1PersonProfile_enable": "true"},
                {PREFIX + "LdbcShortQuery1PersonProfile_enable": "true", PREFIX + "short_read_dissipation": "2"},
            ]
            for changes in invalid:
                write_config(config, changes)
                original = config.read_bytes()
                with self.subTest(changes=changes), self.assertRaises(ValueError) as raised:
                    adapter.generate(root)
                self.assertNotIn(PRIVATE_DUMMY, str(raised.exception))
                self.assertFalse(root.exists())
                self.assertEqual(config.read_bytes(), original)

    def test_valid_short_reads_require_and_preserve_dissipation(self):
        with tempfile.TemporaryDirectory() as tmp:
            parent = Path(tmp)
            adapter, _, config = fixture_queries(parent)
            write_config(config, {PREFIX + "LdbcShortQuery1PersonProfile_enable": "true", PREFIX + "short_read_dissipation": "0.5"})
            result = adapter.generate(parent / "output")
            self.assertEqual((result.output_dir / "ldbc/queries/user.properties").read_bytes(), config.read_bytes())

    def test_mixed_mode_requires_paired_streams_and_enabled_updates(self):
        with tempfile.TemporaryDirectory() as tmp:
            parent = Path(tmp)
            _, parameters, config = fixture_queries(parent)
            root = parent / "output"
            with self.assertRaisesRegex(ValueError, "updates_dir"):
                queries(parameters, config, read_only=False).generate(root)
            updates = write_updates(parent / "updates")
            with self.assertRaisesRegex(ValueError, "read_only=False"):
                queries(parameters, config, updates_dir=updates).generate(root)
            forum = updates / "updateStream_0_0_forum.csv"
            original = forum.read_bytes()
            forum.unlink()
            with self.assertRaisesRegex(ValueError, "matching"):
                queries(parameters, config, updates, read_only=False).generate(root)
            forum.write_bytes(original)
            write_config(config, {PREFIX + f"LdbcUpdate{name}_enable": "false" for name in UPDATES})
            with self.assertRaisesRegex(ValueError, "enabled update"):
                queries(parameters, config, updates, read_only=False).generate(root)
            self.assertFalse(root.exists())

    def test_read_only_plan_rejects_existing_unmanaged_update_streams(self):
        with tempfile.TemporaryDirectory() as tmp:
            parent = Path(tmp)
            adapter, _, _ = fixture_queries(parent)
            root = parent / "output"
            adapter.generate(root)
            unexpected = root / "ldbc/queries/empty-updates/updateStream_9_9_person.csv"
            unexpected.write_bytes(b"keep this user file")
            before = snapshot(root)
            with self.assertRaises(ValueError):
                adapter.generate(root)
            self.assertEqual(snapshot(root), before)

    def test_removed_source_partition_requires_clean_output_without_replacing_files(self):
        with tempfile.TemporaryDirectory() as tmp:
            parent = Path(tmp)
            source = write_data(parent / "source")
            extra = source / "dynamic/person_1_0.csv"
            extra.write_bytes((source / "dynamic/person_0_0.csv").read_bytes())
            root = parent / "output"
            data(source).generate(root)
            expected = snapshot(root)
            extra.unlink()
            source_before = snapshot(source)
            for force in (False, True):
                with self.subTest(force=force), self.assertRaisesRegex(ValueError, "clean output_dir"):
                    data(source).generate(root, force=force)
                self.assertEqual(snapshot(root), expected)
                self.assertEqual(snapshot(source), source_before)

    def test_unexpected_output_partition_is_rejected_even_on_a_cache_hit(self):
        with tempfile.TemporaryDirectory() as tmp:
            parent = Path(tmp)
            source = write_data(parent / "source")
            root = parent / "output"
            data(source).generate(root)
            self.assertTrue(data(source).generate(root).reused_local)
            unexpected = root / "ldbc/data/social_network/dynamic/person_9_0.csv"
            unexpected.write_bytes((source / "dynamic/person_0_0.csv").read_bytes())
            expected = snapshot(root)
            for force in (False, True):
                with self.subTest(force=force), self.assertRaisesRegex(ValueError, "clean output_dir"):
                    data(source).generate(root, force=force)
                self.assertEqual(snapshot(root), expected)

    def test_removed_source_update_pair_requires_clean_output_without_replacing_files(self):
        with tempfile.TemporaryDirectory() as tmp:
            parent = Path(tmp)
            updates = write_updates(parent / "updates")
            extras = []
            for family in ("person", "forum"):
                extra = updates / f"updateStream_1_0_{family}.csv"
                extra.write_bytes((updates / f"updateStream_0_0_{family}.csv").read_bytes())
                extras.append(extra)
            adapter, _, _ = fixture_queries(parent, updates_dir=updates, read_only=False)
            root = parent / "output"
            adapter.generate(root)
            expected = snapshot(root)
            for extra in extras:
                extra.unlink()
            source_before = snapshot(updates)
            for force in (False, True):
                with self.subTest(force=force), self.assertRaisesRegex(ValueError, "clean output_dir"):
                    adapter.generate(root, force=force)
                self.assertEqual(snapshot(root), expected)
                self.assertEqual(snapshot(updates), source_before)

    def test_unexpected_output_update_pair_is_rejected_even_on_a_cache_hit(self):
        with tempfile.TemporaryDirectory() as tmp:
            parent = Path(tmp)
            updates = write_updates(parent / "updates")
            adapter, _, _ = fixture_queries(parent, updates_dir=updates, read_only=False)
            root = parent / "output"
            adapter.generate(root)
            self.assertTrue(adapter.generate(root).reused_local)
            for family in ("person", "forum"):
                unexpected = root / f"ldbc/queries/updates/updateStream_9_0_{family}.csv"
                unexpected.write_bytes((updates / f"updateStream_0_0_{family}.csv").read_bytes())
            expected = snapshot(root)
            for force in (False, True):
                with self.subTest(force=force), self.assertRaisesRegex(ValueError, "clean output_dir"):
                    adapter.generate(root, force=force)
                self.assertEqual(snapshot(root), expected)

    def test_data_source_and_output_overlap_is_rejected_without_changes(self):
        with tempfile.TemporaryDirectory() as tmp:
            parent = Path(tmp)
            source = write_data(parent / "source")
            before = snapshot(source)
            for root in (source, source / "nested", parent):
                with self.subTest(root=root), self.assertRaisesRegex(ValueError, "overlap"):
                    data(source).generate(root)
                self.assertEqual(snapshot(source), before)

    def test_query_output_cannot_overwrite_native_config_or_parameters(self):
        with tempfile.TemporaryDirectory() as tmp:
            parent = Path(tmp)
            adapter, parameters, config = fixture_queries(parent)
            before = snapshot(parent)
            for root in (parameters, parameters / "nested", config.parent, parent):
                with self.subTest(root=root), self.assertRaisesRegex(ValueError, "overlap"):
                    adapter.generate(root)
                self.assertEqual(snapshot(parent), before)

    def test_output_hardlink_collision_leaves_source_and_external_content_untouched(self):
        with tempfile.TemporaryDirectory() as tmp:
            parent = Path(tmp)
            adapter, _, _ = fixture_queries(parent)
            root = parent / "output"
            target = root / "ldbc/queries/overrides.properties"
            target.parent.mkdir(parents=True)
            external = parent / "external.properties"
            external.write_bytes(b"keep")
            os.link(external, target)
            before = snapshot(parent)
            with self.assertRaisesRegex(ValueError, "hardlink"):
                adapter.generate(root)
            self.assertEqual(snapshot(parent), before)

    def test_input_and_output_directory_links_cannot_escape_declared_roots(self):
        with tempfile.TemporaryDirectory() as tmp:
            parent = Path(tmp)
            external_source = write_data(parent / "external-source")
            source = parent / "linked-source"
            source.mkdir()
            shutil.copytree(external_source / "static", source / "static")
            try:
                directory_link(external_source / "dynamic", source / "dynamic")
            except OSError as exc:
                self.skipTest(f"directory links unavailable: {exc}")
            external_before = snapshot(external_source)
            with self.assertRaisesRegex(ValueError, "escapes|symlinks|junctions"):
                data(source).generate(parent / "output")
            self.assertFalse((parent / "output").exists())
            self.assertEqual(snapshot(external_source), external_before)
            outside = parent / "outside"
            outside.mkdir()
            root = parent / "linked-output"
            root.mkdir()
            directory_link(outside, root / "ldbc")
            with self.assertRaisesRegex(ValueError, "escapes|symlinks|junctions"):
                data(external_source).generate(root)
            self.assertEqual(list(outside.iterdir()), [])

    def test_no_native_database_or_network_invocation(self):
        with tempfile.TemporaryDirectory() as tmp:
            parent = Path(tmp)
            source = write_data(parent / "source")
            query_adapter, _, _ = fixture_queries(parent)
            with ExitStack() as stack:
                guards = [stack.enter_context(patch(target, side_effect=AssertionError(target))) for target in (
                    "subprocess.run", "subprocess.Popen", "os.system", "socket.create_connection",
                    "urllib.request.urlopen", "psycopg2.connect",
                )]
                data(source).generate(parent / "output")
                query_adapter.generate(parent / "output")
                for guard in guards:
                    guard.assert_not_called()
