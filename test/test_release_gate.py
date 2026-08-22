from __future__ import annotations

import builtins
import importlib.util
import json
import pathlib
import sys
import tempfile
import types
import unittest
from unittest import mock


ROOT = pathlib.Path(__file__).resolve().parents[1]
SCRIPT_PATH = ROOT / ".github" / "scripts" / "release_gate.py"
PREPARE_PATH = ROOT / ".github" / "workflows" / "prepare-release-branch.yml"
PUBLISH_PATH = ROOT / ".github" / "workflows" / "publish.yml"

SPEC = importlib.util.spec_from_file_location("driftbench_release_gate", SCRIPT_PATH)
assert SPEC is not None and SPEC.loader is not None
release_gate = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = release_gate
SPEC.loader.exec_module(release_gate)


def _load_without_stdlib_tomllib():
    """Load the gate as Python 3.10 would, with a controlled tomli fallback."""
    fallback = types.ModuleType("tomli")
    fallback.loads = release_gate.tomllib.loads
    fallback.TOMLDecodeError = release_gate.tomllib.TOMLDecodeError
    real_import = builtins.__import__

    def controlled_import(name, globals=None, locals=None, fromlist=(), level=0):
        if name == "tomllib":
            raise ModuleNotFoundError("No module named 'tomllib'", name="tomllib")
        if name == "tomli":
            return fallback
        return real_import(name, globals, locals, fromlist, level)

    module_name = "driftbench_release_gate_tomli_fallback"
    spec = importlib.util.spec_from_file_location(module_name, SCRIPT_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    try:
        with mock.patch("builtins.__import__", side_effect=controlled_import):
            spec.loader.exec_module(module)
    finally:
        sys.modules.pop(module_name, None)
    return module, fallback


def _successful_runs(source_sha: str) -> dict:
    return {
        "workflow_runs": [
            {
                "name": name,
                "head_sha": source_sha,
                "status": "completed",
                "conclusion": "success",
            }
            for name in release_gate.REQUIRED_WORKFLOWS
        ]
    }


class ReleaseMetadataGateTests(unittest.TestCase):
    def test_runtime_selects_the_version_appropriate_toml_parser(self) -> None:
        expected = "tomllib" if sys.version_info >= (3, 11) else "tomli"
        self.assertEqual(release_gate.tomllib.__name__, expected)

    def test_missing_stdlib_tomllib_uses_tomli_and_fails_closed(self) -> None:
        fallback_gate, fallback = _load_without_stdlib_tomllib()
        self.assertIs(fallback_gate.tomllib, fallback)
        with self.assertRaisesRegex(
            fallback_gate.ReleaseGateError, "pyproject.toml is invalid"
        ):
            fallback_gate.validate_release_metadata(
                expected_tag="v0.1.0b10",
                pyproject_text="[project\n",
                changelog_text="",
            )

    def test_b10_repository_metadata_passes_exact_release_gate(self) -> None:
        result = release_gate.validate_release_metadata(
            expected_tag="v0.1.0b10",
            pyproject_text=(ROOT / "pyproject.toml").read_text(encoding="utf-8"),
            changelog_text=(ROOT / "CHANGELOG.md").read_text(encoding="utf-8"),
        )
        self.assertEqual(result["expected_version"], "0.1.0b10")
        self.assertEqual(result["release_date"], "2026-08-22")

    def test_unreleased_or_undated_heading_cannot_satisfy_release_gate(self) -> None:
        pyproject = '[project]\nname = "fixture"\nversion = "0.1.0b10"\n'
        invalid_changelogs = (
            "## [Unreleased] - target v0.1.0b10\n\n### Services\n\n### Added\n- x\n",
            "## [v0.1.0b10]\n\n### Services\n\n### Fixed\n- x\n",
            "## [v0.1.0b10] - 2026-02-31\n\n### Services\n\n### Changed\n- x\n",
        )
        for changelog in invalid_changelogs:
            with self.subTest(changelog=changelog.splitlines()[0]), self.assertRaises(
                release_gate.ReleaseGateError
            ):
                release_gate.validate_release_metadata(
                    expected_tag="v0.1.0b10",
                    pyproject_text=pyproject,
                    changelog_text=changelog,
                )

    def test_version_services_and_change_category_are_all_required(self) -> None:
        valid_changelog = (
            "## [v0.1.0b10] - 2026-08-18\n\n"
            "### Services\n- CI\n\n### Added\n- gate\n"
        )
        cases = (
            (
                '[project]\nname="fixture"\nversion="0.1.0b9"\n',
                valid_changelog,
            ),
            (
                '[project]\nname="fixture"\nversion="0.1.0b10"\n',
                valid_changelog.replace("### Services", "### Coverage"),
            ),
            (
                '[project]\nname="fixture"\nversion="0.1.0b10"\n',
                valid_changelog.replace("### Added", "### Notes"),
            ),
        )
        for pyproject, changelog in cases:
            with self.assertRaises(release_gate.ReleaseGateError):
                release_gate.validate_release_metadata(
                    expected_tag="v0.1.0b10",
                    pyproject_text=pyproject,
                    changelog_text=changelog,
                )


class RequiredWorkflowFixtureTests(unittest.TestCase):
    def test_all_required_workflows_must_be_successful_on_exact_source_sha(self) -> None:
        source_sha = "a" * 40
        selected = release_gate.validate_workflow_runs(
            payload=_successful_runs(source_sha), source_sha=source_sha
        )
        self.assertEqual(set(selected), set(release_gate.REQUIRED_WORKFLOWS))
        self.assertIn("Benchmark Regression", selected)

    def test_missing_failed_or_other_sha_benchmark_regression_fails_closed(self) -> None:
        source_sha = "a" * 40
        cases = {}
        missing = _successful_runs(source_sha)
        missing["workflow_runs"] = [
            run
            for run in missing["workflow_runs"]
            if run["name"] != "Benchmark Regression"
        ]
        cases["missing"] = missing
        failed = _successful_runs(source_sha)
        next(
            run
            for run in failed["workflow_runs"]
            if run["name"] == "Benchmark Regression"
        )["conclusion"] = "failure"
        cases["failed"] = failed
        wrong_sha = _successful_runs(source_sha)
        next(
            run
            for run in wrong_sha["workflow_runs"]
            if run["name"] == "Benchmark Regression"
        )["head_sha"] = "b" * 40
        cases["wrong_sha"] = wrong_sha

        for name, payload in cases.items():
            with self.subTest(name=name), self.assertRaisesRegex(
                release_gate.ReleaseGateError, "Benchmark Regression"
            ):
                release_gate.validate_workflow_runs(
                    payload=payload, source_sha=source_sha
                )

    def test_stable_source_passes_and_advanced_source_fails(self) -> None:
        source_sha = "a" * 40
        self.assertEqual(
            release_gate.validate_source_unchanged(
                source_sha=source_sha, latest_source_sha=source_sha
            ),
            source_sha,
        )
        with self.assertRaisesRegex(release_gate.ReleaseGateError, "advanced"):
            release_gate.validate_source_unchanged(
                source_sha=source_sha, latest_source_sha="b" * 40
            )


class PrepareReleaseWorkflowStaticTests(unittest.TestCase):
    def test_permissions_get_query_and_full_slash_refs_are_safe(self) -> None:
        workflow = PREPARE_PATH.read_text(encoding="utf-8")
        self.assertIn("contents: write", workflow)
        self.assertIn("actions: read", workflow)
        self.assertIn("gh api --method GET", workflow)
        self.assertIn('git check-ref-format --branch "${SOURCE_DEV_BRANCH}"', workflow)
        self.assertIn('SOURCE_REF="refs/heads/${SOURCE_DEV_BRANCH}"', workflow)
        self.assertIn('git fetch --no-tags origin "${SOURCE_REF}"', workflow)
        self.assertNotIn("/commits/${SOURCE_DEV_BRANCH}", workflow)
        self.assertNotIn("${SOURCE_DEV_BRANCH##*/}", workflow)
        slash_branch = "dev/foo/bar"
        self.assertEqual(f"refs/heads/{slash_branch}", "refs/heads/dev/foo/bar")

    def test_metadata_is_read_from_immutable_sha_and_publish_uses_same_validator(self) -> None:
        workflow = PREPARE_PATH.read_text(encoding="utf-8")
        publish = PUBLISH_PATH.read_text(encoding="utf-8")
        self.assertIn('git show "${SOURCE_SHA}:pyproject.toml"', workflow)
        self.assertIn('git show "${SOURCE_SHA}:CHANGELOG.md"', workflow)
        self.assertIn('EXPECTED_TAG="${RELEASE_BRANCH#release/}"', workflow)
        invocation = "python .github/scripts/release_gate.py metadata"
        self.assertIn(invocation, workflow)
        self.assertIn(invocation, publish)

    def test_creation_refetches_then_pushes_only_pinned_sha_and_verifies_remote(self) -> None:
        workflow = PREPARE_PATH.read_text(encoding="utf-8")
        creation = workflow.split(
            "- name: Create release branch from the verified immutable commit", 1
        )[1].split("- name: Dry-run summary", 1)[0]
        fetch_at = creation.index('git fetch --no-tags origin "${SOURCE_REF}"')
        compare_at = creation.index("release_gate.py source")
        push_line = 'git push origin "${SOURCE_SHA}:refs/heads/${RELEASE_BRANCH}"'
        push_at = creation.index(push_line)
        self.assertLess(fetch_at, compare_at)
        self.assertLess(compare_at, push_at)
        self.assertIn("if: ${{ inputs.dry_run == false }}", creation)
        self.assertEqual(workflow.count("git push "), 1)
        self.assertIn(push_line, creation)
        self.assertIn("REMOTE_RELEASE_SHA", creation)
        self.assertIn('"${REMOTE_RELEASE_SHA}" != "${SOURCE_SHA}"', creation)

    def test_dry_run_has_no_push_path(self) -> None:
        workflow = PREPARE_PATH.read_text(encoding="utf-8")
        dry_run = workflow.split("- name: Dry-run summary", 1)[1]
        self.assertIn("inputs.dry_run == true", dry_run)
        self.assertNotIn("git push", dry_run)
        self.assertIn("no branch was pushed", dry_run)


if __name__ == "__main__":
    unittest.main()
