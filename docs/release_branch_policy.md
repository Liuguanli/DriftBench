# Release Branch Policy (PyPI)

Starting from `v0.1.0b3`, each PyPI version is prepared on a dedicated release branch.

## Naming

- Branch: `release/vX.Y.Z`, `release/vX.Y.ZbN`, `release-vX.Y.Z`, or `release-vX.Y.ZbN`
- Tag: `vX.Y.Z` or `vX.Y.ZbN`

Example:

- Branch: `release/v0.1.0b3`
- Tag: `v0.1.0b3`

## Workflow

1. Finish development on `dev/**`.
2. Wait for source-branch CI to be green:
   - `CI`
   - `CLI Contract`
   - `Schema and Spec Validation`
   - `Content Safety Check`
3. Create release branch using the gated workflow:
   - GitHub Actions -> `Prepare Release Branch`
   - `source_dev_branch=dev/...`
   - `release_branch=release/...`
   - run with `dry_run=true` first, then `dry_run=false`
4. Update version in `pyproject.toml` on release branch if needed.
5. Update `CHANGELOG.md` with a new section for the exact tag version:
   - heading format: `## [vX.Y.ZbN] - YYYY-MM-DD`
   - include at least:
     - `### Services`
     - `### Added` / `### Changed` (as applicable)
6. Build and verify artifacts locally:
   - `python3 -m build`
7. Verify long description includes tutorials:
   - inspect `dist/*.whl` metadata and `dist/*.tar.gz` PKG-INFO
8. Commit release prep and push to the same release branch.
9. Tag from that release branch head and create a GitHub Release with a description:

```bash
# Tag and push
git tag vX.Y.ZbN
git push origin vX.Y.ZbN

# Create GitHub Release — body pulled from the CHANGELOG section for this version
gh release create vX.Y.ZbN \
  --title "DriftBench vX.Y.ZbN" \
  --notes "$(sed -n '/^## \[vX.Y.ZbN\]/,/^## \[/p' CHANGELOG.md | head -n -1)" \
  --target release/vX.Y.ZbN
```

The release description must include (copy from the CHANGELOG section):
- `### Services` — which surfaces gained new capabilities
- `### Added` — new commands, APIs, or files
- `### Changed` — behaviour changes users need to know about

Do not publish a GitHub Release with an empty or placeholder description.

The publish workflow is tag-driven, so tagging from the release branch ensures
clear provenance for each PyPI version.

## README and CHANGELOG Update Rule

**Every feature addition or behaviour change must update both files before the branch is merged or released.**

- `README.md`: update the relevant section (CLI Quickstart, Benchmark Objects, Troubleshooting, etc.) to reflect the new behaviour.
- `CHANGELOG.md` `[Unreleased]` section: add a bullet under `### Added` or `### Changed` immediately when the work is committed — not at release time.

When cutting a release branch, promote `[Unreleased]` to a versioned section:

```
## [vX.Y.ZbN] - YYYY-MM-DD   ← rename this line
```

Leave a fresh empty `[Unreleased]` block above it for the next cycle:

```markdown
## [Unreleased]

### Services
- (No unreleased service changes recorded yet.)
```

## Branch Lifecycle

- `ui/figma-web` is not used for future releases.
- Future releases should only come from dedicated `release-*` branches.
