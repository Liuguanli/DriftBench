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
5. Build and verify artifacts locally:
   - `python3 -m build`
6. Verify long description includes tutorials:
   - inspect `dist/*.whl` metadata and `dist/*.tar.gz` PKG-INFO
7. Commit release prep and push to the same release branch.
8. Tag from that release branch head:
   - `git tag vX.Y.ZbN`
   - `git push origin vX.Y.ZbN`

The publish workflow is tag-driven, so tagging from the release branch ensures
clear provenance for each PyPI version.

## Branch Lifecycle

- `ui/figma-web` is not used for future releases.
- Future releases should only come from dedicated `release-*` branches.
