# Release Branch Policy (PyPI)

Starting from `v0.1.0b3`, each PyPI version is prepared on a dedicated release branch.

## Naming

- Branch: `release-vX.Y.Z` or `release-vX.Y.ZbN`
- Tag: `vX.Y.Z` or `vX.Y.ZbN`

Example:

- Branch: `release-v0.1.0b3`
- Tag: `v0.1.0b3`

## Workflow

1. Create release branch from the stable tip:
   - `git checkout -b release-vX.Y.ZbN`
2. Update version in `pyproject.toml`.
3. Build and verify artifacts locally:
   - `python3 -m build`
4. Verify long description includes tutorials:
   - inspect `dist/*.whl` metadata and `dist/*.tar.gz` PKG-INFO
5. Commit release prep.
6. Push branch:
   - `git push -u origin release-vX.Y.ZbN`
7. Tag from that branch head:
   - `git tag vX.Y.ZbN`
   - `git push origin vX.Y.ZbN`

The publish workflow is tag-driven, so tagging from the release branch ensures
clear provenance for each PyPI version.

## Branch Lifecycle

- `ui/figma-web` is not used for future releases.
- Future releases should only come from dedicated `release-*` branches.
