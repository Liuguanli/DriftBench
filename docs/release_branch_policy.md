# Release Branch Policy (PyPI and GitHub)

Starting from `v0.1.0b3`, every PyPI version is prepared from an immutable
candidate on a dedicated release branch. The PyPI distribution is
`driftbench-db`; the Python import remains `driftbench`.

## Naming

- Development branch: `dev/<release-purpose>`
- Release branch: `release/vX.Y.Z`, `release/vX.Y.ZbN`, `release-vX.Y.Z`, or
  `release-vX.Y.ZbN`
- Annotated tag: `vX.Y.Z` or `vX.Y.ZbN`

For the first stable release:

- Development branch: `dev/v0.1.0-stable-release`
- Release branch: `release/v0.1.0`
- Tag: `v0.1.0`

## Workflow

1. Start the development branch from the intended `main` commit and complete
   every release change there. Before release-branch preparation, commit the
   exact `pyproject.toml` version, dated `CHANGELOG.md` section, documentation,
   and release tests. Do not plan a metadata commit on the release branch.
2. Push the development candidate and require all five push workflows to finish
   successfully on that exact SHA:
   - `CI`
   - `Benchmark Regression`
   - `CLI Contract`
   - `Schema and Spec Validation`
   - `Content Safety Check`
3. Run GitHub Actions -> `Prepare Release Branch` first with `dry_run=true`,
   then with `dry_run=false`. Supply the exact `dev/**` source and `release/v*`
   destination. The workflow resolves one immutable source SHA, validates its
   version/changelog and exact-SHA workflow evidence, rechecks that the source
   ref has not advanced, and creates the release ref at that same SHA.
4. Confirm the release branch equals the verified development SHA and wait for
   the release-branch workflows, including Release CI and the real PostgreSQL
   benchmark regression, to pass on that exact SHA. Do not add a follow-up
   release-branch commit.
5. After the architecture, test/repro, final-integration, persona, changelog,
   and applicable CI gates pass, advance `main` only by a non-force
   fast-forward to the same candidate. Require all five `main` push workflows
   to pass on that SHA.
6. Reconfirm that the version is unused on PyPI and the tag is absent. After the
   PM CI-policy and releasability gates pass, create an annotated tag on the
   exact shared `main`/release SHA and push only that tag. The tag-driven
   `Publish to PyPI` workflow validates metadata, runs tests and the PostgreSQL
   regression, builds the wheel and sdist, and publishes through trusted
   publishing. During this release window, do not create a matching
   `daily/approved/<date>.md`, do not dispatch `Daily release` with
   `force_publish=true`, and confirm that no Daily release upload is active.
   The only authorized uploader for `v0.1.0` is the tag-triggered `publish.yml`
   workflow.
7. Verify the version-specific PyPI API, filenames, hashes, and non-yanked
   state. For a stable release, also confirm that normal dependency resolution
   selects the new version without `--pre`.
8. Only after PyPI succeeds, create a published, non-prerelease GitHub Release
   from the existing tag and explicitly mark it latest. Include accurate notes,
   PyPI coordinates, artifact hashes, supported scope, and limitations.
9. Close by proving that `main`, the release branch, and the peeled tag all
   equal the approved SHA, and that PyPI plus GitHub `/releases/latest` expose
   the intended stable version.

## Immutability and recovery

- Never force-push a release branch or `main`, move a published tag, overwrite
  a PyPI version, or silently replace release artifacts.
- If PyPI succeeds but GitHub Release creation fails, repair only the missing
  GitHub Release from the existing immutable tag.
- If a gate fails before tagging, correct the development branch and rerun the
  earliest affected gate plus all downstream gates. Create no tag until the
  final candidate is frozen.

## Branch lifecycle

- `ui/figma-web` is not a release source.
- Publish only from a dedicated `release-*`/`release/**` branch whose exact
  candidate is also fast-forwarded into `main` before tagging.
