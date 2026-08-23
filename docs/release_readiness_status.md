# Release Readiness Contract (`v0.1.0`)

This document freezes the evidence contract for the `v0.1.0` stable candidate.
It intentionally does not claim future workflow results, publication, or live
run IDs. Exact-SHA gate reports and the published GitHub Release are the
authoritative live evidence, avoiding a documentation-only commit after the
candidate has been verified.

## Candidate identity

- Base: current `main` commit
  `1a708a120e6eaa023294c744545a9f09121329e1`.
- Development branch: `dev/v0.1.0-stable-release`.
- Release branch: `release/v0.1.0`.
- Annotated tag: `v0.1.0`.
- PyPI distribution/version: `driftbench-db==0.1.0`.
- GitHub Release: `DriftBench v0.1.0`, published, non-prerelease, and latest.

The final development candidate must descend from the base and must be the
same exact commit as `main`, the release branch, and the peeled tag at release
closure.

## Approved release delta

This is a stable metadata/documentation promotion of the tested `0.1.0b10`
feature line. It may change only version identity, release notes and policy,
readiness documentation, workflow UI defaults, and directly corresponding
tests. It does not authorize product, API, CLI, benchmark-generator, schema,
metric, threshold, or publishing-workflow behavior changes. The package keeps
the `Development Status :: 4 - Beta` classifier.

Canonical Visualization artifacts remain byte-for-byte evidence from the beta
candidate, so their embedded `0.1.0b10` producer provenance is intentionally
retained. Runtime and distribution metadata must report `0.1.0`.

## Required evidence before tagging

1. `git diff --check`, the complete `test` suite, the complete
   `visualization/tests` suite, strict Windows CP1252 adapter coverage, package
   build, `twine check`, and clean-wheel CLI/import/content smoke all pass with
   only documented expected skips.
2. The exact read-only `query_workload_artifact_smoke_v1` protocol passes for
   the eight local query/workload artifact generators, without network,
   databases, external commands, or repository mutation.
3. Independent architecture and test/repro gates pass. Researcher,
   industry/vendor, and newcomer reviews record evidence and respect the
   benchmark claim boundaries.
4. All five required push workflows pass on the exact development SHA: `CI`,
   `Benchmark Regression`, `CLI Contract`, `Schema and Spec Validation`, and
   `Content Safety Check`. The PostgreSQL/pgbench 16 regression may not be
   mocked or skipped.
5. `Prepare Release Branch` succeeds first as a dry run and then creates the
   release branch at the unchanged candidate SHA. Release CI and benchmark
   regression pass on that SHA.
6. `main` advances by non-force fast-forward to the identical candidate, and
   all five `main` push workflows pass again.
7. The Product Manager issues `PASS` reports in order for the changelog,
   CI-policy, and releasability gates before the annotated tag is pushed.
8. The release window has no matching daily approval file, no forced Daily
   release dispatch, and no active Daily release upload. The sole authorized
   PyPI upload for `0.1.0` is the `v0.1.0` tag-triggered `publish.yml` run.

## Publication and closure evidence

The tag-driven publish workflow must produce exactly one wheel and one sdist,
both passing metadata, test, visualization-content, and PostgreSQL regression
gates. The version-specific PyPI API must report both artifacts as non-yanked,
with hashes matching the workflow artifacts, and normal installation without
`--pre` must select `0.1.0`.

Only after PyPI success is a stable/latest GitHub Release created from the same
tag. Final closure requires `main == release/v0.1.0 == v0.1.0^{}` and must also
prove that the earlier `v0.1.0b10` branch, tag, and PyPI artifacts were not
changed.

## Benchmark claim boundaries

- Adapter outputs are DriftBench synthetic fixtures/workload artifacts unless
  explicitly documented otherwise; they are not official or audited benchmark
  implementations or scores.
- TPC-H automatic data generation may build an unpinned upstream dbgen
  revision, while query `mode="qgen"` is Python qgen-style parameterization and
  does not execute native qgen.
- BenchBase generates configuration and external-driver scripts rather than a
  local DriftBench dataset.
- The only required real-database performance gate currently covers
  PostgreSQL/pgbench 16 `select-only`. Artifact tests for other adapters do not
  prove database loadability, result correctness, performance, or conformance.
