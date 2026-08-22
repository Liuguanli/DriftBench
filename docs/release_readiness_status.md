# Release Readiness Status (2026-08-22)

This file records the local candidate evidence for `v0.1.0b10` on
`dev/benchmark-reliability-hardening`. It does not claim that a release, tag, or
PyPI publication has occurred.

## Local gates completed

1. Architecture gate: PASS.
   - PM froze four dependency-closed slices: DriftSpec/query drift, benchmark
     reliability, visualization evidence, and release governance.
   - Architecture review approved the final file boundaries and per-commit
     clean-worktree verification strategy.
2. DriftSpec/query-drift slice: 42 tests passed from its committed tree.
3. Benchmark/CLI slice:
   - exactly 47 adapter tests were discovered and passed under strict CP1252;
   - 138 benchmark tests passed with exactly five expected PostgreSQL integration
     skips when `DRIFTBENCH_REQUIRE_PG_INTEGRATION` was unset;
   - 10 deep-validation CLI contract tests passed.
4. Visualization/paper slice:
   - 59 visualization tests passed after installing `.[test,visualization]`;
   - 9 executable paper-example tests passed;
   - wheel/sdist tests verified exactly 40 specs, 40 manifests, and 40 PNGs,
     while excluding runtime `visualization/data` and `visualization/cache`.
5. Final integrated local gate:
   - `python -m unittest discover -s test -p 'test_*.py'` ran 255 tests and
     passed with 10 expected skips: five opt-in real-PostgreSQL tests and five
     legacy manual placeholders;
   - `python -m unittest discover -s visualization/tests -p 'test_*.py'` ran
     59 tests and passed;
   - `python -m build` produced `driftbench_db-0.1.0b10` wheel and sdist;
   - `python -m twine check dist/*` passed for both distributions.

## CI and release policy

- Development CI installs `.[test,visualization]`, executes both test trees,
  checks strict Windows CP1252 adapter behavior, and validates installed-wheel
  visualization contents.
- The required Benchmark Regression workflow uses PostgreSQL/pgbench 16 and
  enables all five real integration tests without mocks or skips.
- Release CI, daily release, and tag-publish paths execute the visualization
  gate; build/publish paths validate distribution metadata and packaged evidence.
- Release-branch preparation resolves one immutable development SHA, requires
  successful `CI` and `Benchmark Regression` runs for that exact SHA, rechecks
  the source ref, and pushes only the verified commit.

## Remaining remote gate

The exact final commit must be pushed before remote evidence can exist. Release
progression remains blocked until both required workflows succeed on that SHA.
No release branch, main-branch merge, tag, or package publication is authorized
from this development branch.
