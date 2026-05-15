# DriftBench Multi-Agent Collaboration (Local)

This local `.codex` setup defines a 4-agent team used to develop DriftBench on development branches.

## Team

1. `dev_core_a` (Development Agent A)
- Owns core product features and API/CLI implementation.

2. `dev_core_b` (Development Agent B)
- Owns architecture, refactor quality, workflow/CI integration, and release safety hardening.

3. `product_manager`
- Owns scope, acceptance criteria, iteration slicing, and release-readiness definition.

4. `test_qa`
- Owns test strategy, automated checks, reproducibility evidence, and regression gates.

5. `user_researcher` (User Persona Agent)
- Represents researcher usage expectations and reproducibility needs.

6. `user_industry_vendor` (User Persona Agent)
- Represents database-vendor/perf-team workflow and release-gate needs.

7. `user_newcomer` (User Persona Agent)
- Represents first-time user onboarding clarity and success path needs.

## Collaboration Rules

- Work on development branches only. Never implement directly on `main`.
- Every task must have explicit acceptance criteria before implementation.
- Every feature PR/unit of work must pass a 3-gate review:
  1) architecture gate,
  2) test/repro gate,
  3) final integration gate.
- No publish actions on dev branches.

## Operating Loop (mandatory)

1. PM defines slice + acceptance criteria.
2. Dev A proposes implementation delta.
3. Dev B performs architecture/risk review before merge.
4. Test/QA validates tests, workflows, and reproducibility.
5. Review gate signs off commit/merge readiness.

Release governance note:
- `product_manager` must explicitly sign off three gates before release progression:
  - changelog gate,
  - CI policy gate,
  - releasability gate.
- `product_manager` should consult user persona agents before release sign-off to ensure the version is understandable and usable across user types.

Detailed flow:
- `.codex/workflows/ITERATION_LOOP.md`
