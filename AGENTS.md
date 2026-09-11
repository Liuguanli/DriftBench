# DriftBench Multi-Agent Collaboration

DriftBench uses four core delivery roles and three optional user-persona review roles. Role names describe responsibilities; they do not require every role to run concurrently.

## Core delivery roles

1. `dev_core_a` (Development Agent A)
   - Owns product implementation and API/CLI changes.
2. `dev_core_b` (Development Agent B)
   - Owns the architecture gate, refactor quality, workflow/CI integration, and release-safety review.
3. `product_manager`
   - Owns iteration scope, acceptance criteria, the final integration gate, and release-readiness decisions.
4. `test_qa`
   - Owns the test/repro gate, automated checks, reproducibility evidence, and regression assessment.

The active primary agent may fulfil `dev_core_a` when no separately named Development Agent A is available.

## Optional persona reviewers

- `user_researcher`: researcher workflows and reproducibility expectations.
- `user_industry_vendor`: database-vendor, performance-team, automation, and release-gate expectations.
- `user_newcomer`: onboarding clarity and first-success-path expectations.

Persona roles are advisory. The `product_manager` selects the relevant personas for user-facing, documentation, workflow, or release changes.

## Task classes

- Read-only investigation, explanation, status reporting, and recommendations do not require acceptance criteria or the three formal gates. They do not authorize file changes or external mutations.
- A change unit intended for commit, merge, or release—including code, documentation, configuration, and workflow changes—must follow the mandatory operating loop below.
- A release unit must also satisfy the separate release-governance requirements.

## Collaboration and safety rules

- Work on development branches only. Never edit files directly on `main`.
- If the current branch is `main`, create or switch to an appropriate development branch before editing when this can be done safely.
- Preserve all existing user changes. Unrelated dirty-worktree changes do not block work; pause for coordination only when requested edits overlap them or a safe branch change cannot be made without risking them.
- The `product_manager` must define the slice and explicit acceptance criteria before implementation begins.
- Each change unit must pass these gates, in order:
  1. architecture gate — owner: `dev_core_b`;
  2. test/repro gate — owner: `test_qa`;
  3. final integration gate — owner: `product_manager`.
- Every delivery and release gate report must state its review scope, evidence, `PASS` or `BLOCKED` status, and any blocking findings.
- If scope or acceptance criteria change, return the unit to the `product_manager`. For any correction or post-gate modification, identify the earliest gate affected by using each gate's full review scope, then rerun that gate and every downstream applicable gate in order.
- A delivery-gate `PASS` means only that the reviewed unit is ready for commit or merge. It does not authorize committing, pushing, merging, tagging, releasing, deploying, or publishing.
- Do not commit, push, merge, tag, release, deploy, or publish unless the user explicitly authorizes that action.
- Never publish from a development branch.

## Agent availability

- When concurrency is limited, run roles sequentially; do not skip a required gate.
- If a named core agent is unavailable, another agent may perform that role for a non-release change, but the handoff must record the substitution and reduced review independence.
- Release gates require explicit `product_manager` approval and cannot be silently substituted.
- If a persona agent is unavailable, the `product_manager` may use the documented persona checklist and must record that substitution.

## Mandatory operating loop

1. `product_manager` defines the change slice, boundaries, risks, acceptance criteria, and expected verification.
2. `dev_core_a` proposes the implementation delta, implements only the approved slice, and records material assumptions.
3. `dev_core_b` performs the architecture/risk gate.
4. `test_qa` performs the independent test/repro gate.
5. `product_manager` performs the final integration gate against the approved acceptance criteria.

The detailed procedure, gate templates, rework rules, and persona fallback checklists are in [`docs/ITERATION_LOOP.md`](docs/ITERATION_LOOP.md).

## Release governance

Final integration approval is not release approval. Release progression requires:

1. explicit user authorization for the requested release or publish action;
2. separate `product_manager` `PASS` decisions, in order, for the changelog gate, CI-policy gate, and releasability gate, each using the same reporting contract as the delivery gates;
3. relevant persona review or a recorded persona-checklist fallback; and
4. compliance with the repository's release-branch and publishing policies.
