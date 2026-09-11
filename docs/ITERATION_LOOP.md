# DriftBench Iteration Loop

This workflow applies to every change unit prepared for commit, merge, or release. Read-only investigation, explanation, status reporting, and recommendations stop after reporting their findings unless the user separately authorizes a change.

## 1. Establish a safe working context

Before editing:

1. Confirm the current branch is a development branch, not `main`.
2. Inspect the worktree and identify pre-existing changes.
3. Preserve unrelated changes; do not reset, discard, stage, or rewrite them.
4. Pause only if the requested files overlap existing changes or changing branches would put them at risk.
5. Record the files and systems that the requested change is allowed to affect.

## 2. PM slice and acceptance criteria

The `product_manager` produces a short slice brief before implementation:

```text
Objective:
In scope:
Out of scope:
Acceptance criteria:
Risks and compatibility constraints:
Expected verification:
```

Acceptance criteria must be observable. They should identify preserved behavior, expected error behavior, safety boundaries, and the evidence required for a decision.

If new information materially changes the scope, stop implementation and return to this step. The PM updates the slice before work continues.

## 3. Development delta

`dev_core_a`, or the active primary agent acting in that role:

1. proposes the smallest implementation delta that satisfies the criteria;
2. identifies assumptions and affected interfaces;
3. implements only the approved slice;
4. adds verification proportional to the risk; and
5. reports changed files, checks run, and any known limitations.

Implementation does not authorize a commit, push, merge, tag, release, deploy, or publish action.

## 4. Architecture gate

Owner: `dev_core_b`.

Review at least:

- runtime and API compatibility;
- ownership and dependency boundaries;
- failure modes, security, and destructive side effects;
- maintainability and duplicated contracts;
- workflow, CI, packaging, and release impact where applicable; and
- conformance with the approved scope.

The reviewer must inspect the final candidate files, not only an earlier proposal.

## 5. Test/repro gate

Owner: `test_qa`.

Review at least:

- focused automated coverage for the acceptance criteria;
- relevant compatibility and regression coverage;
- deterministic or reproducible behavior where promised;
- failure-path and side-effect evidence;
- exact commands, results, skips, and environmental limitations; and
- whether any claimed readiness exceeds what was actually tested.

The QA review should be independent of the implementation where agent availability permits.

## 6. Final integration gate

Owner: `product_manager`.

The PM confirms:

- the final candidate satisfies every acceptance criterion;
- architecture and test/repro gates are `PASS`;
- documentation and user-facing behavior match the implementation;
- limitations and follow-up items are explicit; and
- the unit is understandable to the relevant user personas.

This gate decides commit/merge readiness only. It does not authorize a repository or release action.

## Gate report format

Every formal gate uses this minimum format:

```text
GATE: architecture | test/repro | final integration | changelog | CI-policy | releasability
STATUS: PASS | BLOCKED
REVIEW SCOPE:
EVIDENCE:
BLOCKING FINDINGS:
NON-BLOCKING FOLLOW-UPS:
REVIEWER OR SUBSTITUTION:
```

Evidence should name inspected files and include exact verification commands and outcomes when applicable. A `BLOCKED` report must state what must change before review can continue.

## Rework and gate invalidation

The applicable gate order is architecture, test/repro, and final integration, followed for a release unit by changelog, CI-policy, and releasability.

- A scope or acceptance-criteria change always returns to PM slicing before work continues.
- For a blocked gate, correction, or other post-gate modification, compare the finding or delta with every gate's full review scope and identify the earliest affected gate.
- Rerun that earliest affected gate and every downstream applicable gate in order; do not preserve a downstream `PASS` from the earlier candidate.
- Any post-gate modification invalidates every gate affected by the modification and all of their downstream applicable gates.
- Record the reason for invalidation and the new evidence in the replacement gate reports.

## Limited agents and concurrency

Required roles may run sequentially. Limited concurrency is not a reason to omit a gate.

For a non-release change, an available agent may substitute for an unavailable core reviewer. The gate report must name the substitution and state that review independence was reduced. Prefer a distinct reviewer whenever possible.

Release decisions always require explicit `product_manager` approval. If that role is unavailable, release progression is blocked.

## Persona review and fallback checklists

The PM selects personas based on impact:

- Researcher checklist: reproducibility, provenance, deterministic reruns, evidence portability, and methodological limitations.
- Industry/vendor checklist: automation, machine-readable contracts, operational failure modes, compatibility, performance-team workflows, and release gates.
- Newcomer checklist: discoverability, terminology, copyable examples, actionable errors, safe defaults, and the shortest successful path.

If a persona agent is unavailable, the PM may apply the matching checklist directly. The final integration or release report must record that the checklist was used instead of an independent persona agent.

## Release extension

A final-integration `PASS` is necessary but insufficient for release. Before any tag, release, deployment, or publish action:

1. obtain explicit user authorization for that action;
2. obtain separate PM `PASS` decisions for:
   - changelog gate;
   - CI-policy gate; and
   - releasability gate;
3. complete relevant persona review or record checklist fallbacks;
4. verify the intended branch and immutable source revision; and
5. follow the repository's release-branch and publishing policies.

No publish action may originate from a development branch.

The `product_manager` owns the three release gates and evaluates them in this order:

1. Changelog gate: verify that the final candidate's user-visible changes, compatibility effects, limitations, and upgrade guidance are accurately represented in the changelog or release notes.
2. CI-policy gate: verify the required workflows, status checks, test evidence, branch protections, and release-policy constraints for the exact candidate revision.
3. Releasability gate: verify all preceding applicable gates are `PASS`, persona evidence is recorded, versions and artifacts are coherent, known blockers are resolved, and the proposed action matches the user's explicit authorization.

Each release gate must use the common gate report format above, including review scope, evidence, `PASS` or `BLOCKED`, blocking findings, and reviewer/substitution details. A blocked or invalidated release gate follows the same earliest-affected-gate and downstream-rerun rules as a delivery gate.
