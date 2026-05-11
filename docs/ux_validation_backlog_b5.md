# DriftBench B5 UX Validation Backlog

Status: Planned (for next work session)  
Branch: `dev/ux-validation-checklist-b5`  
Base: latest `main` after B5 finalization

## Goal

Validate whether DriftBench is usable and useful for real users, then optimize project description and onboarding so first-time adoption is easy.

## Scope For This Round

- Focus on user experience and usability evidence.
- Focus on project description quality (PyPI + README + first-run messaging).
- Focus on lowering onboarding friction.
- Do not start new core feature development unless required by UX blockers.

## Primary User Personas To Validate

1. Researcher (database/systems benchmarking)
2. Industry engineer (database vendor/performance team)
3. Newcomer (minimal background)

## Phase 1: Baseline UX Audit

1. Install and first-run audit
- Validate clean install path:
  - `pip install driftbench-db`
- Verify command discoverability:
  - `driftbench-db --help`
  - `driftbench-db init-agent --help`
  - `driftbench-db validate-spec --help`
- Record time-to-first-success for each persona.

2. Documentation path audit
- Evaluate top-to-bottom flow from:
  - PyPI project page
  - README first screen
  - CHANGELOG entry for latest version
- Identify ambiguity:
  - unclear terminology
  - missing “what to do next”
  - unclear expected outputs

3. Error-message audit
- Intentionally trigger common failures:
  - invalid spec path
  - bad YAML field
  - missing output dir
- Check whether error + next-step guidance is actionable.

Deliverable:
- `docs/ux_audit_report_b5.md` with findings ranked as:
  - Critical
  - High
  - Medium
  - Low

## Phase 2: Project Description Optimization

1. Rewrite PyPI short description
- Target: one sentence, concrete, no abstract jargon.
- Must answer:
  - Who is it for?
  - What core action does it enable?
  - Through which interfaces (CLI/MCP)?

2. Rewrite README opening block
- Keep it short.
- Add explicit “start here” sequence.
- Add single-screen expectations:
  - what input user gives
  - what output user gets

3. Add persona-first quick links (minimal)
- Research path
- Industry path
- Newcomer path
- Each path no more than 3 commands to first success.

Deliverable:
- PR-ready docs diff with before/after rationale.

## Phase 3: Usability Flow Hardening

1. First-run flow
- Ensure `init-agent` + demo spec flow is obvious.
- Confirm output location is explicit in docs and CLI feedback.

2. Guided command path
- Define canonical command order:
  - `validate-spec` -> `dry-run` -> `run-yaml` -> `list-outputs`
- Ensure this order appears consistently across docs.

3. MCP usability
- Provide one minimal conversation example with expected artifact outputs.
- Ensure no contradictory instructions across docs.

Deliverable:
- `docs/ux_flow_contract_b5.md`

## Phase 4: Evidence and Acceptance

Acceptance criteria:

1. Persona-first success
- Each persona can reach a successful run in <= 10 minutes on a clean machine setup.

2. Description clarity
- External reviewer can answer in < 30 seconds:
  - what DriftBench does
  - whether it fits their role
  - how to start

3. Command clarity
- No ambiguous “next step” after first-run success.

4. Release confidence
- All changed docs/flows pass current CI checks.

Evidence checklist:

- terminal transcripts for first-run paths
- screenshots/snippets of key docs sections
- list of fixed pain points mapped to commits

## Task Breakdown (Execution Order)

1. Produce baseline UX audit report.
2. Prioritize top 5 pain points.
3. Apply PyPI + README description updates.
4. Apply onboarding command-flow simplifications.
5. Validate with 3 persona walkthroughs.
6. Final QA pass and CI verification.
7. Prepare summary for release decision.

## Out of Scope (For This Round)

- New drift algorithm features.
- Large architecture refactors unrelated to usability.
- Non-user-facing internal optimization unless blocking onboarding.

## Final Handoff Template

- What changed:
- Which persona pain points were solved:
- Remaining UX gaps:
- Recommended next iteration:
