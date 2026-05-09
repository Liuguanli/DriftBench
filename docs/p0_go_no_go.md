# P0 Go/No-Go Decision Record

Date: 2026-05-08
Decision owner: Project team

## Decision

**GO (conditional release candidate)** for P0 foundation.

## Rationale

The following release-critical signals are positive:
- public API boundary is defined and documented,
- operational CLI surface is in place (`run-yaml`, `trace-to-spec`, `validate-spec`, `dry-run`, `list-outputs`),
- MCP/service-to-CLI contract is documented with runnable examples,
- focused and discovery test suites pass,
- service/automation checks pass (job flow, CLI/service output equivalence, restart persistence),
- safety checks pass for path restriction behavior.

## Verification References

- `docs/p0_test_report.md`
- `docs/p0_foundation_release_todo.md`
- `docs/p0_mcp_command_matrix.md`

## Conditions and Follow-Ups

Release is marked conditional because medium-risk items remain:
- packaging metadata/distribution flow finalization,
- clean-room dependency bootstrap formalization,
- optional service parity endpoints for validate/dry-run/output listing.

Tracked in:
- `docs/p0_known_issues.md`

## Major Defect Status

- Open major defects: **0**

## Final Note

If any new major defect is discovered during publication prep or demo-paper artifact packaging, this decision must be re-evaluated immediately.
