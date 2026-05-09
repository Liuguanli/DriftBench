# P0 Known Issues (Non-Blocking)

Date: 2026-05-08

This list tracks known medium-risk items that are not currently release-blocking.

## 1) Packaging metadata is not finalized

Issue:
- Repository currently does not provide finalized packaging metadata (`pyproject.toml`/distribution flow) for pip-style installation.

Impact:
- Downstream integration currently assumes source checkout/module path availability.

Mitigation:
- Use source-based integration with documented API (`driftbench` / `driftbench.api`) for P0.
- Plan packaging finalization in post-P0 hardening.

## 2) Service does not expose validate/dry-run/list-outputs endpoints directly

Issue:
- `validate-spec`, `dry-run`, and output listing are available in CLI but not as first-class service endpoints.

Impact:
- Automation may need mixed service + CLI orchestration.

Mitigation:
- Use MCP matrix fallbacks in `docs/p0_mcp_command_matrix.md`.
- Optionally add service parity endpoints in next iteration.

## 3) Legacy manual script files remain in `test/`

Issue:
- Legacy script-style files are preserved as skipped placeholders for reference/manual usage.

Impact:
- Minor maintenance overhead in test inventory.

Mitigation:
- They are intentionally skipped and side-effect free in discovery runs.
- Can be moved to `scripts/legacy/` in a future cleanup PR.

## 4) Clean-room verification is pending one external run

Issue:
- Scripted bootstrap is now available, but a final external "fresh machine" execution record is still pending.
- P0 dependency lock targets Python 3.10/3.11/3.12 (not 3.13).

Impact:
- Full GO remains conditional until one clean-machine execution is recorded.

Mitigation:
- Use:
  - `requirements/p0.lock.txt`
  - `scripts/bootstrap_p0_env.sh`
  - `scripts/verify_p0_clean_env.sh`
- If default interpreter is 3.13, run with:
  - `P0_PYTHON=/Users/guanlil1/anaconda3/bin/python3.10 ./scripts/verify_p0_clean_env.sh`
- Record one successful clean-machine run and attach logs to the release notes.

---

No major defects are tracked in this list.
