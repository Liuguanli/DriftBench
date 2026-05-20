# Daily release pipeline

This directory drives the **daily DriftBench release cycle**:

> Day N morning (08:00 local) → Claude implements yesterday's approved features, runs tests, bumps version, updates `driftbench-web` and `Driftbench-demo`, writes Day N+1's proposal, then pushes. GHA tests + publishes to PyPI + sends email. Anthropic push notification fires when done.

See the top of `CLAUDE.md` for the full flow. This README is just the directory contract.

## Directory layout

```
daily/
  README.md                    ← you are here
  PROMPT_MORNING.md            ← the prompt Claude follows at 08:00
  proposals/
    YYYY-MM-DD.md              ← Claude writes one of these every morning, for the NEXT day
  approved/
    YYYY-MM-DD.md              ← you create this to approve the prior day's proposal
  rejected/
    YYYY-MM-DD.md              ← you create this to reject (with a one-line reason)
  archive/
    YYYY-MM-DD/                ← auto-moved after the day is shipped
      proposal.md
      approval.md (or rejection.md)
      release_notes.md         ← what actually shipped
```

## File conventions

- **Filename = ISO date** (`2026-05-21.md`). Always YYYY-MM-DD, local time.
- A proposal in `proposals/2026-05-21.md` describes work to be done on **2026-05-22**.
- An approval in `approved/2026-05-21.md` greenlights the proposal of the same date — Claude will implement it on **2026-05-22**.
- Missing approval = the morning routine still runs (tests, bookkeeping, tomorrow's proposal), but **skips PyPI upload** and **skips web/paper updates that are feature-driven**. Bookkeeping commits still happen on a dev branch.
- A rejection causes the morning routine to skip the implementation step entirely and move straight to writing a fresh proposal for tomorrow.

## The approval gate

Approval is a file existence check, nothing more:

```bash
# Approve yesterday's proposal:
touch daily/approved/$(date -v-1d +%Y-%m-%d).md

# Reject it with a reason:
echo "Skip the FK auto-wiring extension; we need a design discussion first." \
  > daily/rejected/$(date -v-1d +%Y-%m-%d).md
```

GHA reads `daily/approved/<yesterday>.md`. If it exists AND tests are green, PyPI upload proceeds.

## Proposal template

Claude follows this shape (defined in `PROMPT_MORNING.md`):

```markdown
# Proposal for YYYY-MM-DD

## Theme
One-line summary.

## Features (1–3)
- [ ] Feature A — what + which files touched + which tests added
- [ ] Feature B — ...

## Risk
- Breaking change? (yes/no, scope)
- Migration needed? (yes/no)
- Affects driftbench-web? (yes/no, which page)
- Affects Driftbench-demo claims? (yes/no, which section)

## Why this, why now
Two sentences max. Pull from TODO.md, recent commits, open issues, or user direction.
```

## Resetting / pausing

- Pause the daily cycle: disable the GHA workflow (`.github/workflows/daily-release.yml`) in the GitHub UI, OR drop a file at `daily/PAUSED` (Claude's morning routine checks this and exits cleanly).
- Resume: re-enable the workflow, or delete `daily/PAUSED`.
- Reset state: archive everything in `proposals/`, `approved/`, `rejected/` into `archive/` and start fresh.
