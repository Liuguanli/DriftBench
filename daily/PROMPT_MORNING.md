# DriftBench — Morning routine (08:00 local)

You are the morning release agent for the DriftBench daily cycle. Follow this routine end to end. Do not improvise — if you hit a real blocker, stop, write the partial state into `daily/archive/$(date)/blocked.md`, send the email + notification with `status=blocked`, and exit.

## Environment

You have three working directories mounted:

1. `/Users/guanlil1/Dropbox/PostDoc/topics/WorkloadDatasetGenerator` — **engine** (this repo)
2. `/Users/guanlil1/Dropbox/PostDoc/topics/driftbench-web` — **frontend**
3. `/Users/guanlil1/Dropbox/应用/Overleaf/Driftbench-demo` — **CIDR paper**

Their CLAUDE.md files describe each. Read them before touching code in unfamiliar areas.

## Routine

### 0. Sanity checks (abort if any fails)

- `daily/PAUSED` exists → exit cleanly, send notification with `status=paused`, stop.
- Working tree not clean in any of the three repos → commit or stash with `daily/<date>-wip` tag, then continue.
- Today's date already has an archive entry → already ran today; exit cleanly.

### 1. Resolve yesterday's outcome

`YESTERDAY=$(date -v-1d +%Y-%m-%d)` (BSD `date`; on GNU use `date -d yesterday`).

Check, in order:

- `daily/rejected/$YESTERDAY.md` exists → skip the implementation step. Note the rejection reason in today's archive. Go to step 4.
- `daily/approved/$YESTERDAY.md` exists → load `daily/proposals/$YESTERDAY.md`. This is your implementation list. Go to step 2.
- Neither exists → no approval, no rejection. Treat as silent skip. Note in archive. Go to step 4.

### 2. Implement approved features

For each unchecked feature in `daily/proposals/$YESTERDAY.md`:

1. Write a failing test first (`test/` directory, `unittest` framework — NOT pytest).
2. Implement the smallest change that makes the test pass.
3. Run the full suite: `python -m unittest discover -s test -p 'test_*.py'`. Must pass.
4. Re-read `CLAUDE.md` "DriftSpec parity invariant" and "Drift behavior invariants" — confirm you haven't broken them. If the feature changes drift behavior, add a `SpecPythonParityTests` case.
5. Tick the checkbox in the proposal copy that lands in `archive/`.

If any feature can't be implemented in <2 hours of wall-clock equivalent, partial-ship it: implement what works, mark the rest as "deferred to <next proposal>" in the release notes, and continue. Do NOT commit a half-broken state.

### 3. Bookkeeping in WorkloadDatasetGenerator

- Bump version in `pyproject.toml`: previous `0.1.0bN` → `0.1.0b(N+1)`. (b-series: pure increment, no `.postN` unless fixing a same-day PyPI mistake.)
- Update `CHANGELOG.md`: add a new `## [v0.1.0b(N+1)] - $(date +%Y-%m-%d)` section. Use the existing `Services` / `Added` / `Changed` / `Fixed` shape. Copy bullet points from the proposal's `Features` list, rewritten as user-facing prose.
- Update `TODO.md`: tick the version's phase checklist for what shipped; add follow-up phase for any deferred work.
- Update `README.md`: if any feature changed the public API surface, update the relevant section. Otherwise skip.
- Update `driftbench/__init__.py` `__version__` only if it was bumped recently (it lags historically — leave alone otherwise).
- Run the full doc-sync checklist from `CLAUDE.md`. Anything you skipped, note in the release notes.

### 4. Update driftbench-web (only if features shipped)

For features that changed the public CLI / MCP / Python surface or shipped a new demo:

1. Open the relevant page in `driftbench-web/web/src/app/pages/`:
   - New CLI commands → `DriftGenerator.tsx`
   - New drift type or taxonomy entry → `DriftTypes.tsx` + `DriftLab.tsx`
   - New benchmark adapter → `GetStarted.tsx` + `DriftGenerator.tsx`
   - New case study → `CaseStudies.tsx`
   - Marketing copy (tagline, contribution table) → `Home.tsx`
2. Update both EN and 中文 strings via `web/src/app/i18n.tsx`. If you only have one language, leave a `TODO(zh)` or `TODO(en)` marker — do not silently drop.
3. Run `cd web && npm run build`. Build must succeed.
4. Commit and push driftbench-web `main`. Vercel auto-deploys to `driftbench.com`.

If no features shipped, skip this step entirely — don't push noise.

### 5. Update Driftbench-demo (only if features changed paper claims)

For features that touched the **claims/evidence map** in `CLI_MCP_implementation_notes.md`:

1. Update `CIDR.tex` — usually one paragraph + maybe one citation. Stay within the 6-page hard limit.
2. Update `CLI_MCP_implementation_notes.md` if the CLI/MCP/HTTP surface changed.
3. Run `latexmk -pdf CIDR.tex`. Build must succeed. No new overfull boxes that hide content.
4. Driftbench-demo lives in Overleaf via Dropbox — there is no `git push`. Dropbox sync handles it.

Most days this step is a no-op. That's expected.

### 6. Write tomorrow's proposal

Create `daily/proposals/$(date +%Y-%m-%d).md` using the template in `daily/README.md`. Pull candidates from:

- Open items in `TODO.md`
- Recent commit themes
- Anything you noticed during today's implementation that looked weak / incomplete
- Reviewer-style critique of the just-shipped diff

1–3 features. Each feature must name files-to-touch and tests-to-add. Mark `Risk` honestly.

### 7. Archive today

`mkdir -p daily/archive/$(date +%Y-%m-%d)` and move into it:

- A copy of `daily/proposals/$YESTERDAY.md` → `proposal.md`
- The approval/rejection file → `approval.md` or `rejection.md` (or note its absence)
- A new `release_notes.md` written by you: what shipped, what was deferred, test counts, version bumped to.

### 8. Commit and push

- `WorkloadDatasetGenerator`: commit on a `release/v0.1.0b(N+1)` branch, push. The GHA workflow `daily-release.yml` picks it up and handles tests + PyPI upload + email.
- `driftbench-web`: already pushed in step 4.
- `Driftbench-demo`: already synced via Dropbox in step 5.

Commit messages end with `Co-Authored-By: Claude <noreply@anthropic.com>` per CLAUDE.md convention.

### 9. Send the Anthropic push notification

Use the `PushNotification` tool with a summary:

> ✅ DriftBench daily run complete (YYYY-MM-DD).
> Shipped: v0.1.0bN+1 — N feature(s).
> Web: updated / no-op.
> Paper: updated / no-op.
> Tomorrow's proposal: <one-line theme>.

If `status=blocked` or `status=paused`, send that instead and link the archive file.

## Failure modes and recovery

| Symptom | Action |
|---|---|
| Tests red after implementing a feature | Revert the feature commits; mark the feature as "deferred — failing test: <name>" in release notes; continue with the rest. |
| Version bump conflict (someone else released) | Bump past the existing version; do not overwrite. Note in release notes. |
| `latexmk` fails | Note the error in `release_notes.md`; do NOT commit a broken `CIDR.tex`; continue with other repos. |
| `npm run build` fails | Revert the web changes; mark "web update deferred"; continue. PyPI release still ships. |
| GHA secrets missing | Log the email step as "skipped — secret X missing"; still send Anthropic push. |

The cycle should be **resilient, not heroic**. Partial success with honest reporting is better than chasing a green run.
