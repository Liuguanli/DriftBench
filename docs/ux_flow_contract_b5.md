# DriftBench UX Flow Contract (B5)

Branch: `dev/ux-validation-checklist-b5`  
Status: Approved for B5 release  
Owner: UX validation pass, 2026-05-14

This document is the single source of truth for:
- canonical CLI command order
- `init-agent` first-run flow
- output location expectations
- minimal MCP conversation pattern
- cross-doc consistency rules

---

## 1. Canonical CLI Command Order

Every guide, quickstart, and example must use this order. Do not skip or reorder steps.

```
validate-spec  →  dry-run  →  run-yaml  →  list-outputs
```

Full example (always use `driftbench-db`, never `python -m driftbench.cli`):

```bash
driftbench-db validate-spec driftspec/examples/demo_data_single.yaml --json
driftbench-db dry-run       driftspec/examples/demo_data_single.yaml --json
driftbench-db run-yaml      driftspec/examples/demo_data_single.yaml
driftbench-db list-outputs  --root output --glob "**/*" --limit 20 --json
```

**What each step does and what you see:**

| Step | What it checks | Exit on failure | Expected output snippet |
|---|---|---|---|
| `validate-spec` | YAML shape, required keys, type mapping | yes | `{"ok": true, "command": "validate-spec", ...}` |
| `dry-run` | execution plan without writing files | yes | `{"ok": true, "command": "dry-run", "plan": [...]}` |
| `run-yaml` | executes and writes output files | yes | per-drift OK lines + output paths |
| `list-outputs` | lists everything written under `--root` | no | `{"ok": true, "count": N, "files": [...]}` |

---

## 2. First-Run Flow: `init-agent`

Use `init-agent` when starting a new project directory that has no DriftSpec yet.  
It scaffolds the agent directory, copies a starter spec, and prints every created file.

```bash
# Step 1 — scaffold agent files into a new directory
driftbench-db init-agent --output ./my-drift-project

# Step 2 — validate the scaffolded spec
driftbench-db validate-spec my-drift-project/driftspec/starter.yaml --json

# Step 3 — run it
driftbench-db run-yaml my-drift-project/driftspec/starter.yaml
```

Expected output of `init-agent`:

```
[OK] initialized DriftBench agent files under: ./my-drift-project
- driftspec/starter.yaml
- README.md
```

If the directory already exists, add `--force` to overwrite, or `--dry-run` to preview without writing.

---

## 3. Output Location Contract

DriftBench always writes to the path you specify in the spec's `output_path` fields.  
By convention, all examples use `./output/` as the root:

```
output/
  data/          ← generated CSV/TBL data files
  workload/      ← generated SQL files
  intermediate_yaml/  ← extracted schemas
  *_manifest.json     ← per-artifact manifest (lists every generated file)
```

**Confirm output after every run:**

```bash
driftbench-db list-outputs --root output --glob "**/*" --limit 30 --json
```

The `files` array in each `*_manifest.json` is the authoritative list of what was generated.  
Use `result.files` in the Python API to access the same list programmatically.

---

## 4. Minimal MCP Conversation Pattern

This is the reference pattern for Claude/Codex-based MCP usage.  
One complete data-drift scenario, showing inputs → tools called → artifacts produced.

---

**User:**
> I want to simulate data drift on my census dataset — scale it 2x and add stronger skew on the age column.

**Assistant (MCP tool calls, in order):**

1. `trace_to_spec` — convert the dataset + intent into a DriftSpec:
   ```
   trace_path: driftspec/trace_inputs/trace_data_mock.csv
   output_path: driftspec/generated/census_drift.yaml
   trace_type: data
   ```
   → produces `driftspec/generated/census_drift.yaml`

2. `validate_spec` — confirm the spec is valid before running:
   ```
   spec_path: driftspec/generated/census_drift.yaml
   ```
   → `{"ok": true}`

3. `run_spec` — execute the spec:
   ```
   spec_path: driftspec/generated/census_drift.yaml
   ```
   → job ID returned; wait for completion

4. `list_outputs` — confirm what was produced:
   ```
   root: output
   glob: "**/*"
   limit: 20
   ```
   → list of generated file paths

**Assistant summary to user:**
> Generated 2 data files under `output/data/`:
> - `census_drift_scale_2.csv` — dataset scaled to 2× rows
> - `census_drift_skew_age.csv` — age column skewed toward 25–35 range
>
> Manifest: `output/data/census_drift_manifest.json`
>
> Suggested next: add a temporal overlay (`uniform` or `periodic`) to make the drift time-aware, or run `dry-run` on a variant spec before executing.

---

**Rules for MCP conversations:**
- Always call `validate_spec` before `run_spec`. Never skip validation.
- Always call `list_outputs` after `run_spec` and include file paths in the summary.
- Summarize what changed in plain language, not just file paths.
- Suggest one concrete next iteration.

---

## 5. Cross-Doc Consistency Rules

These rules apply to all documentation files in this repo.

| Rule | Correct | Wrong |
|---|---|---|
| CLI entry point | `driftbench-db` | `python -m driftbench.cli` |
| Command order | validate → dry-run → run → list | any other order without explanation |
| Output root | `output/` | hardcoded absolute paths |
| Spec path prefix | `driftspec/examples/` for demos | bare filenames |
| JSON flag | `--json` on validate/dry-run/list | omitting `--json` in automation contexts |
| MCP tool name | `validate_spec`, `run_spec`, `list_outputs` | camelCase or hyphenated variants |

**Docs in scope for this contract:**
- `README.md` ✓
- `docs/p0_integration_quickstart.md` ✓
- `docs/p0_mcp_command_matrix.md` ✓
- `docs/p0_mcp_examples.sh` — verify on next pass
- `docs/p0_mcp_server_minimal.md` — verify on next pass

---

## 6. Acceptance Checklist (Phase 3)

- [x] Canonical command order defined and documented
- [x] `init-agent` flow documented with expected output
- [x] Output location expectations explicit in contract and README
- [x] Minimal MCP conversation example written with artifact expectations
- [x] `driftbench-db` normalized across README, integration quickstart, MCP matrix
- [ ] `docs/p0_mcp_examples.sh` verified for command consistency
- [ ] `docs/p0_mcp_server_minimal.md` verified for command consistency
- [ ] Persona walkthroughs executed (Phase 4)
