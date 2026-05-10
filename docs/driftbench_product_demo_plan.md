# DriftBench Productization and Demo Paper Plan (Updated with Project Priorities)

## 1. Product Direction

### 1.1 Core Product Position
DriftBench should be a reusable Python project that can be integrated into other projects and automated pipelines, not only a standalone research prototype.

### 1.2 DriftSpec as the Bridge
DriftSpec is the contract between:
- users and generation logic,
- CLI/MCP operations and execution,
- local workflows and shared reproducible artifacts.

This means every major capability should be accessible through DriftSpec + CLI, and automatable through MCP-compatible interfaces.

## 2. Priority Outcomes (Your Requested Order)

### P0. Reusable Python + CLI/MCP Integration
Goal: use DriftBench as an importable Python package and as an automation-friendly toolchain.

Deliverables:
- Stable Python package structure with documented public APIs.
- CLI as first-class interface (`run-yaml`, `trace-to-spec`, plus packaging-oriented commands).
- MCP-friendly operations mapped to core workflows (generate spec, run spec, list outputs, inspect job/log status).
- Integration guide for embedding DriftBench in external projects.

Acceptance:
- A downstream project can install/import DriftBench and execute an end-to-end run without modifying DriftBench internals.
- The same workflow is executable via CLI and via MCP-driven automation.

### P1. Shareable DriftSpec and Public Spec Catalog
Goal: generated DriftSpecs can be saved, shared, discovered, and reused by others directly.

Deliverables:
- Save-to-path workflow with metadata (owner, tags, source type, drift family, version).
- Spec export/import flow (local file first, registry-ready format).
- Website section listing curated/recommended specs ("official specs" + "community-ready specs").
- One-click import-and-run from listed specs.

Acceptance:
- Users can publish a spec to a known path/collection and others can import it and generate data/workloads directly.
- The site exposes a usable spec list instead of only demo-style pages.

### P2. Website Refactor (After P0/P1)
Goal: split conceptual showcase content from operational product workflows.

Direction:
- Keep conceptual pages in a separate "Showcase" area.
- Make `driftbench_service` primarily task-oriented: source -> spec -> run -> outputs.
- You will drive this step incrementally; we defer deep UI redesign until P0/P1 are done.

### P3. Paper Transition (Vision -> Product Demo Paper)
Goal: convert the current vision-style manuscript into a true demo paper centered on the productized DriftBench workflow.

Current status:
- The main vision content is already drafted in Overleaf at:
  `Overleaf project: Driftbench-demo`
- The current draft is still primarily "vision", not yet "demo-product evidence".

Required transition:
- Shift contribution framing from concept/proposal to system/demo execution.
- Add runnable workflow evidence (CLI/MCP + spec sharing + import-and-run).
- Add concrete demo scenarios with reproducibility details.
- Add product-oriented evaluation narrative (usability, reusability, portability of specs).

Acceptance:
- The final manuscript reads as a demo paper of a working product, not a vision statement.
- Claims are tied to implemented features and reproducible artifacts.

## 3. Execution Model (Incremental, No Fixed Timeline)

Work style:
- Make progress opportunistically: whenever time is available, complete one small, concrete item.
- Keep priority order stable (`P0 -> P1 -> P2 -> P3`) unless you explicitly reprioritize.
- Prefer thin vertical slices that are immediately usable and demoable.

Current execution backlog:

### B1: P0 Foundation (Python/CLI/MCP)
- Define/lock public Python APIs.
- Extend CLI coverage for operational workflows.
- Add automation contract doc for MCP actions.
- Add smoke tests for `schema extract -> spec build -> run`.
- Execution checklist: `docs/driftbench_todo.md`.

### B2: P1 Sharing and Catalog
- Implement spec metadata model and storage layout.
- Add spec list/filter/import endpoints in `driftbench_service`.
- Add curated spec gallery in the website (operational, not conceptual).
- Add reproducibility checks for imported specs.

### B3: P2 UI Split + P3 Demo-Paper Packaging
- Separate "Showcase" from "Workbench" in site information architecture.
- Finalize two official demos (single-table + multi-table) through importable specs.
- Produce demo scripts, screenshots, and artifact package.
- Complete vision-to-demo manuscript rewrite and submission-ready polishing.

## 4. Demo Paper Alignment

The demo paper should highlight the two implemented product pillars first:
1. DriftBench as an integrable Python/CLI/MCP system.
2. DriftSpec sharing and reuse through a spec catalog workflow.

Then show UI support as an execution surface, not the core contribution.

Important scope note:
- Platform-specific benchmark/test environment choices are a separate track.
- This paper track should focus on demonstrating productized DriftBench workflows and reproducibility.

Suggested structure:
1. Motivation: reproducible drift benchmarking needs product-grade workflows.
2. System: package + CLI + MCP + DriftSpec contract.
3. Spec sharing: save, list, import, run.
4. Demo scenarios: two official specs + one advanced case.
5. Lessons and limitations.
6. Artifact and reproducibility.

Rewrite checklist (vision draft -> demo paper draft):
1. Replace future-tense claims with implemented capabilities.
2. Add end-to-end user workflow figure (source -> spec -> run -> outputs).
3. Add spec catalog/import workflow figure.
4. Add at least 2 concrete demo scenarios with commands/screens/results.
5. Add artifact appendix with paths, seeds, and rerun steps.

## 5. Immediate Next Actions

1. Define the CLI/MCP command matrix (what each workflow must support).
2. Define DriftSpec metadata schema for sharing/catalog.
3. Implement initial spec catalog endpoints in `driftbench_service`.
4. Add first curated spec list page (operational spec registry view).
5. Create a manuscript gap list from the current Overleaf vision draft to the target demo-paper structure.
