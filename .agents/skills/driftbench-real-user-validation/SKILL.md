---
name: driftbench-real-user-validation
description: Safely audit DriftBench's eight local query/workload artifact generators from researcher, database-vendor, or newcomer perspectives. Use when checking benchmark usability, public API readiness, artifact provenance, onboarding clarity, automation behavior, or pre-release user acceptance without running databases or external workloads.
---

# DriftBench Real-User Validation

## Purpose

Evaluate the user-visible readiness of DriftBench's eight local benchmark families through their public query/workload artifact APIs. Produce reproducible evidence and persona-specific usability findings without changing the product.

This is an instruction-only skill. Read [references/workflow.md](references/workflow.md) completely before taking validation actions, then run exactly the `query_workload_artifact_smoke_v1` protocol defined there.

## Fixed scope

Validate exactly these families: TPC-H, TPC-DS, TPC-C, TPC-C Skew, JOB, YCSB, DSB, and pgbench. BenchBase is excluded because it is an external driver/config generator, not one of these eight local benchmark families.

The v1 protocol checks only that each public query/workload generator can create its expected managed files and cache manifest safely. It does not generate benchmark data, call `.drift()`, connect to a database, execute SQL/XML/shell artifacts, run pgbench or BenchBase, download or build tools, measure performance, or establish official benchmark conformance.

## Non-negotiable behavior

- Work read-only in the repository. Do not edit or fix product code, docs, configuration, tests, or source-controlled artifacts.
- Generate only inside a new process-owned `TemporaryDirectory` outside the repository, and pass `force=False` explicitly.
- Reject any returned artifact, metadata, or output path outside that owned directory. Do not inspect or clean an escaping path.
- Do not use an existing directory, a repository path, a home directory, or a broad temporary root as output.
- Do not use network access, databases, external workload processes, package installation, builds, permission escalation, Git mutation, or environment/secret dumps.
- Inspect generated SQL, XML, properties, CSV, and shell files as text only. Never execute them.
- Preserve and compare the worktree and benchmark-source state before and after the run.
- Clean up only by leaving the owned `TemporaryDirectory` context.

If the skill, imports, safety guards, source-state capture, or owned temporary directory are unavailable, report `BLOCKED`. If a call or required check fails, report `FAIL`. Report `PASS` only when every one of the eight rows passes.

## Claim boundary

Never say "all benchmarks validated." A `PASS` means only: all eight query/workload artifact generators succeeded under `query_workload_artifact_smoke_v1`. It is not evidence of data-generation readiness, database execution, result correctness, performance, benchmark-spec compliance, end-to-end usability, or release readiness.

The persona agents are advisory evidence producers. They cannot replace the architecture gate, test/repro gate, final-integration gate, Product Manager release gates, or human release judgment.
