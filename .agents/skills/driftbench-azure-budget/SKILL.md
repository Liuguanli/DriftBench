---
name: driftbench-azure-budget
description: Run a read-only Azure cost and full-download capacity preflight before adding, expanding, copying, or materializing DriftBench data in Azure, before approving a new data reader, or when the owner asks about Azure spending. Uses the private AUD whole-subscription budget and requires explicit owner approval for identity-specific dataset access.
---

# DriftBench Azure Budget and Reader Approval

Read `references/workflow.md` before acting. This is a project-scoped,
event-driven assistant workflow, not a timer, Azure spending cap, or access proxy.

## Mandatory triggers

Run before any assistant-initiated Azure data addition, expansion, import,
copy, staging upload, or read-write cache materialization. Run again with the
actual sealed byte/file counts before the first Azure write if an earlier
planning estimate used unknown sizes. Also run before proposing a new reader
grant and whenever the owner requests current cost/read-capacity information.

## Read-only preflight

From the repository root:

```powershell
python -B -m scripts.azure_budget_preflight --mode report
```

For a local-to-HNS upload, supply actual planned bytes and file count,
including metadata, plus the number of full remote verification downloads:

```powershell
python -B -m scripts.azure_budget_preflight --mode pre-upload `
  --planned-upload-bytes 1073741824 --planned-file-count 8 `
  --verification-downloads 2
```

Configuration and reports belong only in ignored `.private` files. Do not
copy the owner's budget, subscription bill, identities, credentials, or reports
into tracked documentation or Copilot Memory. The budget covers the entire
linked subscription; inventory/carrying cost covers the configured project prefix.

Report to the owner, in their language:

1. Observation time, scope, current live size, monthly storage estimate.
2. Subscription month-to-date spend, explicit reserve, unknown future/late costs.
3. Proposed storage growth, upload operations and verification egress, if any.
4. Approximate full Internet downloads affordable for the selected dataset and
   whole catalog. These are alternative uses of ONE budget, not additive quotas.
5. Assumptions, blocked evidence, and the proposed authorization boundary.

**One read means a complete payload download, not one SQL query.** Catalog
browsing is offline. Do not assume the shared monthly free-egress allowance is
still available: the helper credits zero and uses conservative paid estimates.
Never turn a missing or stale bill/price/inventory into zero spend or success.

## Decision and authorization

- `BLOCKED`, nonzero exit, stale/mismatched scope, unsupported operation, or
  projected over-budget cost: pause Azure mutations and ask the owner through
  the question tool. Do not bypass the block, raise the budget, reuse an old
  successful report, or silently omit costs.
- `ESTIMATE_READY` is **not authorization**. Surface limitations; obtain explicit
  authorization if the operation was not already approved. Unknown other Azure
  costs can consume the modeled headroom, so this cannot guarantee a bill cap.
- Unsupported profiles/cross-region copies/other Azure services need a separately
  priced plan, not this Hot-LRS local-upload model relabeled as universal.
- For reader grants, follow the native identity-specific approval procedure in
  the reference. No named identity and dataset plus explicit owner approval
  means **no grant**. Do not issue account keys or SAS links.

This skill itself must not upload data, change Azure permissions or budgets,
provision services, deploy a gateway, or clean up cloud objects. Finish the
preflight and return to the separately authorized operation workflow.
