# Azure budget preflight and owner-approved readers

The project skill `driftbench-azure-budget` runs before participating assistants
add/expand/copy Azure data or propose a reader grant. It is event-driven and
project-scoped, not a recurring notification service, Azure Policy, billing
limit, or interceptor for independently executed Azure clients.

The skill lives at `.agents/skills/driftbench-azure-budget/SKILL.md`. If a CLI
session has already cached its available skills, inspect/reload its skill list
or start a new session; do not claim a newly written skill was loaded without
evidence. Its documented command can be run immediately from this checkout.

## Private setup and current report

Keep owner-specific financial settings in ignored `.private/azure-budget.yaml`.
The schema and authorization workflow are documented in the skill's
`references/workflow.md`. No personal budget or actual spending is versioned.

```powershell
python -B -m scripts.azure_budget_preflight --mode report
```

The command only reads Azure metadata, current AUD retail prices, and
subscription month-to-date cost. It saves financial output only under `.private`;
it does not download data, grant access, generate SAS links, or change resources.
Only an existing owned report with the matching schema may be replaced.

For a local upload, include all data/metadata bytes, all uploaded objects, and
the actual verification readbacks:

```powershell
python -B -m scripts.azure_budget_preflight --mode pre-upload `
  --planned-upload-bytes 1073741824 --planned-file-count 8 `
  --verification-downloads 2
```

`BLOCKED` or exit 2 means do not proceed with the Azure mutation. A successful
estimate is not authorization. Missing evidence is not converted to zero cost;
if a report cannot be refreshed, a previous successful file must not be reused.

## Reading the estimate

The budget is for the whole subscription linked to the storage account.
The live inventory and carrying-cost estimate concern the configured project
prefix. Already incurred MTD spend is separate from future storage obligations:
the latter are prorated to the remaining UTC month, not charged a second full
month on top of MTD spend.

Capacity means complete Internet downloads of a dataset's payload files, not
SQL queries. Each dataset/corpus scenario spends the same available pool and
must not be added to the others. The helper deliberately credits no unknown
remaining free-egress allowance and uses first paid tiers without discounts.
Catalog browsing itself is offline and does not incur Azure data-read traffic.

Estimates are conditional: other subscription activity, billing delay,
index/retained-version costs, optional services, taxes and retries can reduce
headroom. An explicit private reserve may account for known obligations; a zero
reserve does not assert those obligations do not exist. This is not a budget
guarantee or a download quota. Unsupported profiles or materially unpriced
operations require a separately reviewed estimate.

## Download authorization

Catalog metadata may be public while payloads remain private. Anonymous access
being disabled does not prove that only one identity has access.

The owner selected native **per-user approval**: no new read access without
their explicit approval of the Entra principal and specific dataset. Review
existing effective permissions and prepare a least-privilege path-scoped
read-only assignment plan. No recipient has been authorized by installing this
skill, and it ships no IAM mutation or gateway.

Approved users can read repeatedly. Existing broad RBAC/ACL permissions,
shared keys/SAS, and privileged administrators are not revoked by a new
conditional grant. The complete procedure and revocation limitations are in
the skill reference. Strict per-download limits would be a separate service and
budget decision, not a promise made by this workflow.
