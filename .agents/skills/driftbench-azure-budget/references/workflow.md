# Azure budget and native reader approval workflow

## 1. Scope and private settings

This skill applies only to DriftBench. Reminders are triggered before changes;
do not create a recurring schedule. Read `.private/azure-budget.yaml`:

```yaml
schema: driftbench.azure-budget/v1
monthly_budget_aud: "SET_BY_OWNER"
reserved_aud: "0"
azure_cache_config: .private\azure-cache.yaml
```

Only the owner may change the budget or authorize an over-budget operation.
A zero explicit reserve means no allowance was configured; it does NOT prove
other future Azure charges are zero. Ask for a larger reserve when other
subscription workloads or delayed charges are relevant. Keep all financial
settings/reports private; never store them in Copilot Memory.

The current collector requires Azure CLI login, the existing optional Azure
dependencies, and read access to storage metadata and Cost Management for the
subscription containing that account. Do not install dependencies, log in
interactively, escalate permissions, or change identities silently.

## 2. Evidence and report

Run the command documented in `SKILL.md`; inspect its exit status and fresh JSON.
The helper enumerates current scoped blob metadata, including noncatalogued and
transient live objects, and reconciles catalog payload sizes against that listing.
It does not download payloads or verify their hashes.

Account discovery is an ARM read. Storage operations are metadata/list reads.
The only ARM POST is the read-only Cost Management `/query` operation with
`ActualCost`, `MonthToDate`, and aggregate `PreTaxCost`; it is not an Azure change.
Retail pricing requests go to the official public API without credentials.
Billing must be AUD; the helper never guesses a currency conversion.

Supported automatic estimates are HNS/StorageV2/Hot/Standard_LRS with default
Microsoft routing and private storage. Unknown/mixed object tiers, unsupported
SKU/routing/SFTP/NFS, missing prices/currency/units, or unavailable billing block
the preflight. An absent billing row is unknown, not a zero-cost month.

The JSON distinguishes:

- Full-month carrying cost of live data in the configured prefix.
- Remaining-month storage obligations, using the remaining UTC-month fraction.
- Whole-subscription MTD cost already incurred, counted once.
- Explicit reserve, proposed transfer/operation costs, and conditional headroom.
- Separate dataset/corpus capacity scenarios sharing the same headroom.

Storage uses GiB. Internet egress is estimated in decimal GB conservatively;
this is an estimation assumption, not a statement that all invoice meters use
decimal units. First paid tiers are used without discounts or free egress.
The remaining free allowance is unknown and shared, not a fresh allowance per
dataset/person. Request rounding includes ordinary chunk and metadata overhead;
verification downloads are charged as complete readbacks, normally twice.

Full-download counts assume the conditional headroom is devoted to that ONE
scenario. They are not SQL execution counts, guaranteed remaining balance,
or technical download limits. Azure billing can lag; future other-resource
charges, namespace-index capacity, optional features, taxes and retries may
reduce capacity. Soft-deleted versions/snapshots are not a live-blob inventory.
If those components are material or unbounded, block the mutation pending a
separately priced plan or owner-approved reserve; do not describe base storage
cost as the entire subscription bill.

Do not use the reporter to price Azure compute, SQL/Cosmos workloads, other
storage profiles, or cross-region copy operations. A report cannot create an
authorization or enforce the monthly budget. A failed run replaces the owned
private report with `BLOCKED` when possible; if saving fails, discard any old file.

## 3. Before adding data

1. Identify the exact account/filesystem/prefix and whether existing data is kept.
2. Run report mode for initial planning; establish actual retained bytes and
   uploaded object count before final pre-upload mode.
3. Include remote verification traffic, not just the inbound upload. For a
   no-overwrite stage/rename workflow, retain one final copy but include every
   full staged/final readback.
4. Show the owner the costs/assumptions before the first Azure write.
5. A blocked or changed plan requires coordination, not an old report reused as
   approval. An already authorized operation still must satisfy current scope.
6. After the separately approved change, refresh the report/catalog as needed.
   Do not grant more users, make containers public, or alter budgets incidentally.

## 4. Native per-user download approval

The owner chose identity approval, NOT a proxy enforcing each download. Use
Microsoft Entra identities and native least-privilege Azure authorization.
There is no grant to make until the owner specifies and approves a principal
and a concrete dataset. A local allowlist file is not Azure access control.

The default recipient is one verified Entra **User**. A group, service principal,
or batch of recipients changes the approval scope and needs a separate explicit
owner decision. Do not invent an object ID from an email address or send guest
invitations automatically when the identity is absent from the tenant.

Prepare an approval plan containing:

- Verified principal object ID/type and the identity presented to the owner.
- Exact catalog dataset ID and live path, scale, size, and intended access.
- Current effective access review: direct/inherited/group RBAC, relevant HNS
  ACLs, existing SAS/shared-key/admin paths, and any lookup limitations.
- Only read access to that dataset, audit/revocation details, and budget impact.
- Explicit owner confirmation of this identity AND dataset, captured before
  any grant. Approval of this workflow alone is not approval of a recipient.

Prefer a **conditional Storage Blob Data Reader assignment** at the container
resource scope with a resource-path condition limited to that dataset's
descendants. Review the current official condition syntax before preparing the
command. Verify the built-in role's read-only actions; never substitute Owner,
Contributor, or an unconditional container/account-wide reader role for
convenience. If a validated condition cannot express the required scope, stop
for an independently reviewed ACL plan rather than broadening the grant.

Path-condition design for known file paths:

```text
allow the blob read action only when:
  operation is not Blob.List
  AND blob resource path is inside the exact approved dataset prefix
```

Catalog descriptors already supply exact file paths; blanket listing access is
not necessary. Reject wildcard/quote-bearing scope input rather than interpolate
untrusted paths into a condition. No automated grant command is shipped or
executed by this skill. The separately authorized native grant must be verified
with the intended identity: approved objects readable, an unrelated object
denied, and no write/delete capability. Never issue a SAS as a supposed
single-use or person-bound substitute.

Record the actual assignment ID, approved scope, approver and time in private
audit evidence. Revoke only that recorded assignment when explicitly asked.
Revocation can take time to propagate and cannot revoke other permissions or
already downloaded copies.

**Boundary:** Azure permissions are additive. A conditional role does not remove
broader inherited/group permissions or ACL grants; account keys/SAS and
privileged administrators can bypass this workflow. Account shared-key support
was not disabled by this unit. Do not promise "only the owner can read" without
an effective-access audit, or rotate/disable existing keys without separate
authorization and compatibility review.

An approved reader can download repeatedly until their effective access ends.
This mechanism provides no per-user request/byte quota or hard spending cap.
Strict per-download/byte limits require a separately designed, priced and
authorized gateway; do not deploy one under this skill.

## 5. Report contract and references

Use `ESTIMATE_READY` or `BLOCKED`, identify scopes/timestamps/assumptions and
show monthly AUD amounts, planned delta, conditional per-dataset read capacity,
and outstanding owner decisions. Report existing access caveats independently
of budget status. Never claim a grant, revocation, quota, live freshness, or
cloud write that did not happen.

Authoritative references (verify current details before changing workflows):

- https://learn.microsoft.com/rest/api/cost-management/retail-prices/azure-retail-prices
- https://learn.microsoft.com/rest/api/cost-management/query/usage
- https://learn.microsoft.com/azure/storage/blobs/blob-storage-estimate-costs
- https://azure.microsoft.com/pricing/details/bandwidth/
- https://learn.microsoft.com/azure/storage/blobs/storage-auth-abac-examples
- https://learn.microsoft.com/azure/storage/blobs/anonymous-read-access-prevent
- https://learn.microsoft.com/azure/storage/common/storage-sas-overview
- https://docs.github.com/en/copilot/concepts/agents/about-agent-skills
