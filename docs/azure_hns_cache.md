# ADLS Gen2 HNS artifact cache

DriftBench can use an explicit Azure Data Lake Storage Gen2 cache before it
generates an eligible local benchmark artifact. The ordinary adapter
`.generate()` API remains offline. Cache mode is also `off` by default, so a
base DriftBench install never imports the Azure SDK, resolves credentials, or
makes a network request.

The read-through flow is:

1. Compute an exact, versioned artifact identity without generating data.
2. Read that one immutable HNS path; the implementation never lists the
   filesystem.
3. On a verified hit, validate the commit marker, manifest, contained POSIX
   paths, byte sizes, SHA-256 hashes, and portable executable flags before
   atomically exposing the local adapter subtree.
4. Only an authenticated exact-path 404 is a miss. Authentication, RBAC,
   network, service, and integrity errors fail closed.
5. In `read-write` mode, a miss is generated and verified locally, uploaded to
   a unique owned staging directory, read back and verified, then published by
   a non-overwriting HNS directory rename. A concurrent winner is accepted only
   when its complete descriptors and content match.

## 1. Install the optional Azure support

```text
pip install "driftbench-db[azure]"
```

For a source checkout:

```text
python -m pip install -e ".[azure]"
```

The base package does not require `azure-identity` or
`azure-storage-file-datalake`.

## 2. Create the HNS-enabled account

Install the Azure CLI, run `az login`, select the intended subscription, and
then use PowerShell like this. Storage account names are globally unique,
lowercase, and 3–24 characters.

```powershell
$subscriptionId = "YOUR-SUBSCRIPTION-ID"
$location = "australiaeast"
$resourceGroup = "rg-driftbench-dev"
$storageAccount = "YOURUNIQUESTORAGEACCOUNT"
$fileSystem = "driftbench-cache"

az login
az account set --subscription $subscriptionId
az group create --name $resourceGroup --location $location
az storage account create `
  --name $storageAccount `
  --resource-group $resourceGroup `
  --location $location `
  --sku Standard_LRS `
  --kind StorageV2 `
  --hns true `
  --https-only true `
  --min-tls-version TLS1_2
az storage fs create `
  --account-name $storageAccount `
  --name $fileSystem `
  --auth-mode login
```

Confirm HNS before using the cache:

```powershell
az storage account show `
  --name $storageAccount `
  --resource-group $resourceGroup `
  --query "{hns:isHnsEnabled,dfs:primaryEndpoints.dfs}" `
  --output table
```

Microsoft Learn references:

- [Create a storage account](https://learn.microsoft.com/azure/storage/common/storage-account-create)
- [Enable hierarchical namespace](https://learn.microsoft.com/azure/storage/blobs/create-data-lake-storage-account)
- [Azure CLI sign-in](https://learn.microsoft.com/cli/azure/authenticate-azure-cli)

## 3. Grant data-plane access

`Owner` or `Contributor` on the resource does not by itself grant Blob/DFS data
access. Assign the narrowest suitable data-plane role. For a development user
that must populate this cache, `Storage Blob Data Contributor` at the storage
account scope is the straightforward starting point:

```powershell
$accountId = az storage account show `
  --name $storageAccount `
  --resource-group $resourceGroup `
  --query id --output tsv
$userObjectId = az ad signed-in-user show --query id --output tsv

az role assignment create `
  --assignee-object-id $userObjectId `
  --assignee-principal-type User `
  --role "Storage Blob Data Contributor" `
  --scope $accountId
```

Allow time for RBAC propagation. If your filesystem uses restrictive POSIX
HNS ACLs, grant matching traverse/read/write ACLs to the same principal as
well. For read-only consumers, use `Storage Blob Data Reader`. For automation,
prefer managed identity or workload-identity federation over a client secret.

- [Assign Azure roles for blob data](https://learn.microsoft.com/azure/storage/blobs/assign-azure-role-data-access)
- [ADLS Gen2 access-control model](https://learn.microsoft.com/azure/storage/blobs/data-lake-storage-access-control-model)

## 4. Configure DriftBench without committing secrets

Copy the non-secret examples:

```powershell
Copy-Item docs/examples/azure-cache.yaml azure-cache.yaml
Copy-Item docs/examples/artifact-request.yaml artifact-request.yaml
```

Edit `azure-cache.yaml` with the DFS endpoint and filesystem. It deliberately
does not accept account keys, SAS tokens, connection strings, client secrets,
or credential-file paths.

Without an explicit credential file, authentication uses the non-interactive
`DefaultAzureCredential` chain. For local employee/developer use, `az login` is
normally the easiest route. For unattended environments, you can explicitly
select a private service-principal credential file instead:

```powershell
New-Item -ItemType Directory -Force .private | Out-Null
Copy-Item .env.example .private/azure.env
```

Fill the three values locally. `.private/`, `.env`, and `.env.*` are Git-ignored;
`.env.example` remains versioned. The explicit credential parser accepts only
`AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, and `AZURE_CLIENT_SECRET`, limits the file
to 16 KiB, rejects symlinks/non-regular files, and never mutates `os.environ`.
Supplying `--credential-env-file` selects only that service principal; omitting
it selects only `DefaultAzureCredential`. An authentication or authorization
failure never switches identities.

Run a strict request:

```powershell
driftbench cache materialize `
  --request artifact-request.yaml `
  --output-dir artifacts `
  --cache-mode read-write `
  --azure-cache-config azure-cache.yaml `
  --json
```

Add this only when the explicit service-principal mode is required:

```text
--credential-env-file .private/azure.env
```

`read` downloads hits but generates misses locally without uploading.
`read-write` uploads verified misses. An authenticated remote miss always
forces a fresh local generation, even when an older cache-v3 manifest is
present locally, so bytes cannot cross producer or generator revision
boundaries. `off` does not open the Azure config or credential file even if
paths were supplied. Request/config errors use exit 3;
transport, authentication rejection, RBAC, generation, integrity, and collision
failures use exit 4. JSON mode emits exactly one document. A request with
`force: true` deliberately bypasses the remote read and attempts a fresh local
generation; normal read-through behavior uses the default `force: false`.

Every successful Python result summary and CLI JSON document reports
`cache_outcome`, `materialization_source`, `remote_entry_status`, `force`,
`remote_path`, and `warnings`. `materialization_source` distinguishes a new
`generated` artifact from a verified `local_cache` reuse or a `remote_cache`
download. `remote_entry_status` is one of `not_checked`, `miss`, `hit`,
`uploaded`, or `concurrent_identical`, so automation does not have to infer
provenance from a coarse outcome name.

Calls targeting the same local benchmark/artifact subtree are serialized with
an OS-backed lock. The operating system releases the lock if a process exits,
so a leftover lock file is not a stale-lock blocker. A competing call waits up
to 10 seconds and then fails instead of writing concurrently; use distinct
local output roots for intentionally parallel, long-running generations.

## Python API

```python
from driftbench import AzureHNSCacheConfig, RemoteCacheMode, materialize_artifacts
from driftbench.data.ycsb import data as ycsb_data

result = materialize_artifacts(
    adapter=ycsb_data(scale_factor=2),
    output_dir="artifacts",
    remote_cache=AzureHNSCacheConfig(
        account_url="https://YOUR_ACCOUNT.dfs.core.windows.net",
        file_system="driftbench-cache",
        prefix="team-a",
        mode=RemoteCacheMode.READ_WRITE,
        credential_env_file=".private/azure.env",
    ),
)
print(result.summary())
```

## HNS layout and current eligibility

Published entries use this hierarchy:

```text
<prefix>/cache/driftbench-artifact-cache/v1/
  <benchmark>/<artifact-type>/<generator-id>/<fingerprint>/
    manifest.json
    _COMMITTED.json
    artifacts/<benchmark>/<artifact-type>/...
```

`data` and `queries` remain distinct HNS subtrees. `queries` is the current
artifact type for query/workload definitions; that does not mean every external
workload is uploadable. A future `drift` artifact type can use the same
hierarchy, but this MVP intentionally does not upload arbitrary DriftSpec
results. Unknown generators are default-denied.

Eligible today are packaged/default TPC-H queries; TPC-DS data; TPC-C data and
queries; TPC-C Skew data and queries; YCSB data; DSB data and queries; JOB data
and queries; and pgbench data and queries. The following are deliberately not
uploaded:

- TPC-H copied/generated data, because it is external/licensed or currently
  produced by an unpinned `dbgen` path;
- TPC-H queries with external template/distribution paths, including
  `dss_dist` file references nested in custom parameter specifications;
- TPC-DS and YCSB query artifacts while their sample XML contains connection
  placeholders;
- BenchBase artifacts and all unregistered/custom generators.

Remote-cache TPC-H custom parameter specifications must use plain Python
`dict`, `list`, and `tuple` containers with scalar values. Unordered,
subclassed, cyclic, excessively deep, path-like, and custom iterable values are
default-denied so the complete identity can be audited before any remote
request. Their manifest identity is a tagged structure that preserves `list`
versus `tuple`, `int`
versus `float`, mapping-key types, and mapping insertion order wherever those
differences can change the rendered SQL. This representation is intentionally
more explicit than ordinary JSON normalization.

SHA-256 detects accidental or adversarial content changes at the configured
storage boundary; it is not a publisher signature. Trust still depends on TLS,
the selected Azure tenant/account, RBAC, HNS ACLs, and control of identities
that can write the cache.

## Credits and cost control

Microsoft employment does not automatically prove that a particular tenant or
subscription has free Azure credit. Check the benefits assigned to your work
account—commonly through [Visual Studio subscription benefits](https://my.visualstudio.com/benefits)—or your internal benefits/support channel, then select that subscription explicitly with `az account set`. Do not assume an entitlement until it appears in the target subscription. For a small development cache, start with Standard LRS, add a budget/alert, and define a lifecycle policy after you understand retention needs.
