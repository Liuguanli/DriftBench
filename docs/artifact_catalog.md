# Public Azure artifact catalog

The Python catalog describes **artifacts observed in Azure HNS**, not everything
that an adapter can generate. Anyone with the package can browse this public
metadata without Azure SDK packages, an Azure account, or network access.

```python
from driftbench import catalog

print(catalog.info())
for entry in catalog.list(artifact_type="data"):
    print(entry["id"], entry["parameters"], entry["formats"], entry["total_bytes"])

tpch_by_scale = {
    entry["parameters"]["scale_factor"]: entry
    for entry in catalog.list(benchmark="tpch", artifact_type="data")
}
tpch_sf10 = tpch_by_scale["10"]
print(catalog.get(tpch_sf10["id"])["files"])
queries = catalog.list(artifact_type="queries")
```

`import driftbench` does not read the catalog or contact Azure. Explicit calls
read the metadata snapshot shipped in that installed distribution. They do
**not** refresh it from Azure. Always inspect `catalog.info()["observed_at"]`
before relying on availability: objects and access permissions can change
after that UTC observation time.

## Contract

- `catalog.list(*, benchmark=None, artifact_type=None)` returns JSON-compatible
  dictionaries sorted by stable ID. Filters combine; canonical names such as
  `tpch` and `tpcc_skew` are used, not display names such as `TPC-H`.
- `artifact_type` is `data` or `queries`. A valid filter with no recorded matches
  returns `[]`. An invalid filter raises `ValueError`.
- `catalog.get(entry_id)` returns one record. Unknown IDs raise `KeyError`;
  invalid ID arguments raise `ValueError`.
- `catalog.info()` reports schema, observation time, source scope, per-type
  counts, available benchmark names, snapshot hash, and access/freshness limits.
- Every result is independently loaded, including nested dictionaries and lists.
  Editing a returned value cannot change a later result.
- Missing, malformed, unsupported, or inconsistent packaged metadata raises
  `catalog.CatalogError`; it never becomes a success-shaped empty catalog.

Entries distinguish `artifact-cache-v1` from `immutable-dataset-v1`. Each has
benchmark/type, generator/revision, known public parameters, storage path,
payload descriptors, formats, file count, total payload bytes, and manifest
and commit hashes. IDs combine benchmark, type, layout, and artifact fingerprint.
Different parameter variants are separate entries.

`files[*].path` and `manifest_path` are relative to the entry's `path`.
The file system and DFS account endpoint are in `info()["source"]`.
Counts and bytes exclude commit/provenance/generator manifests. Managed SQL
schema files do count as payload artifacts. Parameters are an explicit safe
subset of provenance, **not** a complete materialization request.

The snapshot has ten data records across eight benchmark families:
TPC-H, TPC-DS, TPC-C, TPC-C Skew, YCSB, DSB, JOB, and pgbench. YCSB has two
parameter variants; TPC-H has separate SF 0.01 and SF 10 datasets.
There are **no published query records in this snapshot**,
so the query example returns `[]`; generator support is not fabricated into
availability. Both TPC-H datasets have eight `.tbl` payloads and are independent
datasets, not ordinary remote-cache hits. Their `scale_factor` values are decimal
strings: select `"0.01"` or `"10"` explicitly rather than taking the first result,
whose order is by ID, not by scale.

The SF 10 addition contains 11,232,136,268 payload bytes (about 11.23 GB),
including 15,000,000 orders and 59,986,052 lineitems. It was generated with the
same pinned `tpchgen-cli` 3.0.0 image used for SF 0.01. Every table's shape and
row count, and 1-7 sequential lineitems for every order, were checked locally;
uploaded bytes were fully read back and SHA-256 checked before and after
finalization. The smaller dataset was preserved. These are generation/upload
checks, not official benchmark conformance or database execution evidence.

Public metadata does **not** make the data public. Reading payloads still
requires appropriate Azure authorization. Browsing does not download files;
the separate explicit `materialize` call below can populate a private local cache.
Neither operation generates data, runs SQL, or changes permissions. The catalog does not establish
official benchmark conformance, database execution, or performance results.

## Download once and reuse verified local data

This development-checkout API supports `immutable-dataset-v1` **data** entries,
not the ordinary `artifact-cache-v1` layout or query artifacts. A published
package lacking `catalog.materialize` cannot use it until an appropriate version
is separately released; no automatic checkout fallback is provided.

```python
from pathlib import Path
from driftbench import catalog

sf001 = next(
    entry for entry in catalog.list(benchmark="tpch", artifact_type="data")
    if entry["parameters"]["scale_factor"] == "0.01"
)
result = catalog.materialize(
    sf001["id"],
    cache_dir=Path.home() / "DriftBenchDatasets",
    max_bytes=64 * 1024 * 1024,
)
print(result["cache_outcome"], result["payload_dir"])
```

The first successful population returns `cache_outcome="downloaded"` and reads
the exact catalog commit marker, provenance manifest, and eight listed payloads.
It checks pinned metadata hashes, metadata structure/cross-references, and every
payload's size/SHA-256 before atomically exposing the local entry. The provenance
reference to the generator manifest is checked, but that additional generator
manifest is not downloaded: this is the catalog's payload subset, not a full
remote bundle export.

Repeat the same call to get `cache_outcome="hit"`. **Every hit verifies the full
inventory, all hashes, metadata, and source/descriptor binding locally**, without
constructing an Azure client, importing its SDK, reading a credential file, or
contacting Azure. This still reads the local files to hash them; it is not merely
an existence or timestamp check. An observation-time-only catalog refresh does
not invalidate identical content, but changed source/entry descriptors create a
different cache key.

| Result field | Meaning |
|---|---|
| `entry_id`, `source`, `binding_sha256` | Selected immutable ID and canonical source/descriptor binding; no credentials |
| `local_path`, `payload_dir`, `files` | Dedicated cache entry, common payload parent, and verified absolute file paths |
| `file_count`, `payload_bytes` | Listed payload inventory, not metadata |
| `metadata_bytes`, `materialized_bytes` | Commit/provenance plus local binding bytes; total local content including payloads |
| `downloaded_payload_bytes`, `downloaded_metadata_bytes` | Content obtained on this population; both zero on a hit; local binding is not downloaded |
| `remote_checked`, `catalog_observed_at` | Whether this call read Azure, and the packaged catalog's observation time |

`max_bytes` is an aggregate **materialized-content** limit including metadata
and the local binding, not an Azure bill or HTTP retry quota. Known oversize
requests fail before credentials/client construction; remote metadata is also
bounded to 1 MiB per object, and payload streams cannot exceed their descriptors.
The default therefore rejects SF10. SDK buffering, protocol overhead, and retries
are not reported as downloaded content or bounded billing traffic.

Misses require the existing optional `azure` dependencies and an already
authorized identity from the [Azure setup guide](azure_hns_cache.md).
An explicit `credential_env_file` selects the existing private credential-file
mode; otherwise the existing non-interactive credential chain is used. The call
does not log in, grant access, upload, list the namespace, or alter Azure data.

Unknown IDs raise `KeyError`; invalid catalog metadata raises `CatalogError`.
Unsupported requests/limits/paths use the existing cache configuration errors.
Authentication, transport, integrity, collision, and lock failures remain errors.
A missing remote object is **not** a request to generate replacement data.
Corrupt or incomplete existing local entries fail without repair or overwrite:
inspect them or explicitly choose another dedicated `cache_dir`.

Keep private caches outside the checkout/package installation, separate from
walkthrough outputs. Symlinks/reparse points and unexplained files/directories
are rejected. Forbidden roots are checked by resolved path and directory identity
before local writes, so Windows path aliases cannot bypass that boundary.
Windows-invalid payload filenames are rejected before client construction.
Cooperating callers share the existing bounded OS-backed lock;
failed calls clean only their own staging directory, never older entries.
Cached validity proves agreement with pinned catalog evidence, **not current
Azure freshness or permission**. Revoking remote access does not erase an
already downloaded local copy. Never publish caches, real inputs, or credentials.

## Maintainer refresh (read-only Azure access)

Install the existing optional `azure` dependencies and configure authentication
as described in `azure_hns_cache.md`. From a source checkout:

```powershell
python -m scripts.export_azure_catalog `
  --azure-cache-config .private\azure-cache.yaml `
  --output driftbench\catalog_snapshot.json
```

An explicit `--credential-env-file` uses the existing service-principal mode.
Without it, the existing non-interactive credential chain is used. Configuration
does not accept storage keys, SAS tokens, or connection strings.

The exporter traverses only the configured prefix, follows listing pagination,
and accepts committed `data`/`queries` entries in the two documented layouts.
Drift-result bundles, uncommitted objects, and unknown namespaces are excluded.
Invalid committed in-scope entries or interrupted listings fail the export.
Nothing is uploaded, renamed, deleted, or executed in Azure.

Only bounded commit/provenance/generator metadata is downloaded. Commit
references, manifest hashes, descriptor containment, object presence, and listed
byte counts are checked. **Payload content hashes are recorded from manifests,
not rechecked against downloaded payloads.** This is metadata observation, not
an atomic storage snapshot or a publisher signature.

Limits are 100,000 listed files, 1 MiB per metadata read (plus a one-byte
change-detection sentinel), 10,000 files per entry, and 8 MiB per catalog.
Public export uses a field allowlist: no credentials, signed URLs, local source
paths, build commands, or arbitrary provenance fields. An incomplete/invalid
export does not replace the previous local snapshot.

Review the resulting JSON before distributing it. Updating this file changes
only the checkout; it does not update already installed packages. A new package
release needs the separate release gates and explicit publication authorization.
No anonymous live metadata endpoint is provisioned by this feature.
