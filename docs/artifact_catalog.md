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
requires appropriate Azure authorization. This API does not download files,
generate data, run SQL, or change permissions. The catalog does not establish
official benchmark conformance, database execution, or performance results.

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
