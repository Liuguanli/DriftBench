# Adapter Support Contract

DriftBench reports adapter support per generated artifact and mode. The machine-readable source of truth is `driftbench.data.base.SUPPORT_PROFILES`; every newly generated data or query manifest includes the corresponding `support` block.

## Tiers

| Tier | Name | Contract |
|---:|---|---|
| 0 | Illustrative | Names, sketches, IDs, or configuration examples only; no runnable workload is shipped. |
| 1 | Synthetic-conformant | Deterministic synthetic artifacts preserve useful benchmark shape or workload semantics but are incomplete or intentionally scaled down. |
| 2 | Executable | DriftBench ships data, SQL, scripts, or external-tool configuration that can execute when its documented runtime dependencies are present. |
| 3 | Official-tool/spec-traceable | Generation uses an official-style tool or packaged specification templates with traceable parameters. |

**Strong adapter support means Tier 2 or higher; Tier 3 is used only where the current implementation is tool/spec traceable.** Tier 3 is not a certification claim.

DriftBench does **not** claim official or audited TPC/YCSB compliance, nor certification by the maintainers of JOB, DSB, pgbench, or BenchBase. The metadata describes current DriftBench artifacts, not benchmark-result eligibility or vendor certification.

## Manifest schema

```json
{
  "support": {
    "contract_version": 1,
    "tier": 2,
    "tier_name": "executable",
    "mode": "executable-sql-subset",
    "official": {
      "table_count": null,
      "query_count": 113,
      "transaction_count": null
    },
    "shipped": {
      "table_count": null,
      "query_count": 20,
      "transaction_count": null
    },
    "compliance_disclaimer": "..."
  }
}
```

Counts are artifact-specific. `null` means the dimension is not applicable or DriftBench does not make a verified official-count claim; `0` means the artifact deliberately ships none.

## Current registered claims

| Adapter | Artifact | Registry mode | Tier | Manifest mode | Official count | Shipped count |
|---|---|---|---:|---|---|---|
| `tpch` | data | `copy` | 2 | `copied-official-format` | 8 tables | 8 tables |
| `tpch` | data | `generate` | 3 | `dbgen` | 8 tables | 8 tables |
| `tpch` | queries | `qgen` | 3 | `qgen` | 22 queries | 22 queries |
| `tpch` | queries | `custom` | 2 | `custom-parameterized-sql` | 22 queries | 22 queries |
| `tpcds` | data | default | 1 | `synthetic-subset` | 24 tables | 5 tables |
| `tpcds` | queries | default | 0 | `query-ids-and-config-only` | 99 queries | 0 queries |
| `tpcc` | data | default | 1 | `synthetic-subset` | 9 tables | 9 tables |
| `tpcc` | queries | default | 1 | `sql-transaction-templates` | 5 transactions | 5 transactions |
| `tpcc_skew` | data | default | 1 | `synthetic-subset-with-inert-weights` | 9 tables | 9 tables |
| `tpcc_skew` | queries | default | 1 | `annotated-sql-transaction-templates` | 5 transactions | 5 transactions |
| `job` | data | default | 1 | `synthetic-subset` | 21 tables | 11 physical tables (8 workload + 3 lookup) |
| `job` | queries | default | 2 | `executable-sql-subset` | 113 queries | 20 queries |
| `ycsb` | data | default | 1 | `synthetic-usertable` | 1 table | 1 table |
| `ycsb` | queries | default | 1 | `workload-config-only` | 6 operations | 6 configured operations |
| `dsb` | data | default | 1 | `synthetic-toy-subset` | not claimed | 3 tables |
| `dsb` | queries | default | 2 | `executable-sql-toy-subset` | not claimed | 3 queries |
| `pgbench` | data | default | 1 | `synthetic-pgbench-shape` | 4 tables | 4 tables |
| `pgbench` | queries | default | 2 | `executable-pgbench-script` | 3 built-in workloads | 3 scripts available, 1 selected per artifact |
| `benchbase` | data | default | 2 | `external-benchbase-load-config` | target-dependent | configuration/script artifact |
| `benchbase` | queries | default | 2 | `external-benchbase-execute-config` | target-dependent | target transaction count recorded per manifest |

TPC-C and TPC-C Skew remain Tier 1 because their synthetic item population scales from 10,000 per warehouse to a 100,000 cap rather than using the official fixed 100,000 rows. TPC-C Skew's weights are emitted for a driver to consume; DriftBench does not itself execute that distribution. BenchBase is Tier 2 because the generated scripts are executable with a user-provided BenchBase jar and database, but DriftBench does not invoke the jar during generation.
