# query_workload_artifact_smoke_v1

## Outcome

Run a bounded, repeatable usability smoke of the eight public query/workload artifact generators. Return one report with the fixed overall status `PASS`, `FAIL`, or `BLOCKED` and persona-specific observations.

## 1. Preflight without mutation

1. Confirm the repository root and capture the current UTC time.
2. Capture the current branch, `HEAD` revision, and `git status --porcelain=v1 --untracked-files=all` using read-only Git commands. Do not stage, restore, checkout, commit, merge, push, or otherwise mutate Git state.
3. Hash the readable Python source files under `driftbench/data` so the same source snapshot can be compared after the run.
4. Record the package version if discoverable and whether imports resolve from the source checkout or an installed distribution.
5. Confirm that all eight public `queries` callables import and their module files exist. A missing import or source file is `BLOCKED`, not a skipped benchmark.
6. Install no dependencies. Do not print environment variables, credentials, tokens, connection strings, or unrelated repository contents.

Read-only Git inspection is allowed. External benchmark drivers, workload subprocesses, network calls, and database connections are not.

## 2. Establish the safety envelope

Use one Python process and one new `tempfile.TemporaryDirectory(prefix="driftbench-real-user-")` context. Resolve its path and verify it is outside the repository before any generator runs. Create a distinct child output path for each benchmark.

Guard subprocess and common network entry points during every `.generate()` call so any attempted use fails closed and is recorded. This includes `subprocess.run`, `subprocess.Popen`, `subprocess.call`, `subprocess.check_call`, `subprocess.check_output`, `os.system`, `socket.create_connection`, and `urllib.request.urlopen`. The guard must raise; it must not mock a successful external action.

Always pass `force=False`. Never follow, open, execute, or delete a returned path until its resolved path has been proven to be inside the owned temporary directory.

## 3. Run the fixed public calls

Use these exact public factories and arguments. In every row, call `.generate(output_dir=<owned-temp>/<id>, force=False)`.

| Benchmark | Import | Exact public factory call |
|---|---|---|
| TPC-H | `from driftbench.data.tpch import queries` | `queries(query_ids=[1, 6], queries_per_template=1, mode="qgen", seed=42, shuffle=False)` |
| TPC-DS | `from driftbench.data.tpcds import queries` | `queries()` |
| TPC-C | `from driftbench.data.tpcc import queries` | `queries()` |
| TPC-C Skew | `from driftbench.data.tpcc_skew import queries` | `queries(scale_factor=2, hot_warehouse_fraction=.5, skew_factor=.99)` |
| JOB | `from driftbench.data.job import queries` | `queries()` |
| YCSB | `from driftbench.data.ycsb import queries` | `queries(workload="B", run_seconds=1, target_rate=10)` |
| DSB | `from driftbench.data.dsb import queries` | `queries()` |
| pgbench | `from driftbench.data.pgbench import queries` | `queries(workload="select_only", clients=1, duration=1, rate=0)` |

Do not substitute private helpers, CLI aliases, data generators, `.drift()`, or other parameters.

## 4. Verify each result deeply

For every row:

1. Record the exact public call, elapsed time, stdout/stderr, and any exception type/message. Redact secrets if an unexpected dependency prints them.
2. Require the adapter and result `artifact_type` to equal `queries`.
3. Require the exact managed artifact basenames and count listed below.
4. Require `result.metadata` to exist as a regular JSON file inside the owned temporary directory.
5. Parse the manifest and require matching `benchmark`, `artifact_type`, managed file list, `cache.schema`, `cache.version`, `cache.generator`, `cache.parameters`, `cache.fingerprint`, and an artifact descriptor for every managed file.
6. Treat manifest `files` and descriptor `path` values as paths relative to `result.output_dir`, not as direct children or bare basenames. Resolve them, re-check containment, and require the resolved set to equal `result.files` exactly.
7. Recompute the cache fingerprint as SHA-256 of the UTF-8 JSON encoding of `{"schema": cache.schema, "version": cache.version, "generator": cache.generator, "parameters": cache.parameters}` using `ensure_ascii=True`, `allow_nan=False`, separators `(",", ":")`, and sorted keys. Require an exact match.
8. Recompute each managed file's byte count and SHA-256 and compare them with its descriptor.
9. For TPC-H, require the canonical manifest `query_ids` value `["1", "6"]`. The public numeric IDs `[1, 6]` are intentionally normalized to strings before cache provenance is recorded; this is not a mismatch. Also require the other fixed public values (`mode`, `queries_per_template`, `seed`, and `shuffle`) while accepting resolved source paths for the packaged template and distribution files.
10. Resolve `result.output_dir`, every `result.files` entry, and `result.metadata`; require each path to remain inside the owned temporary directory.
11. Read files only after containment succeeds. Check the content markers below. Inspect `run_pgbench.sh` as text only.

### Expected artifacts and markers

| Benchmark | Exact managed artifacts | Metadata | Required content markers |
|---|---|---|---|
| TPC-H | `tpch_queries.csv`, `tpch_queries.sql` | `tpch_queries_manifest.json` | CSV: `query_id,index,sql`; SQL: `-- TPCH Q1 instance 1` and `-- TPCH Q6 instance 1` |
| TPC-DS | `query_ids.txt`, `sample_tpcds_config.xml` | `tpcds_queries_manifest.json` | IDs: `query01` and `query99`; XML: `<parameters>` and `<type>POSTGRES</type>` |
| TPC-C | `delivery.sql`, `new_order.sql`, `order_status.sql`, `payment.sql`, `stock_level.sql`, `tpcc_all_transactions.sql` | `tpcc_queries_manifest.json` | Each transaction file identifies its TPC-C transaction; bundle contains `-- === NEW_ORDER ===` |
| TPC-C Skew | `delivery.sql`, `new_order.sql`, `order_status.sql`, `payment.sql`, `stock_level.sql`, `tpcc_skew_all_transactions.sql` | `tpcc_skew_queries_manifest.json` | Every file contains `TPC-C Skew: Zipf alpha=0.99`; bundle contains `-- === NEW_ORDER ===` |
| JOB | the 21 files enumerated below | `job_queries_manifest.json` | Each individual file starts with `-- JOB`; bundle contains `-- === 1A_KEYWORD_FILTER ===` and `-- === 20A_FULL_EIGHT_TABLE ===` |
| YCSB | `sample_ycsb_config.xml`, `workload_b.properties` | `ycsb_queries_manifest.json` | XML: `<parameters>`; properties: `maxexecutiontime=1` and `target=10` |
| DSB | `q1_revenue_by_year.sql`, `q2_revenue_by_region.sql`, `q3_margin_trend.sql` | `dsb_queries_manifest.json` | Q1: `SUM(lo.revenue)`; Q2: `c.region`; Q3: `lo.revenue - lo.supply_cost` |
| pgbench | `pgbench_select_only.sql`, `run_pgbench.sh` | `pgbench_queries_manifest.json` | SQL: `pgbench select-only transaction`; script text: `#!/usr/bin/env bash` and `pgbench -c 1 -T 1` |

The exact JOB set is:

- `1a_keyword_filter.sql`
- `2a_company_movies.sql`
- `3a_movie_info_filter.sql`
- `4a_cast_keyword.sql`
- `5a_company_country_cast.sql`
- `6a_info_company.sql`
- `7a_keyword_count.sql`
- `8a_actor_productivity.sql`
- `9a_multi_keyword_movie.sql`
- `10a_full_join.sql`
- `11a_company_keyword_year.sql`
- `12a_cast_info_selective.sql`
- `13a_movie_info_aggregate.sql`
- `14a_company_info_cast.sql`
- `15a_keyword_year_range.sql`
- `16a_actor_company.sql`
- `17a_selective_cast_keyword.sql`
- `18a_movie_info_keyword.sql`
- `19a_company_output_volume.sql`
- `20a_full_eight_table.sql`
- `job_all_queries.sql`

## 5. Close and compare state

Leave the `TemporaryDirectory` context and confirm its exact root no longer exists. Do not run a broad cleanup command. Re-capture the source hashes, branch, revision, and porcelain status and compare them with preflight. Any repository/source change caused during the audit is a `FAIL` and must be reported.

## 6. Apply the persona lens

- Researcher: assess reproducibility, parameter provenance, manifest evidence, and whether claim limitations are clear.
- Industry/vendor: assess stable automation, machine-readable behavior, operational containment, cleanup, and error handling.
- Newcomer: begin from documented entry points and assess discoverability, copyability, prerequisite clarity, output naming, and actionable errors.

Separate observations from execution facts. Do not modify the product to address findings.

## 7. Report contract

Return all of these sections:

1. `Overall status`: exactly one of `PASS`, `FAIL`, or `BLOCKED`.
2. `Protocol`: `query_workload_artifact_smoke_v1`.
3. `Source state`: branch, revision, package/import source, worktree before/after comparison, and benchmark-source hash comparison.
4. `Safety facts`: owned temp root, outside-repository check, subprocess/network/database guards, `force=False`, generated-artifact execution count (must be zero), cleanup result, and repository mutation result.
5. `Benchmark results`: a table containing all eight benchmark rows in the fixed order. Each row must include benchmark name, exact public call, expected artifacts, observed artifacts, metadata/manifest result, elapsed time, and row status.
6. `Persona observations`: evidence-backed usability findings for the assigned persona.
7. `Limitations`: explicitly list everything the protocol did not validate.

At minimum, the limitations must name data generation, database execution, generated SQL/XML/shell execution, result correctness, performance, official benchmark conformance, end-to-end workflows, and release readiness as untested.

Use overall `BLOCKED` when the audit could not safely start or evidence could not be collected. Use overall `FAIL` when it started but any call, artifact, metadata, marker, containment, cleanup, or state-preservation check failed. Use overall `PASS` only if all eight rows passed every check.

Never report “all benchmarks validated.” The only permitted PASS claim is: “All eight query/workload artifact generators passed `query_workload_artifact_smoke_v1`.”
