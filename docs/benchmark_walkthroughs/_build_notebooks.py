"""Build or check the twelve paired companion documents using only the stdlib."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

if __package__:
    from ._walkthrough import RECIPES, TPCH_AZURE_DATASET_ID
else:
    from _walkthrough import RECIPES, TPCH_AZURE_DATASET_ID


NOTES = {
    "tpch": ("TPC-H", "The data step only copies local `.tbl` files that you provide; it does not run dbgen or download SF10 by default. "
             "Point `SOURCE_DIR` to a small TPC-H data directory (all inputs must total no more than 64 MiB) and explicitly set `SOURCE_SCALE_FACTOR`; "
             "the input scale is not inferred from function defaults, and the declaration is not content validation. The example inspects lineitem. "
             "Queries are Python qgen-style SQL instances, not official qgen output, and do not represent TPC certification."),
    "tpcds": ("TPC-DS", "By default, SF1 consists of five synthetic tables, with 10,000 rows in store_sales. "
              "The query step produces only query01–query99 identifiers and configuration, with no SQL text; the stream is also a sequence of identifiers. "
              "Do not treat these artifacts as a complete TPC-DS dataset, executed SQL, or official benchmark results."),
    "tpcc": ("TPC-C", "The default uses one warehouse and nine synthetic tables, and inspects customer (3,000 rows). "
             "The five transaction templates still contain parameter placeholders; they are not executed transactions. The fixed templates do not provide a general SQL rewriting interface, "
             "but this example can change their sampling frequencies. Single-table changes do not guarantee composite primary-key or foreign-key integrity."),
    "tpcc_skew": ("TPC-C Skew", "Data and queries use the same two warehouses and a 0.5 hotspot ratio, and the example inspects customer (6,000 rows). "
                  "Changing skew_factor changes template comments; it does not prove that a database executed the workload with a Zipf distribution. "
                  "Data-access weights, query frequencies, and actual database hotspots are distinct concepts."),
    "job": ("JOB", "The default generates 11 synthetic IMDB-like tables and inspects title (500 rows). "
            "The 20 SQL templates contain joins, and the stream can change template frequencies. The fixed templates do not provide an arbitrary predicate-rewriting interface; "
            "regenerating one table here does not guarantee foreign-key safety across the movie database."),
    "ycsb": ("YCSB", "The default generates 200 records and inspects usertable. The operation weights for Workload B come from the generated manifest, "
             "not from guesses based on function defaults. XML/properties files are driver configuration, not SQL. "
             "Changing B to A or changing target_rate only changes configuration; it does not execute YCSB."),
    "dsb": ("DSB", "By default, SF1 consists of three synthetic star-schema tables, with 5,000 rows in lineorder. "
            "Three SQL templates can form streams with different frequencies. The example provides no general SQL rewriting and does not demonstrate official benchmark compliance."),
    "pgbench": ("pgbench", "The default SF1 still generates 100,000 accounts; it is not a sample_size=200 dataset. "
                "To keep the drift demonstration short, it inspects and changes the 10-row tellers table. Two native scripts are referenced by the stream, "
                "but pgbench/PostgreSQL is never invoked. rate and duration appear only in the run script."),
    "benchbase": ("BenchBase", "The data step generates load configuration and scripts, not database table rows, so local table loading and data drift are NOT_SUPPORTED. "
                  "Operation names and default weights are read from the actual XML; the sampled stream is not a BenchBase replay. "
                  "The example does not execute Java, scripts, or database operations; do not put real credentials in a committed notebook."),
    "sysbench": ("sysbench", "The prepare plan contains no database rows, so local table loading and data drift are NOT_SUPPORTED. "
                 "The query step generates two native configurations, read_only and read_write; the stream samples configuration references rather than individual SQL statements. "
                 "Changing distribution or rate changes only configuration. Older releases may not include this adapter."),
    "ssb": ("SSB", "Provide five local SSB `.tbl` files: customer, part, supplier, date, and lineorder. "
            "TPC-H files with the same names are not substitutes. Imports become CSV files with headers, and queries are 13 SQL definitions selectable with query_ids. "
            "With no input, the default clearly reports NEEDS_INPUT; the example neither downloads nor fabricates SSB data."),
    "ldbc": ("LDBC SNB Interactive v1", "This example requires the 20 local CSV file families under csv_merge_foreign. "
             "The query step also requires 14 substitution-parameter files and an existing driver_config. "
             "Person partitions are inspected with `|` as the delimiter; the example does not generate a graph, run the driver, or claim graph-integrity drift. "
             "Native configuration is copied verbatim to local output and may contain credentials, so keep it private. Older releases may not include this adapter."),
}


def companion(name):
    return f'''"""Step-by-step {name} companion; implementation shared with its notebook."""

if __package__:
    from ._walkthrough import Walkthrough, main
else:
    from _walkthrough import Walkthrough, main


def create(*, output_dir, package_target="installed", **options):
    return Walkthrough("{name}", output_dir=output_dir, package_target=package_target, **options)


if __name__ == "__main__":
    raise SystemExit(main("{name}"))
'''


def notebook(name):
    title, note = NOTES[name]
    cells = []
    introduction = (
        f"This notebook calls the same functions as `{name}.py`. It only generates and inspects local files; it does not connect to Azure or a database,"
        if name != "tpch" else
        "This notebook calls the same functions as `tpch.py`. By default, it only generates and inspects local files; read-only Azure data preparation can be enabled explicitly, but it does not connect to a database,"
    )

    def add(kind, cell_id, source, *, parameters=False):
        cell = {"cell_type": kind, "id": cell_id, "metadata": {}, "source": source}
        if parameters:
            cell["metadata"]["tags"] = ["parameters"]
        if kind == "code":
            cell.update(execution_count=None, outputs=[])
        cells.append(cell)

    add("markdown", "overview", f"""# {title}: A Step-by-Step DriftBench Walkthrough

{introduction}
and it does not execute SQL, shell commands, Java, pgbench, or sysbench, or install or download benchmark tools.

**Benchmark boundaries:** {note}

`PASS` means that the step has concrete evidence; `NEEDS_INPUT` requires local input; `UNAVAILABLE` means the selected package lacks the API or dependency;
`NOT_SUPPORTED` means that the capability does not exist. Any incomplete step makes the overall result `PARTIAL`, not "all benchmark checks succeeded."
Invalid parameters and genuine runtime errors raise exceptions; they are not converted into skips or successes.

## 0. Prepare the environment manually

Copy this entire directory, including `_walkthrough.py` and all companion files, and launch the notebook from this directory.
For PyPI mode, install `driftbench-db` and Jupyter in an isolated virtual environment, and do not start the kernel from the repository root.
For source mode, explicitly install this checkout first (`pip install -e .`), then change the target below to `checkout`.
These companion documents are not guaranteed to be included in an installed package. See the [README](README.md) for detailed commands, status meanings, and input layouts.
""")
    add("code", "settings", f'''from pathlib import Path
from {name} import create

PACKAGE_TARGET = "installed"
OUTPUT_DIR = Path.home() / "DriftBenchWalkthroughs"
SEED = 42
SAMPLE_SIZE = 200
RECORD_COUNT = 200
SOURCE_DIR = None
SOURCE_SCALE_FACTOR = None
PARAMETERS_DIR = None
DRIVER_CONFIG = None
''', parameters=True)
    if name == "tpch":
        cells[-1]["source"] += (
            'AZURE_DATASET_ID = None\n'
            'DATASET_CACHE_DIR = Path.home() / "DriftBenchDatasets"\n'
            'AZURE_CREDENTIAL_ENV_FILE = None\n'
        )
    add("markdown", "identity-notes", """## 1. Confirm exactly which package is under test

Check the distribution version, module version, and actual imported file. A version number alone does not prove identical functionality.
`installed` rejects source-tree shadowing and editable installs; do not silently switch to source mode to make the check pass.
Each call to create makes a new persistent output subdirectory; earlier runs are neither overwritten nor deleted automatically.
""")
    add("code", "identity", f'''demo = create(
    output_dir=OUTPUT_DIR, package_target=PACKAGE_TARGET,
    seed=SEED, sample_size=SAMPLE_SIZE, record_count=RECORD_COUNT,
    source_dir=SOURCE_DIR, source_scale_factor=SOURCE_SCALE_FACTOR,
    parameters_dir=PARAMETERS_DIR, driver_config=DRIVER_CONFIG,
)
demo.identity
''')
    if name == "tpch":
        add("markdown", "azure-notes", f"""## 1a. Optional: Prepare and cache real SF0.01 data from Azure

By default, `AZURE_DATASET_ID = None`, so Azure is not accessed at all. To read data that you are authorized to access, change the settings cell to:

```python
AZURE_DATASET_ID = "{TPCH_AZURE_DATASET_ID}"
```

This is approximately 10.6 MB across all eight complete SF0.01 tables, not a sample of SF10. Keep `SOURCE_DIR` and `SOURCE_SCALE_FACTOR` as `None`;
the source and scale are read from the explicitly selected catalog entry. `PACKAGE_TARGET` must still reflect the package you actually selected.
The current development version provides `catalog.materialize`; an installed package without this API reports `UNAVAILABLE` instead of switching to source code.

The first run uses an existing Azure identity for a read-only download and validation, producing `downloaded`. Later runs first check every cached file,
its size and SHA-256, the commit/provenance metadata, and source binding. A successful check produces `hit` without initializing an Azure client or reading credentials.
Cache corruption raises an error; it is not overwritten or downloaded again automatically. A valid cache does not prove that the remote data still exists, access is still valid, or the catalog has been refreshed in real time.
The cache is stored in a separate directory; data conversion and drift write only to another run directory. Do not commit or publish these private inputs.

Only the explicit preparation step below may access Azure; subsequent steps still prohibit network access, external drivers, and database execution.
It does not install dependencies, sign in, or change permissions. If the optional Azure dependency or an identity is unavailable, prepare the environment manually by following the README first.
""")
        add("code", "prepare-azure", '''if AZURE_DATASET_ID is not None:
    azure_source = demo.prepare_azure_data(
        AZURE_DATASET_ID, cache_dir=DATASET_CACHE_DIR,
        credential_env_file=AZURE_CREDENTIAL_ENV_FILE,
    )
    print({key: azure_source.get(key) for key in (
        "status", "cache_outcome", "payload_dir", "file_count",
        "payload_bytes", "downloaded_payload_bytes", "binding_sha256",
    )})
else:
    print("Azure preparation not requested; using the explicit local-input workflow.")''')
    add("markdown", "data-notes", f"""## 2. Generate or import baseline files

This calls the real public entry point `driftbench.data.{name}.data(...).generate(..., force=False)`.
The fixed small-scale parameters are `{RECIPES[name]["data"]}`; YCSB uses the record_count configured above.
TPC-H, SSB, and LDBC only read explicitly provided local inputs; missing input never falls back to fabricated data.
source_dir and the output directory must be separate, and inputs are limited to 64 MiB. Do not use SF10 as the default tutorial input.
""")
    add("code", "generate-data", "demo.generate_data()")
    add("markdown", "load-notes", """## 3. Load and inspect files

`.tbl` and `.dat` files are converted with the public `GenerationResult.as_csv()` method; existing CSV files are read using their actual delimiter.
The DataFrame is a client-side check performed with Pandas; it does not mean that DriftBench loaded the data into a database.
This reports the selected table's actual row count, column names, and path; other generated tables remain in the output directory.
""")
    add("code", "load-data", "demo.load_data()")
    add("code", "preview", "demo.preview()")
    add("markdown", "drift-notes", """## 4. Change the row count

This calls `GenerationResult.drift(table, "vary_cardinality", scale=..., seed=..., output_path=...)`.
The expected row count is `int(original_row_count * scale)`. This operation regenerates data for the existing columns; it does not simply delete old rows.
Primary keys, foreign keys, join results, and graph integrity are not guaranteed. The original file's SHA-256 must remain unchanged.
You can modify scale below. The tutorial range is `(0, 2]`; a value too small to generate one row produces an explicit message.
""")
    add("code", "drift-data", "demo.drift_data(scale=0.5)")
    add("markdown", "queries-notes", f"""## 5. Generate real query or workload artifacts

The public entry point is `driftbench.data.{name}.queries(...)`. The current profile parameters are:

```python
{RECIPES[name]["queries"]}
```

The results distinguish SQL instances, SQL templates, query IDs, operation names, and native-configuration references. Duplicate SQL collections and table-creation SQL
are not counted as new queries. LDBC does not display the configuration contents. This step only reads text; it executes none of the artifacts.
""")
    add("code", "generate-queries", '''query_result = demo.generate_queries()
{key: value for key, value in query_result.items() if key != "templates"}''')
    add("code", "template-preview", '[(item["id"], item["kind"]) for item in demo.templates[:20]]')
    add("markdown", "stream-notes", """## 6. Produce a baseline stream on demand

This uses `driftbench.api.QueryTemplate` and `execute_query_template_mix_spec` instead of reimplementing the sampler in the tutorial.
`SAMPLE_SIZE` is the stream length, which is distinct from a data table's row count. Default YCSB and BenchBase weights come from generated artifacts;
the other examples explicitly use an equal-weight teaching baseline. You can set `BASELINE_WEIGHTS` to a dictionary covering every template ID.

The report shows normalized weights together with actual sample counts and proportions, and checks length, membership, zero weights, and reproducibility with the same seed.
Weights are not exact count quotas; the stream does not guarantee real-time QPS, arrival times, query costs, selectivities, or database replay.
If the package lacks this public API, the step records UNAVAILABLE instead of substituting a private implementation or a custom random sequence.
""")
    add("code", "query-stream", '''BASELINE_WEIGHTS = None
demo.query_stream(weights=BASELINE_WEIGHTS)''')
    add("markdown", "mix-notes", """## 7. Change query or operation frequencies

By default, the first template ID receives a target weight of 0.8, and the remaining templates share 0.2.
You can also provide complete `TARGET_WEIGHTS`; for example, to retain only one ID, explicitly assign 0 to every other ID.
This changes the mix proportions, not SQL predicates. Both the original baseline and the changed sequence are saved in JSON.
""")
    add("code", "change-mix", '''TARGET_WEIGHTS = None
demo.change_mix(weights=TARGET_WEIGHTS)''')
    add("markdown", "parameter-notes", f"""## 8. Change public parameters or the profile

This example supports changes to `{RECIPES[name].get("options", ())}`.
The default change is `{RECIPES[name].get("change", {})}`. An empty collection means that this kind of rewriting is unavailable,
and the step records NOT_SUPPORTED accordingly. Edit the parameter dictionary below instead of modifying generated files.

The old and new artifacts must differ, while the old query-file hashes remain unchanged. TPC-C Skew changes comments;
driver parameters such as rate, duration, and distribution change configuration but do not prove that the requested workload was actually executed.
""")
    add("code", "change-parameters",
        f'CHANGE_OPTIONS = {RECIPES[name].get("change", {})!r}\ndemo.change_query_parameters(**CHANGE_OPTIONS)')
    add("markdown", "report-notes", """## 9. Review the summary and persistent evidence

Review the actual status of each step; do not treat PARTIAL as a complete pass. The JSON report and all local artifacts remain in this run's exclusive directory.
Committed notebooks contain no execution output. Do not commit results containing real local inputs or credentials to Git.
Each rerun creates a new directory. Delete a specific run directory manually only after confirming that it is no longer needed; never clean the entire output root.
""")
    add("code", "report", '''report = demo.report()
[(name, value["status"], value["reason"]) for name, value in report["steps"].items()]''')
    add("code", "report-location", '{"status": report["status"], "report_file": report["report_file"]}')
    return {"cells": cells, "metadata": {"kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
                                        "language_info": {"name": "python"}},
            "nbformat": 4, "nbformat_minor": 5}


def generated_files():
    for name in RECIPES:
        yield f"{name}.py", companion(name)
        yield f"{name}.ipynb", json.dumps(notebook(name), ensure_ascii=False, indent=1) + "\n"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--write", action="store_true", help="Regenerate the explicitly named companion pairs.")
    args = parser.parse_args()
    root = Path(__file__).resolve().parent
    mismatched = []
    for filename, text in generated_files():
        path = root / filename
        if args.write:
            path.write_text(text, encoding="utf-8", newline="\n")
        elif not path.is_file() or path.read_text(encoding="utf-8") != text:
            mismatched.append(filename)
    if mismatched:
        raise SystemExit("Companion files differ from their builder: " + ", ".join(mismatched))


if __name__ == "__main__":
    main()
