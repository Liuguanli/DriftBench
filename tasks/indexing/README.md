Indexing Tools (LearnedIndexDiskExp)

This folder keeps command scripts plus generated data/query/log outputs
for running LearnedIndexDiskExp with external query files.

Layout (created by scripts):
  data/         - binary key files for index build
  data/raw/     - downloaded large datasets
  queries/      - binary query key files for lookup/scan
  logs/         - command logs
  out/          - index files and run outputs

Key scripts:
  download_dataset.sh    - download a large uint64 dataset to data/raw
  sample_dataset.py      - sample a small dataset from a large binary file
  bin_to_csv.py          - convert sampled binary keys to CSV for DriftSpec
  generate_queries.py    - generate point/range queries from a binary dataset
  generate_baseline_queries.sh - create baseline point/range queries
  generate_experiment_queries.sh - create point/range queries for experiments
  run_query_drift.sh     - generate queries using DriftSpec keylist
  run_data_drift.sh      - generate drifted datasets using DriftSpec
  run_learned_index.sh   - build/run ALEX/B+tree/fiting_tree with query_file

Quick start
1) Download a dataset (SOSD example, uint64, ~800M keys):
   SOSD_DATASET=osm_cellids_800M_uint64 \
     bash tasks/indexing/download_dataset.sh

   Or provide a direct URL to a uint64 binary file:
   DATASET_URL="https://.../your_dataset_uint64.bin" \
     bash tasks/indexing/download_dataset.sh

2) Sample a small dataset for experiments:
   python3 tasks/indexing/sample_dataset.py \
     --input tasks/indexing/data/osm_cellids_800M_uint64 \
     --output tasks/indexing/data/keys_u64.bin \
     --count 1000000 --type u64 --sort

3) Generate query keys from the sampled data (hit rate = 1.0):
   Point queries (10k each):
     python3 tasks/indexing/generate_queries.py \
       --input tasks/indexing/data/keys_u64.bin \
       --output tasks/indexing/queries/point_uniform_10k.bin \
       --count 10000 --type point --distribution uniform

     python3 tasks/indexing/generate_queries.py \
       --input tasks/indexing/data/keys_u64.bin \
       --output tasks/indexing/queries/point_zipf_a2_10k.bin \
       --count 10000 --type point --distribution zipf --zipf-alpha 2

     python3 tasks/indexing/generate_queries.py \
       --input tasks/indexing/data/keys_u64.bin \
       --output tasks/indexing/queries/point_hotspot_1pct_10k.bin \
       --count 10000 --type point --distribution hotspot --hotspot-frac 0.01

   Range queries (start keys; r_size passed at runtime):
     python3 tasks/indexing/generate_queries.py \
       --input tasks/indexing/data/keys_u64.bin \
       --output tasks/indexing/queries/range_uniform_r100_10k.bin \
       --count 10000 --type range --distribution uniform --r-size 100 --sort-keys

     python3 tasks/indexing/generate_queries.py \
       --input tasks/indexing/data/keys_u64.bin \
       --output tasks/indexing/queries/range_uniform_r1000_10k.bin \
       --count 10000 --type range --distribution uniform --r-size 1000 --sort-keys

     python3 tasks/indexing/generate_queries.py \
       --input tasks/indexing/data/keys_u64.bin \
       --output tasks/indexing/queries/range_uniform_r10000_10k.bin \
       --count 10000 --type range --distribution uniform --r-size 10000 --sort-keys

4) Run benchmarks (uses tasks/indexing for logs/data):
   bash tasks/indexing/run_learned_index.sh

One-shot download + sample + delete raw dataset:
   SOSD_DATASET=osm_cellids_800M_uint64 \
     SAMPLE_COUNT=1000000 \
     bash tasks/indexing/download_and_sample.sh

Optional query sampling (if you are not using your own query generator):
   SOSD_DATASET=osm_cellids_800M_uint64 \
     SAMPLE_COUNT=1000000 QUERY_COUNT=200000 \
     bash tasks/indexing/download_and_sample.sh

Experiment steps (end-to-end)
1) Download + sample:
   SOSD_DATASET=osm_cellids_800M_uint64 \
     SAMPLE_COUNT=1000000 \
     bash tasks/indexing/download_and_sample.sh

2) Produce queries with your framework:
   - output binary file: tasks/indexing/queries/*.bin

3) Run all index benchmarks:
   QUERY_FILE=tasks/indexing/queries/point_uniform_10k.bin \
   SEARCH_COUNT=10000 \
   bash tasks/indexing/run_learned_index.sh

Query options (when not using QUERY_FILE):
- CASE_ID=1 (uniform), CASE_ID=2 (zipf)
- OP_TYPE=lookup (point queries) or OP_TYPE=scan (range queries)
- R_SIZE controls range length for scan (default 100)

Example range scan:
   OP_TYPE=scan R_SIZE=1000 \
   SEARCH_COUNT=10000 \
   bash tasks/indexing/run_learned_index.sh

Notes:
- For QUERY_FILE, make sure the file was generated from the same dataset used for indexing.
  This keeps hit rate at 1.0 (no "not found" queries).
- COUNT should match the number of keys in DATA_FILE (e.g., 1,000,000).
- Logs include percentile summaries when available:
  latency_ns_p50/p90/p95/p99 and block_p50/p90/p95/p99.

Data drift generation (DriftBench + DriftSpec)
1) Ensure you have sampled keys at:
   tasks/indexing/data/keys_u64.bin

2) Run the drift spec (produces CSV + BIN):
   bash tasks/indexing/run_data_drift.sh

3) Drift outputs:
   tasks/indexing/data/drift/**.csv
   tasks/indexing/data/drift/**.bin

Notes:
- Drift CSVs may contain floats (e.g., outlier injection). The converter rounds them.
- For u64 outputs, values are clipped to [0, 2^64-1] by default.
  Override:
    ALLOW_FLOAT=0 CLIP_U64=0 bash tasks/indexing/run_data_drift.sh

Query generation (DriftBench + DriftSpec)
1) Ensure you have sampled keys at:
   tasks/indexing/data/keys_u64.bin

2) Run query spec (writes .bin directly, requires pandas):
   FORCE=1 KEY_LIMIT=1000000 bash tasks/indexing/run_query_drift.sh

3) Query outputs:
   tasks/indexing/queries/**.bin

Generate updated data + queries (one-pass)
1) Data drift:
   bash tasks/indexing/run_data_drift.sh

2) Query drift:
   bash tasks/indexing/run_query_drift.sh

Override inputs via env vars:
  DATA_FILE, QUERY_FILE, COUNT, SEARCH_COUNT, HAS_SIZE, QUERY_HAS_SIZE
  RUN_ALEX, RUN_BPLUS, RUN_FITING, RUN_PGM
