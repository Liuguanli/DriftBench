# Trace input mocks

These files are small, mocked trace summaries used to generate DriftSpec YAMLs.

## CSV format (data drift)
- `record_type=meta` supplies shared metadata (trace_type, pattern_id, base_table, data_source_kind/path).
- `record_type=drift` rows become entries under `variables.drifts`.

## JSON format (workload drift)
The JSON file mirrors a summary view of the trace with keys like:
- `trace_type`, `pattern_id`, `seed`, `data_source`, `base_table`.
- `template_defaults`, `template_runs`, `query_runs`.

## RedBench stats.csv
The generator recognizes the RedBench `stats.csv` format (e.g. `workload_type`, `number_of_queries`).
It produces a workload DriftSpec with `variables.trace_stats` populated; add data source + template/query
sections if you want to run it end-to-end.

To generate a runnable workload spec without changing DriftSpec logic, use the mapping file:

```bash
python -m driftbench.cli trace-to-spec driftspec/trace_inputs/redbench_stats_0_10.csv \\
  driftspec/generated/redbench_stats_0_10.yaml \\
  --mapping driftspec/trace_inputs/redbench_mapping.json
```

The mapping selects only the needed columns (e.g. `workload_type`, `number_of_queries`,
`n_distinct_readsets`, `n_distinct_num_joins`) and fills the rest from defaults.
