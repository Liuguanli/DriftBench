# Index Testing Example: DriftBench for Learned Indexes


**The Case for Learned Index Structures** (Kraska et al., SIGMOD 2018) introduced the learned index idea by modeling the CDF and predicting record positions. We use this paper as a running example to show how DriftBench can extend evaluation beyond the original setup by systematically injecting data and workload drift.

Scope and assumptions:
- Dataset file name: `base.csv`
- Single numeric key column: `key`
- Read-only RMI setting (static index, no online inserts/deletes)
- RMI supports point and range queries only
- Learned index predicts `pos = F(key) * N`


## Selected drifts and rationale
- From the index design perspective, a 1D learned index is most sensitive to scale, skew, and outliers because they reshape the CDF and error bounds.
- From supported query types, the index targets point and range queries only, so workload drift varies key distributions and range selectivity.

---

## A) Data Drift Specs

### A1. Dataset scale (cardinality drift)

```yaml
pattern_id: rmi-data-scale
seed: 42

type:
  family: data
  category: cardinality
  subtype: scaling

data_source:
  kind: csv
  path: ./data/base.csv

variables:
  key_column: key
  drifts:
    - name: scale_2
      op: scale_cardinality
      scale: 2.0
      output_path: ./output/data/scale/base_scale_2.csv

    - name: scale_4
      op: scale_cardinality
      scale: 4.0
      output_path: ./output/data/scale/base_scale_4.csv

    - name: scale_8
      op: scale_cardinality
      scale: 8.0
      output_path: ./output/data/scale/base_scale_8.csv
```

What it stresses (RMI):
- Error amplification via `F(key) * N`
- Model generalization under larger/smaller CDF support

---

### A2. Skew robustness (distributional drift)

```yaml
pattern_id: rmi-data-skew
seed: 42

type:
  family: data
  category: distribution
  subtype: column_shift

data_source:
  kind: csv
  path: ./data/base.csv

variables:
  key_column: key
  params:
    numeric:
      bounds: [min_key, max_key]
      preserve_mean: true
  drifts:
    - name: skew_mild
      mode: skew_right
      skew_intensity: 0.3
      output_path: ./output/data/skew/base_skew_mild.csv

    - name: skew_medium
      mode: skew_right
      skew_intensity: 0.6
      output_path: ./output/data/skew/base_skew_medium.csv

    - name: skew_severe
      mode: skew_right
      skew_intensity: 0.9
      output_path: ./output/data/skew/base_skew_severe.csv
```

What it stresses:
- Nonlinear CDF regions
- Last-stage model load imbalance
- Guard-band size inflation

---

### A3. Poisoned points (outlier injection)

```yaml
pattern_id: rmi-data-poison
seed: 42

type:
  family: data
  category: distribution
  subtype: outlier_injection

data_source:
  kind: csv
  path: ./data/base.csv

variables:
  key_column: key
  params:
    numeric:
      bounds: [min_key, max_key]
  drifts:
    - name: poison_0_1pct
      op: inject_outliers
      outlier_rate: 0.001
      outlier_position: boundary
      output_path: ./output/data/poison/base_poison_0_1pct.csv

    - name: poison_0_5pct
      op: inject_outliers
      outlier_rate: 0.005
      outlier_position: boundary
      output_path: ./output/data/poison/base_poison_0_5pct.csv

    - name: poison_1pct
      op: inject_outliers
      outlier_rate: 0.01
      outlier_position: boundary
      output_path: ./output/data/poison/base_poison_1pct.csv
```

What it stresses:
- Worst-case prediction error
- Robustness of error bounds
- Sensitivity to adversarial tails

---

## B) Workload Drift Specs

### B1. Point-query key distribution drift

```yaml
pattern_id: rmi-workload-point
seed: 42

type:
  family: workload
  category: templates
  subtype: point_1d

data_source:
  kind: csv
  path: ./data/base.csv

variables:
  key_column: key
  templates:
    - name: point_lookup
      sql: "SELECT * FROM T WHERE key = ${key};"

  runs:
    - name: point_uniform
      queries: 1000
      dist:
        key: { distribution: uniform, min: min_key, max: max_key }
      output_path: ./output/workload/base_point_uniform.sql

    - name: point_normal
      queries: 1000
      dist:
        key: { distribution: normal, mean: mid_key, std: sigma }
      output_path: ./output/workload/base_point_normal.sql

    - name: point_zipf
      queries: 1000
      dist:
        key: { distribution: zipf, a: 2.0, min: min_key, max: max_key }
      output_path: ./output/workload/base_point_zipf.sql
```

What it stresses (RMI):
- Hot last-stage models
- Cache-locality vs model imbalance
- Tail latency

---

### B2. Range-query selectivity drift

```yaml
pattern_id: rmi-workload-range
seed: 42

type:
  family: workload
  category: templates
  subtype: range_1d

data_source:
  kind: csv
  path: ./data/base.csv

variables:
  key_column: key
  templates:
    - name: range_scan
      sql: "SELECT * FROM T WHERE key BETWEEN ${k_l} AND ${k_u};"

  runs:
    - name: range_sel_1pct
      queries: 1000
      target_selectivity: 0.01
      center_dist:
        key: { distribution: uniform, min: min_key, max: max_key }
      output_path: ./output/workload/base_range_sel_1pct.sql

    - name: range_sel_5pct
      queries: 1000
      target_selectivity: 0.05
      center_dist:
        key: { distribution: uniform, min: min_key, max: max_key }
      output_path: ./output/workload/base_range_sel_5pct.sql

    - name: range_sel_20pct
      queries: 1000
      target_selectivity: 0.20
      center_dist:
        key: { distribution: uniform, min: min_key, max: max_key }
      output_path: ./output/workload/base_range_sel_20pct.sql
```

What it stresses (RMI):
- Error-bound sensitivity
- Search-window expansion
- Boundary effects

---

## Run the generators
Save the data drift YAML and workload drift YAML into files (for example,
`driftspec/examples/base_data.yaml` and `driftspec/examples/base_workload.yaml`),
then run:

```bash
python -m driftbench.cli run-yaml driftspec/examples/base_data.yaml
python -m driftbench.cli run-yaml driftspec/examples/base_workload.yaml
```

## Summary
This guide provides a name-agnostic DriftSpec suite for evaluating 1D learned indexes under scale, skew, outlier, key-distribution, and range-selectivity drift, along with the minimal commands to generate data and workloads.
