import pandas as pd
import numpy as np
from typing import Dict, Any, List, Optional, Iterable


def _filter_mask(df: pd.DataFrame, flt: Optional[Dict[str, Any]]) -> pd.Series:
    if not flt:
        return pd.Series([True] * len(df), index=df.index)
    col = flt.get("column")
    if not col:
        raise ValueError("filter requires 'column'.")
    if col not in df.columns:
        raise ValueError(f"filter column '{col}' not in dataframe.")
    mask = pd.Series([True] * len(df), index=df.index)
    if "equals" in flt:
        mask &= df[col] == flt["equals"]
    if "in" in flt:
        mask &= df[col].isin(flt["in"])
    if "min" in flt:
        mask &= df[col] >= flt["min"]
    if "max" in flt:
        mask &= df[col] <= flt["max"]
    if "contains" in flt:
        mask &= df[col].astype(str).str.contains(str(flt["contains"]), regex=False)
    return mask


class MultiTableDriftGenerator:
    def __init__(
        self,
        tables: Dict[str, pd.DataFrame],
        relationships: List[Dict[str, Any]],
        table_keys: Optional[Dict[str, str]] = None,
        seed: int = 42,
    ) -> None:
        self.tables = tables
        self.table_keys = table_keys or {}
        self.seed = seed
        self.rng = np.random.default_rng(seed)
        self.relationships = {}
        self.scale_meta: Dict[str, Dict[str, Any]] = {}
        for rel in relationships or []:
            name = rel.get("name")
            if not name:
                raise ValueError("Each relationship needs 'name'.")
            for k in ("fact", "fk", "dim", "pk"):
                if k not in rel:
                    raise ValueError(f"Relationship '{name}' missing '{k}'.")
            self.relationships[name] = rel

    def apply_steps(self, steps: List[Dict[str, Any]]) -> None:
        for step in steps or []:
            if step.get("enabled") is False:
                continue
            op = step.get("op")
            if not op:
                raise ValueError("Each drift_step requires 'op'.")
            if op == "delete_keys":
                self._step_delete_keys(step)
            elif op == "reassign_fk":
                self._step_reassign_fk(step)
            elif op == "scale_tables":
                self._step_scale_tables(step)
            elif op == "scale_sample":
                self._step_scale_sample(step)
            elif op == "add_dimension_keys":
                self._step_add_dimension_keys(step)
            elif op == "skew_fk":
                self._step_skew_fk(step)
            elif op == "skew_column":
                self._step_skew_column(step)
            elif op == "insert_outliers":
                self._step_insert_outliers(step)
            elif op == "rewrite_columns":
                self._step_rewrite_columns(step)
            else:
                raise ValueError(f"Unsupported drift_step op: {op}")

    def validate_integrity(self) -> None:
        for rel in self.relationships.values():
            fact = rel["fact"]
            fk = rel["fk"]
            dim = rel["dim"]
            pk = rel["pk"]
            fact_df = self._get_table(fact)
            dim_df = self._get_table(dim)
            if fk not in fact_df.columns:
                raise ValueError(f"FK column '{fk}' missing from fact table '{fact}'.")
            if pk not in dim_df.columns:
                raise ValueError(f"PK column '{pk}' missing from dim table '{dim}'.")
            dim_keys = set(dim_df[pk].dropna().unique())
            missing = set(fact_df[fk].dropna().unique()) - dim_keys
            if missing:
                raise ValueError(
                    f"Referential integrity broken for {fact}.{fk} -> {dim}.{pk} "
                    f"(missing {len(missing)} keys)."
                )

    def _step_delete_keys(self, step: Dict[str, Any]) -> None:
        target = step.get("target")
        if not target:
            raise ValueError("delete_keys requires 'target'.")
        df = self._get_table(target)
        key_col = step.get("key_column") or self.table_keys.get(target)
        if not key_col:
            raise ValueError(f"delete_keys on '{target}' requires key_column or table_keys entry.")
        if key_col not in df.columns:
            raise ValueError(f"Key column '{key_col}' not found in '{target}'.")

        mask = _filter_mask(df, step.get("filter"))
        candidates = df.loc[mask, key_col].dropna().unique()
        if len(candidates) == 0:
            return

        count = step.get("count")
        if count is None and "fraction" in step:
            count = int(round(len(candidates) * float(step["fraction"])))
        if count is None:
            raise ValueError("delete_keys needs 'count' or 'fraction'.")
        count = max(0, min(int(count), len(candidates)))
        delete_keys = set(self.rng.choice(candidates, size=count, replace=False).tolist())

        self.tables[target] = df[~df[key_col].isin(delete_keys)].reset_index(drop=True)

        propagate = step.get("propagate")
        if propagate:
            for p in propagate if isinstance(propagate, list) else [propagate]:
                rel_name = p.get("relationship")
                policy = p.get("policy", "drop")
                self._propagate_deleted_keys(rel_name, delete_keys, policy)

    def _step_reassign_fk(self, step: Dict[str, Any]) -> None:
        rel_name = step.get("relationship")
        if not rel_name:
            raise ValueError("reassign_fk requires 'relationship'.")
        rel = self._get_relationship(rel_name)
        fact = rel["fact"]
        fk = rel["fk"]
        dim = rel["dim"]
        pk = rel["pk"]

        fact_df = self._get_table(fact)
        dim_df = self._get_table(dim)
        if fk not in fact_df.columns:
            raise ValueError(f"FK column '{fk}' missing from fact table '{fact}'.")
        if pk not in dim_df.columns:
            raise ValueError(f"PK column '{pk}' missing from dim table '{dim}'.")

        fact_mask = _filter_mask(fact_df, step.get("fact_filter"))
        fact_indices = fact_df.loc[fact_mask].index.to_numpy()
        if len(fact_indices) == 0:
            return

        count = step.get("count")
        if count is None and "fraction" in step:
            count = int(round(len(fact_indices) * float(step["fraction"])))
        if count is None:
            raise ValueError("reassign_fk needs 'count' or 'fraction'.")
        count = max(0, min(int(count), len(fact_indices)))
        pick_idx = self.rng.choice(fact_indices, size=count, replace=False)

        keys = step.get("dim_keys")
        if keys is None:
            dim_mask = _filter_mask(dim_df, step.get("dim_filter"))
            keys = dim_df.loc[dim_mask, pk].dropna().unique()
        else:
            keys = np.array(keys)
        if len(keys) == 0:
            raise ValueError(f"No dimension keys available for relationship '{rel_name}'.")

        new_keys = self.rng.choice(keys, size=len(pick_idx), replace=True)
        fact_df.loc[pick_idx, fk] = new_keys
        self.tables[fact] = fact_df.reset_index(drop=True)

    def _propagate_deleted_keys(self, rel_name: str, deleted_keys: set, policy: str) -> None:
        rel = self._get_relationship(rel_name)
        fact = rel["fact"]
        fk = rel["fk"]
        dim = rel["dim"]
        pk = rel["pk"]
        fact_df = self._get_table(fact)
        dim_df = self._get_table(dim)

        if policy == "drop":
            self.tables[fact] = fact_df[~fact_df[fk].isin(deleted_keys)].reset_index(drop=True)
            return
        if policy == "reassign":
            remaining = dim_df[pk].dropna().unique()
            if len(remaining) == 0:
                raise ValueError(f"No remaining keys to reassign in '{dim}'.")
            mask = fact_df[fk].isin(deleted_keys)
            reassign_idx = fact_df.index[mask].to_numpy()
            if len(reassign_idx) == 0:
                return
            new_keys = self.rng.choice(remaining, size=len(reassign_idx), replace=True)
            fact_df.loc[reassign_idx, fk] = new_keys
            self.tables[fact] = fact_df.reset_index(drop=True)
            return
        raise ValueError(f"Unsupported propagate policy: {policy}")

    def _get_table(self, name: str) -> pd.DataFrame:
        if name not in self.tables:
            raise ValueError(f"Table '{name}' not loaded.")
        return self.tables[name]

    def _get_relationship(self, name: str) -> Dict[str, Any]:
        if name not in self.relationships:
            raise ValueError(f"Relationship '{name}' not defined.")
        return self.relationships[name]

    def _step_scale_tables(self, step: Dict[str, Any]) -> None:
        factor = int(step.get("factor", 1))
        if factor <= 1:
            return
        tables = step.get("tables") or list(self.tables.keys())
        key_columns = step.get("key_columns") or {}
        key_offsets = step.get("key_offsets") or {}

        fk_map: Dict[str, List[Dict[str, str]]] = {}
        dim_tables = set()
        for rel in self.relationships.values():
            fact = rel["fact"]
            if fact in tables:
                fk_map.setdefault(fact, []).append({"fk": rel["fk"], "dim": rel["dim"]})
            dim_tables.add(rel["dim"])

        offsets: Dict[str, int] = {}
        for name in tables:
            key_col = key_columns.get(name) or self.table_keys.get(name)
            if not key_col:
                if name in dim_tables:
                    raise ValueError(f"scale_tables requires key_column for dim table '{name}'.")
                continue
            df = self._get_table(name)
            if key_col not in df.columns:
                raise ValueError(f"Key column '{key_col}' missing from '{name}'.")
            if not np.issubdtype(df[key_col].dropna().dtype, np.number):
                raise ValueError(f"Key column '{key_col}' in '{name}' must be numeric for scale_tables.")
            offsets[name] = int(key_offsets.get(name, int(df[key_col].max()) + 1))
            self.scale_meta[name] = {"key_column": key_col, "original_max": int(df[key_col].max())}

        for rel in self.relationships.values():
            if rel["fact"] in tables and rel["dim"] not in offsets:
                raise ValueError(f"scale_tables requires dim table '{rel['dim']}' to be included.")

        base_tables = {name: self._get_table(name) for name in tables}
        replicas: Dict[str, List[pd.DataFrame]] = {name: [] for name in tables}
        for r in range(1, factor):
            for name in tables:
                df = base_tables[name].copy()
                key_col = key_columns.get(name) or self.table_keys.get(name)
                if key_col and key_col in df.columns and name in offsets:
                    is_fk_key = any(fk["fk"] == key_col for fk in fk_map.get(name, []))
                    if not is_fk_key:
                        df[key_col] = df[key_col] + r * offsets[name]
                for fk in fk_map.get(name, []):
                    fk_col = fk["fk"]
                    dim_table = fk["dim"]
                    if fk_col in df.columns:
                        df[fk_col] = df[fk_col] + r * offsets[dim_table]
                replicas[name].append(df)

        for name in tables:
            combined = [base_tables[name]] + replicas[name]
            self.tables[name] = pd.concat(combined, ignore_index=True)

    def _step_scale_sample(self, step: Dict[str, Any]) -> None:
        factor = float(step.get("factor", 1.0))
        if factor <= 1.0:
            return
        tables = step.get("tables") or list(self.tables.keys())
        key_columns = step.get("key_columns") or {}
        include_base = bool(step.get("include_base", True))
        ensure_all_keys = bool(step.get("ensure_all_keys", True))
        fk_only_new = bool(step.get("fk_only_new", True))

        rels = [
            r for r in self.relationships.values()
            if r["fact"] in tables and r["dim"] in tables
        ]
        deps = {t: set() for t in tables}
        for rel in rels:
            deps[rel["fact"]].add(rel["dim"])
        order: list[str] = []
        remaining = set(tables)
        while remaining:
            ready = sorted([t for t in remaining if deps[t].issubset(set(order))])
            if not ready:
                raise ValueError("scale_sample detected cyclic table dependencies.")
            order.extend(ready)
            remaining -= set(ready)

        dim_tables = {r["dim"] for r in rels}
        dim_maps: Dict[str, Dict[Any, list]] = {}

        for name in order:
            df = self._get_table(name)
            target_rows = int(np.ceil(len(df) * factor))
            if include_base:
                extra_n = max(0, target_rows - len(df))
                base = df.copy()
                base["_is_extra"] = False
                if extra_n > 0:
                    extra = df.sample(n=extra_n, replace=True, random_state=self.seed).copy()
                    extra["_is_extra"] = True
                    combined = pd.concat([base, extra], ignore_index=True)
                else:
                    combined = base
            else:
                sample_n = max(0, target_rows)
                combined = df.sample(n=sample_n, replace=True, random_state=self.seed).copy()
                combined["_is_extra"] = True

            key_col = key_columns.get(name) or self.table_keys.get(name)
            fk_cols = [r["fk"] for r in rels if r["fact"] == name]
            if key_col and key_col in combined.columns and key_col not in fk_cols:
                mapping: Dict[Any, list] = {}
                if include_base and len(df) > 0:
                    for old_key in df[key_col].values:
                        mapping.setdefault(old_key, []).append(old_key)
                    extra_mask = combined["_is_extra"]
                    extra_old = combined.loc[extra_mask, key_col].values
                    start = int(df[key_col].max()) + 1
                    new_keys = np.arange(start, start + len(extra_old))
                    combined.loc[extra_mask, key_col] = new_keys
                    for old, new in zip(extra_old, new_keys):
                        mapping.setdefault(old, []).append(new)
                else:
                    extra_old = combined[key_col].values
                    new_keys = np.arange(1, 1 + len(extra_old))
                    combined[key_col] = new_keys
                    for old, new in zip(extra_old, new_keys):
                        mapping.setdefault(old, []).append(new)

                if name in dim_tables:
                    dim_maps[name] = mapping
                if len(df) > 0:
                    self.scale_meta[name] = {"key_column": key_col, "original_max": int(df[key_col].max())}

            for rel in [r for r in rels if r["fact"] == name]:
                mapping = dim_maps.get(rel["dim"])
                if not mapping:
                    continue
                fk_col = rel["fk"]
                if fk_col not in combined.columns:
                    continue
                if fk_only_new and include_base:
                    idx = combined.index[combined["_is_extra"]]
                else:
                    idx = combined.index
                if len(idx) == 0:
                    continue
                all_keys = [k for keys in mapping.values() for k in keys]
                old_vals = combined.loc[idx, fk_col].values
                new_vals = [
                    self.rng.choice(mapping.get(v, all_keys))
                    for v in old_vals
                ]
                combined.loc[idx, fk_col] = new_vals

            combined = combined.drop(columns=["_is_extra"])
            self.tables[name] = combined

    def _step_add_dimension_keys(self, step: Dict[str, Any]) -> None:
        target = step.get("target")
        if not target:
            raise ValueError("add_dimension_keys requires 'target'.")
        df = self._get_table(target)
        key_col = step.get("key_column") or self.table_keys.get(target)
        if not key_col:
            raise ValueError(f"add_dimension_keys on '{target}' requires key_column.")
        if key_col not in df.columns:
            raise ValueError(f"Key column '{key_col}' not found in '{target}'.")
        if not np.issubdtype(df[key_col].dropna().dtype, np.number):
            raise ValueError(f"Key column '{key_col}' in '{target}' must be numeric.")

        count = step.get("count")
        if count is None and "fraction" in step:
            count = int(round(len(df) * float(step["fraction"])))
        if count is None:
            raise ValueError("add_dimension_keys needs 'count' or 'fraction'.")
        count = max(0, int(count))
        if count == 0:
            return

        sampled = df.sample(n=count, replace=True, random_state=self.seed).copy()
        start = int(df[key_col].max()) + 1
        new_keys = np.arange(start, start + count)
        sampled[key_col] = new_keys
        self.tables[target] = pd.concat([df, sampled], ignore_index=True)

        propagate = step.get("propagate")
        if propagate:
            props = propagate if isinstance(propagate, list) else [propagate]
            for prop in props:
                rel_name = prop.get("relationship")
                if not rel_name:
                    raise ValueError("add_dimension_keys propagate requires 'relationship'.")
                policy = prop.get("policy", "reassign")
                if policy != "reassign":
                    raise ValueError("add_dimension_keys propagate only supports policy: reassign")
                rel = self._get_relationship(rel_name)
                if rel["dim"] != target:
                    raise ValueError(f"Relationship '{rel_name}' does not target dim '{target}'.")
                fact_step = {
                    "relationship": rel_name,
                    "count": prop.get("fact_count"),
                    "fraction": prop.get("fact_fraction"),
                    "dim_keys": new_keys.tolist(),
                }
                self._step_reassign_fk(fact_step)

    def _step_skew_fk(self, step: Dict[str, Any]) -> None:
        rel_name = step.get("relationship")
        if not rel_name:
            raise ValueError("skew_fk requires 'relationship'.")
        rel = self._get_relationship(rel_name)
        fact = rel["fact"]
        fk = rel["fk"]
        dim = rel["dim"]
        pk = rel["pk"]
        fact_df = self._get_table(fact)
        dim_df = self._get_table(dim)

        fact_mask = _filter_mask(fact_df, step.get("fact_filter"))
        fact_indices = fact_df.loc[fact_mask].index.to_numpy()
        if len(fact_indices) == 0:
            return

        count = step.get("count")
        if count is None and "fraction" in step:
            count = int(round(len(fact_indices) * float(step["fraction"])))
        if count is None:
            raise ValueError("skew_fk needs 'count' or 'fraction'.")
        count = max(0, min(int(count), len(fact_indices)))
        pick_idx = self.rng.choice(fact_indices, size=count, replace=False)

        keys = step.get("keys")
        if keys is None:
            dim_mask = _filter_mask(dim_df, step.get("dim_filter"))
            keys = dim_df.loc[dim_mask, pk].dropna().unique()
        else:
            keys = np.array(keys)
        if len(keys) == 0:
            raise ValueError(f"No dimension keys available for relationship '{rel_name}'.")

        top_k = int(step.get("top_k", min(5, len(keys))))
        if top_k < len(keys):
            keys = self.rng.choice(keys, size=top_k, replace=False)
        keys = np.array(keys)

        skewness = float(step.get("skewness", 2.0))
        probs = np.ones(len(keys), dtype=np.float64)
        if len(keys) > 0 and skewness > 0:
            probs[0] *= pow(2.0, skewness)
        probs /= probs.sum()

        fact_df.loc[pick_idx, fk] = self.rng.choice(keys, size=len(pick_idx), replace=True, p=probs)
        self.tables[fact] = fact_df.reset_index(drop=True)

    def _step_skew_column(self, step: Dict[str, Any]) -> None:
        target = step.get("target")
        column = step.get("column")
        if not target or not column:
            raise ValueError("skew_column requires 'target' and 'column'.")
        df = self._get_table(target)
        if column not in df.columns:
            raise ValueError(f"Column '{column}' not found in '{target}'.")
        mask = _filter_mask(df, step.get("filter"))
        indices = df.loc[mask].index.to_numpy()
        if len(indices) == 0:
            return

        count = step.get("count")
        if count is None and "fraction" in step:
            count = int(round(len(indices) * float(step["fraction"])))
        if count is None:
            raise ValueError("skew_column needs 'count' or 'fraction'.")
        count = max(0, min(int(count), len(indices)))
        pick_idx = self.rng.choice(indices, size=count, replace=False)

        series = df[column].dropna()
        if series.empty:
            return
        skewness = float(step.get("skewness", 2.0))
        top_k = int(step.get("top_k", min(10, series.nunique())))

        if np.issubdtype(series.dtype, np.number):
            uniq = np.sort(series.unique())
            if len(uniq) == 0:
                return
            top_vals = uniq[-top_k:] if top_k <= len(uniq) else uniq
            probs = np.ones(len(top_vals), dtype=np.float64)
            probs[-1] *= pow(2.0, skewness)
            probs /= probs.sum()
            df.loc[pick_idx, column] = self.rng.choice(top_vals, size=len(pick_idx), replace=True, p=probs)
        else:
            value_counts = series.value_counts()
            top_vals = value_counts.head(top_k)
            values = top_vals.index.to_numpy()
            probs = top_vals.values.astype(np.float64)
            probs[0] *= pow(2.0, skewness)
            probs /= probs.sum()
            df.loc[pick_idx, column] = self.rng.choice(values, size=len(pick_idx), replace=True, p=probs)

        self.tables[target] = df.reset_index(drop=True)

    def _step_insert_outliers(self, step: Dict[str, Any]) -> None:
        target = step.get("target")
        column = step.get("column")
        if not target or not column:
            raise ValueError("insert_outliers requires 'target' and 'column'.")
        df = self._get_table(target)
        if column not in df.columns:
            raise ValueError(f"Column '{column}' not found in '{target}'.")

        count = step.get("count")
        if count is None and "fraction" in step:
            count = int(round(len(df) * float(step["fraction"])))
        if count is None:
            raise ValueError("insert_outliers needs 'count' or 'fraction'.")
        count = max(0, int(count))
        if count == 0:
            return

        sampled = df.sample(n=count, replace=True, random_state=self.seed).copy()
        series = df[column].dropna()
        if series.empty:
            return
        if not np.issubdtype(series.dtype, np.number):
            raise ValueError(f"insert_outliers supports numeric column only: {column}")

        min_extreme = step.get("min_extreme")
        max_extreme = step.get("max_extreme")
        extreme_value = step.get("extreme_value")
        extreme_scale = float(step.get("extreme_scale", 1.0))

        if min_extreme is not None or max_extreme is not None:
            if min_extreme is None or max_extreme is None:
                raise ValueError("Both min_extreme and max_extreme must be set when using a range.")
            outlier_values = self.rng.uniform(float(min_extreme), float(max_extreme), size=count)
        else:
            if extreme_value is None or (isinstance(extreme_value, str) and extreme_value.lower() == "auto"):
                col_min = series.min()
                col_max = series.max()
                span = col_max - col_min
                if span == 0:
                    span = max(abs(col_min), 1.0)
                low = col_min - span * extreme_scale
                high = col_max + span * extreme_scale
                outlier_values = self.rng.choice([low, high], size=count)
            else:
                outlier_values = np.full(count, float(extreme_value))

        if np.issubdtype(series.dtype, np.integer):
            outlier_values = np.round(outlier_values).astype(int)
        sampled[column] = outlier_values

        key_col = step.get("key_column")
        if key_col and key_col in df.columns and np.issubdtype(df[key_col].dropna().dtype, np.number):
            start = int(df[key_col].max()) + 1
            sampled[key_col] = np.arange(start, start + count)

        self.tables[target] = pd.concat([df, sampled], ignore_index=True)

    def _step_rewrite_columns(self, step: Dict[str, Any]) -> None:
        target = step.get("target")
        if not target:
            raise ValueError("rewrite_columns requires 'target'.")
        df = self._get_table(target)
        columns = step.get("columns") or {}
        if not columns:
            return

        mask = _filter_mask(df, step.get("filter"))
        if step.get("only_new"):
            meta = self.scale_meta.get(target)
            if meta and meta.get("key_column") in df.columns:
                key_col = meta["key_column"]
                mask &= df[key_col] > meta["original_max"]

        idx = df.index[mask]
        if len(idx) == 0:
            return

        for col, cfg in columns.items():
            if col not in df.columns:
                raise ValueError(f"Column '{col}' not found in '{target}'.")
            ctype = (cfg.get("type") if isinstance(cfg, dict) else None) or "template"
            if ctype == "template":
                template = cfg.get("template")
                if not template:
                    raise ValueError("rewrite_columns template requires 'template'.")
                df.loc[idx, col] = df.loc[idx].apply(lambda r: template.format(**r.to_dict()), axis=1)
            elif ctype == "numeric_jitter":
                series = df[col].dropna()
                if series.empty:
                    continue
                if not np.issubdtype(series.dtype, np.number):
                    raise ValueError(f"numeric_jitter requires numeric column '{col}'.")
                scale = float(cfg.get("scale", 0.1))
                std = float(series.std() or 0.0)
                if std == 0:
                    continue
                noise = self.rng.normal(0.0, std * scale, size=len(idx))
                df.loc[idx, col] = df.loc[idx, col].astype(float) + noise
                if "min" in cfg:
                    df.loc[idx, col] = df.loc[idx, col].clip(lower=float(cfg["min"]))
                if "max" in cfg:
                    df.loc[idx, col] = df.loc[idx, col].clip(upper=float(cfg["max"]))
            elif ctype == "categorical_resample":
                series = df[col].dropna()
                if series.empty:
                    continue
                counts = series.value_counts()
                values = counts.index.to_numpy()
                probs = counts.values.astype(np.float64)
                probs = probs / probs.sum()
                df.loc[idx, col] = self.rng.choice(values, size=len(idx), replace=True, p=probs)
            else:
                raise ValueError(f"Unsupported rewrite_columns type: {ctype}")

        self.tables[target] = df.reset_index(drop=True)
