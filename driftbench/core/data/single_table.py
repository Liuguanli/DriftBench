import pandas as pd
import numpy as np
from typing import List, Optional
from scipy.stats import gaussian_kde
from scipy.stats import skewnorm
from driftbench.core.data.distribution_simulator import DataDistributionSimulator
from driftbench.core.data.sampler import Sampler
from driftbench.core.temporal.time_stamp_generator import generate_timestamps
import os


class SingleTableDriftGenerator:
    def __init__(self, csv_path, schema, base_table, seed=42):
        self.csv_path = csv_path
        # self.schema = schema

        self.base_table = base_table
        # self.source = schema["source"]
        self.table = schema["tables"][base_table]
        self.columns = self.table["columns"]

        self.col_names = list(self.columns.keys())


        self.seed = seed
        self.df = pd.read_csv(csv_path)
        np.random.seed(seed)

    def apply_drift(self, drift_type="outlier_injection", **kwargs):
        if drift_type == "outlier_injection":
            # return self._inject_outliers(**kwargs)
            return self.inject_outliers_from_csv(**kwargs)
        elif drift_type == "value_skew":
            return self._inject_skew(**kwargs)
        elif drift_type == "vary_cardinality":
            return self._vary_cardinality(**kwargs)
        elif drift_type == "selective_deletion":
            return self._delete_records(**kwargs)
        elif drift_type == "insert_records":
            return self._insert_records(**kwargs)
        elif drift_type == "add_timestamp":
            return self._add_timestamp(**kwargs)
        elif drift_type == "concat_csvs":
            return self._concat_csvs(**kwargs)
        else:
            raise ValueError(f"Unsupported drift type: {drift_type}")

    def _inject_outliers(self, column, n=10, extreme_value=1e6):
        drifted_df = self.df.copy()
        outliers = drifted_df.sample(n=n).copy()
        outliers[column] = extreme_value
        return pd.concat([drifted_df, outliers], ignore_index=True)

    def _add_timestamp(
        self,
        timestamp_column: str = "timestamp",
        start_time: str = "2025-07-01T00:00:00",
        pattern: str = "uniform",
        queries_per_minute: int = 60,
        source_path: Optional[str] = None,
    ) -> pd.DataFrame:
        drifted_df = pd.read_csv(source_path) if source_path else self.df.copy()
        timestamps = generate_timestamps(
            count=len(drifted_df),
            start_time=start_time,
            pattern=pattern,
            queries_per_minute=int(queries_per_minute),
        )
        drifted_df[timestamp_column] = timestamps
        return drifted_df

    def _concat_csvs(
        self,
        input_paths: List[str],
        sort_by: Optional[str] = None,
    ) -> pd.DataFrame:
        if not input_paths:
            raise ValueError("concat_csvs requires input_paths.")
        frames = [pd.read_csv(path) for path in input_paths]
        combined = pd.concat(frames, ignore_index=True)
        if sort_by and sort_by in combined.columns:
            combined = combined.sort_values(sort_by).reset_index(drop=True)
        return combined
    
    def inject_outliers_from_csv(
        self,
        outlier_csv_path: str,
        inject_count: Optional[int] = None,
        target_columns: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        outlier_df = pd.read_csv(outlier_csv_path)

        if inject_count is not None and inject_count < len(outlier_df):
            outlier_df = outlier_df.sample(n=inject_count, random_state=self.seed)

        if target_columns is not None:
            outlier_df = outlier_df[target_columns]

        # Align with self.df (fill missing columns if needed)
        full_outlier_df = pd.DataFrame(columns=self.df.columns)
        for col in self.df.columns:
            if col in outlier_df.columns:
                full_outlier_df[col] = outlier_df[col]
            else:
                full_outlier_df[col] = pd.NA

        # Concatenate to copy of self.df
        return pd.concat([self.df.copy(), full_outlier_df], ignore_index=True)


    def _inject_skew(self, columns, portion=1.0, skewness=2):
        drifted_df = self.df.copy()
        n = int(len(drifted_df) * portion)

        indices = drifted_df.sample(n=n, random_state=self.seed).index

        for column in columns:
            if pd.api.types.is_numeric_dtype(drifted_df[column]):
                original_data = drifted_df[column].dropna()
                mean = original_data.mean()
                std = original_data.std()

                skewed_values = skewnorm.rvs(a=skewness, loc=mean, scale=std, size=n, random_state=self.seed)
                if pd.api.types.is_integer_dtype(drifted_df[column]):
                    skewed_values = skewed_values.astype(int)
            else:
                value_counts = drifted_df[column].value_counts()
                if len(value_counts) == 0:
                    continue  # skip this column
                value_counts = drifted_df[column].value_counts(normalize=True)
                top_k = 10 if len(value_counts) > 10 else len(value_counts)
                top_values = value_counts.head(top_k)
                top_categories = top_values.index.tolist()
                top_probs = top_values.values

                skewed_probs = np.array(top_probs, dtype=np.float64)
                skewed_probs[0] *= pow(2, skewness)
                skewed_probs /= skewed_probs.sum()

                skewed_values = np.random.choice(top_categories, size=n, p=skewed_probs)

            drifted_df.loc[indices, column] = skewed_values

        return drifted_df


    def _vary_cardinality(self, scale: float = 0.1):

        assert scale > 0, f"scale should be larger than 0, current scale {scale} is not correct!"
        current_rows = len(self.df)
        target_rows = int(current_rows * scale)
        return self._generate_rows_like_existing(target_rows)

    # MAX_KDE_SAMPLES = 1000  # adjust if needed


    def _generate_rows_like_existing(self, n):

        MAX_KDE_SAMPLES = 50000
        
        simulator = DataDistributionSimulator(self.df, self.columns)
        new_data = {}

        for col in self.col_names:

            col_data = self.df[col].dropna()

            if len(col_data) == 0:
                new_data[col] = [None] * n
                continue

            logical_type = self.columns[col]["logical_type"]
            
            if logical_type == "numeric":
                if len(col_data) > MAX_KDE_SAMPLES:
                    sample_data = pd.Series(
                        np.random.choice(col_data, size=MAX_KDE_SAMPLES, replace=False),
                        index=None
                    )
                else:
                    sample_data = col_data
                samples = simulator.generate(sample_data, n, strategy_name="kde" if logical_type == "numeric" else "default")
                new_data[col] = samples
            elif logical_type == "string" or logical_type == "categorical":
                col_data = self.df[col].dropna().astype(str)
                new_data[col] = np.random.choice(col_data, n, replace=True)
            elif logical_type == "datetime":
                col_data = pd.to_datetime(self.df[col].dropna())
                new_data[col] = np.random.choice(col_data, n, replace=True)
            else:
                new_data[col] = [None] * n  # fallback for unknown types
        return pd.DataFrame(new_data)

    # def _generate_rows_like_existing(self, n):
    #     new_data = {}
    #     for col in self.col_names:

    #         col_data = self.df[col].dropna()

    #         if len(col_data) == 0:
    #             new_data[col] = [None] * n
    #             continue

    #         logical_type = self.columns[col]["logical_type"]

    #         if logical_type == "numeric":

    #             if len(col_data) > MAX_KDE_SAMPLES:
    #                 sample_data = np.random.choice(col_data, size=MAX_KDE_SAMPLES, replace=False)
    #             else:
    #                 sample_data = col_data

    #             kde = gaussian_kde(sample_data)
    #             samples = kde.resample(n).flatten()
    #             samples = np.clip(samples, col_data.min(), col_data.max())

    #             if pd.api.types.is_integer_dtype(col_data):
    #                 samples = np.round(samples).astype(int)
    #             else:
    #                 samples = samples.astype(float)

    #             new_data[col] = samples
    #         elif logical_type == "string" or logical_type == "categorical":
    #             col_data = self.df[col].dropna().astype(str)
    #             new_data[col] = np.random.choice(col_data, n, replace=True)
    #         elif logical_type == "datetime":
    #             col_data = pd.to_datetime(self.df[col].dropna())
    #             new_data[col] = np.random.choice(col_data, n, replace=True)
    #         else:
    #             new_data[col] = [None] * n  # fallback for unknown types
    #     return pd.DataFrame(new_data)
    

    def _insert_records(self, n=10, filter_column=None, filter_func=None):
        """
        Insert `n` new rows generated from existing data distribution.

        - Can filter source data before generation using filter_column and filter_func.
        - Uses _generate_rows_like_existing for generation.

        Args:
            n (int): Number of rows to insert.
            filter_column (str): Column to apply filter on (optional).
            filter_func (function): A function that filters Series to boolean mask (optional).

        Returns:
            pd.DataFrame: Updated dataframe after insertion.
        """
        source_df = self.df

        if filter_column and filter_func:
            if filter_column not in self.col_names:
                raise ValueError(f"Column {filter_column} not found.")
            source_df = self.df[filter_func(self.df[filter_column])]
            if len(source_df) == 0:
                raise ValueError("No rows match the filter condition.")

        # Now generate n rows like filtered data
        new_rows = self._generate_rows_like_existing(source_df, n=n)
        self.df = pd.concat([self.df, new_rows], ignore_index=True)
        return self.df


    # def _delete_records(self, n=10, filter_column=None, filter_func=None):
    #     """
    #     Randomly delete `n` rows from the dataframe.
    #     Optionally filter rows using `filter_column` and `filter_func` before sampling.

    #     Args:
    #         n (int): Number of rows to delete.
    #         filter_column (str): Column to apply filter on (optional).
    #         filter_func (function): A function that takes a Series and returns a boolean Series (optional).

    #     Returns:
    #         pd.DataFrame: The deleted rows.
    #     """
    #     candidate_df = self.df

    #     if filter_column and filter_func:
    #         if filter_column not in self.col_names:
    #             raise ValueError(f"Column {filter_column} not in dataframe")
    #         candidate_df = self.df[filter_func(self.df[filter_column])]

    #     if n > len(candidate_df):
    #         raise ValueError(f"Cannot delete {n} rows from filtered dataframe with only {len(candidate_df)} rows")

    #     deleted_df = candidate_df.sample(n=n, random_state=self.seed).copy()
    #     self.df = self.df.drop(deleted_df.index).reset_index(drop=True)
    #     return deleted_df


    def _delete_records(self, n=None, filter=None, n_ratio=None,
                        strategy="uniform", strategy_config=None, **legacy):
        """
        Sample `n` rows from the dataframe for deletion (not actually dropped).
        
        Args:
            n (int): Number of records to sample.
            n_ratio (float): Ratio of eligible records to sample (0, 1].
            filter (dict): Optional eligibility filter (boolean mask only):
                - column: target column name (required when filter is set)
                - func: python callable(series, config) -> mask (python only)
                - func_name: registered filter name (YAML-friendly)
                - config: optional config for func/func_name
                - op: one of >, >=, <, <=, ==, !=, in, not_in
                - value: value for op (list for in/not_in)
                - min / max: inclusive range bounds
            strategy (str): Deletion strategy (uniform, key_weighted, time_weighted).
            strategy_config (dict): Optional sampling config.

        Returns:
            pd.DataFrame: The records selected for deletion.
        """
        candidate_df = self.df

        def _coerce_datetime(series, value):
            if pd.api.types.is_datetime64_any_dtype(series):
                return series, pd.to_datetime(value, errors="coerce")
            if isinstance(value, str):
                series_dt = pd.to_datetime(series, errors="coerce")
                value_dt = pd.to_datetime(value, errors="coerce")
                if series_dt.notna().any() and pd.notna(value_dt):
                    return series_dt, value_dt
            return series, value

        if filter is not None and legacy:
            raise ValueError("Use 'filter' or legacy filter_* args, not both.")

        if filter is None and legacy:
            filter = {
                "column": legacy.get("filter_column"),
                "func": legacy.get("filter_func"),
                "func_name": legacy.get("filter_func_name"),
                "config": legacy.get("filter_func_config"),
                "op": legacy.get("filter_op"),
                "value": legacy.get("filter_value"),
                "min": legacy.get("filter_min"),
                "max": legacy.get("filter_max"),
            }

        if filter:
            column = filter.get("column")
            if not column:
                raise ValueError("filter.column is required when filter is provided.")
            if column not in self.df.columns:
                raise ValueError(f"Column {column} not in dataframe")
            series = self.df[column]

            if filter.get("func"):
                cfg = filter.get("config") or {}
                mask = filter["func"](series, cfg)
                candidate_df = self.df[mask]
            elif filter.get("func_name"):
                from driftbench.core.data.filter_registry import get_filter
                fn = get_filter(filter["func_name"])
                cfg = filter.get("config") or {}
                mask = fn(series, cfg)
                candidate_df = self.df[mask]
            elif filter.get("op") or filter.get("min") is not None or filter.get("max") is not None:
                if filter.get("op"):
                    if filter.get("value") is None:
                        raise ValueError("filter.value is required when filter.op is provided")
                    series, value = _coerce_datetime(series, filter.get("value"))
                    op = filter.get("op")
                    if op == ">":
                        mask = series > value
                    elif op == ">=":
                        mask = series >= value
                    elif op == "<":
                        mask = series < value
                    elif op == "<=":
                        mask = series <= value
                    elif op == "==":
                        mask = series == value
                    elif op == "!=":
                        mask = series != value
                    elif op == "in":
                        if not isinstance(value, list):
                            raise ValueError("filter.value must be a list for 'in'")
                        mask = series.isin(value)
                    elif op == "not_in":
                        if not isinstance(value, list):
                            raise ValueError("filter.value must be a list for 'not_in'")
                        mask = ~series.isin(value)
                    else:
                        raise ValueError(f"Unsupported filter.op: {op}")
                    candidate_df = self.df[mask]
                else:
                    series, min_val = _coerce_datetime(series, filter.get("min"))
                    _, max_val = _coerce_datetime(series, filter.get("max"))
                    if filter.get("min") is not None and filter.get("max") is not None:
                        mask = (series >= min_val) & (series <= max_val)
                    elif filter.get("min") is not None:
                        mask = series >= min_val
                    else:
                        mask = series <= max_val
                    candidate_df = self.df[mask]
            else:
                raise ValueError("filter must include func/func_name, op, or min/max.")

        if n_ratio is not None:
            if n is not None:
                raise ValueError("Use 'n' or 'n_ratio', not both.")
            if not (0 < float(n_ratio) <= 1.0):
                raise ValueError("n_ratio must be in (0, 1].")
            n = int(len(candidate_df) * float(n_ratio))

        if n is None:
            n = 10

        if n <= 0:
            raise ValueError("n must be a positive integer after applying filters.")

        if len(candidate_df) < n:
            raise ValueError(
                f"Cannot sample {n} rows from filtered dataframe with only {len(candidate_df)} rows."
            )

        config = strategy_config or {}

        def _normalize_weights(weights: pd.Series) -> pd.Series:
            weights = weights.astype(float).fillna(0.0)
            weights[~np.isfinite(weights)] = 0.0
            total = weights.sum()
            if total <= 0:
                raise ValueError("Computed weights are all zero or invalid.")
            return weights

        def _time_to_numeric(series: pd.Series) -> pd.Series:
            if pd.api.types.is_datetime64_any_dtype(series):
                return series.view("int64")
            series_dt = pd.to_datetime(series, errors="coerce")
            if series_dt.notna().any():
                return series_dt.view("int64")
            return pd.to_numeric(series, errors="coerce")

        def _build_weights(df: pd.DataFrame) -> pd.Series:
            if strategy == "uniform":
                return pd.Series(1.0, index=df.index)

            if strategy == "key_weighted":
                key_col = config.get("key_col")
                if key_col is None or key_col not in df.columns:
                    raise ValueError("key_weighted requires 'key_col' in strategy_config")
                bias = config.get("bias", "dense")
                alpha = float(config.get("alpha", 1.0))
                freqs = df[key_col].value_counts()
                if bias == "dense":
                    row_weights = df[key_col].map(lambda k: pow(freqs.get(k, 1), alpha))
                elif bias == "sparse":
                    row_weights = df[key_col].map(lambda k: pow(1.0 / max(freqs.get(k, 1), 1), alpha))
                else:
                    raise ValueError("key_weighted bias must be 'dense' or 'sparse'")
                return row_weights

            if strategy == "time_weighted":
                time_col = config.get("time_col")
                if time_col is None or time_col not in df.columns:
                    raise ValueError("time_weighted requires 'time_col' in strategy_config")
                bias = config.get("bias", "recent")
                alpha = float(config.get("alpha", 1.0))
                numeric = _time_to_numeric(df[time_col])
                if numeric.notna().sum() == 0:
                    raise ValueError("time_weighted requires a valid time column")
                min_v = numeric.min()
                max_v = numeric.max()
                if pd.isna(min_v) or pd.isna(max_v):
                    raise ValueError("time_weighted requires a valid time column")
                if max_v == min_v:
                    norm = pd.Series(1.0, index=df.index)
                else:
                    norm = (numeric - min_v) / (max_v - min_v)
                if bias == "recent":
                    row_weights = pow(norm, alpha)
                elif bias == "old":
                    row_weights = pow(1.0 - norm, alpha)
                else:
                    raise ValueError("time_weighted bias must be 'recent' or 'old'")
                return row_weights

            raise ValueError(f"Unsupported deletion strategy: {strategy}")

        weights = _normalize_weights(_build_weights(candidate_df))
        return candidate_df.sample(n=n, weights=weights, replace=False, random_state=self.seed).reset_index(drop=True)

        # # Weighted sampling
        # sampler.sample_rows(n=n, strategy_name="weighted", config={"weight_col": "popularity"})

        # # Zipf sampling
        # sampler.sample_rows(n=n, strategy_name="zipf", config={"a": 1.8})

        # # Stratified sampling
        # sampler.sample_rows(n=n, strategy_name="stratified", config={"strata_col": "category"})

        # # Long-tail sampling
        # sampler.sample_rows(n=n, strategy_name="long_tail", config={"count_col": "access_count"})

        # # Fixed ID sampling
        # sampler.sample_rows(n=n, strategy_name="fixed_ids", config={"id_col": "user_id", "ids": [101, 205, 309]})
