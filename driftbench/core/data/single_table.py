import pandas as pd
import numpy as np
from typing import List, Optional
from scipy.stats import gaussian_kde
from scipy.stats import skewnorm
from driftbench.core.data.distribution_simulator import DataDistributionSimulator
from driftbench.core.data.sampler import Sampler
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
        else:
            raise ValueError(f"Unsupported drift type: {drift_type}")

    def _inject_outliers(self, column, n=10, extreme_value=1e6):
        drifted_df = self.df.copy()
        outliers = drifted_df.sample(n=n).copy()
        outliers[column] = extreme_value
        return pd.concat([drifted_df, outliers], ignore_index=True)
    
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


    def _delete_records(self, n=10, filter_column=None, filter_func=None,
                        strategy="uniform", strategy_config=None):
        """
        Sample `n` rows from the dataframe for deletion (not actually dropped).
        
        Args:
            n (int): Number of records to sample.
            filter_column (str): Optional column to apply filter on.
            filter_func (function): A function to filter rows on `filter_column`.
            strategy (str): Sampling strategy name.
            strategy_config (dict): Optional sampling config.

        Returns:
            pd.DataFrame: The records selected for deletion.
        """
        candidate_df = self.df

        if filter_column and filter_func:
            if filter_column not in self.df.columns:
                raise ValueError(f"Column {filter_column} not in dataframe")
            candidate_df = self.df[filter_func(self.df[filter_column])]

        if len(candidate_df) < n:
            raise ValueError(f"Cannot sample {n} rows from filtered dataframe with only {len(candidate_df)} rows")

        # use Sampler to sample from candidate_df
        sampler = Sampler(candidate_df, self.columns, default_strategy=strategy)

        # Uniform sampling
        return sampler.sample_rows(n=n, strategy_name="uniform")

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
