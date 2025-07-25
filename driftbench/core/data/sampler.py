import pandas as pd
import numpy as np
from typing import Dict, Callable
from .sampling_strategy import STRATEGY_REGISTRY

class Sampler:
    def __init__(self, df: pd.DataFrame, columns: dict, default_strategy: str = "kde", seed: int = 42, default_config: dict = None):
        self.df = df
        self.columns = columns
        self.default_strategy = default_strategy
        self.default_config = default_config or {}
        self.seed = seed

    def sample(self, n: int, strategy_map: dict = None, config_map: dict = None) -> pd.DataFrame:
        strategy_map = strategy_map or {}
        config_map = config_map or {}
        new_data = {}

        for col in self.df.columns:
            col_data = self.df[col].dropna()
            logical_type = self.columns.get(col, {}).get("logical_type", "unknown")
            strategy_name = strategy_map.get(col, self.default_strategy)
            config = config_map.get(col, self.default_config)

            if strategy_name not in STRATEGY_REGISTRY:
                raise ValueError(f"Unknown strategy: {strategy_name}")

            strategy = STRATEGY_REGISTRY[strategy_name]
            new_data[col] = strategy.sample(col_data, n, config)

        return pd.DataFrame(new_data)

    def sample_rows(
        self,
        n: int,
        strategy_name: str = "uniform",
        config: dict = None,
        filter_func: Callable[[pd.DataFrame], pd.DataFrame] = None,
        random_state: int = None,
    ) -> pd.DataFrame:
        config = config or {}
        candidate_df = self.df

        if filter_func is not None:
            candidate_df = filter_func(candidate_df)

        if len(candidate_df) < n:
            raise ValueError(f"Cannot sample {n} rows from {len(candidate_df)} candidates")

        if strategy_name == "uniform":
            return candidate_df.sample(n=n, random_state=self.seed).reset_index(drop=True)

        elif strategy_name == "weighted":
            weight_col = config.get("weight_col")
            if weight_col is None or weight_col not in candidate_df.columns:
                raise ValueError("For 'weighted' strategy, 'weight_col' must be specified in config")
            weights = candidate_df[weight_col]
            return candidate_df.sample(n=n, weights=weights, random_state=self.seed).reset_index(drop=True)

        elif strategy_name == "zipf":
            a = config.get("a", 2.0)
            indices = np.random.zipf(a, size=n)
            indices = np.clip(indices, 1, len(candidate_df)) - 1
            sampled = candidate_df.iloc[indices].reset_index(drop=True)
            return sampled

        elif strategy_name == "stratified":
            strata_col = config.get("strata_col")
            if strata_col is None or strata_col not in candidate_df.columns:
                raise ValueError("For 'stratified' strategy, 'strata_col' must be specified")
            return candidate_df.groupby(strata_col, group_keys=False).apply(
                lambda x: x.sample(min(len(x), n // candidate_df[strata_col].nunique()), random_state=self.seed)
            ).reset_index(drop=True)

        elif strategy_name == "long_tail":
            count_col = config.get("count_col")
            if count_col is None or count_col not in candidate_df.columns:
                raise ValueError("For 'long_tail', specify a frequency/count column via 'count_col'")
            weights = 1.0 / (candidate_df[count_col] + 1e-6)
            return candidate_df.sample(n=n, weights=weights, random_state=self.seed).reset_index(drop=True)

        elif strategy_name == "fixed_ids":
            id_col = config.get("id_col")
            ids = config.get("ids", [])
            if id_col is None or not ids:
                raise ValueError("Specify 'id_col' and 'ids' for 'fixed_ids' strategy")
            return candidate_df[candidate_df[id_col].isin(ids)].sample(n=n).reset_index(drop=True)

        else:
            raise NotImplementedError(f"Row sampling strategy '{strategy_name}' is not supported yet.")
