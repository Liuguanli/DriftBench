import pandas as pd
from driftbench.core.data.distribution_strategy import STRATEGY_REGISTRY


class DataDistributionSimulator:
    def __init__(self, df: pd.DataFrame, columns: dict, default_strategy="kde"):
        self.df = df
        self.columns = columns
        self.default_strategy = default_strategy

    def generate(self, col_data: pd.Series, n: int, strategy_name: str = None) -> pd.DataFrame:

        # if not col_data:
        #     return None

        # col_data = self.df[col].dropna()
        # logical_type = self.columns.get(col, {}).get("logical_type", "unknown")

        if strategy_name not in STRATEGY_REGISTRY:
            raise ValueError(f"Unknown strategy: {strategy_name}")

        strategy_cls = STRATEGY_REGISTRY[strategy_name]
        strategy = strategy_cls()
        return strategy.sample(col_data, n, config=None)
