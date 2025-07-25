# distribution_strategies.py
import numpy as np
import pandas as pd
from scipy.stats import gaussian_kde

# Registry
STRATEGY_REGISTRY = {}

def register_strategy(name: str):
    def decorator(cls):
        STRATEGY_REGISTRY[name] = cls
        return cls
    return decorator

# Abstract base class
class DistributionStrategy:
    def sample(self, col_data: pd.Series, n: int, config: dict) -> np.ndarray:
        raise NotImplementedError


@register_strategy("kde")
@register_strategy("default")
class KDEStrategy(DistributionStrategy):
    def sample(self, col_data, n, config):
        # col_data = col_data.dropna()
        kde = gaussian_kde(col_data)
        samples = kde.resample(n).flatten()
        return np.clip(samples, col_data.min(), col_data.max())


@register_strategy("zipf")
class ZipfStrategy(DistributionStrategy):
    def sample(self, col_data, n, config):
        a = config.get("a", 2.0)
        min_val = config.get("min", 1)
        max_val = config.get("max", 100)
        raw = np.random.zipf(a, n)
        return np.clip(raw, min_val, max_val)


@register_strategy("normal")
class NormalStrategy(DistributionStrategy):
    def sample(self, col_data, n, config):
        mean = config.get("mean", 0)
        std = config.get("std", 1)
        return np.random.normal(mean, std, n)


@register_strategy("uniform")
class UniformStrategy(DistributionStrategy):
    def sample(self, col_data, n, config):
        low = config.get("low", col_data.min())
        high = config.get("high", col_data.max())
        return np.random.uniform(low, high, n)


@register_strategy("fixed")
class FixedStrategy(DistributionStrategy):
    def sample(self, col_data, n, config):
        value = config.get("value", col_data.mode().iloc[0] if not col_data.empty else 0)
        return np.full(n, value)

