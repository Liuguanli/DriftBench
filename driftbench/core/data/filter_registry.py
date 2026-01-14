from typing import Callable, Dict, Any

import pandas as pd

FILTER_REGISTRY: Dict[str, Callable[[pd.Series, Dict[str, Any]], pd.Series]] = {}


def register_filter(name: str):
    def decorator(fn: Callable[[pd.Series, Dict[str, Any]], pd.Series]):
        FILTER_REGISTRY[name] = fn
        return fn
    return decorator


def get_filter(name: str) -> Callable[[pd.Series, Dict[str, Any]], pd.Series]:
    if name not in FILTER_REGISTRY:
        raise ValueError(f"Unknown filter function: {name}")
    return FILTER_REGISTRY[name]
