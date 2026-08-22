# driftbench/spec/registry.py
from typing import Any, Callable, Dict, Tuple

Handler = Callable[..., Any]
_REGISTRY: Dict[Tuple[str, str, str], Handler] = {}

def register(family: str, category: str, subtype: str):
    def deco(fn: Handler) -> Handler:
        key = (family, category, subtype)
        if key in _REGISTRY:
            raise ValueError(f"Duplicate handler for {key}")
        _REGISTRY[key] = fn
        return fn
    return deco

def get_handler(key: Tuple[str, str, str]) -> Handler:
    if key not in _REGISTRY:
        raise ValueError(f"No handler registered for {key}")
    return _REGISTRY[key]
