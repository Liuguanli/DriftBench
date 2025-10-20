# driftbench/spec/registry.py
from typing import Callable, Dict, Tuple, Any

Handler = Callable[[dict], None]
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
