"""DriftBench package root.

Use exports from this module (or `driftbench.api`) as the stable integration
surface for P0 instead of importing deep internal modules.
"""

__version__ = "0.1.0b10"

from driftbench.api import (
    get_filter,
    get_schema_extractor,
    load_and_validate_spec,
    load_spec,
    register_filter,
    run_spec,
    run_spec_and_return_summary,
    trace_to_spec,
    validate_spec,
)
from driftbench.cache import (
    AzureHNSCacheConfig,
    MaterializationResult,
    RemoteCacheMode,
    materialize_artifacts,
)
from driftbench import catalog

__all__ = [
    "__version__",
    "AzureHNSCacheConfig",
    "MaterializationResult",
    "RemoteCacheMode",
    "catalog",
    "get_filter",
    "get_schema_extractor",
    "load_and_validate_spec",
    "load_spec",
    "materialize_artifacts",
    "register_filter",
    "run_spec",
    "run_spec_and_return_summary",
    "trace_to_spec",
    "validate_spec",
]
