"""DriftBench package root.

Use exports from this module (or `driftbench.api`) as the stable integration
surface for P0 instead of importing deep internal modules.
"""

__version__ = "0.1.0b2"

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

__all__ = [
    "__version__",
    "get_filter",
    "get_schema_extractor",
    "load_and_validate_spec",
    "load_spec",
    "register_filter",
    "run_spec",
    "run_spec_and_return_summary",
    "trace_to_spec",
    "validate_spec",
]
