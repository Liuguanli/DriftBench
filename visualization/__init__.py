"""Reproducible DriftBench data and query drift visualizations.

The package intentionally has no plotting imports at module import time.  Use
``python -m visualization.cli`` for artifact generation.
"""

VISUALIZATION_SCHEMA_VERSION = 4

__all__ = ["VISUALIZATION_SCHEMA_VERSION"]
