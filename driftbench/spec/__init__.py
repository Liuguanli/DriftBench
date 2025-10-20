# driftbench/spec/__init__.py
from .core import run_all  # convenience export
from .types import workload_templates  # existing handler(s)
from .types import data_drift          # <-- add this line so @register runs
