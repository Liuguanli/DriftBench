"""High-level benchmark adapters.

Examples:
    from driftbench.data.tpch import data as tpch_data, queries as tpch_queries

    tpch_data(scale_factor=1).generate(output_dir="./artifacts")
    tpch_queries(mode="qgen", queries_per_template=3).generate(output_dir="./artifacts")
"""

from .base import GenerationResult, OutputDirRequiredError
from .tpch import TPCHData, TPCHQueries
from .tpcds import TPCDSData, TPCDSQueries
from .ycsb import YCSBData, YCSBQueries
from .dsb import DSBData, DSBQueries
from .tpcc import TPCCData, TPCCQueries
from .tpcc_skew import TPCCSkewData, TPCCSkewQueries
from .job import JOBData, JOBQueries

__all__ = [
    "GenerationResult",
    "OutputDirRequiredError",
    "TPCHData",
    "TPCHQueries",
    "TPCDSData",
    "TPCDSQueries",
    "YCSBData",
    "YCSBQueries",
    "DSBData",
    "DSBQueries",
    "TPCCData",
    "TPCCQueries",
    "TPCCSkewData",
    "TPCCSkewQueries",
    "JOBData",
    "JOBQueries",
]
