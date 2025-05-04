"""
DuckDB Data Viewer module.

This module provides tools for exploring, analyzing and validating data
stored in DuckDB databases, with a focus on energy market data.
"""

from hypermvp.tools.duckdb_viewer.connection import (
    get_connection,
    query_to_polars,
    get_table_schema,
    get_sample_data
)

from hypermvp.tools.duckdb_viewer.analysis import (
    get_basic_table_profile,
    profile_column,
    find_data_quality_issues,
    analyze_note_column
)

# Version information
__version__ = "0.1.0"

# Public API
__all__ = [
    "get_connection",
    "query_to_polars",
    "get_table_schema",
    "get_sample_data",
    "get_basic_table_profile",
    "profile_column",
    "find_data_quality_issues",
    "analyze_note_column",
]