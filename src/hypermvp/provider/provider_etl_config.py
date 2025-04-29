"""
Configuration settings for the provider ETL pipeline.

Centralizes all schema definitions, required columns, and performance-related settings.
Import this in other modules to ensure consistency and easy maintenance.

Plain English summary:
- Defines table schemas for raw and clean provider data.
- Lists required columns for validation.
- Sets performance and parallelism options for large file processing.
- Contains constants for DuckDB and Polars integration.
"""

import os

# Table schema for raw provider data (matches Excel input)
RAW_TABLE_SCHEMA = {
    "DELIVERY_DATE": "VARCHAR",
    "PRODUCT": "VARCHAR",
    "ENERGY_PRICE_[EUR/MWh]": "DOUBLE",
    "ENERGY_PRICE_PAYMENT_DIRECTION": "VARCHAR",
    "ALLOCATED_CAPACITY_[MW]": "DOUBLE",
    "NOTE": "VARCHAR",
    "source_file": "VARCHAR",
    "load_timestamp": "TIMESTAMP"
}

# Table schema for cleaned provider data (after ETL)
CLEAN_TABLE_SCHEMA = {
    "DELIVERY_DATE": "TIMESTAMP",
    "PRODUCT": "VARCHAR",
    "ENERGY_PRICE__EUR_MWh_": "DOUBLE",
    "ALLOCATED_CAPACITY__MW_": "DOUBLE",
    "period": "TIMESTAMP",
    "source_file": "VARCHAR"
}

# Required columns for validation (must exist in every Excel sheet)
REQUIRED_COLUMNS = [
    "DELIVERY_DATE",
    "PRODUCT",
    "ENERGY_PRICE_[EUR/MWh]",
    "ENERGY_PRICE_PAYMENT_DIRECTION",
    "ALLOCATED_CAPACITY_[MW]"
    # "NOTE" is now optional and not required
]

# Polars read_excel options for performance and compatibility
POLARS_READ_OPTS = {
    "engine": "calamine",
    # Remove "read_options" and "dtype" keys that are not supported by your Polars version
    # Only include supported arguments for your installed version of Polars
}

# Performance settings
MAX_PARALLEL_SHEETS = min(4, os.cpu_count() or 4)  # Parallel Excel reading
BATCH_SIZE = 100_000  # Rows per batch insert into DuckDB

# DuckDB settings
DUCKDB_THREADS = min(6, os.cpu_count() or 4)  # Default thread count for DuckDB

# Progress bar settings
PROGRESS_BAR_COLOR = "green"
PROGRESS_BAR_DISABLE = False  # Set to True in automated environments

# Date/time formats (used for standardization)
ISO_DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
ISO_DATE_FORMAT = "%Y-%m-%d"

def standardize_date_column(df, column, fmt=ISO_DATETIME_FORMAT):
    """
    Standardizes a date column in a DataFrame to the specified format.
    Returns a new DataFrame with the column converted.
    """
    import polars as pl
    return df.with_columns(
        pl.col(column).str.strptime(pl.Datetime, fmt, strict=False).alias(column)
    )