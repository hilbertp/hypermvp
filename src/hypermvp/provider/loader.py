"""
DuckDB loading operations for the provider ETL pipeline.
Handles database connections, table creation, and atomic loading.
"""
import logging
import os
import duckdb
import polars as pl
from typing import List, Tuple

from .provider_etl_config import RAW_TABLE_SCHEMA

def get_duckdb_connection(db_path: str = "provider_data.duckdb") -> duckdb.DuckDBPyConnection:
    """
    Returns a DuckDB connection to the specified database file.
    """
    return duckdb.connect(db_path)

def create_table_if_not_exists(conn: duckdb.DuckDBPyConnection, table_name: str, schema: dict):
    """
    Creates a table with the given schema if it does not exist.
    Properly quotes column names to support special characters (e.g., brackets).
    """
    # Quote column names with double quotes for DuckDB compatibility
    columns = ", ".join([f'"{col}" {dtype}' for col, dtype in schema.items()])
    sql = f'CREATE TABLE IF NOT EXISTS "{table_name}" ({columns});'
    conn.execute(sql)

def insert_dataframes(
    conn: duckdb.DuckDBPyConnection,
    table_name: str,
    dfs: List[pl.DataFrame]
):
    """
    Efficiently inserts Polars DataFrames into DuckDB.
    Uses DuckDB's in-memory registration for fast, zero-copy bulk insert.
    """
    for i, df in enumerate(dfs):
        df = ensure_all_columns(df, RAW_TABLE_SCHEMA)
        temp_view = f"_temp_df_{i}"
        conn.register(temp_view, df.to_pandas())
        logging.info(f"Loading {len(df):,} rows into DuckDB table '{table_name}'...")  # <-- thousands separator
        conn.execute(f'INSERT INTO "{table_name}" SELECT * FROM "{temp_view}"')
        conn.unregister(temp_view)

def load_provider_data(
    dfs: List[pl.DataFrame],
    db_path: str = "provider_data.duckdb",
    table_name: str = "provider_raw"
):
    """
    Loads validated provider data into DuckDB.
    """
    conn = get_duckdb_connection(db_path)
    create_table_if_not_exists(conn, table_name, RAW_TABLE_SCHEMA)
    insert_dataframes(conn, table_name, dfs)
    conn.close()

def ensure_all_columns(df: pl.DataFrame, schema: dict) -> pl.DataFrame:
    """
    Ensures the DataFrame has all columns required by the schema.
    Adds missing columns with null values.
    """
    for col in schema:
        if col not in df.columns:
            df = df.with_columns(pl.lit(None).alias(col))
    # Reorder columns to match schema
    return df.select(list(schema.keys()))