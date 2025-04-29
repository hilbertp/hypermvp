import os
import tempfile
import duckdb
import polars as pl
import pytest

from hypermvp.provider.loader import (
    get_duckdb_connection,
    create_table_if_not_exists,
    insert_dataframes,
    load_provider_data,
)
from hypermvp.provider.provider_etl_config import RAW_TABLE_SCHEMA  # <-- updated import

@pytest.fixture
def sample_polars_dfs():
    """
    Returns a list of two small Polars DataFrames with the RAW_TABLE_SCHEMA columns.
    """
    df1 = pl.DataFrame({
        "DELIVERY_DATE": ["2024-01-01"],
        "PRODUCT": ["aFRR"],
        "ENERGY_PRICE_[EUR/MWh]": [10.5],
        "ENERGY_PRICE_PAYMENT_DIRECTION": ["TSO to BSP"],
        "ALLOCATED_CAPACITY_[MW]": [100.0],
        "load_timestamp": ["2024-01-01T00:00:00"]
    })
    df2 = pl.DataFrame({
        "DELIVERY_DATE": ["2024-01-02"],
        "PRODUCT": ["aFRR"],
        "ENERGY_PRICE_[EUR/MWh]": [12.3],
        "ENERGY_PRICE_PAYMENT_DIRECTION": ["BSP to TSO"],
        "ALLOCATED_CAPACITY_[MW]": [120.0],
        "load_timestamp": ["2024-01-02T00:00:00"]
    })
    return [df1, df2]

def test_create_table_and_insert(tmp_path, sample_polars_dfs):
    """
    Test that the loader creates a table and inserts data correctly.
    """
    db_path = tmp_path / "test.duckdb"
    table_name = "provider_raw"
    conn = get_duckdb_connection(str(db_path))
    create_table_if_not_exists(conn, table_name, RAW_TABLE_SCHEMA)
    insert_dataframes(conn, table_name, sample_polars_dfs)
    # Query back the data
    result = conn.execute(f"SELECT * FROM {table_name}").fetchall()
    assert len(result) == 2
    conn.close()

def test_load_provider_data(tmp_path, sample_polars_dfs):
    """
    Test the high-level load_provider_data function.
    """
    db_path = tmp_path / "test2.duckdb"
    table_name = "provider_raw"
    load_provider_data(sample_polars_dfs, db_path=str(db_path), table_name=table_name)
    # Check data in DuckDB
    conn = duckdb.connect(str(db_path))
    result = conn.execute(f"SELECT * FROM {table_name}").fetchall()
    assert len(result) == 2
    # Check column names match schema
    col_names = [desc[1] for desc in conn.execute(f"PRAGMA table_info({table_name})").fetchall()]
    for col in RAW_TABLE_SCHEMA:
        assert col in col_names
    conn.close()