"""
Unit tests for the DuckDB viewer connection module.

These tests check that the connection functions work, queries return expected results, and errors are handled.
"""

import pytest
import duckdb
import polars as pl
from pathlib import Path

from hypermvp.tools.duckdb_viewer.connection import (
    get_connection,
    query_to_polars,
    get_table_schema,
    get_sample_data,
)

@pytest.fixture(scope="module")
def test_db(tmp_path_factory):
    """
    Creates a temporary DuckDB database with a sample table for testing.
    """
    db_path = tmp_path_factory.mktemp("data") / "test.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("CREATE TABLE test_table (id INTEGER, name VARCHAR, value DOUBLE)")
    conn.execute("INSERT INTO test_table VALUES (1, 'A', 10.0), (2, 'B', 20.0), (3, NULL, 30.0)")
    conn.close()
    return str(db_path)

def test_get_connection_success(test_db):
    conn = get_connection(test_db)
    assert conn is not None
    conn.close()

def test_get_connection_fail():
    with pytest.raises(FileNotFoundError):
        get_connection("/nonexistent/path.duckdb")

def test_query_to_polars(test_db):
    df = query_to_polars("SELECT * FROM test_table", get_connection(test_db))
    assert isinstance(df, pl.DataFrame)
    assert df.shape == (3, 3)

def test_get_table_schema(test_db):
    schema = get_table_schema("test_table", get_connection(test_db))
    assert isinstance(schema, list)
    assert schema[0]["name"] == "id"

def test_get_sample_data(test_db):
    df = get_sample_data("test_table", limit=2, conn=get_connection(test_db))
    assert len(df) == 2
