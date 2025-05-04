"""
Unit tests for the DuckDB viewer query_templates module.

These tests check that the SQL query templates generate valid queries and return expected results.
"""

from hypermvp.tools.duckdb_viewer.query_templates import (
    list_tables_query,
    table_preview_query,
    table_summary_query,
    column_stats_query,
)
from hypermvp.tools.duckdb_viewer.connection import query_to_polars, get_connection
import pytest
import duckdb

@pytest.fixture(scope="module")
def test_db(tmp_path_factory):
    db_path = tmp_path_factory.mktemp("data") / "test.duckdb"
    # Use duckdb.connect to create the file and table before using get_connection
    conn = duckdb.connect(str(db_path))
    conn.execute("CREATE TABLE test_table (id INTEGER, name VARCHAR, value DOUBLE)")
    conn.execute("INSERT INTO test_table VALUES (1, 'A', 10.0), (2, 'B', 20.0), (3, NULL, 30.0)")
    conn.close()
    return str(db_path)

def test_list_tables_query(test_db):
    query = list_tables_query()
    df = query_to_polars(query, get_connection(test_db))
    assert "test_table" in df["table_name"].to_list()

def test_table_preview_query(test_db):
    query = table_preview_query("test_table", limit=1)
    df = query_to_polars(query, get_connection(test_db))
    assert len(df) == 1

def test_table_summary_query(test_db):
    query = table_summary_query("test_table")
    df = query_to_polars(query, get_connection(test_db))
    assert "row_count" in df.columns

def test_column_stats_query(test_db):
    query = column_stats_query("test_table", "id")
    df = query_to_polars(query, get_connection(test_db))
    assert "unique_values" in df.columns
