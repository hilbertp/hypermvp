"""
Unit tests for the DuckDB viewer analysis module.

These tests check that the analysis functions return correct summaries and handle edge cases.
"""

import pytest
import duckdb
from hypermvp.tools.duckdb_viewer.analysis import (
    get_basic_table_profile,
    profile_column,
    find_data_quality_issues,
)
from hypermvp.tools.duckdb_viewer.connection import get_connection

@pytest.fixture(scope="module")
def test_db(tmp_path_factory):
    db_path = tmp_path_factory.mktemp("data") / "test.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("CREATE TABLE test_table (id INTEGER, name VARCHAR, value DOUBLE)")
    conn.execute("INSERT INTO test_table VALUES (1, 'A', 10.0), (2, 'B', 20.0), (3, NULL, 30.0)")
    conn.close()
    return str(db_path)

def test_get_basic_table_profile(test_db):
    profile = get_basic_table_profile("test_table", conn=get_connection(test_db))
    assert profile["row_count"] == 3
    assert "id" in profile["columns"]

def test_profile_column(test_db):
    col_profile = profile_column("test_table", "name", conn=get_connection(test_db))
    assert "unique_values" in col_profile

def test_find_data_quality_issues(test_db):
    issues = find_data_quality_issues("test_table", conn=get_connection(test_db))
    assert isinstance(issues, dict)
