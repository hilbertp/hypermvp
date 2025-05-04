"""
Unit tests for provider_db_cleaner.py

This test checks that the provider table cleaning logic:
- Removes rows where PRODUCT starts with 'POS_'
- Multiplies ENERGY_PRICE_EUR_MWh by -1 for PROVIDER_TO_GRID
- Sorts by DELIVERY_DATE, then ENERGY_PRICE_EUR_MWh

Uses a temporary DuckDB database for isolation.
"""
import duckdb
import pytest
from pathlib import Path
from src.hypermvp.provider.provider_db_cleaner import clean_provider_table

@pytest.fixture
def duckdb_test_db(tmp_path):
    """Creates a temporary DuckDB database for testing."""
    db_path = tmp_path / "test_provider_cleaner.duckdb"
    con = duckdb.connect(str(db_path))
    yield str(db_path)
    con.close()

def test_clean_provider_table_removes_pos_and_adjusts_price(duckdb_test_db):
    """
    Test that clean_provider_table:
    - Removes rows where PRODUCT starts with 'POS_'
    - Multiplies ENERGY_PRICE_EUR_MWh by -1 for PROVIDER_TO_GRID
    - Sorts by DELIVERY_DATE, then ENERGY_PRICE_EUR_MWh
    """
    con = duckdb.connect(duckdb_test_db)
    # Create raw table with explicit schema
    con.execute("""
        CREATE TABLE provider_raw (
            DELIVERY_DATE VARCHAR,
            PRODUCT VARCHAR,
            ENERGY_PRICE_EUR_MWh DOUBLE,
            PAYMENT_DIRECTION VARCHAR,
            ALLOCATED_CAPACITY_MW DOUBLE,
            NOTE VARCHAR,
            source_file VARCHAR,
            load_timestamp VARCHAR
        );
    """)
    # Insert test data
    con.execute("""
        INSERT INTO provider_raw VALUES
            ('2024-09-01 00:00:00', 'FRR', 100.0, 'PROVIDER_TO_GRID', 5.0, 'note1', 'file1.xlsx', '2024-09-01 10:00:00'),
            ('2024-09-01 00:00:00', 'FRR', 50.0, 'GRID_TO_PROVIDER', 3.0, 'note2', 'file1.xlsx', '2024-09-01 10:00:00'),
            ('2024-09-01 00:00:00', 'POS_X', 200.0, 'PROVIDER_TO_GRID', 2.0, 'note3', 'file2.xlsx', '2024-09-01 10:00:00'),
            ('2024-09-02 00:00:00', 'FRR', 10.0, 'PROVIDER_TO_GRID', 1.0, 'note4', 'file3.xlsx', '2024-09-01 10:00:00');
    """)
    con.close()

    # Run the cleaning function
    clean_provider_table(duckdb_test_db)

    # Check the cleaned table
    con = duckdb.connect(duckdb_test_db)
    result = con.execute("""
        SELECT DELIVERY_DATE, PRODUCT, ENERGY_PRICE_EUR_MWh, PAYMENT_DIRECTION, ALLOCATED_CAPACITY_MW
        FROM provider_clean
        ORDER BY DELIVERY_DATE, ENERGY_PRICE_EUR_MWh
    """).fetchall()

    # Expected: POS_X row is removed, prices for PROVIDER_TO_GRID are negated
    expected = [
        ('2024-09-01 00:00:00', 'FRR', -100.0, 'PROVIDER_TO_GRID', 5.0),
        ('2024-09-01 00:00:00', 'FRR', 50.0, 'GRID_TO_PROVIDER', 3.0),
        ('2024-09-02 00:00:00', 'FRR', -10.0, 'PROVIDER_TO_GRID', 1.0)
    ]
    assert result == expected
    con.close()
