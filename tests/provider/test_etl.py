import os
import duckdb
import polars as pl
import pytest
from hypermvp.provider import etl

@pytest.fixture
def sample_excel_files(tmp_path):
    """Creates two Excel files: one valid, one missing required columns."""
    import pandas as pd
    valid_path = tmp_path / "valid.xlsx"
    invalid_path = tmp_path / "invalid.xlsx"
    df_valid = pd.DataFrame({
        "DELIVERY_DATE": ["2024-01-01"],
        "PRODUCT": ["aFRR"],
        "ENERGY_PRICE_[EUR/MWh]": [10.5],
        "ENERGY_PRICE_PAYMENT_DIRECTION": ["TSO to BSP"],
        "ALLOCATED_CAPACITY_[MW]": [100],
        "NOTE": [""]
    })
    df_invalid = pd.DataFrame({
        "DELIVERY_DATE": ["2024-01-01"],
        "PRODUCT": ["aFRR"]
        # Missing required columns
    })
    df_valid.to_excel(valid_path, index=False)
    df_invalid.to_excel(invalid_path, index=False)
    return [str(valid_path), str(invalid_path)]

def test_run_etl(sample_excel_files, tmp_path):
    db_path = tmp_path / "test.duckdb"
    summary = etl.run_etl(sample_excel_files, db_path=str(db_path))
    assert summary["files_processed"] == 2
    assert summary["rows_loaded"] == 1
    assert len(summary["errors"]) == 1
    # Check data in DuckDB
    conn = duckdb.connect(str(db_path))
    result = conn.execute("SELECT * FROM provider_raw").fetchall()
    assert len(result) == 1
    conn.close()