"""
Tests for the provider validators module.
"""
import pytest
import tempfile
import os
import pandas as pd
import polars as pl
from hypermvp.provider.validators import (
    validate_excel_columns, 
    extract_date_range, 
    validate_all_excels_in_directory
)

@pytest.fixture
def sample_excel_file():
    """Creates a sample Excel file for testing."""
    with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as temp:
        # Create sample DataFrame
        df = pd.DataFrame({
            "DELIVERY_DATE": ["2024-01-01", "2024-01-02", "2024-01-03"],
            "PRODUCT": ["aFRR", "aFRR", "aFRR"],
            "ENERGY_PRICE_[EUR/MWh]": [10.5, 12.3, 9.8],
            "ENERGY_PRICE_PAYMENT_DIRECTION": ["TSO to BSP", "TSO to BSP", "BSP to TSO"],
            "ALLOCATED_CAPACITY_[MW]": [100, 120, 95],
            "NOTE": ["", "", ""]
        })
        
        # Save to Excel with multiple sheets
        with pd.ExcelWriter(temp.name) as writer:
            df.to_excel(writer, sheet_name="Sheet1", index=False)
            df.to_excel(writer, sheet_name="Sheet2", index=False)
            
    yield temp.name
    # Clean up
    os.unlink(temp.name)

@pytest.fixture
def sample_directory(sample_excel_file):
    """Creates a sample directory with Excel files for testing."""
    with tempfile.TemporaryDirectory() as temp_dir:
        # Copy sample file to the directory
        import shutil
        dest_file = os.path.join(temp_dir, "test_file.xlsx")
        shutil.copy(sample_excel_file, dest_file)
        
        yield temp_dir

def test_validate_excel_columns():
    """Test validation of Excel columns."""
    df = pl.DataFrame({
        "DELIVERY_DATE": ["2024-01-01"],
        "PRODUCT": ["aFRR"],
        "EXTRA_COLUMN": [1]
    })
    
    # Test with all required columns present
    valid, missing = validate_excel_columns(
        df, 
        "test.xlsx", 
        "Sheet1", 
        ["DELIVERY_DATE", "PRODUCT"]
    )
    assert valid is True
    assert missing == []
    
    # Test with missing columns
    valid, missing = validate_excel_columns(
        df, 
        "test.xlsx", 
        "Sheet1", 
        ["DELIVERY_DATE", "PRODUCT", "MISSING_COLUMN"]
    )
    assert valid is False
    assert "MISSING_COLUMN" in missing

def test_extract_date_range():
    """Test extraction of date range."""
    df = pl.DataFrame({
        "DELIVERY_DATE": ["2024-01-01", "2024-01-05", "2024-01-03"],
        "PRODUCT": ["aFRR", "aFRR", "aFRR"]
    })
    
    min_date, max_date = extract_date_range(df, "test.xlsx", "Sheet1")
    assert min_date == "2024-01-01"
    assert max_date == "2024-01-05"
    
    # Test with empty DataFrame
    empty_df = pl.DataFrame({
        "DELIVERY_DATE": [],
        "PRODUCT": []
    })
    min_date, max_date = extract_date_range(empty_df, "test.xlsx", "Sheet1")
    assert min_date is None
    assert max_date is None

def test_validate_all_excels_in_directory(sample_directory):
    """Test validation of all Excel files in a directory."""
    success, result = validate_all_excels_in_directory(
        sample_directory,
        required_columns=["DELIVERY_DATE", "PRODUCT"]
    )
    
    assert success is True
    min_date, max_date, file_sheet_dfs = result
    
    assert min_date == "2024-01-01"
    assert max_date == "2024-01-03"
    assert len(file_sheet_dfs) == 2  # 2 sheets