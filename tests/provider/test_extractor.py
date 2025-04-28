import pytest
import os
import polars as pl
from hypermvp.provider.extractor import read_excel_file, extract_excels

@pytest.fixture
def sample_excel_file(tmp_path):
    """Creates a sample Excel file with two sheets for testing."""
    import pandas as pd
    file_path = tmp_path / "test.xlsx"
    df1 = pd.DataFrame({
        "DELIVERY_DATE": ["2024-01-01", "2024-01-02"],
        "PRODUCT": ["aFRR", "aFRR"],
        "ENERGY_PRICE_[EUR/MWh]": [10.5, 12.3],
        "ENERGY_PRICE_PAYMENT_DIRECTION": ["TSO to BSP", "TSO to BSP"],
        "ALLOCATED_CAPACITY_[MW]": [100, 120],
        "NOTE": ["", ""]
    })
    df2 = pd.DataFrame({
        "DELIVERY_DATE": ["2024-01-03"],
        "PRODUCT": ["aFRR"],
        "ENERGY_PRICE_[EUR/MWh]": [9.8],
        "ENERGY_PRICE_PAYMENT_DIRECTION": ["BSP to TSO"],
        "ALLOCATED_CAPACITY_[MW]": [95],
        "NOTE": [""]
    })
    with pd.ExcelWriter(file_path) as writer:
        df1.to_excel(writer, sheet_name="Sheet1", index=False)
        df2.to_excel(writer, sheet_name="Sheet2", index=False)
    return str(file_path)

def test_read_excel_file(sample_excel_file):
    """Test reading all sheets from a sample Excel file."""
    sheets = read_excel_file(sample_excel_file)
    assert isinstance(sheets, dict)
    assert set(sheets.keys()) == {"Sheet1", "Sheet2"}
    assert isinstance(sheets["Sheet1"], pl.DataFrame)
    assert sheets["Sheet1"].shape == (2, 6)
    assert sheets["Sheet2"].shape == (1, 6)

def test_extract_excels(sample_excel_file):
    """Test extracting multiple Excel files."""
    # Duplicate the file to simulate multiple files
    file2 = sample_excel_file.replace("test.xlsx", "test2.xlsx")
    import shutil
    shutil.copy(sample_excel_file, file2)
    files = [sample_excel_file, file2]
    result = extract_excels(files)
    assert isinstance(result, list)
    assert len(result) == 2
    for sheets in result:
        assert set(sheets.keys()) == {"Sheet1", "Sheet2"}