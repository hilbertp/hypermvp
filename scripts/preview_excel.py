"""
Preview specific rows and all columns of a provider Excel file.

Plain English:
This script prints rows 11 to 21 (inclusive, 0-based index 10 to 20) of any Excel file you specify, so you can inspect the data and column types.

Usage:
    python scripts/preview_excel.py /path/to/your/file.xlsx
"""

import sys
import polars as pl

# Hardcoded file path for your use case
excel_path = "/home/philly/hypermvp/data/01_raw/provider/RESULT_LIST_ANONYM_ENERGY_MARKET_aFRR_DE_2024-09-01_2024-09-30.xlsx"

try:
    # Read all sheets and print rows 11 to 21 of each
    sheets = pl.read_excel(excel_path, sheet_id=None, engine="calamine")
    if isinstance(sheets, dict):
        for sheet_name, df in sheets.items():
            print(f"\n=== Sheet: {sheet_name} ===")
            print(df.slice(10, 11))  # rows 11 to 21 (0-based: 10 to 20)
    elif isinstance(sheets, pl.DataFrame):
        print("\n=== Sheet: (only sheet) ===")
        print(sheets.slice(10, 11))
    else:
        print("No sheets found or unknown format.")
except Exception as e:
    print(f"Error reading Excel file: {e}")