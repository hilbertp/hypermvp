"""
Preview the first 10 rows and all columns of a provider Excel file.

Plain English:
This script prints the first 10 rows of any Excel file you specify, so you can inspect the data and column types.

Usage:
    python scripts/preview_excel.py /path/to/your/file.xlsx
"""

import sys
import polars as pl

if len(sys.argv) != 2:
    print("Usage: python scripts/preview_excel.py /path/to/your/file.xlsx")
    sys.exit(1)

excel_path = sys.argv[1]

try:
    # Read all sheets and print the first 10 rows of each
    sheets = pl.read_excel(excel_path, sheet_id=None, engine="calamine")
    if isinstance(sheets, dict):
        for sheet_name, df in sheets.items():
            print(f"\n=== Sheet: {sheet_name} ===")
            print(df.head(10))
    elif isinstance(sheets, pl.DataFrame):
        print("\n=== Sheet: (only sheet) ===")
        print(sheets.head(10))
    else:
        print("No sheets found or unknown format.")
except Exception as e:
    print(f"Error reading Excel file: {e}")