"""
Excel extraction utilities for the provider ETL pipeline.

- Reads Excel files using Polars with the Calamine engine for high performance.
- Handles large files and multiple sheets.
- Returns data in a format ready for validation and transformation.
"""

import polars as pl
from typing import List, Dict, Any
import os
import fastexcel

from .provider_etl_config import POLARS_READ_OPTS, REQUIRED_COLUMNS, MAX_PARALLEL_SHEETS
from .progress import progress_bar, format_size

def read_excel_file(filepath: str) -> Dict[str, pl.DataFrame]:
    """
    Reads all sheets from an Excel file into a dictionary of Polars DataFrames.
    Always returns a dict, even for single-sheet files.
    """
    sheets = pl.read_excel(filepath, sheet_id=None, **POLARS_READ_OPTS)
    if isinstance(sheets, pl.DataFrame):
        # Only one sheet: get the sheet name using fastexcel
        with open(filepath, "rb") as f:
            reader = fastexcel.ExcelReader(f)
            print(type(reader))
            print(reader.sheet_names)
            sheet_names = reader.sheet_names
            sheet_name = sheet_names[0] if sheet_names else "Sheet1"
        sheets = {sheet_name: sheets}
    return sheets

def extract_excels(filepaths: List[str]) -> List[Dict[str, pl.DataFrame]]:
    """
    Reads multiple Excel files in parallel, showing progress.

    Args:
        filepaths: List of Excel file paths.

    Returns:
        List of dictionaries (one per file) mapping sheet names to DataFrames.
    """
    results = []
    for filepath in progress_bar(filepaths, desc="Reading Excel files"):
        if not os.path.exists(filepath):
            print(f"File not found: {filepath}")
            continue
        print(f"Reading {os.path.basename(filepath)} ({format_size(os.path.getsize(filepath))})")
        sheets = read_excel_file(filepath)
        results.append(sheets)
    return results

# Plain English summary:
# - `read_excel_file` loads all sheets from a single Excel file into Polars DataFrames.
# - `extract_excels` processes a list of files, showing progress and skipping missing files.
# - Both functions are optimized for large files and can be extended for parallelism if needed.