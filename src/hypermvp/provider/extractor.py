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
import logging

from .provider_etl_config import POLARS_READ_OPTS, REQUIRED_COLUMNS, MAX_PARALLEL_SHEETS
from .progress import progress_bar, format_size

def read_excel_file(filepath: str) -> Dict[str, pl.DataFrame]:
    """
    Reads all sheets from an Excel file into a dictionary of Polars DataFrames.
    Always returns a dict, even for single-sheet files.
    If the last column (e.g., 'NOTE') is entirely empty, it is dropped.
    If not, a warning is logged and the column is retained.

    Args:
        filepath: Path to the Excel file.

    Returns:
        A dictionary mapping sheet names to Polars DataFrames.
    """
    try:
        # First get sheet names using fastexcel
        reader = fastexcel.read_excel(filepath)
        sheet_names = reader.sheet_names

        result = {}
        for sheet_name in sheet_names:
            df = pl.read_excel(filepath, sheet_name=sheet_name, engine="calamine")
            # Check if the last column is entirely empty
            last_col = df.columns[-1]
            non_empty = df.filter(pl.col(last_col).is_not_null() & (pl.col(last_col) != "")).height
            if non_empty == 0:
                # Drop the last column if it's all empty
                df = df.select(df.columns[:-1])
            else:
                logging.warning(
                    f"Sheet '{sheet_name}' in '{filepath}' has non-empty values in last column '{last_col}'. Column will be kept."
                )
            result[sheet_name] = df

        return result
    except Exception as e:
        # Fallback to Polars' multi-sheet reading if fastexcel fails
        sheets = pl.read_excel(filepath, sheet_id=None, engine="calamine")
        # Apply the same logic for the fallback
        if isinstance(sheets, dict):
            for sheet_name, df in sheets.items():
                last_col = df.columns[-1]
                non_empty = df.filter(pl.col(last_col).is_not_null() & (pl.col(last_col) != "")).height
                if non_empty == 0:
                    df = df.select(df.columns[:-1])
                else:
                    logging.warning(
                        f"Sheet '{sheet_name}' in '{filepath}' has non-empty values in last column '{last_col}'. Column will be kept."
                    )
                sheets[sheet_name] = df
            return sheets
        elif isinstance(sheets, pl.DataFrame):
            last_col = sheets.columns[-1]
            non_empty = sheets.filter(pl.col(last_col).is_not_null() & (pl.col(last_col) != "")).height
            if non_empty == 0:
                sheets = sheets.select(sheets.columns[:-1])
            else:
                logging.warning(
                    f"Single sheet in '{filepath}' has non-empty values in last column '{last_col}'. Column will be kept."
                )
            return {"Sheet1": sheets}
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

def find_excel_files(directory: str) -> List[str]:
    """
    Find all Excel files in a directory.
    
    Args:
        directory: Directory to search
        
    Returns:
        List of Excel file paths
    """
    if not os.path.exists(directory):
        raise FileNotFoundError(f"Directory not found: {directory}")
        
    excel_files = [os.path.join(directory, f) for f in os.listdir(directory) 
                  if f.lower().endswith('.xlsx')]
    
    if not excel_files:
        logging.warning(f"No Excel files found in {directory}")
    else:
        logging.info(f"Found {len(excel_files)} Excel files in {directory}")
        
    return sorted(excel_files)  # Sort for consistent processing order

# Plain English summary:
# - `read_excel_file` loads all sheets from a single Excel file into Polars DataFrames.
# - `extract_excels` processes a list of files, showing progress and skipping missing files.
# - Both functions are optimized for large files and can be extended for parallelism if needed.