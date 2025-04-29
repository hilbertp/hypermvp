"""
ETL Orchestration for Provider Data

This module coordinates the Extract-Transform-Load process:
- Extracts Excel files into Polars DataFrames
- Validates required columns and data integrity
- Loads validated data into DuckDB

Plain English:
Call `run_etl` with a list of Excel file paths and a DuckDB path to process all files in one go.
"""

import logging
from typing import List, Dict, Any

import polars as pl

from .extractor import read_excel_file
from .validators import validate_sheets
from .loader import load_provider_data
from .provider_etl_config import REQUIRED_COLUMNS

def run_etl(
    excel_files: List[str],
    db_path: str = "provider_data.duckdb",
    table_name: str = "provider_raw"
) -> Dict[str, Any]:
    """
    Runs the ETL pipeline: extract, validate, and load Excel files.

    Args:
        excel_files: List of Excel file paths to process.
        db_path: Path to DuckDB database file.
        table_name: Name of the DuckDB table to load data into.

    Returns:
        Dictionary with ETL summary stats.
    """
    logging.info(f"Starting ETL for {len(excel_files)} files.")
    extracted = []
    errors = []
    total_rows = 0

    for file in excel_files:
        try:
            logging.info(f"Extracting: {file}")
            sheets = read_excel_file(file)
            # Validate each sheet
            for sheet_name, df in sheets.items():
                valid, msg = validate_sheets(df, REQUIRED_COLUMNS)
                if not valid:
                    errors.append({"file": file, "sheet": sheet_name, "error": msg})
                    logging.warning(f"Validation failed: {file} [{sheet_name}] - {msg}")
                    continue
                # Add source file info for traceability
                df = df.with_columns(pl.lit(file).alias("source_file"))
                extracted.append(df)
                total_rows += df.height
        except Exception as e:
            errors.append({"file": file, "error": str(e)})
            logging.error(f"Failed to process {file}: {e}")

    if extracted:
        logging.info(f"Loading {total_rows} rows into DuckDB table '{table_name}'...")
        load_provider_data(extracted, db_path=db_path, table_name=table_name)
        loaded = total_rows
    else:
        loaded = 0

    summary = {
        "files_processed": len(excel_files),
        "sheets_loaded": len(extracted),
        "rows_loaded": loaded,
        "errors": errors,
    }
    logging.info(f"ETL complete: {summary}")
    return summary