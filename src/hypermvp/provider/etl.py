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
from .loader import load_provider_data, create_table_if_not_exists
from .provider_etl_config import REQUIRED_COLUMNS

def run_etl(
    excel_files: List[str],
    db_path: str = "provider_data.duckdb",
    table_name: str = "provider_raw"
) -> Dict[str, Any]:
    """
    Runs the ETL pipeline: extract, validate, and load Excel files.
    Now performs atomic import: deletes all rows in the date range of new data before inserting.

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
    min_date = None
    max_date = None

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
                # Track min/max DELIVERY_DATE
                if "DELIVERY_DATE" in df.columns and df.height > 0:
                    col = df["DELIVERY_DATE"].to_list()
                    try:
                        dates = [str(d) for d in col if d]
                        if dates:
                            sheet_min = min(dates)
                            sheet_max = max(dates)
                            if min_date is None or sheet_min < min_date:
                                min_date = sheet_min
                            if max_date is None or sheet_max > max_date:
                                max_date = sheet_max
                    except Exception as e:
                        logging.warning(f"Could not determine date range in {file} [{sheet_name}]: {e}")
        except Exception as e:
            errors.append({"file": file, "error": str(e)})
            logging.error(f"Failed to process {file}: {e}")

    if extracted:
        # Atomic import: delete all rows in date range before insert
        import duckdb
        conn = duckdb.connect(db_path)
        from .provider_etl_config import RAW_TABLE_SCHEMA
        create_table_if_not_exists(conn, table_name, RAW_TABLE_SCHEMA)
        if min_date and max_date:
            logging.info(f"Deleting existing rows in '{table_name}' for DELIVERY_DATE between {min_date} and {max_date}...")
            conn.execute(f"DELETE FROM {table_name} WHERE DELIVERY_DATE BETWEEN ? AND ?", [min_date, max_date])
        else:
            logging.warning("Could not determine date range for deletion; skipping delete step.")
        conn.close()
        # Now insert new data
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
    # Format numbers with European decimal separators for output
    def euro_fmt(val):
        if isinstance(val, int):
            return f"{val:,}".replace(",", ".")
        if isinstance(val, float):
            return f"{val:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        return str(val)
    logging.info(
        "Provider ETL Summary: files_processed=%s, sheets_loaded=%s, rows_loaded=%s, errors=%s",
        euro_fmt(len(excel_files)),
        euro_fmt(len(extracted)),
        euro_fmt(loaded),
        errors
    )
    return summary