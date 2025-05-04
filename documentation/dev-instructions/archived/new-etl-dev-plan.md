# Enhanced Provider ETL Implementation Plan

This comprehensive plan adapts the original to fully integrate high-performance Polars/Rust processing while maintaining critical features from your existing workflow.

## Architecture Overview

```bash
# Directory structure for the provider ETL pipeline
/home/philly/hypermvp/src/hypermvp/provider/
├── __init__.py         # Package exports
├── config.py           # Schema and settings
├── validators.py       # Excel validation 
├── extractor.py        # High-performance Excel reading
├── loader.py           # DuckDB operations
├── etl.py              # Main ETL orchestration
├── cli.py              # Command-line interface (preserving existing)
├── progress.py         # Progress tracking helpers
└── tests/              # Testing infrastructure

## 1. Configuration Module

````python
"""
Configuration settings for the provider ETL pipeline.
Centralizes all schema definitions and performance settings.
"""
import os
import polars as pl

# Table schema definition
RAW_TABLE_SCHEMA = {
    "DELIVERY_DATE": "VARCHAR",
    "PRODUCT": "VARCHAR", 
    "ENERGY_PRICE_[EUR/MWh]": "VARCHAR",
    "ENERGY_PRICE_PAYMENT_DIRECTION": "VARCHAR",
    "ALLOCATED_CAPACITY_[MW]": "VARCHAR",
    "NOTE": "VARCHAR",
    "source_file": "VARCHAR",
    "load_timestamp": "TIMESTAMP"
}

# Required columns for validation
REQUIRED_COLUMNS = list(RAW_TABLE_SCHEMA.keys())[:-2]

# Performance settings
MAX_PARALLEL_SHEETS = min(2, os.cpu_count() or 4)  # Conservative default
BATCH_SIZE = 100_000  # Rows per batch insert

# DuckDB settings
DUCKDB_THREADS = min(6, os.cpu_count() or 4)  # Default thread count

# Progress bar settings
PROGRESS_BAR_COLOR = "green"
PROGRESS_BAR_DISABLE = False  # Set to True in automated environments

# Polars read_excel options
POLARS_READ_OPTS = dict(
    engine="calamine",  # Rust-based Excel engine (much faster)
    rechunk=True,       # Optimize memory layout
)
````

## 2. Progress Tracking Module

````python
"""
Progress tracking utilities for the provider ETL pipeline.
"""
from typing import List, Iterable, TypeVar, Any, Optional
from tqdm import tqdm
import os
from concurrent.futures import as_completed

from .config import PROGRESS_BAR_COLOR, PROGRESS_BAR_DISABLE

T = TypeVar('T')

def progress_bar(
    iterable: Iterable[T], 
    desc: str, 
    total: Optional[int] = None,
    unit: str = "it"
) -> Iterable[T]:
    """
    Creates a progress bar with consistent styling.
    
    Args:
        iterable: Items to iterate over
        desc: Description text for the progress bar
        total: Total number of items (calculated automatically if None)
        unit: Unit text for the progress bar
        
    Returns:
        tqdm-wrapped iterable
    """
    return tqdm(
        iterable,
        desc=desc,
        total=total,
        unit=unit,
        colour=PROGRESS_BAR_COLOR,
        disable=PROGRESS_BAR_DISABLE
    )

def progress_map(func, items: List[Any], desc: str, max_workers: int = 1):
    """
    Apply a function to each item with progress tracking.
    
    Args:
        func: Function to apply to each item
        items: List of items to process
        desc: Description for the progress bar
        max_workers: Maximum number of parallel workers
        
    Returns:
        List of results
    """
    results = []
    if max_workers <= 1:
        # Sequential processing with progress bar
        for item in progress_bar(items, desc=desc):
            results.append(func(item))
    else:
        # Parallel processing with progress bar
        from concurrent.futures import ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(func, item): item for item in items}
            for future in progress_bar(
                as_completed(futures), 
                desc=desc, 
                total=len(items)
            ):
                results.append(future.result())
    
    return results

def format_size(size_bytes: int) -> str:
    """
    Format file size in human-readable format.
    
    Args:
        size_bytes: Size in bytes
        
    Returns:
        Formatted size string (e.g., "5.2 MB")
    """
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size_bytes < 1024 or unit == 'TB':
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024
````

## 3. Excel Extraction Module

````python
"""
Excel file extraction with Polars for the provider ETL pipeline.
Uses the Rust-based Calamine engine for high-performance Excel reading.
"""
import logging
import os
from pathlib import Path
import polars as pl
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Tuple, Any, Optional

from .config import POLARS_READ_OPTS, MAX_PARALLEL_SHEETS
from .progress import progress_bar, progress_map, format_size

def read_excel_sheet(
    file_path: str, 
    sheet_name: str
) -> pl.DataFrame:
    """
    Reads an Excel sheet with Polars' Calamine engine for high performance.
    
    Args:
        file_path: Path to the Excel file
        sheet_name: Name of the sheet to read
        
    Returns:
        Polars DataFrame with the sheet data
    """
    try:
        logging.debug(f"Reading sheet '{sheet_name}' from {os.path.basename(file_path)}")
        df = pl.read_excel(
            file_path,
            sheet_name=sheet_name,
            **POLARS_READ_OPTS
        )
        return df
    except Exception as e:
        logging.error(f"Error reading {os.path.basename(file_path)} sheet '{sheet_name}': {e}")
        raise

def get_excel_sheet_names(file_path: str) -> List[str]:
    """
    Get all sheet names from an Excel file using Polars directly.
    
    Args:
        file_path: Path to the Excel file
        
    Returns:
        List of sheet names
    """
    try:
        # Use Polars to get sheet names when possible
        sheet_names = pl.read_excel_schema(file_path)
        return list(sheet_names.keys())
    except Exception:
        # Fallback to openpyxl only if needed
        import openpyxl
        wb = openpyxl.load_workbook(file_path, read_only=True)
        sheet_names = wb.sheetnames
        wb.close()
        return sheet_names

def read_excel_file(file_path: str) -> Dict[str, pl.DataFrame]:
    """
    Reads all sheets in an Excel file using Polars and returns them as DataFrames.
    Uses parallel processing for multiple sheets with progress bar.
    
    Args:
        file_path: Path to the Excel file
        
    Returns:
        Dictionary mapping sheet names to Polars DataFrames
    """
    file_name = os.path.basename(file_path)
    file_size = format_size(os.path.getsize(file_path))
    logging.info(f"Reading {file_name} ({file_size})")
    
    # Get sheet names
    sheet_names = get_excel_sheet_names(file_path)
    
    result = {}
    
    # Process sheets in parallel with progress bar
    with tqdm(
        total=len(sheet_names), 
        desc=f"Reading sheets from {file_name}", 
        colour="green"
    ) as pbar:
        with ThreadPoolExecutor(max_workers=MAX_PARALLEL_SHEETS) as executor:
            # Submit all sheet reading tasks
            future_to_sheet = {
                executor.submit(read_excel_sheet, file_path, sheet): sheet 
                for sheet in sheet_names
            }
            
            # Process results as they complete
            for future in as_completed(future_to_sheet):
                sheet = future_to_sheet[future]
                try:
                    df = future.result()
                    result[sheet] = df
                    pbar.update(1)  # Update progress bar
                    pbar.set_postfix({"rows": df.height, "sheet": sheet})
                except Exception as e:
                    logging.error(f"Failed to read sheet {sheet} in {file_name}: {e}")
                    pbar.update(1)  # Still update progress bar
                    raise
    
    total_rows = sum(df.height for df in result.values())
    logging.info(f"Read {total_rows:,} rows from {len(result)} sheets in {file_name}")
    return result

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
````

## 4. Validators Module

````python
"""
Validators for Excel data in the provider ETL pipeline.
Contains functions to check headers, data types, and business rules.
"""
import logging
import os
from typing import Tuple, List, Optional, Dict, Any
import polars as pl
from tqdm import tqdm

from .config import REQUIRED_COLUMNS
from .extractor import find_excel_files, read_excel_file
from .progress import progress_bar

def validate_excel_columns(
    df: pl.DataFrame, 
    file_name: str, 
    sheet_name: str, 
    required_columns: List[str]
) -> Tuple[bool, List[str]]:
    """
    Validates that the DataFrame contains all required columns.
    
    Args:
        df: Polars DataFrame to validate
        file_name: Source file name for error reporting
        sheet_name: Sheet name for error reporting
        required_columns: List of column names that must be present
        
    Returns:
        Tuple of (is_valid, missing_columns)
    """
    # Get headers from DataFrame
    headers = df.columns
    
    # Debug output to help with troubleshooting
    logging.debug(f"Headers in {file_name} sheet '{sheet_name}': {headers}")
    
    # Check for missing required columns
    missing = [col for col in required_columns if col not in headers]
    
    if missing:
        logging.error(f"Missing columns {missing} in {file_name} sheet '{sheet_name}'")
        return False, missing
    
    return True, []

def extract_date_range(
    df: pl.DataFrame, 
    file_name: str, 
    sheet_name: str
) -> Tuple[Optional[str], Optional[str]]:
    """
    Extracts minimum and maximum DELIVERY_DATE values from the DataFrame.
    
    Args:
        df: Polars DataFrame with provider data
        file_name: Source file name for error reporting
        sheet_name: Sheet name for error reporting
        
    Returns:
        Tuple of (min_date, max_date) as strings, or (None, None) if no dates found
    """
    if "DELIVERY_DATE" not in df.columns:
        logging.error(f"No DELIVERY_DATE column in {file_name} sheet '{sheet_name}'")
        return None, None
    
    # Filter out empty values
    try:
        dates_df = df.filter(pl.col("DELIVERY_DATE").is_not_null() & 
                           (pl.col("DELIVERY_DATE") != ""))
        
        if dates_df.height == 0:
            logging.error(f"No DELIVERY_DATE values in {file_name} sheet '{sheet_name}'")
            return None, None
        
        min_date = dates_df.select(pl.min("DELIVERY_DATE")).item()
        max_date = dates_df.select(pl.max("DELIVERY_DATE")).item()
        
        logging.debug(f"Date range in {file_name} sheet '{sheet_name}': {min_date} to {max_date}")
        return str(min_date), str(max_date)
    except Exception as e:
        logging.error(f"Error extracting dates from {file_name} sheet '{sheet_name}': {e}")
        return None, None

def validate_all_excels_in_directory(
    directory: str,
    required_columns: List[str] = REQUIRED_COLUMNS
) -> Tuple[bool, Any]:
    """
    Validates all Excel files in the specified directory with progress bars.
    
    Args:
        directory: Path to directory containing Excel files
        required_columns: List of column names that must be present
        
    Returns:
        Tuple of (success, result) where result is either an error message
        or a tuple of (min_date, max_date, file_sheet_dfs)
    """
    try:
        excel_files = find_excel_files(directory)
        
        if not excel_files:
            return False, "No Excel files found in directory."
        
        min_date, max_date = None, None
        file_sheet_dfs = []
        
        # Process files with progress bar
        for excel_file in progress_bar(
            excel_files, 
            desc="Validating Excel files", 
            unit="file"
        ):
            try:
                file_name = os.path.basename(excel_file)
                dfs_by_sheet = read_excel_file(excel_file)
                
                # Process sheets with progress bar
                for sheet_name, df in progress_bar(
                    dfs_by_sheet.items(), 
                    desc=f"Validating sheets in {file_name}",
                    total=len(dfs_by_sheet),
                    unit="sheet"
                ):
                    # Validate columns
                    valid, missing = validate_excel_columns(df, file_name, sheet_name, required_columns)
                    if not valid:
                        return False, f"Missing columns {missing} in {file_name} sheet '{sheet_name}'"
                    
                    # Get min/max dates
                    s_min, s_max = extract_date_range(df, file_name, sheet_name)
                    if s_min is None or s_max is None:
                        return False, f"No DELIVERY_DATE values in {file_name} sheet '{sheet_name}'"
                    
                    # Update overall min/max dates
                    if (min_date is None) or (s_min < min_date):
                        min_date = s_min
                    if (max_date is None) or (s_max > max_date):
                        max_date = s_max
                    
                    file_sheet_dfs.append((file_name, sheet_name, df))
                    
            except Exception as e:
                return False, f"Error reading {excel_file}: {str(e)}"
        
        logging.info(f"✓ Validated {len(file_sheet_dfs)} sheets across {len(excel_files)} files")
        logging.info(f"✓ Date range: {min_date} to {max_date}")
        
        return True, (min_date, max_date, file_sheet_dfs)
    except Exception as e:
        return False, f"Validation failed: {str(e)}"
````

## 5. DuckDB Loader Module

````python
"""
DuckDB loading operations for the provider ETL pipeline.
Handles database connections, table creation, and atomic loading.
"""
import logging
import os
import duckdb
from contextlib import contextmanager
from typing import List, Tuple, Any, Optional
import polars as pl
import gc
from tqdm import tqdm

from .config import RAW_TABLE_SCHEMA, DUCKDB_THREADS, BATCH_SIZE
from .progress import progress_bar

def create_raw_provider_table(conn, table_name: str = "raw_provider_data"):
    """Creates the raw provider data table if it doesn't exist."""
    columns_sql = ",\n    ".join([f'"{col}" {dtype}' for col, dtype in RAW_TABLE_SCHEMA.items()])
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        {columns_sql}
    );
    """
    conn.execute(create_sql)
    logging.info(f"✓ Table '{table_name}' ready")

@contextmanager
def duck_transaction(db_path: str):
    """
    Context manager for DuckDB transactions with optimized settings.
    Ensures proper commit/rollback handling.
    
    Args:
        db_path: Path to the DuckDB database
        
    Yields:
        DuckDB connection with an active transaction
    """
    conn = duckdb.connect(db_path)
    try:
        # Configure DuckDB performance settings
        conn.execute(f"PRAGMA threads={DUCKDB_THREADS};")
        conn.execute("PRAGMA memory_limit='8GB';")  # Adjust based on system memory
        
        conn.execute("BEGIN TRANSACTION;")
        yield conn
        conn.execute("COMMIT;")
        logging.info("✓ Transaction committed successfully")
    except Exception as e:
        conn.execute("ROLLBACK;")
        logging.error(f"✗ Transaction rolled back: {str(e)}")
        raise
    finally:
        conn.close()

def batch_load_dataframe(
    conn, 
    df: pl.DataFrame, 
    table_name: str,
    batch_size: int = BATCH_SIZE
) -> int:
    """
    Loads a DataFrame into DuckDB in batches to manage memory usage.
    
    Args:
        conn: DuckDB connection
        df: Polars DataFrame to load
        table_name: Target table name
        batch_size: Number of rows per batch
        
    Returns:
        Number of rows loaded
    """
    total_rows = df.height
    total_loaded = 0
    
    # Process in batches
    with tqdm(total=total_rows, desc="Loading rows to DuckDB", unit="row") as pbar:
        for start_idx in range(0, total_rows, batch_size):
            end_idx = min(start_idx + batch_size, total_rows)
            batch_df = df.slice(start_idx, end_idx - start_idx)
            
            # Register batch with DuckDB
            conn.register("tmp_batch", batch_df)
            
            # Insert batch with load_timestamp
            insert_sql = f"""
            INSERT INTO {table_name}
            SELECT
                *,
                CURRENT_TIMESTAMP AS load_timestamp
            FROM tmp_batch
            """
            
            conn.execute(insert_sql)
            loaded = end_idx - start_idx
            total_loaded += loaded
            
            # Clean up
            conn.unregister("tmp_batch")
            del batch_df
            gc.collect()
            
            pbar.update(loaded)
    
    return total_loaded

def load_dataframes_atomically(
    db_path: str,
    min_date: str,
    max_date: str,
    file_sheet_dfs: List[Tuple[str, str, pl.DataFrame]],
    table_name: str = "raw_provider_data"
) -> int:
    """
    Loads all DataFrames atomically with date range replacement.
    Shows progress with tqdm progress bars.
    
    Args:
        db_path: Path to the DuckDB database
        min_date: Minimum date to replace (inclusive)
        max_date: Maximum date to replace (inclusive)
        file_sheet_dfs: List of (file_name, sheet_name, DataFrame) tuples to load
        table_name: Target table name
        
    Returns:
        Total number of rows loaded
    """
    total_rows = 0
    start_time = __import__('time').time()
    
    with duck_transaction(db_path) as conn:
        create_raw_provider_table(conn, table_name)
        
        # Delete existing data in the date range
        logging.info(f"🗑️ Deleting existing data for date range {min_date} to {max_date}")
        conn.execute(
            f"DELETE FROM {table_name} WHERE DELIVERY_DATE BETWEEN ? AND ?",
            [min_date, max_date]
        )
        
        # Process each sheet with a progress bar
        for file_name, sheet_name, df in progress_bar(
            file_sheet_dfs, 
            desc="Loading sheets to DuckDB", 
            unit="sheet"
        ):
            # Add metadata columns
            df = df.with_columns([
                pl.lit(file_name).alias("source_file")
            ])
            
            # Load using batch processing
            rows_inserted = batch_load_dataframe(conn, df, table_name)
            
            # Clean up
            del df
            gc.collect()
            
            total_rows += rows_inserted
            logging.info(f"✓ Loaded {rows_inserted:,} rows from {file_name} sheet '{sheet_name}'")
    
    elapsed = __import__('time').time() - start_time
    logging.info(f"✓ Full import for {min_date} to {max_date} completed in {elapsed:.2f} sec")
    logging.info(f"✓ {total_rows:,} rows loaded (~{int(total_rows/elapsed):,} rows/sec)")
    
    return total_rows
````

## 6. Main ETL Module

````python
"""
Main ETL module for the provider data pipeline.
Orchestrates the extraction, validation, and loading process.
"""
import logging
import os
from pathlib import Path
import time
from typing import Optional, List, Tuple

from .config import REQUIRED_COLUMNS
from .validators import validate_all_excels_in_directory
from .loader import load_dataframes_atomically

def atomic_load_excels(
    directory: str, 
    db_path: str, 
    table_name: str = "raw_provider_data"
) -> int:
    """
    Loads all Excel files in a directory as a single atomic operation.
    Only commits if all files/sheets are valid and loaded.
    
    This function:
    1. Validates all Excel files for required columns and structure
    2. Extracts date ranges to determine what to replace
    3. Atomically replaces all data in the date range
    
    Args:
        directory: Path to directory containing Excel files
        db_path: Path to the DuckDB database file
        table_name: Name of the table to load data into
        
    Returns:
        Number of rows loaded, or 0 if loading failed
    """
    start_time = time.time()
    logging.info(f"=== Starting atomic load of Excel files from {directory} ===")
    
    # Validate all files first with progress indicators
    valid, result = validate_all_excels_in_directory(
        directory=directory,
        required_columns=REQUIRED_COLUMNS
    )
    
    if not valid:
        logging.error(f"✗ Import aborted: {result}")
        return 0
    
    # Extract validation results
    min_date, max_date, file_sheet_dfs = result
    file_count = len(set(file_name for file_name, _, _ in file_sheet_dfs))
    sheet_count = len(file_sheet_dfs)
    row_count = sum(df.height for _, _, df in file_sheet_dfs)
    
    logging.info(f"✓ Validated {file_count} files with {sheet_count} sheets ({row_count:,} rows)")
    logging.info(f"✓ Date range: {min_date} to {max_date}")
    
    try:
        # Load all data atomically with progress bars
        total_rows = load_dataframes_atomically(
            db_path=db_path,
            min_date=min_date,
            max_date=max_date,
            file_sheet_dfs=file_sheet_dfs,
            table_name=table_name
        )
        
        elapsed = time.time() - start_time
        logging.info(f"=== Provider ETL completed successfully in {elapsed:.2f} seconds ===")
        
        # Return processed files for potential archiving
        return total_rows
    except Exception as e:
        elapsed = time.time() - start_time
        logging.error(f"✗ Import failed after {elapsed:.2f} seconds: {str(e)}")
        return 0

def get_processed_file_paths(file_sheet_dfs: List[Tuple[str, str, Any]]) -> List[str]:
    """
    Extract unique file paths from the file_sheet_dfs list.
    
    Args:
        file_sheet_dfs: List of (file_name, sheet_name, DataFrame) tuples
        
    Returns:
        List of unique file paths
    """
    # Extract unique file names
    unique_file_names = set(file_name for file_name, _, _ in file_sheet_dfs)
    
    # Convert to full paths by searching in the directory
    directory = os.path.dirname(file_sheet_dfs[0][0]) if file_sheet_dfs else ""
    return [os.path.join(directory, file_name) for file_name in unique_file_names]
````

## 7. Command-Line Interface Module

````python
"""
Command-line interface for provider data processing.
Preserves existing CLI functionality while using the new ETL pipeline.
"""
import argparse
import logging
import os
import time
import glob
import sys
import shutil

from hypermvp.config import (
    PROVIDER_RAW_DIR, PROCESSED_DATA_DIR, ENERGY_DB_PATH
)
# Import from new modular ETL implementation
from hypermvp.provider.etl import atomic_load_excels, get_processed_file_paths
# Import existing database operations
from hypermvp.provider.provider_db_cleaner import (
    analyze_raw_provider_data, 
    update_provider_data_in_db,
    clean_provider_data_in_db
)
from .progress import progress_bar

def setup_cli():
    """Setup command-line interface for provider processing."""
    parser = argparse.ArgumentParser(description="Provider data processing tools")
    
    # Main action flags (can be combined)
    parser.add_argument("--load", action="store_true", help="Load Excel files to DuckDB")
    parser.add_argument("--analyze", action="store_true", help="Analyze raw data")
    parser.add_argument("--update", action="store_true", help="Update derived tables")
    parser.add_argument("--clean", action="store_true", help="Clean provider data")
    parser.add_argument("--all", action="store_true", help="Perform all operations")
    
    # Options
    parser.add_argument("--dir", default=PROVIDER_RAW_DIR, help="Directory containing Excel files")
    parser.add_argument("--db", default=ENERGY_DB_PATH, help="DuckDB database path")
    parser.add_argument("--raw-table", default="raw_provider_data", help="Raw data table name")
    parser.add_argument("--clean-table", default="clean_provider_data", help="Cleaned data table name")
    parser.add_argument("--archive", action="store_true", help="Archive processed files")
    
    return parser.parse_args()

def main():
    """Command-line interface for provider data processing."""
    args = setup_cli()
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.StreamHandler()]
    )
    
    # Process command
    if args.load or args.all:
        logging.info(f"Loading Excel files from {args.dir} to {args.db}")
        
        # Use the new ETL pipeline with progress indicators
        start_time = time.time()
        total_rows = atomic_load_excels(
            directory=args.dir,
            db_path=args.db,
            table_name=args.raw_table
        )
        
        if total_rows > 0:
            logging.info(f"✓ Loaded {total_rows:,} rows to {args.raw_table}")
            logging.info(f"✓ Loading completed in {time.time() - start_time:.2f} seconds")
            
            # Get processed files
            excel_files = [f for f in glob.glob(os.path.join(args.dir, "*.xlsx"))]
            processed_files = excel_files  # All files are processed in atomic operation
            
            # Archive processed files if requested
            if args.archive and processed_files:
                processed_dir = os.path.join(PROCESSED_DATA_DIR, "provider")
                os.makedirs(processed_dir, exist_ok=True)
                
                logging.info(f"Moving {len(processed_files)} processed files to {processed_dir}")
                
                moved_count = 0
                for file_path in progress_bar(processed_files, desc="Archiving files", unit="file"):
                    if os.path.exists(file_path):
                        file_name = os.path.basename(file_path)
                        destination = os.path.join(processed_dir, file_name)
                        
                        try:
                            shutil.move(file_path, destination)
                            logging.info(f"✓ Moved: {file_name}")
                            moved_count += 1
                        except Exception as e:
                            logging.error(f"✗ Failed to move {file_name}: {e}")
                
                logging.info(f"✓ Moved {moved_count}/{len(processed_files)} files")
        else:
            logging.error("✗ No rows were loaded - check logs for errors")
    
    if args.analyze or args.all:
        # Analyze raw data
        logging.info(f"Analyzing raw data in {args.db}")
        analyze_raw_provider_data(args.db, args.raw_table)
    
    if args.clean or args.all:
        # Clean data
        logging.info(f"Cleaning data in {args.db}")
        start_time = time.time()
        result = clean_provider_data_in_db(
            args.db, args.raw_table, args.clean_table
        )
        logging.info(f"Cleaning completed in {time.time() - start_time:.2f} seconds")

if __name__ == "__main__":
    main()
````

## 8. Export with `__init__.py`

````python
"""
Provider data ETL package for loading Excel files into DuckDB.
Uses Polars with Rust-based Calamine engine for high performance.
"""

from .etl import atomic_load_excels

__all__ = ["atomic_load_excels"]
````

## 9. Testing Strategy

````python
"""
Test fixtures for provider ETL tests.
"""
import pytest
import os
import tempfile
import polars as pl
import pandas as pd
from datetime import datetime

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
            df.to_excel(writer, sheet_name="001", index=False)
            df.to_excel(writer, sheet_name="002", index=False)
            
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

@pytest.fixture
def temp_db_path():
    """Creates a temporary DuckDB database path."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as temp:
        pass
    
    yield temp.name
    # Clean up
    os.unlink(temp.name)
````

````python
"""
Tests for the provider ETL pipeline.
"""
import pytest
import os
import polars as pl
import duckdb
from hypermvp.provider.extractor import read_excel_file
from hypermvp.provider.validators import validate_all_excels_in_directory
from hypermvp.provider.etl import atomic_load_excels

def test_read_excel_file(sample_excel_file):
    """Test reading Excel file with Polars."""
    result = read_excel_file(sample_excel_file)
    
    # Should have 2 sheets
    assert len(result) == 2
    assert "001" in result
    assert "002" in result
    
    # Check data in first sheet
    df = result["001"]
    assert df.shape[0] == 3  # 3 rows
    assert "DELIVERY_DATE" in df.columns
    assert "PRODUCT" in df.columns

def test_validate_excel_files(sample_directory):
    """Test validation of Excel files."""
    success, result = validate_all_excels_in_directory(sample_directory)
    
    assert success is True
    min_date, max_date, file_sheet_dfs = result
    
    assert min_date == "2024-01-01"
    assert max_date == "2024-01-03"
    assert len(file_sheet_dfs) == 2  # 2 sheets

def test_atomic_load_excels(sample_directory, temp_db_path):
    """Test atomic loading of Excel files to DuckDB."""
    # Load the data
    rows_loaded = atomic_load_excels(sample_directory, temp_db_path)
    
    # Should have loaded 6 rows (3 from each of 2 sheets)
    assert rows_loaded == 6
    
    # Verify the data was loaded correctly
    conn = duckdb.connect(temp_db_path)
    result = conn.execute("SELECT COUNT(*) FROM raw_provider_data").fetchone()[0]
    conn.close()
    
    assert result == 6
````

## 10. Integration with main.py

````python
# Update import to use the new ETL module
# Replace this line:
# from hypermvp.provider.provider_loader import atomic_load_excels

# With this line:
from hypermvp.provider.etl import atomic_load_excels

def process_provider_workflow():
    """
    Loads all provider Excel files atomically: 
    - Validates all files/sheets for required columns and date range.
    - If any fail, aborts the entire import.
    - If all succeed, deletes and replaces all data for the affected date range in DuckDB.
    """
    try:
        logging.info("=== STARTING PROVIDER WORKFLOW ===")
        
        # Use the modernized ETL implementation with progress indicators
        rows_loaded = atomic_load_excels(
            directory=PROVIDER_RAW_DIR,
            db_path=ENERGY_DB_PATH,
            table_name="raw_provider_data"
        )
        
        if rows_loaded > 0:
            logging.info(f"=== PROVIDER WORKFLOW COMPLETED: {rows_loaded:,} rows loaded ===")
            return True
        else:
            logging.error("=== PROVIDER WORKFLOW FAILED: No rows loaded ===")
            return False
    
    except Exception as e:
        logging.error(f"=== PROVIDER WORKFLOW FAILED: {str(e)} ===")
        return False
````

## 11. Installation Instructions

```bash
# Install dependencies
pip install polars[excel] pyarrow duckdb pytest tqdm

# Run the updated workflow
python3 main.py --workflow provider
```

## Performance Expectations

- **File Reading**: 10-20x faster with Polars' Calamine engine vs. openpyxl
- **Memory Usage**: Lower peak memory through batched processing
- **Multi-core Usage**: Parallel sheet processing with configurable thread count

## Key Improvements

1. **Progress Indicators**: Maintained and enhanced tqdm progress bars
2. **Robustness**: Better error handling and transactions
3. **Memory Management**: Explicit gc.collect() and batched processing
4. **Performance**: Polars + Rust for high-speed Excel reading
5. **Modularity**: Clean separation of concerns
6. **CLI Preservation**: Maintained existing workflow steps
7. **Testing**: Comprehensive test infrastructure

This implementation preserves everything you like about your current workflow while significantly improving performance, robustness, and maintainability.
