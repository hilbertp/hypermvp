"""
Validators for Excel data in the provider ETL pipeline.
Contains functions to check headers, data types, and business rules.
"""
import logging
import os
from typing import Tuple, List, Optional, Dict, Any
import polars as pl
from datetime import datetime

from .provider_etl_config import REQUIRED_COLUMNS, standardize_polars_date_column
from .extractor import read_excel_file, find_excel_files
from .progress import progress_bar
from hypermvp.global_config import ISO_DATE_FORMAT

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
        
        # Try to standardize dates if they're in string format
        try:
            if isinstance(min_date, str):
                min_date = datetime.strptime(min_date, ISO_DATE_FORMAT).strftime(ISO_DATE_FORMAT)
            else:
                min_date = str(min_date)
                
            if isinstance(max_date, str):
                max_date = datetime.strptime(max_date, ISO_DATE_FORMAT).strftime(ISO_DATE_FORMAT)
            else:
                max_date = str(max_date)
        except ValueError:
            # If date parsing fails, just use the string representation
            min_date = str(min_date)
            max_date = str(max_date)
            logging.warning(f"Date format conversion failed in {file_name} sheet '{sheet_name}'")
        
        return min_date, max_date
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
                    if min_date is None or s_min < min_date:
                        min_date = s_min
                    if max_date is None or s_max > max_date:
                        max_date = s_max
                    
                    file_sheet_dfs.append((file_name, sheet_name, df))
                    
            except Exception as e:
                return False, f"Error reading {excel_file}: {str(e)}"
        
        logging.info(f"✓ Validated {len(file_sheet_dfs)} sheets across {len(excel_files)} files")
        logging.info(f"✓ Date range: {min_date} to {max_date}")
        
        return True, (min_date, max_date, file_sheet_dfs)
    except Exception as e:
        return False, f"Validation failed: {str(e)}"

def validate_sheets(
    df: pl.DataFrame,
    required_columns: List[str] = REQUIRED_COLUMNS,
    file_name: str = "",
    sheet_name: str = ""
) -> Tuple[bool, str]:
    """
    Validates a single DataFrame (sheet) for required columns.
    Returns (True, "") if valid, else (False, error_message).
    """
    valid, missing = validate_excel_columns(df, file_name, sheet_name, required_columns)
    if not valid:
        msg = f"Missing columns {missing} in {file_name} sheet '{sheet_name}'"
        return False, msg
    return True, ""