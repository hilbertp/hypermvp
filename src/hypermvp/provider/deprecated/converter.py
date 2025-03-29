"""
Excel to CSV Converter for Provider Data

This module converts large Excel files to CSV format for faster processing.
CSV files are typically processed 5-10x faster than Excel files and use significantly less memory.

IMPORTANT: This module is designed to be integrated with the provider workflow:
1. Convert XLSX to CSV (this module)
2. Load CSV data
3. Clean data
4. Update database

Performance benefits:
- Reduces load time from minutes to seconds
- Reduces memory usage by 50-80%
- Enables more efficient data processing
"""

import os
import time
import glob
import logging
import pandas as pd
import threading
import sys
from concurrent.futures import ThreadPoolExecutor
from hypermvp.config import (
    PROVIDER_RAW_DIR,
    ISO_DATETIME_FORMAT,
    ISO_DATE_FORMAT,
    standardize_date_column
)

def convert_sheet_to_csv(excel_file, sheet_name, output_dir):
    """Convert a single Excel sheet to CSV file with progress indicator."""
    file_base = os.path.splitext(os.path.basename(excel_file))[0]
    csv_file = os.path.join(output_dir, f"{file_base}_{sheet_name}.csv")
    
    # Skip if CSV already exists and is newer than the Excel file
    if os.path.exists(csv_file):
        csv_mtime = os.path.getmtime(csv_file)
        excel_mtime = os.path.getmtime(excel_file)
        if csv_mtime > excel_mtime:
            logging.info(f"Skipping existing up-to-date CSV: {os.path.basename(csv_file)}")
            return csv_file, 0
    
    # Setup progress indicator
    stop_indicator = threading.Event()
    
    def show_progress():
        start = time.time()
        i = 0
        indicators = "|/-\\"
        while not stop_indicator.is_set():
            elapsed = time.time() - start
            mins, secs = divmod(int(elapsed), 60)
            progress_msg = f"\rReading sheet {sheet_name}... {mins:02d}:{secs:02d} elapsed {indicators[i % len(indicators)]}"
            sys.stdout.write(progress_msg)
            sys.stdout.flush()
            i += 1
            time.sleep(0.5)
        sys.stdout.write("\r" + " " * 50 + "\r")
        sys.stdout.flush()
    
    # Start progress thread
    progress_thread = threading.Thread(target=show_progress)
    progress_thread.daemon = True
    progress_thread.start()
    
    try:
        # Read the Excel sheet
        start_time = time.time()
        df = pd.read_excel(excel_file, sheet_name=sheet_name)
        rows = len(df)
        
        # ADDED: Standardize DELIVERY_DATE before writing to CSV
        if "DELIVERY_DATE" in df.columns:
            df = standardize_date_column(df, "DELIVERY_DATE")
            logging.info(f"Standardized DELIVERY_DATE column in sheet {sheet_name}")
        
        # Stop progress indicator
        stop_indicator.set()
        if progress_thread.is_alive():
            progress_thread.join(timeout=1.0)
        
        read_time = time.time() - start_time
        logging.info(f"Read sheet '{sheet_name}' with {rows:,} rows in {read_time:.2f} seconds")
        
        # Write to CSV
        csv_start = time.time()
        df.to_csv(csv_file, index=False)
        csv_time = time.time() - csv_start
        
        total_time = time.time() - start_time
        logging.info(f"✓ Converted to CSV: {os.path.basename(csv_file)} in {csv_time:.2f} seconds (total: {total_time:.2f}s)")
        
        return csv_file, rows
    except Exception as e:
        stop_indicator.set()
        if progress_thread.is_alive():
            progress_thread.join(timeout=1.0)
        logging.error(f"Error converting sheet '{sheet_name}': {e}")
        raise
    
def convert_excel_to_csv(excel_file, output_dir=None):
    """Convert an Excel file to CSV files (one per sheet)."""
    if output_dir is None:
        # Use same directory as Excel file but in 'csv' subdirectory
        excel_dir = os.path.dirname(excel_file)
        output_dir = os.path.join(excel_dir, "csv")
    
    os.makedirs(output_dir, exist_ok=True)
    
    file_size_mb = os.path.getsize(excel_file) / (1024 * 1024)
    file_name = os.path.basename(excel_file)
    logging.info(f"Converting {file_name} ({file_size_mb:.2f} MB) to CSV")
    
    # Get sheet names
    start_time = time.time()
    xlsx = pd.ExcelFile(excel_file)
    sheet_names = xlsx.sheet_names
    logging.info(f"Found {len(sheet_names)} sheets: {', '.join(sheet_names)}")
    
    # Convert each sheet to CSV
    csv_files = []
    total_rows = 0
    
    for i, sheet_name in enumerate(sheet_names, 1):
        logging.info(f"Processing sheet {i}/{len(sheet_names)}: {sheet_name}")
        csv_file, rows = convert_sheet_to_csv(excel_file, sheet_name, output_dir)
        csv_files.append(csv_file)
        total_rows += rows
    
    total_time = time.time() - start_time
    mins, secs = divmod(int(total_time), 60)
    logging.info(f"Conversion complete: {total_rows:,} total rows across {len(csv_files)} CSV files in {mins:02d}:{secs:02d}")
    
    return csv_files

def convert_all_provider_xlsx(directory=None, output_dir=None):
    """Convert all provider XLSX files to CSV format."""
    # MODIFIED: Removed redundant import by using the one at top of file
    
    if directory is None:
        directory = PROVIDER_RAW_DIR
    
    if output_dir is None:
        output_dir = os.path.join(directory, "csv")
    
    os.makedirs(output_dir, exist_ok=True)
    
    # Find all Excel files
    xlsx_pattern = os.path.join(directory, "*.xlsx")
    excel_files = glob.glob(xlsx_pattern)
    
    if not excel_files:
        logging.warning(f"No Excel files found in {directory}")
        return []
    
    logging.info(f"Found {len(excel_files)} Excel files to convert")
    for i, excel_file in enumerate(excel_files, 1):
        file_name = os.path.basename(excel_file)
        file_size_mb = os.path.getsize(excel_file) / (1024 * 1024)
        logging.info(f"{i}. {file_name} ({file_size_mb:.2f} MB)")
    
    # Process each Excel file
    all_csv_files = []
    for excel_file in excel_files:
        csv_files = convert_excel_to_csv(excel_file, output_dir)
        all_csv_files.extend(csv_files)
    
    logging.info(f"Conversion complete: {len(all_csv_files)} CSV files created in {output_dir}")
    return all_csv_files

# Add this functionality to the main workflow
def integrated_convert_and_load(directory=None):
    """
    Convert XLSX to CSV and load the CSV data.
    Returns a DataFrame with all provider data using standardized datetime format.
    """
    # MODIFIED: Removed redundant import by using the one at top of file
    
    if directory is None:
        directory = PROVIDER_RAW_DIR
    
    csv_dir = os.path.join(directory, "csv")
    
    # First convert Excel files to CSV if needed
    convert_all_provider_xlsx(directory, csv_dir)
    
    # Then load from CSV (much faster than from Excel)
    csv_pattern = os.path.join(csv_dir, "*.csv")
    csv_files = glob.glob(csv_pattern)
    
    if not csv_files:
        logging.warning("No CSV files found after conversion")
        return pd.DataFrame()
    
    # Load CSV files
    logging.info(f"Loading {len(csv_files)} CSV files")
    all_dfs = []
    total_rows = 0
    
    for i, csv_file in enumerate(csv_files, 1):
        start = time.time()
        file_name = os.path.basename(csv_file)
        
        try:
            # MODIFIED: Use parse_dates parameter for consistent handling
            df = pd.read_csv(csv_file, parse_dates=["DELIVERY_DATE"])
            rows = len(df)
            total_rows += rows
            
            # MODIFIED: Use standardize_date_column from config
            if "DELIVERY_DATE" in df.columns:
                df = standardize_date_column(df, "DELIVERY_DATE")
                
                # Sample dates with ISO format for logging
                if not df.empty:
                    sample = df["DELIVERY_DATE"].iloc[0]
                    if hasattr(sample, 'strftime'):
                        sample_str = sample.strftime(ISO_DATETIME_FORMAT)
                        logging.info(f"Sample date from {file_name}: {sample_str}")
            
            all_dfs.append(df)
            elapsed = time.time() - start
            logging.info(f"Loaded CSV {i}/{len(csv_files)}: {file_name} with {rows:,} rows in {elapsed:.2f}s")
        except Exception as e:
            logging.error(f"Error loading CSV {file_name}: {e}")
    
    if not all_dfs:
        return pd.DataFrame()
    
    # Combine all dataframes
    result = pd.concat(all_dfs, ignore_index=True)
    
    # MODIFIED: Show date range with standardized format
    if "DELIVERY_DATE" in result.columns and not result.empty:
        min_date = result["DELIVERY_DATE"].min()
        max_date = result["DELIVERY_DATE"].max()
        min_date_str = min_date.strftime(ISO_DATETIME_FORMAT)
        max_date_str = max_date.strftime(ISO_DATETIME_FORMAT)
        logging.info(f"Date range in loaded data: {min_date_str} to {max_date_str}")
    
    logging.info(f"Successfully loaded {total_rows:,} rows from {len(csv_files)} CSV files")
    
    return result

if __name__ == "__main__":
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    
    import argparse
    parser = argparse.ArgumentParser(description="Convert provider Excel files to CSV")
    parser.add_argument("--dir", help="Directory with Excel files", default=None)
    parser.add_argument("--outdir", help="Output directory for CSV files", default=None)
    args = parser.parse_args()
    
    convert_all_provider_xlsx(args.dir, args.outdir)