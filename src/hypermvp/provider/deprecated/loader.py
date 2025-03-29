import os
import pandas as pd
import logging
import time
import glob
import threading
import sys
from hypermvp.config import (
    PROVIDER_FILE_PATHS, 
    PROVIDER_RAW_DIR,
    ISO_DATETIME_FORMAT,
    ISO_DATE_FORMAT,
    standardize_date_column,
    AFRR_DATE_FORMAT
)

def load_provider_file(filepath):
    """Load a provider file (XLSX) and return a DataFrame with progress indicator."""
    logging.debug(f"Loading file: {filepath}")
    if not filepath.endswith(".xlsx"):
        raise ValueError(f"Unsupported file format: {filepath}")
    
    # Get file size for context
    file_size_mb = os.path.getsize(filepath) / (1024 * 1024)
    logging.info(f"Starting to load file ({file_size_mb:.2f} MB) - this may take several minutes...")
    
    # Create a progress indicator that updates every second
    stop_indicator = threading.Event()
    
    def show_progress():
        """Show a progress indicator with elapsed time."""
        start = time.time()
        i = 0
        indicators = "|/-\\"
        
        while not stop_indicator.is_set():
            elapsed = time.time() - start
            mins, secs = divmod(int(elapsed), 60)
            
            # Show elapsed time and a spinner
            progress_msg = f"\rLoading file... {mins:02d}:{secs:02d} elapsed {indicators[i % len(indicators)]}"
            sys.stdout.write(progress_msg)
            sys.stdout.flush()
            
            i += 1
            time.sleep(0.5)
        
        # Clear the line
        sys.stdout.write("\r" + " " * 50 + "\r")
        sys.stdout.flush()
    
    try:
        # Time the loading operation
        start_time = time.time()
        
        # First get the sheet names
        xlsx = pd.ExcelFile(filepath)
        sheet_names = xlsx.sheet_names
        logging.info(f"Found {len(sheet_names)} sheets in Excel file: {', '.join(sheet_names)}")
        
        # Based on observed data, it takes ~3 minutes per 1M rows
        total_estimate_minutes = int(file_size_mb / 35) * len(sheet_names)
        total_estimate_seconds = (total_estimate_minutes * 60) % 60
        logging.info(f"Estimated total loading time: ~{total_estimate_minutes:02d}:{total_estimate_seconds:02d}")
        
        # Load all sheets and combine them
        all_dataframes = []
        total_rows = 0
        
        for sheet_idx, sheet_name in enumerate(sheet_names, 1):
            sheet_start = time.time()
            
            # Estimate time for this sheet based on either:
            # 1. Previous sheet times if available
            # 2. File size and sheet count otherwise
            if sheet_idx == 1:
                # First sheet: estimate based on file size and sheet count
                est_sheet_time = (file_size_mb / 35) * 60  # seconds per sheet
            else:
                # Use the average of previous sheets
                previous_times = []
                for prev_df in all_dataframes:
                    rows_ratio = len(prev_df) / (total_rows or 1)  # Avoid div by zero
                    prev_time = (time.time() - start_time) * rows_ratio
                    previous_times.append(prev_time)
                est_sheet_time = sum(previous_times) / len(previous_times)
            
            est_sheet_mins = int(est_sheet_time // 60)
            est_sheet_secs = int(est_sheet_time % 60)
            
            logging.info(f"Loading sheet {sheet_idx}/{len(sheet_names)}: {sheet_name} (est. time: ~{est_sheet_mins:02d}:{est_sheet_secs:02d})")
            
            # Start progress thread for this sheet
            stop_indicator.set()  # Stop previous thread if running
            if 'progress_thread' in locals() and progress_thread.is_alive():
                progress_thread.join(timeout=1.0)
            
            stop_indicator = threading.Event()
            progress_thread = threading.Thread(target=show_progress)
            progress_thread.daemon = True
            progress_thread.start()
            
            # Load the sheet (this is the long-running operation)
            df_sheet = pd.read_excel(filepath, sheet_name=sheet_name)
            
            # Stop the progress indicator
            stop_indicator.set()
            if progress_thread.is_alive():
                progress_thread.join(timeout=1.0)
            
            rows = len(df_sheet)
            total_rows += rows
            all_dataframes.append(df_sheet)
            
            sheet_elapsed = time.time() - sheet_start
            mins, secs = divmod(int(sheet_elapsed), 60)
            
            logging.info(f"Sheet {sheet_name} loaded: {rows:,} rows in {mins:02d}:{secs:02d}")
            
            # Recalculate estimates for remaining sheets based on actual performance
            if sheet_idx < len(sheet_names):
                # Calculate new estimate for remaining sheets
                avg_time_per_row = sheet_elapsed / max(rows, 1)  # Avoid div by zero
                remaining_sheets = len(sheet_names) - sheet_idx
                
                # Estimate remaining time (assuming similar row counts)
                remaining_time = avg_time_per_row * rows * remaining_sheets
                est_mins, est_secs = divmod(int(remaining_time), 60)
                logging.info(f"Estimated time for remaining {remaining_sheets} sheets: ~{est_mins:02d}:{est_secs:02d}")
        
        # Combine all sheets
        df = pd.concat(all_dataframes, ignore_index=True)
        
        # Standardize DELIVERY_DATE column using the centralized helper
        if 'DELIVERY_DATE' in df.columns:
            df = standardize_date_column(df, 'DELIVERY_DATE')
            
        # Calculate and log total elapsed time
        elapsed = time.time() - start_time
        mins, secs = divmod(int(elapsed), 60)
        
        logging.info(f"File fully loaded: {total_rows:,} rows from {len(sheet_names)} sheets in {mins:02d}:{secs:02d}")
        
        # Check if we have DELIVERY_DATE column and show date range using standardized format
        if 'DELIVERY_DATE' in df.columns and not df.empty:
            if pd.api.types.is_datetime64_any_dtype(df['DELIVERY_DATE']):
                min_date = df['DELIVERY_DATE'].min()
                max_date = df['DELIVERY_DATE'].max()
                min_date_str = min_date.strftime(ISO_DATETIME_FORMAT)
                max_date_str = max_date.strftime(ISO_DATETIME_FORMAT)
                logging.info(f"Date range in file: {min_date_str} to {max_date_str}")
            else:
                logging.warning("DELIVERY_DATE column exists but is not datetime format")
        
        return df
    except Exception as e:
        # Make sure to stop progress indicator on error
        stop_indicator.set()
        if 'progress_thread' in locals() and progress_thread.is_alive():
            progress_thread.join(timeout=1.0)
        
        logging.error(f"Error loading Excel file: {e}")
        raise


def load_provider_data(directory=None):
    """
    Load all provider XLSX files from directory.
    """
    start_time = time.time()
    
    # Log the location we're checking
    if directory is None:
        directory = PROVIDER_RAW_DIR
        logging.info(f"Loading provider files from {directory}")
    else:
        logging.info(f"Loading provider files from custom directory: {directory}")
    
    # Add immediate feedback about file discovery process
    logging.info("Scanning directory for Excel files...")
    
    if directory is None:
        # Use files from the configuration
        file_paths = PROVIDER_FILE_PATHS
        logging.info(f"Using {len(file_paths)} files from configuration")
    else:
        # Find XLSX files in the specified directory
        logging.info(f"Searching for .xlsx files in {directory}")
        xlsx_pattern = os.path.join(directory, "*.xlsx")
        file_paths = glob.glob(xlsx_pattern)
        logging.info(f"Found {len(file_paths)} Excel files")
    
    # If no files found, return empty DataFrame
    if not file_paths:
        logging.warning(f"No provider XLSX files found in {directory}")
        return pd.DataFrame()
    
    # Log detailed file list before processing
    logging.info(f"Found {len(file_paths)} provider files to process:")
    for i, path in enumerate(file_paths, 1):
        file_size_mb = os.path.getsize(path) / (1024 * 1024)
        file_name = os.path.basename(path)
        logging.info(f"  {i}. {file_name} ({file_size_mb:.2f} MB)")
    
    logging.info(f"Starting file processing...")
    
    all_dfs = []
    total_files = len(file_paths)
    total_rows = 0
    
    # Process each file
    for i, file_path in enumerate(file_paths, 1):
        file_start = time.time()
        file_name = os.path.basename(file_path)
        
        logging.info(f"Loading file {i}/{total_files}: {file_name}")
        
        try:
            # Load the file
            df = load_provider_file(file_path)
            
            # Show some stats about the file
            file_rows = len(df)
            total_rows += file_rows
            
            # Get date range in the file if available using standardized format
            if 'DELIVERY_DATE' in df.columns and file_rows > 0:
                min_date = df['DELIVERY_DATE'].min()
                max_date = df['DELIVERY_DATE'].max()
                min_date_str = min_date.strftime(ISO_DATETIME_FORMAT)
                max_date_str = max_date.strftime(ISO_DATETIME_FORMAT)
                date_range = f"date range: {min_date_str} to {max_date_str}"
            else:
                date_range = "no date information available"
            
            # Add file to list
            all_dfs.append(df)
            
            # Log completion with statistics
            elapsed_sec = time.time() - file_start
            logging.info(f"✓ Loaded {file_name}: {file_rows:,} rows, {date_range} ({elapsed_sec:.2f} seconds)")
            
            # Show overall progress
            pct_complete = (i / total_files) * 100
            elapsed_total = time.time() - start_time
            estimated_total = (elapsed_total / i) * total_files
            remaining = estimated_total - elapsed_total
            
            if i < total_files:  # Don't show for the last file
                logging.info(f"Progress: {pct_complete:.1f}% complete, ~{remaining:.1f} seconds remaining")
            
        except Exception as e:
            logging.error(f"Error loading file {file_name}: {e}")
    
    # Combine all dataframes
    if not all_dfs:
        logging.warning("No data was loaded from any files")
        return pd.DataFrame()
    
    result = pd.concat(all_dfs, ignore_index=True)
    
    # Ensure DELIVERY_DATE is properly standardized in the final result
    if 'DELIVERY_DATE' in result.columns:
        result = standardize_date_column(result, 'DELIVERY_DATE')
    
    # Log summary after all files are processed
    elapsed_total = time.time() - start_time
    logging.info(f"=== PROVIDER DATA LOADING COMPLETE ===")
    logging.info(f"Loaded {total_rows:,} rows from {len(file_paths)} files in {elapsed_total:.2f} seconds")
    
    if 'DELIVERY_DATE' in result.columns and not result.empty:
        min_date = result['DELIVERY_DATE'].min()
        max_date = result['DELIVERY_DATE'].max()
        min_date_str = min_date.strftime(ISO_DATETIME_FORMAT)
        max_date_str = max_date.strftime(ISO_DATETIME_FORMAT)
        logging.info(f"Total date range: {min_date_str} to {max_date_str}")
    
    # Return the combined data
    return result


def load_afrr_data(file_path):
    """
    Load AFRR data from CSV file and standardize date columns.
    
    Args:
        file_path: Path to the AFRR CSV file
        
    Returns:
        DataFrame with standardized date columns
    """
    try:
        # Load the CSV file
        df = pd.read_csv(file_path, sep=';')
        
        # Standardize date columns
        if 'Datum' in df.columns:
            df = standardize_date_column(df, 'Datum', AFRR_DATE_FORMAT)
        
        return df
    except Exception as e:
        logging.error(f"Error loading AFRR data: {e}")
        return None
