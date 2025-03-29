"""
Command-line interface for provider data processing.
"""

import argparse
import logging
import os
import time
import glob
import duckdb
import sys
from hypermvp.config import (
    PROVIDER_RAW_DIR, PROCESSED_DATA_DIR, ENERGY_DB_PATH,
    ISO_DATETIME_FORMAT, ISO_DATE_FORMAT
)
# Updated imports with the new module name
from hypermvp.provider.provider_loader import load_excel_to_duckdb
from hypermvp.provider.provider_db_cleaner import (
    analyze_raw_provider_data, 
    update_provider_data_in_db,
    clean_provider_data_in_db  # Add this import
)

import duckdb
duckdb.sql("INSTALL excel;")
duckdb.sql("LOAD excel;")

def main():
    """Command-line interface for provider data processing."""
    parser = argparse.ArgumentParser(
        description="Provider Data Processing",
        epilog="Example: python -m hypermvp.provider.provider_cli --load"
    )
    
    # Create a group for mutually exclusive commands
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--load", action="store_true", help="Load Excel files to DuckDB")
    group.add_argument("--analyze", action="store_true", help="Analyze raw data in DuckDB")
    group.add_argument("--clean", action="store_true", help="Clean raw data in DuckDB")
    group.add_argument("--update", action="store_true", help="Update data with date range handling")  # Add this option
    group.add_argument("--all", action="store_true", help="Load, analyze, and update data")  # Changed clean to update
    
    # Other options
    parser.add_argument("--dir", help="Directory with Excel files", default=None)
    parser.add_argument("--db", help="DuckDB database path", default=None)
    parser.add_argument("--raw-table", help="Raw data table name", default="raw_provider_data")
    parser.add_argument("--clean-table", help="Clean data table name", default="provider_data")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose logging")
    parser.add_argument("--archive", action="store_true", help="Archive processed files")
    
    args = parser.parse_args()
    
    # Configure logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        stream=sys.stdout,  # add this line
        level=log_level,
        format="%(asctime)s [%(levelname)s] %(message)s"
    )
    
    # Set default paths if not provided
    if args.dir is None:
        args.dir = PROVIDER_RAW_DIR
    
    if args.db is None:
        args.db = ENERGY_DB_PATH
    
    # Ensure output directories exist
    os.makedirs(os.path.dirname(args.db), exist_ok=True)
    
    # Process command
    if args.load or args.all:
        # Find Excel files
        excel_pattern = os.path.join(args.dir, "*.xlsx")
        excel_files = sorted(glob.glob(excel_pattern))
        
        if not excel_files:
            logging.warning(f"No Excel files found in {args.dir}")
            return
        
        # Log file information
        logging.info(f"Found {len(excel_files)} Excel files to process")
        for i, excel_file in enumerate(excel_files, 1):
            file_name = os.path.basename(excel_file)
            file_size_mb = os.path.getsize(excel_file) / (1024 * 1024)
            logging.info(f"{i}. {file_name} ({file_size_mb:.2f} MB)")
        
        # Load Excel files to DuckDB
        start_time = time.time()
        success_count, total_rows, processed_files = load_excel_to_duckdb(
            excel_files, args.db, args.raw_table
        )
        
        logging.info(f"Loaded {total_rows:,} rows from {success_count}/{len(excel_files)} files")
        logging.info(f"Loading completed in {time.time() - start_time:.2f} seconds")
        
        # Archive processed files if requested
        if args.archive and processed_files:
            processed_dir = os.path.join(PROCESSED_DATA_DIR, "provider")
            os.makedirs(processed_dir, exist_ok=True)
            
            logging.info(f"Moving {len(processed_files)} processed files to {processed_dir}")
            
            import shutil
            moved_count = 0
            for file_path in processed_files:
                if os.path.exists(file_path):
                    file_name = os.path.basename(file_path)
                    destination = os.path.join(processed_dir, file_name)
                    
                    try:
                        shutil.move(file_path, destination)
                        logging.info(f"✓ Moved: {file_name}")
                        moved_count += 1
                    except Exception as e:
                        logging.error(f"✗ Failed to move {file_name}: {e}")
            
            logging.info(f"Moved {moved_count}/{len(processed_files)} files")
        
        logging.info("Loaded")  # This line logs that loading is complete.
    
    if args.analyze or args.all:
        # Analyze raw data
        logging.info(f"Analyzing raw data in {args.db}")
        analyze_raw_provider_data(args.db, args.raw_table)
    
    if args.clean:
        # Clean data
        logging.info(f"Cleaning data in {args.db}")
        start_time = time.time()
        result = clean_provider_data_in_db(
            args.db, args.raw_table, args.clean_table
        )
        
        # Check if result is a tuple and extract just the row count
        if isinstance(result, tuple):
            # Extract the third element (rows_affected)
            clean_count = result[2]
        else:
            clean_count = result
            
        logging.info(f"Cleaning completed in {time.time() - start_time:.2f} seconds")
        logging.info(f"Created clean table '{args.clean_table}' with {clean_count:,} rows")
    
    if args.update or args.all:
        # Update data with date range handling
        logging.info(f"Updating data in {args.db} with date range handling")
        start_time = time.time()
        
        min_date, max_date, rows_updated = update_provider_data_in_db(
            args.db, args.raw_table, args.clean_table
        )
        
        if min_date and max_date:
            logging.info(f"Updated {rows_updated:,} rows for {min_date} to {max_date}")
            logging.info(f"Update completed in {time.time() - start_time:.2f} seconds")
        else:
            logging.error("Update failed")

if __name__ == "__main__":
    main()