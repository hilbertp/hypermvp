#!/usr/bin/env python3
"""
Main processing workflow for energy data.
This script:
  1. Provider workflow: Loads XLSX files, cleans data, updates DuckDB directly.
  2. AFRR workflow: Loads CSV files, cleans data, saves directly to DuckDB.
"""

import argparse
import logging
import os
import time
from datetime import datetime
import pandas as pd
import re

# Add after your imports
import warnings
warnings.filterwarnings("ignore", category=UserWarning, 
                      message="Workbook contains no default style, apply openpyxl's default")

# Import configuration
from hypermvp.config import (
    DATA_DIR, RAW_DATA_DIR, PROCESSED_DATA_DIR, 
    OUTPUT_DATA_DIR, AFRR_FILE_PATH, DUCKDB_DIR,
    AFRR_DUCKDB_PATH, PROVIDER_DUCKDB_PATH, ENERGY_DB_PATH  # Added ENERGY_DB_PATH here
)

# Import provider modules
from hypermvp.provider.loader import load_provider_file, load_provider_data
from hypermvp.provider.cleaner import clean_provider_data
from hypermvp.provider.update_provider_data import update_provider_data

# Import AFRR modules
from hypermvp.afrr.loader import load_afrr_data
from hypermvp.afrr.cleaner import filter_negative_50hertz
from hypermvp.afrr.save_to_duckdb import save_afrr_to_duckdb

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)

def process_provider_workflow():
    """End-to-end workflow for processing provider data."""
    try:
        logging.info("=== STARTING PROVIDER WORKFLOW ===")
        start = time.time()

        # LOAD PHASE: Use the loader module to read files from PROVIDER_RAW_DIR
        load_start = time.time()
        from hypermvp.config import PROVIDER_RAW_DIR, PROCESSED_DATA_DIR
        import shutil
        
        logging.info("=" * 60)
        logging.info("LOADING DATA")
        logging.info("=" * 60)
        
        # Create processed directory for provider if it doesn't exist
        processed_provider_dir = os.path.join(PROCESSED_DATA_DIR, "provider")
        os.makedirs(processed_provider_dir, exist_ok=True)
        
        raw_data = load_provider_data(PROVIDER_RAW_DIR)  # Use the specific directory
        logging.info("Loaded %d records in %.2f seconds",
                     len(raw_data), time.time() - load_start)

        # After loading data, add more detailed progress info:
        if len(raw_data) > 0:
            logging.info(f"Loaded {len(raw_data):,} rows with columns: {', '.join(raw_data.columns.tolist())}")
            start_date = raw_data["DELIVERY_DATE"].min()
            end_date = raw_data["DELIVERY_DATE"].max()
            logging.info(f"Date range: {start_date} to {end_date}")
            
            products = raw_data["PRODUCT"].unique().tolist()
            logging.info(f"Products: {', '.join(products[:10])}{'...' if len(products) > 10 else ''}")

        # Get the list of files that were processed
        from hypermvp import config
        processed_files = config.PROVIDER_FILE_PATHS  # These were the files loaded
        
        # CLEAN PHASE: Clean the raw data.
        clean_start = time.time()
        logging.info("=" * 60)
        logging.info("CLEANING DATA")
        logging.info("=" * 60)
        logging.info("Cleaning data...")
        # Ensure clean_provider_data returns a DataFrame!
        cleaned_data = clean_provider_data(raw_data)
        logging.info(f"Cleaned data contains {len(cleaned_data):,} records in {time.time() - clean_start:.2f} seconds")

        # Force problematic columns to strings:
        if "TYPE_OF_RESERVES" in cleaned_data.columns:
            cleaned_data["TYPE_OF_RESERVES"] = cleaned_data["TYPE_OF_RESERVES"].astype(str)

        if "PRODUCT" in cleaned_data.columns:
            cleaned_data["PRODUCT"] = cleaned_data["PRODUCT"].astype(str)

        # (Add any other forced conversions for VARCHAR columns as needed)

        # ADD PERIOD COLUMN: Ensure the table schema includes the period column.
        cleaned_data["period"] = cleaned_data["DELIVERY_DATE"].dt.floor("4h")

        # DATABASE UPDATE PHASE: Update or create the DuckDB table.
        db_start = time.time()
        db_path = ENERGY_DB_PATH  # Updated from PROVIDER_DUCKDB_PATH
        logging.info("=" * 60)
        logging.info("UPDATING DATABASE")
        logging.info("=" * 60)

        # Add this to show total time periods being processed:
        total_periods = cleaned_data['period'].nunique()
        logging.info(f"Updating database at {db_path} with {len(cleaned_data):,} rows across {total_periods} time periods")
        logging.info("(Showing progress for only a subset of periods to avoid cluttering the output)")

        # *** IMPORTANT: Pass the cleaned_data (a DataFrame), not RAW_DATA_DIR ***
        update_provider_data(cleaned_data, db_path, "provider_data")
        logging.info("Database update complete at %s in %.2f seconds",
                     db_path, time.time() - db_start)

        # After database update is complete, move the files to processed directory
        logging.info("=" * 60)
        logging.info("ARCHIVING FILES")
        logging.info("=" * 60)
        logging.info("=== MOVING PROCESSED FILES TO ARCHIVE ===")
        for file_path in processed_files:
            if os.path.exists(file_path):
                file_name = os.path.basename(file_path)
                destination = os.path.join(processed_provider_dir, file_name)
                
                try:
                    shutil.move(file_path, destination)
                    logging.info(f"✓ File moved: {file_path} → {destination}")
                except Exception as e:
                    logging.error(f"✗ Failed to move file: {file_path} → {destination}")
                    logging.error(f"  Error: {str(e)}")
        
        # Count moved files
        moved_count = sum(1 for f in processed_files if not os.path.exists(f))
        logging.info(f"=== MOVED {moved_count}/{len(processed_files)} FILES ===")
                    
        logging.info("Total workflow took %.2f seconds", time.time() - start)
        logging.info("=== PROVIDER WORKFLOW COMPLETE ===")
    except Exception as e:
        logging.error("Workflow failed: %s", e)
        raise

def process_afrr_workflow():
    try:
        logging.info("=== STARTING AFRR WORKFLOW ===")
        start = time.time()
        
        # Create processed directory for AFRR if it doesn't exist
        from hypermvp.config import AFRR_RAW_DIR, PROCESSED_DATA_DIR, AFRR_FILE_PATH
        import shutil
        
        processed_afrr_dir = os.path.join(PROCESSED_DATA_DIR, "afrr")
        os.makedirs(processed_afrr_dir, exist_ok=True)

        # LOAD PHASE
        load_start = time.time()
        logging.info("Loading AFRR data from %s", AFRR_FILE_PATH)
        # Get the DataFrame directly
        afrr_data = load_afrr_data(AFRR_FILE_PATH)
        # Add debug prints
        print(f"Type of afrr_data: {type(afrr_data)}")
        if isinstance(afrr_data, tuple):
            print(f"Tuple contents: {len(afrr_data)} items")
            afrr_data = afrr_data[0]  # Extract just the DataFrame
        logging.info("Loaded AFRR data in %.2f seconds", time.time() - load_start)

        # CLEAN PHASE
        clean_start = time.time()
        cleaned_afrr_data = filter_negative_50hertz(afrr_data)
        logging.info("Cleaned AFRR data in %.2f seconds", time.time() - clean_start)

        # SAVE TO DUCKDB PHASE
        db_start = time.time()
        db_path = ENERGY_DB_PATH  # Updated from AFRR_DUCKDB_PATH
        os.makedirs(os.path.dirname(AFRR_DUCKDB_PATH), exist_ok=True)
        save_afrr_to_duckdb(cleaned_afrr_data, 9, 2024, "afrr_data", db_path)
        logging.info("AFRR data saved to DuckDB at %s in %.2f seconds", db_path, time.time() - db_start)

        # After database update is complete, move the file to processed directory
        logging.info("=== MOVING PROCESSED FILES TO ARCHIVE ===")
        if os.path.exists(AFRR_FILE_PATH):
            file_name = os.path.basename(AFRR_FILE_PATH)
            destination = os.path.join(processed_afrr_dir, file_name)
            
            try:
                shutil.move(AFRR_FILE_PATH, destination)
                logging.info(f"✓ File moved: {AFRR_FILE_PATH} → {destination}")
            except Exception as e:
                logging.error(f"✗ Failed to move file: {AFRR_FILE_PATH} → {destination}")
                logging.error(f"  Error: {str(e)}")
        else:
            logging.warning(f"! Source file not found: {AFRR_FILE_PATH}")
        
        # Verify the move
        if not os.path.exists(AFRR_FILE_PATH) and os.path.exists(os.path.join(processed_afrr_dir, file_name)):
            logging.info("=== FILE SUCCESSFULLY ARCHIVED ===")
        else:
            logging.warning("=== FILE ARCHIVING INCOMPLETE ===")
                
        logging.info("Total workflow took %.2f seconds", time.time() - start)
        logging.info("=== AFRR WORKFLOW COMPLETE ===")
    except Exception as e:
        logging.error("Workflow failed: %s", e)
        raise

def process_analysis_workflow(start_date="2024-09-01", end_date=None):
    """Calculate marginal prices and other analytics."""
    from hypermvp.analysis.marginal_price import calculate_marginal_prices, save_marginal_prices
    
    logging.info("=== STARTING ANALYSIS WORKFLOW ===")
    start = time.time()
    
    # Calculate marginal prices
    results = calculate_marginal_prices(start_date, end_date)
    
    # Save results to database
    if not results.empty:
        save_marginal_prices(results)
        logging.info(f"Saved {len(results)} marginal price calculations")
    
    logging.info("Total analysis took %.2f seconds", time.time() - start)
    logging.info("=== ANALYSIS WORKFLOW COMPLETE ===")

# Fix the main() function to handle all workflow options:
def main():
    # Setup command-line argument parsing
    parser = argparse.ArgumentParser(
        description="Hypermvp Data Processing Workflows",
        epilog="Example: python main.py --workflow provider"
    )
    parser.add_argument(
        "--workflow",
        choices=["provider", "afrr", "analysis", "all"],  # This line needs the updated choices
        default="provider",
        help="Select processing workflow"
    )
    parser.add_argument(
        "--start-date",
        default="2024-09-01",
        help="Start date for analysis (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--end-date",
        default=None,
        help="End date for analysis (YYYY-MM-DD)"
    )
    
    args = parser.parse_args()
    logging.info("Starting %s workflow", args.workflow.upper())
    
    if args.workflow == "provider":
        process_provider_workflow()
    elif args.workflow == "afrr":
        process_afrr_workflow()
    elif args.workflow == "analysis":
        process_analysis_workflow(args.start_date, args.end_date)
    elif args.workflow == "all":
        process_provider_workflow()
        process_afrr_workflow()
        process_analysis_workflow(args.start_date, args.end_date)
    # Optional enhancement (not required)
    elif args.workflow == "visualize":
        from hypermvp.analysis.plot_marginal_prices import plot_marginal_prices
        plot_marginal_prices(args.start_date)
        logging.info("Visualizations generated")

if __name__ == "__main__":
    main()