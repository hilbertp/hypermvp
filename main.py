#!/usr/bin/env python3
"""
Main processing workflow for energy data.
This script:
  1. Provider workflow: Loads XLSX files directly to DuckDB and processes in-database.
  2. AFRR workflow: Loads CSV files, cleans data, saves directly to DuckDB.
  3. Analysis workflow: Calculates marginal prices and other analytics.
"""

import argparse
import logging
import os
import time
import subprocess
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
    AFRR_DUCKDB_PATH, PROVIDER_DUCKDB_PATH, ENERGY_DB_PATH
)

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
    """
    End-to-end workflow for processing provider data using direct database operations.
    
    This implementation follows the requirements in req_provider_flow.md, particularly
    the critical business logic for handling anonymized bids by using date range replacement.
    """
    try:
        import os
        import time
        import glob
        import subprocess
        
        logging.info("=== STARTING PROVIDER WORKFLOW ===")
        start = time.time()
        
        # Run the provider CLI as a module
        from hypermvp.config import PROVIDER_RAW_DIR, ENERGY_DB_PATH
        
        # 1. First load the data to DuckDB
        logging.info("=" * 60)
        logging.info("STEP 1: LOADING DATA TO DUCKDB")
        logging.info("=" * 60)
        
        load_cmd = [
            "python", "-m", "hypermvp.provider.provider_cli",
            "--load", 
            "--dir", PROVIDER_RAW_DIR,
            "--db", ENERGY_DB_PATH,
            "--verbose"
        ]
        
        load_result = subprocess.run(load_cmd, capture_output=True, text=True)
        for line in load_result.stdout.splitlines():
            logging.info(line)
        
        if load_result.returncode != 0:
            for line in load_result.stderr.splitlines():
                logging.error(line)
            raise RuntimeError("Data loading failed")
        
        # 2. Analyze the raw data
        logging.info("=" * 60)
        logging.info("STEP 2: ANALYZING RAW DATA")
        logging.info("=" * 60)
        
        analyze_cmd = [
            "python", "-m", "hypermvp.provider.provider_cli",
            "--analyze", 
            "--db", ENERGY_DB_PATH,
            "--verbose"
        ]
        
        analyze_result = subprocess.run(analyze_cmd, capture_output=True, text=True)
        for line in analyze_result.stdout.splitlines():
            logging.info(line)
        
        # 3. Update the data
        logging.info("=" * 60)
        logging.info("STEP 3: UPDATING DATA WITH DATE RANGE HANDLING")
        logging.info("=" * 60)
        
        update_cmd = [
            "python", "-m", "hypermvp.provider.provider_cli",
            "--update", 
            "--db", ENERGY_DB_PATH,
            "--verbose"
        ]
        
        update_result = subprocess.run(update_cmd, capture_output=True, text=True)
        for line in update_result.stdout.splitlines():
            logging.info(line)
        
        if update_result.returncode != 0:
            for line in update_result.stderr.splitlines():
                logging.error(line)
            raise RuntimeError("Data updating failed")
        
        # 4. Archive the processed files
        logging.info("=" * 60)
        logging.info("STEP 4: ARCHIVING FILES")
        logging.info("=" * 60)
        
        processed_provider_dir = os.path.join(PROCESSED_DATA_DIR, "provider")
        os.makedirs(processed_provider_dir, exist_ok=True)
        
        archive_cmd = [
            "python", "-m", "hypermvp.provider.provider_cli",
            "--load", 
            "--dir", PROVIDER_RAW_DIR,
            "--db", ENERGY_DB_PATH,
            "--archive",
            "--verbose"
        ]
        
        archive_result = subprocess.run(archive_cmd, capture_output=True, text=True)
        for line in archive_result.stdout.splitlines():
            logging.info(line)
        
        logging.info(f"Total workflow took {time.time() - start:.2f} seconds")
        logging.info("=== PROVIDER WORKFLOW COMPLETE ===")
    
    except Exception as e:
        logging.error(f"Workflow failed: {e}")
        raise

def process_afrr_workflow(month=None, year=None, file_path=None):
    """
    Process AFRR data workflow.
    
    Args:
        month (int, optional): Month to process (1-12). If None, extracts from filename.
        year (int, optional): Year to process. If None, extracts from filename.
        file_path (str, optional): Path to AFRR file. If None, uses config AFRR_FILE_PATH.
    """
    try:
        logging.info("=== STARTING AFRR WORKFLOW ===")
        start = time.time()
        
        # Create processed directory for AFRR if it doesn't exist
        from hypermvp.config import AFRR_RAW_DIR, PROCESSED_DATA_DIR
        import shutil
        import glob
        
        processed_afrr_dir = os.path.join(PROCESSED_DATA_DIR, "afrr")
        os.makedirs(processed_afrr_dir, exist_ok=True)

        # Determine which file(s) to process
        if file_path:
            file_paths = [file_path]
        elif month and year:
            # Look for files matching the pattern for specific month/year
            pattern = os.path.join(AFRR_RAW_DIR, f"*{year}*{month:02d}*.csv")
            file_paths = glob.glob(pattern)
            if not file_paths:
                logging.error(f"No AFRR files found for {year}-{month:02d}")
                return
        else:
            # Process all files in the raw directory
            file_paths = glob.glob(os.path.join(AFRR_RAW_DIR, "*.csv"))
            if not file_paths:
                logging.error(f"No AFRR files found in {AFRR_RAW_DIR}")
                return

        logging.info(f"Found {len(file_paths)} AFRR file(s) to process")
        
        # Process each file
        for current_file_path in file_paths:
            file_name = os.path.basename(current_file_path)
            logging.info(f"Processing file: {file_name}")
            
            # Extract month and year from filename if not provided
            if not (month and year):
                # Try to extract date info from filename
                # Assuming format like: ReglerleistungAbrufDeutschland_2024_09.csv
                match = re.search(r'(\d{4})_(\d{2})', file_name)
                if match:
                    extracted_year = int(match.group(1))
                    extracted_month = int(match.group(2))
                    logging.info(f"Extracted date: {extracted_year}-{extracted_month:02d}")
                else:
                    logging.warning(f"Could not extract date from filename: {file_name}")
                    # Use current month/year as fallback
                    now = datetime.now()
                    extracted_month = now.month
                    extracted_year = now.year
            else:
                extracted_month = month
                extracted_year = year

            # LOAD PHASE
            load_start = time.time()
            logging.info(f"Loading AFRR data from {current_file_path}")
            # Get the DataFrame directly
            afrr_data = load_afrr_data(current_file_path)
            # Add debug prints
            if isinstance(afrr_data, tuple):
                afrr_data = afrr_data[0]  # Extract just the DataFrame
            logging.info(f"Loaded AFRR data with {len(afrr_data)} rows in {time.time() - load_start:.2f} seconds")

            # CLEAN PHASE
            clean_start = time.time()
            cleaned_afrr_data = filter_negative_50hertz(afrr_data)
            logging.info(f"Cleaned AFRR data ({len(cleaned_afrr_data)} rows) in {time.time() - clean_start:.2f} seconds")

            # SAVE TO DUCKDB PHASE
            db_start = time.time()
            db_path = ENERGY_DB_PATH  # Updated from AFRR_DUCKDB_PATH
            os.makedirs(os.path.dirname(db_path), exist_ok=True)
            
            # Save with extracted month and year
            save_afrr_to_duckdb(cleaned_afrr_data, extracted_month, extracted_year, "afrr_data", db_path)
            logging.info(f"AFRR data saved to DuckDB at {db_path} for {extracted_year}-{extracted_month:02d} in {time.time() - db_start:.2f} seconds")

            # After database update is complete, move the file to processed directory
            logging.info("=== MOVING PROCESSED FILE TO ARCHIVE ===")
            if os.path.exists(current_file_path):
                destination = os.path.join(processed_afrr_dir, file_name)
                
                try:
                    shutil.move(current_file_path, destination)
                    logging.info(f"✓ File moved: {current_file_path} → {destination}")
                except Exception as e:
                    logging.error(f"✗ Failed to move file: {current_file_path} → {destination}")
                    logging.error(f"  Error: {str(e)}")
            else:
                logging.warning(f"! Source file not found: {current_file_path}")
        
        logging.info(f"Total workflow took {time.time() - start:.2f} seconds")
        logging.info("=== AFRR WORKFLOW COMPLETE ===")
    except Exception as e:
        logging.error(f"Workflow failed: {str(e)}")
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

def main():
    # Setup command-line argument parsing
    parser = argparse.ArgumentParser(
        description="Hypermvp Data Processing Workflows",
        epilog="Example: python main.py --workflow provider"
    )
    parser.add_argument(
        "--workflow",
        choices=["provider", "afrr", "analysis", "all", "visualize"],
        default="provider",
        help="Select processing workflow"
    )
    parser.add_argument(
        "--month",
        type=int,
        help="Month to process (1-12)",
        default=None
    )
    parser.add_argument(
        "--year",
        type=int,
        help="Year to process (e.g., 2024)",
        default=None
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
    parser.add_argument(
        "--file",
        help="Specific file to process (for AFRR workflow)",
        default=None
    )
    
    args = parser.parse_args()
    logging.info("Starting %s workflow", args.workflow.upper())
    
    if args.workflow == "provider":
        process_provider_workflow()
    elif args.workflow == "afrr":
        process_afrr_workflow(args.month, args.year, args.file)
    elif args.workflow == "analysis":
        process_analysis_workflow(args.start_date, args.end_date)
    elif args.workflow == "all":
        process_provider_workflow()
        process_afrr_workflow(args.month, args.year, args.file)
        process_analysis_workflow(args.start_date, args.end_date)
    elif args.workflow == "visualize":
        from hypermvp.analysis.plot_marginal_prices import plot_marginal_prices
        plot_marginal_prices(args.start_date)
        logging.info("Visualizations generated")

if __name__ == "__main__":
    main()