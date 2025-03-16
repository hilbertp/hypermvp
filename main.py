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

# Import configuration
from hypermvp.config import (
    DATA_DIR, RAW_DATA_DIR, PROCESSED_DATA_DIR, 
    OUTPUT_DATA_DIR, AFRR_FILE_PATH, DUCKDB_DIR,
    AFRR_DUCKDB_PATH, PROVIDER_DUCKDB_PATH  # Added PROVIDER_DUCKDB_PATH here
)

import sys
print(f"Config variables loaded: {dir(sys.modules['hypermvp.config'])}")

# Add after imports
from hypermvp import config
print("Available config variables:")
for item in dir(config):
    if not item.startswith("__"):
        print(f"- {item}")

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

        # LOAD PHASE: Use the loader module to read files once.
        load_start = time.time()
        raw_data = load_provider_data(RAW_DATA_DIR)
        logging.info("Loaded %d records in %.2f seconds",
                     len(raw_data), time.time() - load_start)

        # CLEAN PHASE: Clean the raw data.
        clean_start = time.time()
        # Ensure clean_provider_data returns a DataFrame!
        cleaned_data = clean_provider_data(raw_data)
        logging.info("Cleaned data contains %d records in %.2f seconds",
                     len(cleaned_data), time.time() - clean_start)

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
        db_path = PROVIDER_DUCKDB_PATH  # Make sure this is set correctly in config.py (e.g., data/03_output/provider_data.duckdb)
        # *** IMPORTANT: Pass the cleaned_data (a DataFrame), not RAW_DATA_DIR ***
        update_provider_data(cleaned_data, db_path, "provider_data")
        logging.info("Database update complete at %s in %.2f seconds",
                     db_path, time.time() - db_start)

        logging.info("Total workflow took %.2f seconds", time.time() - start)
        logging.info("=== PROVIDER WORKFLOW COMPLETE ===")
    except Exception as e:
        logging.error("Workflow failed: %s", e)
        raise

def process_afrr_workflow():
    try:
        logging.info("=== STARTING AFRR WORKFLOW ===")
        start = time.time()

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
        db_path = AFRR_DUCKDB_PATH
        os.makedirs(os.path.dirname(AFRR_DUCKDB_PATH), exist_ok=True)
        save_afrr_to_duckdb(cleaned_afrr_data, 9, 2024, "afrr_data", db_path)
        logging.info("AFRR data saved to DuckDB at %s in %.2f seconds", db_path, time.time() - db_start)

        logging.info("Total workflow took %.2f seconds", time.time() - start)
        logging.info("=== AFRR WORKFLOW COMPLETE ===")
    except Exception as e:
        logging.error("Workflow failed: %s", e)
        raise

def main():
    # Setup command-line argument parsing
    parser = argparse.ArgumentParser(
        description="Hypermvp Data Processing Workflows",
        epilog="Example: python main.py --workflow provider"
    )
    parser.add_argument(
        "--workflow",
        choices=["provider", "afrr"],
        default="provider",
        help="Select processing workflow"
    )
    
    args = parser.parse_args()
    logging.info("Starting %s workflow", args.workflow.upper())
    
    if args.workflow == "provider":
        process_provider_workflow()
    elif args.workflow == "afrr":
        process_afrr_workflow()

if __name__ == "__main__":
    main()