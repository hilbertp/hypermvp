#!/usr/bin/env python3
"""
Main processing workflow for provider data.
This script:
  1. Loads all provider list XLSX files from RAW_DATA_DIR.
  2. Cleans the data.
  3. Dumps the cleaned CSV to PROCESSED_DATA_DIR.
  4. Updates the provider data in the DuckDB database.
"""

import argparse
import logging
import os
import time
from datetime import datetime
import pandas as pd

# Import configuration
from config import DATA_DIR, RAW_DATA_DIR, PROCESSED_DATA_DIR, OUTPUT_DATA_DIR

# Import functions from our modules
from hypermvp.provider.loader import load_provider_file
from hypermvp.provider.cleaner import clean_provider_data
from hypermvp.provider.dump_csv import save_to_csv
from hypermvp.provider.update_provider_data import update_provider_data

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)

def process_provider_workflow():
    """End-to-end workflow for processing provider data with timing logs."""
    try:
        logging.info("=== STARTING PROVIDER WORKFLOW ===")
        start = time.time()

        # LOAD PHASE
        load_start = time.time()
        provider_dfs = []
        logging.info("Loading provider files from %s", RAW_DATA_DIR)
        for filename in os.listdir(RAW_DATA_DIR):
            if filename.endswith(".xlsx"):
                filepath = os.path.join(RAW_DATA_DIR, filename)
                logging.info("Loading file: %s", filepath)
                df = load_provider_file(filepath)
                provider_dfs.append(df)
        if not provider_dfs:
            logging.error("No provider files found in %s", RAW_DATA_DIR)
            return
        raw_data = pd.concat(provider_dfs, ignore_index=True)
        logging.info("Loaded %d records in %.2f seconds",
                     len(raw_data), time.time() - load_start)

        # CLEAN PHASE
        clean_start = time.time()
        cleaned_data = clean_provider_data(raw_data)
        logging.info("Cleaned data contains %d records in %.2f seconds",
                     len(cleaned_data), time.time() - clean_start)

        # CSV DUMP PHASE
        csv_start = time.time()
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_filename = f"provider_cleaned_{timestamp}.csv"
        csv_path = os.path.join(PROCESSED_DATA_DIR, csv_filename)
        save_to_csv(cleaned_data, PROCESSED_DATA_DIR, csv_filename)
        logging.info("CSV dumped to %s in %.2f seconds",
                     csv_path, time.time() - csv_start)

        # DATABASE UPDATE PHASE
        db_start = time.time()
        db_path = os.path.join(PROCESSED_DATA_DIR, "provider_data.duckdb")
        update_provider_data(RAW_DATA_DIR, db_path, "provider_data")
        logging.info("Database update complete at %s in %.2f seconds",
                     db_path, time.time() - db_start)

        logging.info("Total workflow took %.2f seconds", time.time() - start)
        logging.info("=== PROVIDER WORKFLOW COMPLETE ===")
    except Exception as e:
        logging.error("Workflow failed: %s", e)
        raise

def process_afrr_workflow():
    """Placeholder for AFRR processing workflow."""
    logging.warning("AFRR workflow not yet implemented")
    # Add your AFRR processing logic here

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