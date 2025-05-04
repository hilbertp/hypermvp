#!/usr/bin/env python3
import sys
import argparse
import logging
import os
import time
import subprocess
from datetime import datetime
import pandas as pd
import re
import glob
import duckdb
import threading
import queue
from tqdm import tqdm

"""
Main processing workflow for energy data.
This script:
  1. Provider workflow: Loads XLSX files directly to DuckDB and processes in-database.
  2. AFRR workflow: Loads CSV files, cleans data, saves directly to DuckDB.
  3. Analysis workflow: Calculates marginal prices and other analytics.
"""

# Add after your imports
import warnings
warnings.filterwarnings("ignore", category=UserWarning, 
                      message="Workbook contains no default style, apply openpyxl's default")

# Import configuration - update the list to include PROVIDER_RAW_DIR
from hypermvp.global_config import (
    DATA_DIR, RAW_DATA_DIR, PROCESSED_DATA_DIR, 
    OUTPUT_DATA_DIR, AFRR_FILE_PATH, DUCKDB_DIR,
    AFRR_DUCKDB_PATH, PROVIDER_DUCKDB_PATH, ENERGY_DB_PATH,
    PROVIDER_RAW_DIR
)

# Import AFRR modules
from hypermvp.afrr.loader import load_afrr_data
from hypermvp.afrr.cleaner import filter_negative_50hertz
from hypermvp.afrr.save_to_duckdb import save_afrr_to_duckdb

# Import provider loader
from hypermvp.provider.etl import run_etl
# Import provider cleaning logic
from hypermvp.provider.provider_db_cleaner import clean_provider_table

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)

def read_stdout(pipe, q):
    # Read lines from the pipe and put them into a queue
    for line in iter(pipe.readline, ''):
        q.put(line.strip())
    pipe.close()

def parse_number(num_str):
    """Parse a number that might have commas as thousand separators"""
    return int(num_str.replace(',', ''))

def process_provider_workflow():
    """
    Loads all provider Excel files from PROVIDER_RAW_DIR into DuckDB using the atomic ETL workflow.
    No NOTE column filtering or logging; all NOTE values are imported as-is.
    After loading, runs the provider table cleaning logic.
    """
    from pathlib import Path
    import polars as pl

    excel_files = list(Path(PROVIDER_RAW_DIR).glob("*.xlsx"))
    if not excel_files:
        logging.warning(f"No Excel files found in {PROVIDER_RAW_DIR}. Nothing to load.")
        return

    # No NOTE column cleaning or logging, just run the ETL
    logging.info(f"Found {len(excel_files):,} provider Excel files. Starting ETL...")
    summary = run_etl(
        [str(f) for f in excel_files],
        db_path=PROVIDER_DUCKDB_PATH,
        table_name="provider_raw"
    )
    files_processed = f"{summary['files_processed']:,}"
    sheets_loaded = f"{summary['sheets_loaded']:,}"
    rows_loaded = f"{summary['rows_loaded']:,}"
    logging.info(
        f"Provider ETL Summary: files_processed={files_processed}, "
        f"sheets_loaded={sheets_loaded}, rows_loaded={rows_loaded}, "
        f"errors={summary['errors']}"
    )
    # Run cleaning logic after ETL
    logging.info("Running provider table cleaning logic...")
    clean_provider_table(PROVIDER_DUCKDB_PATH)
    logging.info("Provider table cleaning complete.")

def main():
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