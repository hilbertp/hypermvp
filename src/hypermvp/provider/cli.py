"""
Command Line Interface for Provider ETL Pipeline

This CLI allows you to:
- Load Excel files into DuckDB
- Analyze and validate raw data
- (Future) Clean and archive processed files

Plain English:
Run this script from the command line to process provider Excel files and load them into your DuckDB database.
"""

import argparse
import sys
import logging
from pathlib import Path

from .etl import run_etl

def main():
    parser = argparse.ArgumentParser(
        description="Provider ETL CLI: Load Excel files into DuckDB and validate data."
    )
    parser.add_argument(
        "--input-dir",
        type=str,
        required=True,
        help="Directory containing Excel files to process."
    )
    parser.add_argument(
        "--db-path",
        type=str,
        default="provider_data.duckdb",
        help="Path to DuckDB database file (default: provider_data.duckdb)"
    )
    parser.add_argument(
        "--table-name",
        type=str,
        default="provider_raw",
        help="DuckDB table name to load data into (default: provider_raw)"
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        help="Logging level (DEBUG, INFO, WARNING, ERROR)"
    )

    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), "INFO"),
        format="%(asctime)s %(levelname)s %(message)s"
    )

    input_dir = Path(args.input_dir)
    if not input_dir.exists() or not input_dir.is_dir():
        logging.error(f"Input directory '{input_dir}' does not exist or is not a directory.")
        sys.exit(1)

    # Find all Excel files in the input directory
    excel_files = list(input_dir.glob("*.xlsx"))
    if not excel_files:
        logging.warning(f"No Excel files found in '{input_dir}'. Nothing to do.")
        sys.exit(0)

    logging.info(f"Found {len(excel_files)} Excel files in '{input_dir}'. Starting ETL process...")

    summary = run_etl(
        [str(f) for f in excel_files],
        db_path=args.db_path,
        table_name=args.table_name
    )

    logging.info(f"ETL Summary: {summary}")

if __name__ == "__main__":
    main()