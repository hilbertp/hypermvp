"""
Provider CLI for provider data ETL workflow.

Usage examples:
    python -m src.hypermvp.provider.provider_cli --load --input-dir /path/to/xlsx --db-path /path/to/your.duckdb
    python -m src.hypermvp.provider.provider_cli --clean --db-path /path/to/your.duckdb
    python -m src.hypermvp.provider.provider_cli --all --input-dir /path/to/xlsx --db-path /path/to/your.duckdb
"""
import argparse
import sys
from pathlib import Path
from .provider_db_cleaner import clean_provider_table
from .etl import run_etl

def main():
    parser = argparse.ArgumentParser(description="Provider Data Workflow CLI")
    parser.add_argument("--load", action="store_true", help="Load provider Excel files into DuckDB")
    parser.add_argument("--clean", action="store_true", help="Clean provider table in DuckDB")
    parser.add_argument("--all", action="store_true", help="Run load and clean in sequence")
    parser.add_argument("--input-dir", type=str, help="Directory containing provider Excel files")
    parser.add_argument("--db-path", type=str, required=True, help="Path to DuckDB database")
    parser.add_argument("--log-level", type=str, default="INFO", help="Logging level")
    args = parser.parse_args()

    import logging
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), "INFO"))

    # If no action flag is set but input-dir and db-path are provided, default to --load for user-friendliness and test compatibility
    if not (args.load or args.clean or args.all):
        if args.input_dir and args.db_path:
            args.load = True

    if args.load or args.all:
        if not args.input_dir:
            print("Error: --input-dir is required for --load or --all")
            sys.exit(1)
        input_dir = Path(args.input_dir)
        if not input_dir.exists() or not input_dir.is_dir():
            print(f"Error: Input directory {input_dir} does not exist or is not a directory.")
            sys.exit(1)
        # Always create/connect to the DuckDB file for robustness, even if no Excel files are found
        import duckdb
        duckdb.connect(args.db_path).close()
        excel_files = list(input_dir.glob("*.xlsx"))
        if not excel_files:
            print(f"No Excel files found in {input_dir}")
            # Do not exit(1); the DB file is now created for downstream steps and test compatibility
            return
        print(f"Starting ETL process for {len(excel_files)} files...")
        summary = run_etl([str(f) for f in excel_files], db_path=args.db_path, table_name="provider_raw")
        print(f"ETL Summary: {summary}")

    if args.clean or args.all:
        if not Path(args.db_path).exists():
            print(f"Error: DuckDB database not found at {args.db_path}")
            sys.exit(1)
        clean_provider_table(args.db_path)
        print("Provider table cleaned and saved as 'provider_clean'.")

if __name__ == "__main__":
    main()
