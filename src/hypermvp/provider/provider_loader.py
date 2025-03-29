"""
Direct DuckDB Loader for Provider Data

This module provides functions to load Excel files directly into DuckDB
without going through pandas first, significantly improving performance.
"""

import os
import logging
import time
import duckdb

def load_excel_to_duckdb(excel_files, db_path, raw_table_name="raw_provider_data"):
    """
    Load Excel files directly to DuckDB without pandas intermediary.
    
    Args:
        excel_files: List of Excel file paths to load
        db_path: Path to DuckDB database
        raw_table_name: Name for the raw data table
        
    Returns:
        tuple: (success_count, total_rows, processed_files)
    """
    if not excel_files:
        logging.warning("No Excel files provided for loading")
        return 0, 0, []
    
    # Connect to DuckDB
    con = duckdb.connect(db_path)
    
    # Install and load Excel extension
    try:
        # First try installing the extension (might already be installed)
        con.execute("INSTALL excel")
        con.execute("LOAD excel")
        logging.info("Excel extension loaded successfully")
    except Exception as e:
        logging.error(f"Failed to load Excel extension: {e}")
        logging.error("Please install DuckDB Excel extension manually")
        con.close()
        return 0, 0, []
    
    # Create raw data table if it doesn't exist
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {raw_table_name} (
            DELIVERY_DATE VARCHAR,
            PRODUCT VARCHAR,
            "ENERGY_PRICE_[EUR/MWh]" VARCHAR,
            ENERGY_PRICE_PAYMENT_DIRECTION VARCHAR,
            "ALLOCATED_CAPACITY_[MW]" VARCHAR,
            NOTE VARCHAR,
            source_file VARCHAR,
            load_timestamp TIMESTAMP
        )
    """)
    
    # Track successful files and row count
    processed_files = []
    total_rows_loaded = 0
    success_count = 0
    
    # Process each Excel file
    for excel_file in excel_files:
        file_name = os.path.basename(excel_file)
        file_start = time.time()
        logging.info(f"Loading {file_name} to DuckDB")
        
        try:
            # Get Excel file size for metrics
            file_size_mb = os.path.getsize(excel_file) / (1024 * 1024)
            
            # Create a temporary view of the Excel file
            temp_view_name = f"temp_excel_view_{int(time.time())}"
            con.execute(f"""
                CREATE OR REPLACE TEMPORARY VIEW {temp_view_name} AS
                SELECT * FROM read_xlsx('{excel_file}')
            """)
            
            # Verify required columns exist
            required_columns = [
                "DELIVERY_DATE", "PRODUCT", "ENERGY_PRICE_[EUR/MWh]",
                "ENERGY_PRICE_PAYMENT_DIRECTION", "ALLOCATED_CAPACITY_[MW]", "NOTE"
            ]
            
            col_info = con.execute(f"DESCRIBE {temp_view_name}").df()
            actual_columns = col_info['column_name'].tolist()
            
            missing_columns = [col for col in required_columns if col not in actual_columns]
            if missing_columns:
                logging.warning(f"File {file_name} is missing columns: {missing_columns}")
                con.execute(f"DROP VIEW IF EXISTS {temp_view_name}")
                continue
            
            # Count total rows in source file
            source_rows = con.execute(f"SELECT COUNT(*) FROM {temp_view_name}").fetchone()[0]
            logging.info(f"Source file contains {source_rows:,} rows")
            
            # Insert data into raw table, keeping all rows (including POS_ products)
            # We'll filter during the cleaning phase
            insert_result = con.execute(f"""
                INSERT INTO {raw_table_name}
                SELECT 
                    "DELIVERY_DATE",
                    "PRODUCT",
                    "ENERGY_PRICE_[EUR/MWh]",
                    "ENERGY_PRICE_PAYMENT_DIRECTION",
                    "ALLOCATED_CAPACITY_[MW]",
                    "NOTE",
                    '{file_name}' AS source_file,
                    CURRENT_TIMESTAMP AS load_timestamp
                FROM {temp_view_name}
            """)
            
            rows_loaded = insert_result.fetchone()[0]
            total_rows_loaded += rows_loaded
            
            # Clean up the temporary view
            con.execute(f"DROP VIEW IF EXISTS {temp_view_name}")
            
            # Calculate loading metrics
            elapsed = time.time() - file_start
            load_speed = file_size_mb / max(elapsed, 0.001)  # MB per second
            
            logging.info(f"Loaded {rows_loaded:,} rows from {file_name} in {elapsed:.2f} seconds")
            logging.info(f"Loading speed: {load_speed:.2f} MB/s")
            
            # Track processed files
            processed_files.append(excel_file)
            success_count += 1
            
        except Exception as e:
            logging.error(f"Failed to load {file_name}: {e}")
            # Try to clean up any temporary objects
            try:
                con.execute(f"DROP VIEW IF EXISTS {temp_view_name}")
            except:
                pass
    
    # Get table statistics
    if total_rows_loaded > 0:
        stats = con.execute(f"""
            SELECT 
                COUNT(*) AS row_count,
                COUNT(DISTINCT source_file) AS file_count,
                MIN(load_timestamp) AS first_loaded,
                MAX(load_timestamp) AS last_loaded
            FROM {raw_table_name}
        """).fetchone()
        
        row_count, file_count, first_loaded, last_loaded = stats
        
        logging.info(f"Raw table '{raw_table_name}' contains {row_count:,} total rows from {file_count} files")
        logging.info(f"Data loaded between {first_loaded} and {last_loaded}")
    
    # Close the connection
    con.close()
    
    return success_count, total_rows_loaded, processed_files

# Import functions from provider_db_cleaner.py for backward compatibility
# This ensures existing code that uses provider_loader.py still works
from hypermvp.provider.provider_db_cleaner import (
    analyze_raw_provider_data,
    update_provider_data_in_db,
    clean_provider_data_in_db
)