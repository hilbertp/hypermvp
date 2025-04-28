"""
DuckDB loading operations for the provider ETL pipeline.
Handles database connections, table creation, and atomic loading.
"""
import logging
import os
import duckdb
from contextlib import contextmanager
from typing import List, Tuple, Any, Optional

from .provider_etl_config import RAW_TABLE_SCHEMA

def create_raw_provider_table(conn, table_name: str = "raw_provider_data") -> None:
    """
    Creates the raw provider data table if it doesn't exist.
    
    Args:
        conn: DuckDB connection
        table_name: Name of the table to create
    """
    columns_sql = ",\n    ".join([f'"{col}" {dtype}' for col, dtype in RAW_TABLE_SCHEMA.items()])
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        {columns_sql}
    );
    """
    conn.execute(create_sql)
    logging.info(f"Ensured table '{table_name}' exists.")

@contextmanager
def duck_transaction(db_path: str):
    """
    Context manager for DuckDB transactions.
    Ensures proper commit/rollback handling.
    
    Args:
        db_path: Path to the DuckDB database
        
    Yields:
        DuckDB connection with an active transaction
    """
    conn = duckdb.connect(db_path)
    try:
        conn.execute("BEGIN TRANSACTION;")
        yield conn
        conn.execute("COMMIT;")
        logging.info("Transaction committed successfully")
    except Exception as e:
        conn.execute("ROLLBACK;")
        logging.error(f"Transaction rolled back: {e}")
        raise
    finally:
        conn.close()

def load_excel_files_to_duckdb(
    db_path: str,
    min_date: str,
    max_date: str,
    file_sheet_pairs: List[Tuple[str, str]],
    table_name: str = "raw_provider_data"
) -> int:
    """
    Loads all Excel files to DuckDB with proper date range handling.
    
    Args:
        db_path: Path to the DuckDB database
        min_date: Minimum date in the data (for date range replacement)
        max_date: Maximum date in the data (for date range replacement)
        file_sheet_pairs: List of (file_path, sheet_name) tuples to load
        table_name: Name of the table to load data into
        
    Returns:
        Total number of rows loaded
    """
    total_rows = 0
    
    with duck_transaction(db_path) as conn:
        # Create table if needed
        create_raw_provider_table(conn, table_name)
        
        # Delete existing data in the date range
        logging.info(f"Deleting existing data for date range {min_date} to {max_date}")
        conn.execute(
            f"DELETE FROM {table_name} WHERE DELIVERY_DATE BETWEEN ? AND ?",
            [min_date, max_date]
        )
        
        # Process each file/sheet
        for excel_file, sheet in file_sheet_pairs:
            file_name = os.path.basename(excel_file)
            logging.info(f"Loading {file_name} sheet '{sheet}'")
            
            # Use DuckDB's native read_excel function
            sql = f"""
            INSERT INTO {table_name}
            SELECT
                DELIVERY_DATE,
                PRODUCT,
                "ENERGY_PRICE_[EUR/MWh]",
                ENERGY_PRICE_PAYMENT_DIRECTION,
                "ALLOCATED_CAPACITY_[MW]",
                NOTE,
                '{file_name}' AS source_file,
                CURRENT_TIMESTAMP AS load_timestamp
            FROM read_excel('{excel_file}', sheet='{sheet}', header=TRUE)
            """
            
            try:
                res = conn.execute(sql)
                rows = res.rowcount if hasattr(res, "rowcount") else 0
                total_rows += rows
                logging.info(f"Loaded {rows:,} rows from {file_name} sheet '{sheet}'")
            except Exception as e:
                logging.error(f"Error loading {file_name} sheet '{sheet}': {e}")
                raise
                
    return total_rows