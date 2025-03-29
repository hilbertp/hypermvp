"""
Database operations for provider data.

This module provides functions for analyzing, cleaning, and updating provider data
directly in the database, leveraging SQL for optimal performance.
"""

import os
import logging
import time
import duckdb

def analyze_raw_provider_data(db_path, raw_table="raw_provider_data"):
    """
    Analyze the raw provider data in the database.
    This is a helper function for examining the data before cleaning.
    
    Args:
        db_path: Path to DuckDB database
        raw_table: Name of the raw data table
    """
    # Connect to DuckDB
    con = duckdb.connect(db_path)
    
    # Check if table exists
    table_exists = con.execute(f"""
        SELECT count(*) FROM information_schema.tables 
        WHERE table_name = '{raw_table}'
    """).fetchone()[0]
    
    if not table_exists:
        logging.error(f"Table '{raw_table}' does not exist")
        con.close()
        return
    
    # Get basic stats
    stats = con.execute(f"""
        SELECT 
            COUNT(*) AS row_count,
            COUNT(DISTINCT source_file) AS file_count
        FROM {raw_table}
    """).fetchone()
    
    row_count, file_count = stats
    logging.info(f"Raw table '{raw_table}' contains {row_count:,} rows from {file_count} files")
    
    # Get product distribution
    product_stats = con.execute(f"""
        SELECT 
            substring(PRODUCT, 1, 3) AS product_type,
            COUNT(*) AS count,
            COUNT(*) * 100.0 / {max(row_count, 1)} AS percentage
        FROM {raw_table}
        GROUP BY product_type
        ORDER BY count DESC
    """).fetchdf()
    
    logging.info("Product type distribution:")
    for _, row in product_stats.iterrows():
        logging.info(f"  {row['product_type']}: {row['count']:,} rows ({row['percentage']:.1f}%)")
    
    # Date range
    date_stats = con.execute(f"""
        SELECT 
            MIN(TRY_STRPTIME(DELIVERY_DATE, '%Y-%m-%d')) AS min_date,
            MAX(TRY_STRPTIME(DELIVERY_DATE, '%Y-%m-%d')) AS max_date,
            COUNT(DISTINCT TRY_STRPTIME(DELIVERY_DATE, '%Y-%m-%d')::DATE) AS date_count
        FROM {raw_table}
        WHERE DELIVERY_DATE IS NOT NULL
    """).fetchone()
    
    min_date, max_date, date_count = date_stats
    if min_date and max_date:
        logging.info(f"Date range: {min_date} to {max_date} ({date_count} unique dates)")
    
    # Close connection
    con.close()

def update_provider_data_in_db(db_path, raw_table="raw_provider_data", clean_table="provider_data"):
    """
    Update provider data in the database using the date range replacement method.
    
    This function implements the critical "clean slate" approach for handling anonymized bids:
    1. Determine date range of the new data
    2. Delete ALL existing data in that date range
    3. Insert the new data for that range
    
    This prevents duplicate anonymized bids which would distort marginal price calculations.
    
    Args:
        db_path: Path to DuckDB database
        raw_table: Name of the raw data table
        clean_table: Name of the clean data table
        
    Returns:
        tuple: (min_date, max_date, rows_affected)
    """
    logging.info("Updating provider data with date range replacement method")
    start_time = time.time()
    
    # Connect to DuckDB
    con = duckdb.connect(db_path)
    
    try:
        # 1. Check if raw table exists and has data
        table_exists = con.execute(f"""
            SELECT count(*) FROM information_schema.tables 
            WHERE table_name = '{raw_table}'
        """).fetchone()[0]
        
        if not table_exists:
            logging.error(f"Raw table '{raw_table}' does not exist")
            return None, None, 0
        
        raw_row_count = con.execute(f"SELECT COUNT(*) FROM {raw_table}").fetchone()[0]
        if raw_row_count == 0:
            logging.warning(f"Raw table '{raw_table}' is empty")
            return None, None, 0
        
        # 2. Check if clean table exists, create if not
        clean_table_exists = con.execute(f"""
            SELECT count(*) FROM information_schema.tables 
            WHERE table_name = '{clean_table}'
        """).fetchone()[0]
        
        if not clean_table_exists:
            logging.info(f"Clean table '{clean_table}' does not exist, creating it")
            con.execute(f"""
                CREATE TABLE {clean_table} (
                    DELIVERY_DATE TIMESTAMP,
                    PRODUCT VARCHAR,
                    ENERGY_PRICE__EUR_MWh_ DOUBLE,
                    ALLOCATED_CAPACITY__MW_ DOUBLE,
                    period TIMESTAMP,
                    source_file VARCHAR
                )
            """)
        
        # 3. Determine date range of raw data
        date_range = con.execute(f"""
            SELECT 
                MIN(TRY_STRPTIME(DELIVERY_DATE, '%Y-%m-%d')) AS min_date,
                MAX(TRY_STRPTIME(DELIVERY_DATE, '%Y-%m-%d')) AS max_date
            FROM {raw_table}
            WHERE DELIVERY_DATE IS NOT NULL
        """).fetchone()
        
        min_date, max_date = date_range
        
        if not min_date or not max_date:
            logging.error("Could not determine date range of raw data")
            return None, None, 0
        
        logging.info(f"Date range of new data: {min_date} to {max_date}")
        
        # 4. Check if we have existing data in the clean table for this date range
        existing_count = 0
        if clean_table_exists:
            existing_count = con.execute(f"""
                SELECT COUNT(*) 
                FROM {clean_table}
                WHERE DELIVERY_DATE BETWEEN '{min_date}' AND '{max_date}'
            """).fetchone()[0]
        
        if existing_count > 0:
            logging.warning(f"Found {existing_count:,} existing rows in date range {min_date} to {max_date}")
            
            # Confirm the deletion with user in interactive mode
            if os.isatty(0):  # Check if running interactively
                confirmation = input(f"Delete {existing_count} existing rows for {min_date} to {max_date}? (y/n): ")
                if confirmation.lower() != 'y':
                    logging.info("Aborted by user")
                    return min_date, max_date, 0
            
            # DELETE existing data in this date range
            logging.info(f"Deleting {existing_count:,} existing rows in date range")
            con.execute(f"""
                DELETE FROM {clean_table}
                WHERE DELIVERY_DATE BETWEEN '{min_date}' AND '{max_date}'
            """)
            logging.info(f"Deleted {existing_count:,} rows")
        
        # 5. Process and insert new data for this date range
        logging.info("Preparing clean data from raw table")
        
        # Create temporary view with cleaned data
        con.execute(f"""
            CREATE OR REPLACE TEMPORARY VIEW clean_data_view AS
            SELECT
                -- 1. Standardize DELIVERY_DATE (ISO format)
                TRY_STRPTIME(DELIVERY_DATE, '%Y-%m-%d') AS DELIVERY_DATE,
                
                -- 2. Product remains as is
                PRODUCT,
                
                -- 3. Clean numeric price: replace comma with dot and apply payment direction
                CASE 
                    WHEN ENERGY_PRICE_PAYMENT_DIRECTION = 'PROVIDER_TO_GRID' 
                    THEN -1 * CAST(REPLACE("ENERGY_PRICE_[EUR/MWh]", ',', '.') AS DOUBLE)
                    ELSE CAST(REPLACE("ENERGY_PRICE_[EUR/MWh]", ',', '.') AS DOUBLE)
                END AS ENERGY_PRICE__EUR_MWh_,
                
                -- 4. Clean capacity: replace comma with dot
                CAST(REPLACE("ALLOCATED_CAPACITY_[MW]", ',', '.') AS DOUBLE) AS ALLOCATED_CAPACITY__MW_,
                
                -- 5. Add period column for time period grouping
                DELIVERY_DATE::DATE::TIMESTAMP AS period,
                
                -- 6. Keep track of source file
                source_file
            FROM {raw_table}
            
            -- 7. Filter POS_ products and NULL dates/products
            WHERE PRODUCT NOT LIKE 'POS_%'
              AND DELIVERY_DATE IS NOT NULL 
              AND PRODUCT IS NOT NULL
              AND TRY_STRPTIME(DELIVERY_DATE, '%Y-%m-%d') BETWEEN '{min_date}' AND '{max_date}'
            
            -- 8. Sort data
            ORDER BY DELIVERY_DATE, PRODUCT, ENERGY_PRICE__EUR_MWh_
        """)
        
        # 6. Insert the clean data into the final table
        insert_result = con.execute(f"""
            INSERT INTO {clean_table}
            SELECT * FROM clean_data_view
        """)
        
        rows_inserted = insert_result.fetchone()[0]
        
        # 7. Get statistics on the update
        clean_stats = con.execute(f"""
            SELECT 
                COUNT(*) AS row_count,
                COUNT(DISTINCT PRODUCT) AS product_count,
                COUNT(DISTINCT DATE_TRUNC('day', DELIVERY_DATE)) AS date_count,
                COUNT(DISTINCT source_file) AS file_count
            FROM {clean_table}
            WHERE DELIVERY_DATE BETWEEN '{min_date}' AND '{max_date}'
        """).fetchone()
        
        total_rows, product_count, date_count, file_count = clean_stats
        
        # 8. Log results
        logging.info(f"Inserted {rows_inserted:,} rows for date range {min_date} to {max_date}")
        logging.info(f"Clean table now has {total_rows:,} rows in this date range")
        logging.info(f"Contains {product_count} products across {date_count} days from {file_count} files")
        logging.info(f"Update completed in {time.time() - start_time:.2f} seconds")
        
        # Return date range and rows affected
        return min_date, max_date, rows_inserted
    
    except Exception as e:
        logging.error(f"Error updating provider data: {e}")
        return None, None, 0
    
    finally:
        # Clean up temporary objects and close connection
        try:
            con.execute("DROP VIEW IF EXISTS clean_data_view")
        except:
            pass
        con.close()

# For backward compatibility
clean_provider_data_in_db = update_provider_data_in_db