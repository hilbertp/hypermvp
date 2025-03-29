"""
Provider Data Update Logic

IMPORTANT: THIS MODULE HANDLES CRITICAL BUSINESS LOGIC FOR PROVIDER DATA UPDATES

The provider data update process follows these business rules:

1. For time periods with NO existing data:
   - All provider data is added directly to the database
   
2. For time periods that ALREADY HAVE data:
   - ALL existing data for those specific time periods is DELETED first
   - Then the new data is added as a complete replacement
   - This "clean slate" approach prevents duplicate bids that would distort marginal price calculations
   
CRITICAL ISSUE: Duplicating bids within a time period would severely distort marginal price calculations
  - Providers may submit identical bids (e.g., 5MW at 0 EUR/MWh)
  - When calculating the merit order, duplicate bids incorrectly inflate available capacity
  - This leads to completely incorrect marginal price determinations
  
The solution is to bulk delete all data in the time range being imported:
  - For the entire date range being imported, delete all existing data first
  - Then add all the new data at once
  - This maintains data integrity while being computationally efficient
"""

import os
import time
import logging
import pandas as pd
import duckdb
from hypermvp.utils.db_versioning import create_duckdb_snapshot, add_version_metadata

def update_provider_data(cleaned_data, db_path, table_name="provider_data", create_snapshot=False):
    """Update provider data in DuckDB with proper handling of overlapping periods."""
    import duckdb
    import pandas as pd
    import logging
    import time
    
    start_time = time.time()
    con = None
    
    try:
        con = duckdb.connect(db_path)
        
        # Check if table exists
        table_exists = con.execute(f"""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name='{table_name}'
        """).fetchone()
        
        # Create snapshot if requested
        if create_snapshot and table_exists:
            create_duckdb_snapshot(db_path)
            
        # Ensure the table has the right schema
        if not table_exists:
            con.execute(f"""
                CREATE TABLE {table_name} (
                    DELIVERY_DATE TIMESTAMP,
                    PRODUCT VARCHAR,
                    TYPE_OF_RESERVES VARCHAR,
                    ENERGY_PRICE__EUR_MWh_ DOUBLE,
                    OFFERED_CAPACITY__MW_ DOUBLE,
                    ALLOCATED_CAPACITY__MW_ DOUBLE,
                    COUNTRY VARCHAR,
                    period TIMESTAMP
                )
            """)
            logging.info(f"Created new table: {table_name}")
        
        # Find min and max dates in the new data
        min_import_date = cleaned_data['DELIVERY_DATE'].min()
        max_import_date = cleaned_data['DELIVERY_DATE'].max()
        
        logging.info(f"Import data range: {min_import_date} to {max_import_date}")
        
        # CRITICAL BUSINESS LOGIC:
        # Delete ALL existing data in the date range being imported to prevent duplicates
        if table_exists:
            deleted_count = con.execute(f"""
                DELETE FROM {table_name}
                WHERE DELIVERY_DATE BETWEEN ? AND ?
            """, [min_import_date, max_import_date]).fetchone()[0]
            
            logging.info(f"Deleted {deleted_count} existing records in the import date range")
        
        # Append all the new data at once (more efficient for bulk imports)
        records_to_add = len(cleaned_data)
        
        # For very large datasets, process in chunks to avoid memory issues
        if records_to_add > 1000000:  # 1 million records threshold
            chunk_size = 500000
            for i in range(0, records_to_add, chunk_size):
                end_idx = min(i + chunk_size, records_to_add)
                chunk = cleaned_data.iloc[i:end_idx]
                con.append(table_name, chunk)
                logging.info(f"Added chunk {i//chunk_size + 1}: {len(chunk)} records")
        else:
            con.append(table_name, cleaned_data)
        
        # Add version metadata
        add_version_metadata(con, f"Added {records_to_add} provider records between {min_import_date} and {max_import_date}", "IMPORT")
        
        logging.info(f"Added {records_to_add} records to {table_name} in {time.time() - start_time:.2f} seconds")
        return records_to_add
        
    except Exception as e:
        logging.error(f"Error updating provider data: {e}")
        raise
    finally:
        if con:
            con.close()