import os
import time
import logging
import pandas as pd
import duckdb
from hypermvp.utils.db_versioning import create_duckdb_snapshot, add_version_metadata

def update_provider_data(cleaned_data, db_path, table_name="provider_data", create_snapshot=False):
    """Update provider data in DuckDB with deduplication."""
    import duckdb
    import pandas as pd
    import logging
    import time
    
    start_time = time.time()
    con = None
    
    try:
        con = duckdb.connect(db_path)
        
        # Check if table exists and get existing date range
        table_exists = con.execute(f"""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name='{table_name}'
        """).fetchone()
        
        if table_exists:
            # Get current date range
            current_range = con.execute(f"""
                SELECT 
                    MIN(DELIVERY_DATE) as min_date,
                    MAX(DELIVERY_DATE) as max_date
                FROM {table_name}
            """).fetchdf()
            
            min_date = current_range['min_date'].iloc[0]
            max_date = current_range['max_date'].iloc[0]
            
            logging.info(f"Existing data range: {min_date} to {max_date}")
            
            # Filter out data that might already exist to avoid duplicates
            new_data = cleaned_data[
                (cleaned_data['DELIVERY_DATE'] < min_date) | 
                (cleaned_data['DELIVERY_DATE'] > max_date)
            ]
            
            if len(new_data) == 0:
                logging.info("No new data to add within date range")
                return 0
            
            logging.info(f"Adding {len(new_data)} new records outside existing date range")
            cleaned_data = new_data
        
        # Create snapshot if requested
        if create_snapshot:
            # Your existing snapshot code
            pass
            
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
        
        # Process by period to avoid memory issues with large datasets
        total_periods = len(cleaned_data['period'].unique())
        periods = cleaned_data['period'].unique()
        records_added = 0
        
        for i, period in enumerate(periods, 1):
            if i % 10 == 0 or i == 1 or i == total_periods:
                logging.info(f"Processing period {i}/{total_periods}: {period}")
            
            # Get data for this period
            period_data = cleaned_data[cleaned_data['period'] == period]
            
            # Append the DataFrame directly to the table
            con.append(table_name, period_data)
            
            records_added += len(period_data)
        
        logging.info(f"Added {records_added} records to {table_name} in {time.time() - start_time:.2f} seconds")
        return records_added
        
    except Exception as e:
        logging.error(f"Error updating provider data: {e}")
        raise
    finally:
        if con:
            con.close()