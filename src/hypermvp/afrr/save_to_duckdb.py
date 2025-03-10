import os
import time
import logging
import pandas as pd
import duckdb
from hypermvp.config import PROCESSED_DATA_DIR, DUCKDB_PATH

def save_afrr_to_duckdb(cleaned_afrr_data, month, year, table_name="afrr_data", db_path=None):
    """
    Save cleaned aFRR data to a DuckDB database, appending month and year metadata. Handles deduplication by removing any existing data for the same month/year.

    Args:
        cleaned_afrr_data (pd.DataFrame): Cleaned aFRR data.
        month (int): Month of the data (e.g., 9 for September).
        year (int): Year of the data (e.g., 2024).
        table_name (str): Name of the table in the DuckDB database.
        db_path (str, optional): Path to DuckDB database file. Defaults to DUCKDB_PATH from config.
    
    Returns:
        int: Number of rows inserted into the database.
    """
    start_time = time.time()
    
    if db_path is None:
        db_path = DUCKDB_PATH
    
    try:
        # Ensure the processed data directory exists
        os.makedirs(os.path.dirname(db_path), exist_ok=True)

        # Add month and year columns to the data
        data = cleaned_afrr_data.copy()
        data["month"] = month
        data["year"] = year

        # Connect to the DuckDB database
        conn = duckdb.connect(db_path)

        # Check if the table exists
        table_exists = conn.execute(
            f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'"
        ).fetchone() is not None

        if not table_exists:
            # Create the table if it doesn't exist
            logging.info(f"Creating new table '{table_name}' in DuckDB")
            conn.register("temp_df", data)
            conn.execute(
                f"CREATE TABLE {table_name} AS SELECT * FROM temp_df WHERE 1=0"
            )
            conn.unregister("temp_df")
        
        # Delete any existing data for this month and year
        delete_start = time.time()
        deleted_rows = conn.execute(
            f"DELETE FROM {table_name} WHERE month = {month} AND year = {year}"
        ).fetchone()[0]
        
        if deleted_rows > 0:
            logging.info(f"Removed {deleted_rows} existing rows for {month}/{year} from '{table_name}' in {time.time() - delete_start:.2f} seconds")
        
        # Insert new data
        insert_start = time.time()
        conn.register("temp_df", data)
        conn.execute(f"INSERT INTO {table_name} SELECT * FROM temp_df")
        row_count = len(data)
        conn.unregister("temp_df")
        
        # Commit changes and close
        conn.commit()
        
        logging.info(
            f"Inserted {row_count} rows of aFRR data for {month}/{year} into '{table_name}' in {time.time() - insert_start:.2f} seconds"
        )
        logging.info(f"Total save operation took {time.time() - start_time:.2f} seconds")
        
        return row_count
        
    except Exception as e:
        logging.error(f"Error saving aFRR data to DuckDB: {e}")
        return 0
    finally:
        # Close the DuckDB connection
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    # Example usage - only run when the script is executed directly
    test_data = pd.DataFrame({
        'Datum': pd.to_datetime(['2024-09-01', '2024-09-01']),
        'von': ['00:00', '00:15'],
        'bis': ['00:15', '00:30'],
        '50Hertz (Negativ)': [4.364, 10.052]
    })
    example_db_path = os.path.join(PROCESSED_DATA_DIR, "example_afrr.duckdb")
    save_afrr_to_duckdb(test_data, 9, 2024, "afrr_example", example_db_path)