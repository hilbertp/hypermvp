import os
import logging
import pandas as pd
from hypermvp.config import RAW_DATA_DIR, PROCESSED_DATA_DIR  # If you need these constants
import duckdb
from hypermvp.provider.loader import load_provider_file
from hypermvp.provider.cleaner import clean_provider_data

def update_provider_data(input_dir, db_path, table_name="provider_data"):
    """
    Process new provider XLSX files from input_dir. The raw files are loaded and cleaned,
    and their data is grouped into 4-hour periods based on DELIVERY_DATE.
    For each period, any existing rows in the provider table are deleted and the full period’s data is inserted.
    
    Args:
        input_dir (str): Directory with raw XLSX files.
        db_path (str): Path to the DuckDB database.
        table_name (str): Name of the table to store provider data.
        
    Returns:
        pd.DataFrame: The combined new data that was applied.
    """
    # Connect to DuckDB
    con = duckdb.connect(database=db_path, read_only=False)
    
    # Load and clean all new data from raw XLSX files in input_dir.
    new_dfs = []
    for filename in os.listdir(input_dir):
        if filename.endswith(".xlsx"):
            filepath = os.path.join(input_dir, filename)
            print(f"Loading file: {filepath}")
            raw_data = load_provider_file(filepath)
            print(f"Loaded columns: {raw_data.columns.tolist()}")
            cleaned_data = clean_provider_data(raw_data)
            new_dfs.append(cleaned_data)
    
    if not new_dfs:
        print("No new provider data files found to process.")
        con.close()
        return pd.DataFrame()
    
    # Combine all new data into one DataFrame.
    combined_new = pd.concat(new_dfs, ignore_index=True)
    
    # Ensure the DELIVERY_DATE is a datetime and create a 4-hour period key.
    combined_new["DELIVERY_DATE"] = pd.to_datetime(combined_new["DELIVERY_DATE"], errors='coerce')
    if combined_new["DELIVERY_DATE"].isnull().any():
        raise ValueError("Some DELIVERY_DATE entries could not be parsed.")
    
    # Create a period column by rounding down to the nearest 4-hour period.
    combined_new["period"] = combined_new["DELIVERY_DATE"].dt.floor("4h")
    
    # Group data by the period key.
    for period, group in combined_new.groupby("period"):
        print(f"Processing period {period}...")
        # Delete existing records for this period from the provider table.
        try:
            con.execute(f"DELETE FROM {table_name} WHERE period = ?", [period])
        except Exception:
            # If the table does not exist yet, it will be created below.
            pass
        # Insert new rows; using a bulk insertion method would be more efficient:
        con.register("temp_df", group)
        con.execute(f"INSERT INTO {table_name} SELECT * FROM temp_df")
        con.unregister("temp_df")
    
    con.commit()
    print(f"Processed and updated provider data for periods: {list(combined_new['period'].unique())}")
    con.close()
    return combined_new