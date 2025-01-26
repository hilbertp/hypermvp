import os
import pandas as pd
import duckdb
from hypermvp.config import PROCESSED_DATA_DIR, DUCKDB_PATH


def save_afrr_to_duckdb(cleaned_afrr_data, month, year, table_name="afrr_data"):
    """
    Save cleaned aFRR data to a DuckDB database, appending month and year metadata.

    Args:
        cleaned_afrr_data (pd.DataFrame): Cleaned aFRR data.
        month (int): Month of the data (e.g., 9 for September).
        year (int): Year of the data (e.g., 2024).
        table_name (str): Name of the table in the DuckDB database.
    """
    try:
        # Ensure the processed data directory exists
        os.makedirs(PROCESSED_DATA_DIR, exist_ok=True)

        # Add month and year columns to the data
        cleaned_afrr_data["month"] = month
        cleaned_afrr_data["year"] = year

        # Connect to the DuckDB database
        conn = duckdb.connect(DUCKDB_PATH)

        # Create the table if it doesn't exist
        conn.execute(
            f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM cleaned_afrr_data LIMIT 0;"
        )

        # Insert data into the table
        conn.execute(f"INSERT INTO {table_name} SELECT * FROM cleaned_afrr_data;")

        print(
            f"aFRR data for {month}/{year} successfully saved to table '{table_name}' in DuckDB."
        )
    except Exception as e:
        print(f"Error saving aFRR data to DuckDB: {e}")
    finally:
        # Close the DuckDB connection
        conn.close()


# Example usage:
# Assume `cleaned_afrr` is a pandas DataFrame containing the cleaned aFRR data.
# save_afrr_to_duckdb(cleaned_afrr, month=9, year=2024)
