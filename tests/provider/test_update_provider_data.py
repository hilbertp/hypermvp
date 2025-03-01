import os
import shutil
import tempfile
import unittest
import pandas as pd
import duckdb
from hypermvp.provider.update_provider_data import update_provider_data
from hypermvp.provider.loader import load_provider_file  # Add this import
from hypermvp.provider.cleaner import clean_provider_data  # Add this import

class TestUpdateProviderData(unittest.TestCase):
    def setUp(self):
        # Create temporary directories/files for testing
        self.test_dir = tempfile.mkdtemp()
        self.input_dir = os.path.join(self.test_dir, 'test_raw')
        self.db_path = os.path.join(self.test_dir, 'test_processed_data.duckdb')
        self.table_name = 'provider_data'
        
        os.makedirs(self.input_dir, exist_ok=True)
        
        # Mock data with original column names
        sample_data = {
            'DELIVERY_DATE': ['1/1/2025', '2/1/2025'],
            'PRODUCT': ['NEG_001', 'NEG_002'],
            'ENERGY_PRICE_[EUR/MWh]': [100.00, 200.00],
            'ALLOCATED_CAPACITY_[MW]': [5, 10],
            'ENERGY_PRICE_PAYMENT_DIRECTION': ['GRID_TO_PROVIDER', 'PROVIDER_TO_GRID'],
            'NOTE': ['', '']
        }
        sample_df = pd.DataFrame(sample_data)
        sample_filepath = os.path.join(self.input_dir, 'sample_provider_data.xlsx')
        
        # Save using pandas, with error handling
        try:
            sample_df.to_excel(sample_filepath, index=False)
        except Exception as e:
            self.skipTest(f"Could not create Excel test file: {e}")
    
    def tearDown(self):
        # Clean up temporary files after test
        try:
            shutil.rmtree(self.test_dir)
        except (OSError, IOError) as e:
            print(f"Error cleaning up test directory: {e}")
    
    def test_update_provider_data(self):
        # Run the update process
        try:
            result_df = update_provider_data(self.input_dir, self.db_path, self.table_name)
        except Exception as e:
            self.fail(f"update_provider_data failed with error: {e}")
        
        # Connect to the database and fetch the data
        try:
            con = duckdb.connect(self.db_path)
            db_df = con.execute(f"SELECT * FROM {self.table_name}").fetchdf()
            
            # DuckDB schema query - use the same connection
            tables = con.execute("SHOW TABLES").fetchall()
            table_exists = any(table[0].lower() == self.table_name.lower() for table in tables)
            
            # Now close the connection
            con.close()
        except Exception as e:
            self.fail(f"Error fetching data from DuckDB: {e}")
        
        # Define expected columns after cleaning and processing
        # Note: These must match EXACTLY what update_provider_data produces
        expected_columns = [
            'DELIVERY_DATE',
            'PRODUCT',
            'ENERGY_PRICE__EUR_MWh_',
            'ALLOCATED_CAPACITY__MW_',
            'period'
        ]
        
        # Debug output
        print("Result DF columns:", result_df.columns.tolist())
        print("Database DF columns:", db_df.columns.tolist())
        print("Expected columns:", expected_columns)
        
        # Assertions with descriptive messages
        self.assertEqual(len(db_df), len(result_df), 
                        f"Row counts do not match: {len(db_df)} vs {len(result_df)}")
        
        # Check column presence rather than exact order if order isn't critical
        for col in expected_columns:
            self.assertIn(col, db_df.columns, f"Column '{col}' missing from database")
            self.assertIn(col, result_df.columns, f"Column '{col}' missing from result")
        
        # If column order matters, uncomment these:
        # self.assertListEqual(list(db_df.columns), expected_columns, 
        #                     f"Database columns {list(db_df.columns)} don't match expected {expected_columns}")
        # self.assertListEqual(list(result_df.columns), expected_columns, 
        #                     f"Result columns {list(result_df.columns)} don't match expected {expected_columns}")

if __name__ == "__main__":
    unittest.main()

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
    combined_new["DELIVERY_DATE"] = pd.to_datetime(combined_new["DELIVERY_DATE"], errors='coerce')
    if combined_new["DELIVERY_DATE"].isnull().any():
        raise ValueError("Some DELIVERY_DATE entries could not be parsed.")
    combined_new["period"] = combined_new["DELIVERY_DATE"].dt.floor("4h")
    
    # Register the temporary table first
    con.register("temp_df", combined_new)

    # ADD THIS: Check if table exists, create it if it doesn't
    try:
        table_exists = con.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'").fetchone() is not None
    except:
        table_exists = False
    
    # Create the table if it doesn't exist, based on the structure of temp_df
    if not table_exists:
        print(f"Creating table {table_name}...")
        con.execute(f"""
            CREATE TABLE {table_name} AS 
            SELECT * FROM temp_df WHERE 1=0
        """)

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

    con.close()
    return combined_new