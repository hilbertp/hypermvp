import unittest
import os
import pandas as pd
import duckdb
from hypermvp.provider.save_to_duckdb import save_to_duckdb

class TestSaveToDuckDB(unittest.TestCase):

    def setUp(self):
        self.processed_dir = 'data/test_processed'
        self.db_path = 'data/test_processed_data.duckdb'
        self.table_name = 'provider_data'
        
        # Create test directory
        os.makedirs(self.processed_dir, exist_ok=True)
        
        # Create a small sample CSV file for testing
        sample_data = {
            'DELIVERY_DATE': ['2025-01-01', '2025-02-01'],
            'PRODUCT': ['NEG_001', 'NEG_002'],
            'ENERGY_PRICE_[EUR/MWh]': [100.00, 200.00],
            'ALLOCATED_CAPACITY_[MW]': [5, 10]
        }
        sample_df = pd.DataFrame(sample_data)
        sample_df.to_csv(os.path.join(self.processed_dir, 'sample_provider_data.csv'), index=False)

    def tearDown(self):
        # Remove test directory and files
        if os.path.exists(self.processed_dir):
            for file in os.listdir(self.processed_dir):
                os.remove(os.path.join(self.processed_dir, file))
            os.rmdir(self.processed_dir)
        
        if os.path.exists(self.db_path):
            os.remove(self.db_path)

    def test_save_to_duckdb(self):
        # Initialize an empty DataFrame for combined data
        combined_df = pd.DataFrame()

        # Process each file in the processed directory
        for filename in os.listdir(self.processed_dir):
            if filename.endswith('.csv'):
                filepath = os.path.join(self.processed_dir, filename)
                
                # Read the cleaned data from CSV
                cleaned_data = pd.read_csv(filepath)
                
                # Append the cleaned data to the combined DataFrame
                combined_df = pd.concat([combined_df, cleaned_data], ignore_index=True)

        # Save the combined data to DuckDB
        save_to_duckdb(combined_df, self.db_path, self.table_name)
        
        # Verify the DuckDB table is created and contains the expected data
        con = duckdb.connect(self.db_path)
        result_df = con.execute(f"SELECT * FROM {self.table_name}").fetchdf()
        con.close()
        
        self.assertEqual(len(result_df), len(combined_df))
        self.assertEqual(list(result_df.columns), list(combined_df.columns))

if __name__ == '__main__':
    unittest.main()