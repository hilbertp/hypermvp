import os
import duckdb
import pandas as pd
from hypermvp.provider.update_provider_data import update_provider_data

class TestUpdateProviderData:
    def setUp(self):
        self.input_dir = 'data/test_raw'
        self.db_path = 'data/test_processed_data.duckdb'
        self.table_name = 'provider_data'
        
        os.makedirs(self.input_dir, exist_ok=True)
        
        # Mock data with original column names
        sample_data = {
            'DELIVERY_DATE': ['1/1/2025', '2/1/2025'],
            'PRODUCT': ['NEG_001', 'NEG_002'],
            'ENERGY_PRICE_[EUR/MWh]': [100.00, 200.00],  # Original name
            'ALLOCATED_CAPACITY_[MW]': [5, 10],           # Original name
            'ENERGY_PRICE_PAYMENT_DIRECTION': ['GRID_TO_PROVIDER', 'PROVIDER_TO_GRID'],
            'NOTE': ['', '']
        }
        sample_df = pd.DataFrame(sample_data)
        sample_filepath = os.path.join(self.input_dir, 'sample_provider_data.xlsx')
        sample_df.to_excel(sample_filepath, index=False)
    
    def test_update_provider_data(self):
        # Run the update process
        result_df = update_provider_data(self.input_dir, self.db_path, self.table_name)
        
        # Connect to the database and fetch the data
        con = duckdb.connect(self.db_path)
        db_df = con.execute(f"SELECT * FROM {self.table_name}").fetchdf()
        con.close()
        
        # Define expected columns after cleaning and processing
        expected_columns = [
            'DELIVERY_DATE',
            'PRODUCT',
            'ENERGY_PRICE__EUR_MWh_',  # Cleaned name
            'ALLOCATED_CAPACITY__MW_',  # Cleaned name
            'period'                    # Added during processing
        ]
        
        # Optional: Add print statements for debugging
        print("Result DF columns:", result_df.columns.tolist())
        print("Database DF columns:", db_df.columns.tolist())
        print("Expected columns:", expected_columns)
        
        # Assertions
        assert len(db_df) == len(result_df), "Row counts do not match"
        assert list(db_df.columns) == expected_columns, "Database columns do not match expected"
        assert list(result_df.columns) == expected_columns, "Result columns do not match expected"

if __name__ == "__main__":
    test = TestUpdateProviderData()
    test.setUp()
    test.test_update_provider_data()
    print("Test passed successfully!")