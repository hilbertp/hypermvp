import os
import unittest
import pandas as pd
from hypermvp.provider.loader import load_provider_file

class TestLoader(unittest.TestCase):

    def setUp(self):
        # Point to the actual .xlsx file in your directory
        self.valid_xlsx = "G:/hyperMVP/data/01_raw/provider_list_2024_09_01.xlsx"

    def test_load_xlsx_file(self):
        """Test loading a valid XLSX file."""
        if os.path.exists(self.valid_xlsx):
            df = load_provider_file(self.valid_xlsx)
            self.assertIsInstance(df, pd.DataFrame)
            self.assertFalse(df.empty)
            
            # Print some rows and columns to confirm it loaded correctly
            print("\n--- DataFrame Info ---")
            print(df.info())  # General info about the DataFrame
            print("\n--- First Few Rows ---")
            print(df.head())  # First 5 rows of the DataFrame
            print("\n--- Columns ---")
            print(df.columns.tolist())  # List of column names
        else:
            self.skipTest(f"File {self.valid_xlsx} does not exist.")

if __name__ == "__main__":
    unittest.main()
