import os
import unittest
import pandas as pd
from hypermvp.provider.loader import load_provider_file

class TestLoader(unittest.TestCase):

    def setUp(self):
        # Define the path to the test provider file
        self.valid_xlsx = "/home/philly/hypermvp/tests/tests_data/raw/provider_list_2024_09_01_MOCK.xlsx"

    def test_load_xlsx_file(self):
        # Print test header
        print("\n=== Provider File Loader Test ===")
        print(f"Testing file: {self.valid_xlsx}")
        
        # Check if the test file exists
        if not os.path.exists(self.valid_xlsx):
            print(f"ERROR: Test file not found at {self.valid_xlsx}")
            self.skipTest("Test file not found")
            return

        # Load the provider file using the loader function
        df = load_provider_file(self.valid_xlsx)
        
        # Assert that the result is a DataFrame and it's not empty
        self.assertIsInstance(df, pd.DataFrame)
        self.assertFalse(df.empty)
        
        # Print the content of the DataFrame
        print("\nDataFrame Content:")
        print(df.head())
        
        # Print success message with the number of rows loaded
        print(f"\nSuccess: Loaded {len(df)} rows")
        print("=======================\n")

if __name__ == "__main__":
    # Run the unit test with verbosity level 2 for detailed output
    unittest.main(verbosity=2)
