import os
import unittest
import pandas as pd
import warnings
from hypermvp.provider.loader import load_provider_file
from hypermvp.provider.cleaner import clean_provider_data
from hypermvp.provider.dump_csv import save_to_csv

# Suppress specific warnings
warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")

class TestDumpCSV(unittest.TestCase):

    def setUp(self):
        # Define input and output directories for test data
        self.input_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../tests_data/raw"))
        self.output_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../tests_data/processed"))
        
        # Create test directories if they don't exist
        os.makedirs(self.input_dir, exist_ok=True)
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Create a small sample XLSX file with test data
        self.sample_data = pd.DataFrame({
            "DELIVERY_DATE": ["9/1/2024", "9/2/2024"],
            "PRODUCT": ["NEG_001", "NEG_002"],
            "ENERGY_PRICE_[EUR/MWh]": ["138,85", "140,50"],
            "ENERGY_PRICE_PAYMENT_DIRECTION": ["GRID_TO_PROVIDER", "GRID_TO_PROVIDER"],
            "ALLOCATED_CAPACITY_[MW]": [50, 30],
            "NOTE": [None, "Test note"]
        })
        self.sample_file = os.path.join(self.input_dir, "test_provider_list.xlsx")
        self.sample_data.to_excel(self.sample_file, index=False)

    def tearDown(self):
        # Clean up test directories and files (commented out to keep files)
        # for f in os.listdir(self.input_dir):
        #     os.remove(os.path.join(self.input_dir, f))
        # for f in os.listdir(self.output_dir):
        #     os.remove(os.path.join(self.output_dir, f))
        # os.rmdir(self.input_dir)
        # os.rmdir(self.output_dir)
        pass

    def test_dump_csv(self):
        print("\n=== Test: Dump CSV ===")
        
        # Load the raw data
        raw_data = load_provider_file(self.sample_file)
        print("Step 1: Loaded raw data successfully.")
        
        # Clean the data
        cleaned_data = clean_provider_data(raw_data)
        print("Step 2: Cleaned the data successfully.")
        
        # Save the cleaned data to a CSV file
        output_filename = "test_provider_list.csv"
        save_to_csv(cleaned_data, self.output_dir, output_filename)
        print(f"Step 3: Saved the cleaned data to CSV successfully at {os.path.join(self.output_dir, output_filename)}.")
        
        # Verify the CSV file is created
        output_file = os.path.join(self.output_dir, output_filename)
        self.assertTrue(os.path.exists(output_file))
        print("Step 4: Verified the CSV file was created.")
        
        # Load the CSV file and ensure consistent data types
        loaded_csv = pd.read_csv(output_file)
        loaded_csv["DELIVERY_DATE"] = pd.to_datetime(loaded_csv["DELIVERY_DATE"], format="%Y-%m-%d")
        
        # Verify the content of the CSV file matches the cleaned data
        pd.testing.assert_frame_equal(loaded_csv, cleaned_data)
        print("Step 5: Verified the content of the CSV file matches the cleaned data.")
        
        print("\nTest Result: All steps passed successfully.")
        print("===============================\n")

if __name__ == "__main__":
    # Run the unit test with minimal verbosity for cleaner output
    unittest.TextTestRunner(verbosity=1).run(unittest.TestLoader().loadTestsFromTestCase(TestDumpCSV))