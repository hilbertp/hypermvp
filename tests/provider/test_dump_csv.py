import unittest
import os
import pandas as pd
from hypermvp.provider.loader import load_provider_file
from hypermvp.provider.cleaner import clean_provider_data
from hypermvp.provider.dump_csv import save_to_csv

class TestDumpCSV(unittest.TestCase):

    def setUp(self):
        self.input_dir = 'data/test_raw'
        self.output_dir = 'data/test_processed'
        self.sample_xlsx = os.path.join(self.input_dir, 'sample_provider_data.xlsx')
        
        # Create test directories
        os.makedirs(self.input_dir, exist_ok=True)
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Create a small sample XLSX file for testing
        sample_data = {
            'DELIVERY_DATE': ['01/01/2025', '02/01/2025'],
            'PRODUCT': ['NEG_001', 'NEG_002'],
            'ENERGY_PRICE_[EUR/MWh]': ['100,00', '200,00'],
            'ENERGY_PRICE_PAYMENT_DIRECTION': ['GRID_TO_PROVIDER', 'PROVIDER_TO_GRID'],
            'ALLOCATED_CAPACITY_[MW]': [5, 10],
            'NOTE': [None, 'Sample note']
        }
        sample_df = pd.DataFrame(sample_data)
        sample_df.to_excel(self.sample_xlsx, index=False)

    def tearDown(self):
        # Remove test directories and files
        if os.path.exists(self.input_dir):
            for file in os.listdir(self.input_dir):
                os.remove(os.path.join(self.input_dir, file))
            os.rmdir(self.input_dir)
        
        if os.path.exists(self.output_dir):
            for file in os.listdir(self.output_dir):
                os.remove(os.path.join(self.output_dir, file))
            os.rmdir(self.output_dir)

    def test_dump_csv(self):
        # Load the raw data
        raw_data = load_provider_file(self.sample_xlsx)
        
        # Clean the data
        cleaned_data = clean_provider_data(raw_data)
        
        # Save the cleaned data to CSV
        csv_filename = 'sample_provider_data.csv'
        save_to_csv(cleaned_data, self.output_dir, csv_filename)
        
        # Verify the CSV file is created
        csv_filepath = os.path.join(self.output_dir, csv_filename)
        self.assertTrue(os.path.exists(csv_filepath))
        
        # Verify the content of the CSV file
        loaded_csv = pd.read_csv(csv_filepath)
        self.assertEqual(len(loaded_csv), len(cleaned_data))
        self.assertEqual(list(loaded_csv.columns), list(cleaned_data.columns))

if __name__ == '__main__':
    unittest.main()