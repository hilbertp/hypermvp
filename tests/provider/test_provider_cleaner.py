import unittest
import pandas as pd
from hypermvp.provider.cleaner import clean_provider_data

class TestCleaner(unittest.TestCase):

    def setUp(self):
        # Sample raw DataFrame with some notes and edge cases
        self.raw_data = pd.DataFrame({
            "DELIVERY_DATE": ["9/1/2024", "9/2/2024", "9/1/2024", "9/3/2024"],
            "PRODUCT": ["NEG_001", "NEG_002", "POS_003", "NEG_004"],
            "ENERGY_PRICE_[EUR/MWh]": ["138,85", "140,50 ", "N/A", " 135,10"],
            "ENERGY_PRICE_PAYMENT_DIRECTION": ["GRID_TO_PROVIDER", "GRID_TO_PROVIDER", "PROVIDER_TO_GRID", "PROVIDER_TO_GRID"],
            "ALLOCATED_CAPACITY_[MW]": [50, 30, 70, 20],
            "NOTE": [None, "Test note 2", None, "Test note 4"]
        })

    def test_clean_provider_data(self):
        print("\n=== Provider Data Cleaner Test ===")
        
        # Print warnings about rows with notes
        if "NOTE" in self.raw_data.columns and self.raw_data["NOTE"].notnull().any():
            rows_with_notes = self.raw_data[self.raw_data["NOTE"].notnull()]
            print("Warning: The following rows contain notes and will be dropped. Please review them:")
            print(rows_with_notes)
        
        # Print before price adjustment
        print("\nBefore ENERGY_PRICE adjustment:")
        print(self.raw_data[["PRODUCT", "ENERGY_PRICE_[EUR/MWh]", "ENERGY_PRICE_PAYMENT_DIRECTION"]])
        
        # Clean the data
        cleaned_data = clean_provider_data(self.raw_data)
        
        # Print after price adjustment
        print("\nAfter ENERGY_PRICE adjustment:")
        print(cleaned_data[["PRODUCT", "ENERGY_PRICE_[EUR/MWh]"]])
        
        # Assert that the DELIVERY_DATE column is converted to datetime
        self.assertTrue(pd.api.types.is_datetime64_any_dtype(cleaned_data["DELIVERY_DATE"]))
        
        # Assert that the ENERGY_PRICE_[EUR/MWh] column is converted to float64
        self.assertTrue(pd.api.types.is_float_dtype(cleaned_data["ENERGY_PRICE_[EUR/MWh]"]))
        
        # Assert that the NOTE column is dropped
        self.assertNotIn("NOTE", cleaned_data.columns)
        
        # Assert that the ENERGY_PRICE_PAYMENT_DIRECTION column is dropped
        self.assertNotIn("ENERGY_PRICE_PAYMENT_DIRECTION", cleaned_data.columns)
        
        # Assert that rows with PRODUCT starting with "POS_" are dropped
        self.assertFalse(cleaned_data["PRODUCT"].str.startswith("POS_").any())
        
        print("\nTest Result: All assertions passed")
        print("===============================\n")

if __name__ == "__main__":
    unittest.main(verbosity=2)