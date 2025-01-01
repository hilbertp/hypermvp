import unittest
import pandas as pd
from src.provider.cleaner import clean_provider_data

class TestCleaner(unittest.TestCase):

    def setUp(self):
        # Sample raw DataFrame
        self.raw_data = pd.DataFrame({
            "DELIVERY_DATE": ["2024-09-01", "2024-09-01"],
            "PRODUCT": ["neg_001", "pos_001"],
            "ENERGY_PRICE_[EUR/MWh]": [10.0, 15.0],
            "ENERGY_PRICE_PAYMENT_DIRECTION": ["PROVIDER_TO_GRID", "GRID_TO_PROVIDER"],
            "ALLOCATED_CAPACITY_[MW]": [50, 30]
        })

    def test_clean_provider_data(self):
        cleaned_data = clean_provider_data(self.raw_data)
        self.assertEqual(len(cleaned_data), 1)  # Only 'neg_*' should remain
        self.assertEqual(cleaned_data.iloc[0]["ENERGY_PRICE_[EUR/MWh]"], -10.0)
        self.assertTrue("ENERGY_PRICE_PAYMENT_DIRECTION" not in cleaned_data.columns)

if __name__ == "__main__":
    unittest.main()
