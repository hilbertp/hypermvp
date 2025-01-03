import unittest
import pandas as pd
from provider.cleaner import clean_provider_data

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
        cleaned_data = clean_provider_data(self.raw_data)

        # Test if DELIVERY_DATE is converted to datetime
        self.assertTrue(pd.api.types.is_datetime64_any_dtype(cleaned_data['DELIVERY_DATE']))

        # Test if ENERGY_PRICE_[EUR/MWh] values are converted to float64
        self.assertEqual(cleaned_data['ENERGY_PRICE_[EUR/MWh]'].dtype, 'float64')

        # Test if rows with notes are handled correctly
        self.assertIn("Warning: Found rows with notes", self._get_last_warning())
        
        # Test if NOTE column is dropped
        self.assertNotIn('NOTE', cleaned_data.columns)

        # Test if unnecessary columns are dropped
        self.assertNotIn('ENERGY_PRICE_PAYMENT_DIRECTION', cleaned_data.columns)

    def test_energy_price_adjustment(self):
        cleaned_data = clean_provider_data(self.raw_data)

        # Test adjusted values for PROVIDER_TO_GRID
        self.assertAlmostEqual(cleaned_data.iloc[2]['ENERGY_PRICE_[EUR/MWh]'], -135.10, places=2)
        self.assertAlmostEqual(cleaned_data.iloc[3]['ENERGY_PRICE_[EUR/MWh]'], -140.50, places=2)

        # Test conversion of invalid ENERGY_PRICE values
        self.assertTrue(pd.isna(cleaned_data.iloc[2]['ENERGY_PRICE_[EUR/MWh]']))

    def _get_last_warning(self):
        # Helper to capture the last warning printed (mock logging system or output capture needed)
        # For simplicity, assume captured logs or printed output can be parsed here.
        return "Stub for testing warning messages"

if __name__ == "__main__":
    unittest.main()
