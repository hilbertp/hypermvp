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
        # Print dates before conversion
        print("Before Conversion:")
        for index, date in enumerate(self.raw_data["DELIVERY_DATE"]):
            print(f"Row {index}: {date}, Type: {type(date)}")
        print(self.raw_data.dtypes)

        cleaned_data = clean_provider_data(self.raw_data)

        # Print dates after conversion
        print("\nAfter Conversion:")
        for index, date in enumerate(cleaned_data["DELIVERY_DATE"]):
            print(f"Row {index}: {date}, Type: {type(date)}")
        print(cleaned_data.dtypes)

        # Print column type explicitly
        print(f"Column dtype: {cleaned_data['DELIVERY_DATE'].dtype}")

        # Test if DELIVERY_DATE is converted to datetime
        self.assertEqual(cleaned_data["DELIVERY_DATE"].dtype, "datetime64[ns]")

        # Test if ENERGY_PRICE_[EUR/MWh] values are converted to float64
        self.assertEqual(cleaned_data['ENERGY_PRICE_[EUR/MWh]'].dtype, 'float64')

        # Test if NOTE column is dropped
        self.assertNotIn('NOTE', cleaned_data.columns)

        # Test if unnecessary columns are dropped
        self.assertNotIn('ENERGY_PRICE_PAYMENT_DIRECTION', cleaned_data.columns)

        # Test if POS_* rows are dropped
        self.assertFalse((cleaned_data['PRODUCT'].str.startswith('POS_')).any())

    def test_energy_price_adjustment(self):
        cleaned_data = clean_provider_data(self.raw_data)

        # Test adjusted values for NEG_* products
        self.assertAlmostEqual(cleaned_data.iloc[0]['ENERGY_PRICE_[EUR/MWh]'], 138.85, places=2)
        self.assertAlmostEqual(cleaned_data.iloc[1]['ENERGY_PRICE_[EUR/MWh]'], 140.50, places=2)
        self.assertAlmostEqual(cleaned_data.iloc[2]['ENERGY_PRICE_[EUR/MWh]'], -135.10, places=2)

        # Ensure POS_* rows are not present
        self.assertFalse((cleaned_data['PRODUCT'].str.startswith('POS_')).any())

if __name__ == "__main__":
    unittest.main()