import unittest
import pandas as pd

class TestDelimiterAndNumericConversion(unittest.TestCase):

    def setUp(self):
        # Sample raw data with comma-delimited values
        self.raw_data = pd.DataFrame({
            "ENERGY_PRICE_[EUR/MWh]": ["138,85", "140,50", "130,75", "135,10", "invalid"]
        })

    def test_conversion_to_numeric(self):
        print("Before conversion:")
        print(self.raw_data["ENERGY_PRICE_[EUR/MWh]"])  # Print raw string values

        # Perform the conversion
        self.raw_data["ENERGY_PRICE_[EUR/MWh]"] = (
            self.raw_data["ENERGY_PRICE_[EUR/MWh]"]
            .str.replace(",", ".")
            .astype("float32", errors="ignore")  # Ensure float32 is explicitly used
            .apply(pd.to_numeric, errors="coerce")
        )

        print("\nAfter conversion:")
        print(self.raw_data["ENERGY_PRICE_[EUR/MWh]"])  # Print numeric values after conversion

        # Check that the conversion is successful for valid rows
        self.assertEqual(self.raw_data["ENERGY_PRICE_[EUR/MWh]"].iloc[0], 138.85)
        self.assertEqual(self.raw_data["ENERGY_PRICE_[EUR/MWh]"].iloc[1], 140.50)
        self.assertEqual(self.raw_data["ENERGY_PRICE_[EUR/MWh]"].iloc[2], 130.75)
        self.assertEqual(self.raw_data["ENERGY_PRICE_[EUR/MWh]"].iloc[3], 135.10)

        # Check that invalid values were coerced to NaN
        self.assertTrue(pd.isna(self.raw_data["ENERGY_PRICE_[EUR/MWh]"].iloc[4]))

if __name__ == "__main__":
    unittest.main()
