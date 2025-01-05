import unittest
import pandas as pd
from hypermvp.provider.merger import merge_cleaned_data

class TestMerger(unittest.TestCase):

    def setUp(self):
        self.df1 = pd.DataFrame({
            "DELIVERY_DATE": ["2024-09-01"],
            "PRODUCT": ["neg_001"],
            "ENERGY_PRICE_[EUR/MWh]": [-10.0],
            "ALLOCATED_CAPACITY_[MW]": [50]
        })

        self.df2 = pd.DataFrame({
            "DELIVERY_DATE": ["2024-09-01"],
            "PRODUCT": ["neg_002"],
            "ENERGY_PRICE_[EUR/MWh]": [-15.0],
            "ALLOCATED_CAPACITY_[MW]": [30]
        })

    def test_merge_cleaned_data(self):
        combined_df = merge_cleaned_data(self.df1, self.df2)
        self.assertEqual(len(combined_df), 2)
        self.assertListEqual(combined_df["PRODUCT"].tolist(), ["neg_001", "neg_002"])

if __name__ == "__main__":
    unittest.main()
