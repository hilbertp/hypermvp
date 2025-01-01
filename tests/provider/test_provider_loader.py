import os
import unittest
import pandas as pd
from src.provider.loader import load_provider_file

class TestLoader(unittest.TestCase):

    def setUp(self):
        self.valid_csv = "tests/data/sample_provider_data.csv"
        self.valid_xlsx = "tests/data/sample_provider_data.xlsx"
        self.invalid_file = "tests/data/sample_provider_data.txt"

    def test_load_csv_file(self):
        df = load_provider_file(self.valid_csv)
        self.assertIsInstance(df, pd.DataFrame)
        self.assertFalse(df.empty)

    def test_load_xlsx_file(self):
        df = load_provider_file(self.valid_xlsx)
        self.assertIsInstance(df, pd.DataFrame)
        self.assertFalse(df.empty)

    def test_load_invalid_file(self):
        with self.assertRaises(ValueError):
            load_provider_file(self.invalid_file)

if __name__ == "__main__":
    unittest.main()
