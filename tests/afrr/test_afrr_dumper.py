import os
import unittest
import pandas as pd
from unittest.mock import patch
from hypermvp.afrr.dumper import dump_afrr_data
from tests.tests_config import TEST_RAW_DIR

class TestAfrrDumper(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.test_data_dir = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "../../tests_data/processed")
        )
        os.makedirs(cls.test_data_dir, exist_ok=True)
        # Clear previous test CSV files
        for f in os.listdir(cls.test_data_dir):
            if f.startswith("cleaned_test_afrr_") and f.endswith(".csv"):
                os.remove(os.path.join(cls.test_data_dir, f))

    def setUp(self):
        # Load test data that matches real data format
        self.cleaned_data = pd.read_csv(
            os.path.join(TEST_RAW_DIR, "test_afrr_sept.csv"),
            sep=";",
            decimal=",",
            parse_dates=["Datum"],
            dayfirst=True
        )

    @patch('hypermvp.afrr.dumper.PROCESSED_DATA_DIR', new_callable=lambda: TestAfrrDumper.test_data_dir)
    def test_dump_afrr_data(self, mock_processed_data_dir):
        print("\n=== aFRR Dumper Test ===")
        print(f"Testing: Export of cleaned aFRR data")
        print(f"Sample size: {len(self.cleaned_data)} rows")
        
        test_identifier = "test_afrr"
        dump_afrr_data(self.cleaned_data.copy(), test_identifier)

        files = os.listdir(self.test_data_dir)
        test_files = [f for f in files if test_identifier in f and f.endswith('.csv')]
        self.assertTrue(len(test_files) > 0, "The dump file was not created.")

        dumped_file = os.path.join(self.test_data_dir, test_files[0])
        dumped_data = pd.read_csv(
            dumped_file,
            parse_dates=["Datum"], 
            date_format="%d.%m.%Y"
        )
        
        pd.testing.assert_frame_equal(dumped_data, self.cleaned_data)
        print(f"Result: All assertions passed")
        print("=======================\n")

if __name__ == "__main__":
    unittest.main(verbosity=0)  # Reduce default unittest output