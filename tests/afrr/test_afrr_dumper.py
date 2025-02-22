import os
import unittest
import pandas as pd
from unittest.mock import patch
from hypermvp.afrr.dumper import dump_afrr_data
from config import PROCESSED_TEST_DIR, OUTPUT_TEST_DIR  # use test directories from config

class TestAfrrDumper(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Use the processed test directory for test input data
        cls.processed_data_dir = PROCESSED_TEST_DIR
        os.makedirs(cls.processed_data_dir, exist_ok=True)
        # Clear previously dumped output files in the test output folder.
        cls.base_output_dir = OUTPUT_TEST_DIR
        os.makedirs(cls.base_output_dir, exist_ok=True)
        for f in os.listdir(cls.base_output_dir):
            if f.startswith("cleaned_test_afrr_") and f.endswith('.csv'):
                os.remove(os.path.join(cls.base_output_dir, f))

    def setUp(self):
        """Load static sample data from the processed test folder."""
        # Load the sample processed file – this is the output from loader & cleaner.
        processed_file_path = os.path.join(self.processed_data_dir, "test_afrr_sept_clean.csv")
        self.sample_data = pd.read_csv(
            processed_file_path,
            sep=";",
            decimal=",",
            parse_dates=["Datum"]
        )

    def test_dump_afrr_data(self):
        """Test that dumper correctly writes AFRR data to the output folder."""
        test_identifier = "test_afrr"
        
        # Act: Dump the data to the test output folder by patching OUTPUT_DATA_DIR in dumper.py.
        with patch('config.OUTPUT_DATA_DIR', self.base_output_dir):
            dump_afrr_data(self.sample_data.copy(), test_identifier)

        # Assert: Check that at least one file is created in the test output folder.
        files = [f for f in os.listdir(self.base_output_dir)
                 if f.startswith(f"cleaned_{test_identifier}_") and f.endswith('.csv')]
        self.assertGreaterEqual(len(files), 1, "Expected at least one output file in the output directory")
        
        # Read back the dumped file.
        dumped_file = os.path.join(self.base_output_dir, files[0])
        dumped_data = pd.read_csv(
            dumped_file,
            sep=";",
            decimal=",",
            parse_dates=["Datum"]
        )
        # Since ISO 8601 dates are maintained throughout the pipeline, the dumped data should match.
        pd.testing.assert_frame_equal(dumped_data, self.sample_data)

        print("Dumped file header:")
        print(dumped_data.columns.to_list())
        print("First five rows:")
        print(dumped_data.head())

if __name__ == "__main__":
    unittest.main(verbosity=0)