import unittest
import os
import pandas as pd
from unittest.mock import patch
from hypermvp.afrr.dumper import dump_afrr_data

class TestAfrrDumper(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Define the tests_data/processed directory for testing
        cls.test_data_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../tests_data/processed"))
        os.makedirs(cls.test_data_dir, exist_ok=True)

    def setUp(self):
        # Create a sample cleaned DataFrame as mock data
        self.cleaned_data = pd.DataFrame({
            'Datum': ['2024-09-01', '2024-09-01'],
            'von': ['00:00', '00:15'],
            'bis': ['00:15', '00:30'],
            '50Hertz (Negativ)': [4.364, 10.052]
        })

    @patch('hypermvp.afrr.dumper.PROCESSED_DATA_DIR', new_callable=lambda: TestAfrrDumper.test_data_dir)
    def test_dump_afrr_data(self, mock_processed_data_dir):
        # Define a test identifier
        test_identifier = "test_afrr"

        # Call the dumper function
        dump_afrr_data(self.cleaned_data, test_identifier)

        # Verify if the file was created in the mocked directory
        files = os.listdir(self.test_data_dir)
        test_files = [f for f in files if test_identifier in f and f.endswith('.csv')]
        print(f"Files in test directory: {files}")
        self.assertTrue(len(test_files) > 0, "The dump file was not created.")

        # Verify content of the dumped file
        for file in test_files:
            file_path = os.path.join(self.test_data_dir, file)
            dumped_data = pd.read_csv(file_path)
            pd.testing.assert_frame_equal(dumped_data, self.cleaned_data)

if __name__ == "__main__":
    unittest.main(verbosity=2)