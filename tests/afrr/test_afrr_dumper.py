import unittest
import os
import pandas as pd
from hypermvp.afrr.dumper import dump_afrr_data
from hypermvp.config import PROCESSED_DATA_DIR

class TestAfrrDumper(unittest.TestCase):

    def setUp(self):
        # Create a sample cleaned DataFrame as mock data
        self.cleaned_data = pd.DataFrame({
            'Datum': ['2024-09-01', '2024-09-01'],
            'von': ['00:00', '00:15'],
            'bis': ['00:15', '00:30'],
            '50Hertz (Negativ)': [4.364, 10.052]
        })

        # Ensure the processed directory exists
        os.makedirs(PROCESSED_DATA_DIR, exist_ok=True)

    def test_dump_afrr_data(self):
        # Define a test identifier
        test_identifier = "test_afrr"

        # Call the dumper function
        dump_afrr_data(self.cleaned_data, test_identifier)

        # Verify if the file was created
        files = os.listdir(PROCESSED_DATA_DIR)
        test_files = [f for f in files if test_identifier in f and f.endswith('.csv')]
        self.assertTrue(len(test_files) > 0, "The dump file was not created.")

        # Verify content of the dumped file
        for file in test_files:
            file_path = os.path.join(PROCESSED_DATA_DIR, file)
            dumped_data = pd.read_csv(file_path)
            pd.testing.assert_frame_equal(dumped_data, self.cleaned_data)

        # Clean up test files
        for file in test_files:
            os.remove(os.path.join(PROCESSED_DATA_DIR, file))

if __name__ == "__main__":
    unittest.main()