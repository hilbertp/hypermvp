import unittest
import pandas as pd
from src.afrr.cleaner import filter_negative_50hertz

class TestFilterNegative50Hertz(unittest.TestCase):
    
    def test_valid_data(self):
        # Create a mock DataFrame with the relevant columns
        data = pd.DataFrame({
            'Datum': ['01.09.2024', '01.09.2024'],
            'Zeitzone': ['CEST', 'CEST'],
            'von': ['00:00', '00:15'],
            'bis': ['00:15', '00:30'],
            '50Hertz (Negativ)': [4.364, 10.052],
            'Amprion (Negativ)': [0.000, 0.000],
            'TenneT TSO (Negativ)': [0.148, 0.804]
        })
        
        # Strip any extra spaces from column names
        data.columns = data.columns.str.strip()
        
        # Check columns before filtering
        print("Columns in mock data:", data.columns.tolist())
        
        # Apply the filter function
        filtered_data = filter_negative_50hertz(data)
        
        # Assertions
        self.assertIsNotNone(filtered_data)  # Ensure the result is not None
        self.assertListEqual(filtered_data.columns.tolist(), ['Datum', 'von', 'bis', '50Hertz (Negativ)'])
        self.assertEqual(filtered_data.shape[0], 2)  # Ensure the number of rows is correct

    def test_missing_column(self):
        # Create a mock DataFrame missing the '50Hertz (Negativ)' column
        data = pd.DataFrame({
            'Datum': ['01.09.2024', '01.09.2024'],
            'Zeitzone': ['CEST', 'CEST'],
            'von': ['00:00', '00:15'],
            'bis': ['00:15', '00:30']
        })
        
        # Apply the filter function
        filtered_data = filter_negative_50hertz(data)
        
        # Check if None is returned due to the missing column
        self.assertIsNone(filtered_data)

    def test_empty_dataframe(self):
        # Create an empty DataFrame
        data = pd.DataFrame()
        
        # Apply the filter function
        filtered_data = filter_negative_50hertz(data)
        
        # Check if None is returned due to empty data
        self.assertIsNone(filtered_data)

if __name__ == '__main__':
    unittest.main()
