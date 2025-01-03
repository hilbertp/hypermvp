import os
import unittest
import pandas as pd
from afrr.cleaner import filter_negative_50hertz  # Make sure this import path is correct

class TestFilterNegative50Hertz(unittest.TestCase):
    
    def test_valid_data(self):
        # Create a mock DataFrame with all values set to 0
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

        # Check columns and values before filtering
        print("Data before filtering:")
        print(data)
        print("Columns in mock data before filtering:", data.columns.tolist())
        
        # Apply the filter function (it should only return the specified columns)
        filtered_data = filter_negative_50hertz(data)
        
        # Check columns and values after filtering
        print("Data after filtering:")
        print(filtered_data)
        print("Columns in filtered data:", filtered_data.columns.tolist())
        
        # Assertions
        self.assertIsNotNone(filtered_data)  # Ensure the result is not None
        self.assertListEqual(filtered_data.columns.tolist(), ['Datum', 'von', 'bis', '50Hertz (Negativ)'])  # Ensure we only have the expected columns

if __name__ == '__main__':
    unittest.main()
