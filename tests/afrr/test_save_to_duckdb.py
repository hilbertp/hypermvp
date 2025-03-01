import os
import unittest
import pandas as pd
import duckdb
from datetime import datetime
from tempfile import TemporaryDirectory
from hypermvp.afrr.save_to_duckdb import save_afrr_to_duckdb

class TestSaveAfrrToDuckDB(unittest.TestCase):
    
    def setUp(self):
        # Create a temporary directory for the test database
        self.temp_dir = TemporaryDirectory()
        self.db_path = os.path.join(self.temp_dir.name, "test_afrr.duckdb")
        
        # Create sample test data
        self.test_data = pd.DataFrame({
            'Datum': pd.to_datetime(['2024-09-01', '2024-09-01']),
            'von': ['00:00', '00:15'],
            'bis': ['00:15', '00:30'],
            '50Hertz (Negativ)': [4.364, 10.052]
        })
        
        # Test parameters
        self.month = 9
        self.year = 2024
        self.table_name = "test_afrr_data"
    
    def tearDown(self):
        # Close and delete the temporary directory
        self.temp_dir.cleanup()
    
    def test_save_afrr_to_duckdb_creates_table(self):
        """Test if save_afrr_to_duckdb creates a table when it doesn't exist."""
        # Act: Save data to a new database
        rows_inserted = save_afrr_to_duckdb(
            self.test_data,
            self.month,
            self.year,
            self.table_name,
            self.db_path
        )
        
        # Assert
        self.assertEqual(rows_inserted, 2, "Should have inserted 2 rows")
        
        # Verify data was saved correctly
        conn = duckdb.connect(self.db_path)
        table_exists = conn.execute(
            f"SELECT name FROM sqlite_master WHERE type='table' AND name='{self.table_name}'"
        ).fetchone() is not None
        self.assertTrue(table_exists, "Table should exist in the database")
        
        # Check data content
        result = conn.execute(f"SELECT * FROM {self.table_name}").fetchdf()
        conn.close()
        
        # Validate results
        self.assertEqual(len(result), 2, "Should have 2 records in the database")
        self.assertEqual(result['month'].tolist(), [9, 9], "Month should be 9")
        self.assertEqual(result['year'].tolist(), [2024, 2024], "Year should be 2024")
        pd.testing.assert_series_equal(
            result['50Hertz (Negativ)'], 
            self.test_data['50Hertz (Negativ)'],
            check_names=False
        )
    
    def test_save_afrr_to_duckdb_updates_existing_data(self):
        """Test if save_afrr_to_duckdb correctly updates existing data."""
        # Arrange: First insert initial data
        save_afrr_to_duckdb(
            self.test_data,
            self.month,
            self.year,
            self.table_name,
            self.db_path
        )
        
        # Create updated data with same month/year but different values
        updated_data = pd.DataFrame({
            'Datum': pd.to_datetime(['2024-09-01', '2024-09-01', '2024-09-02']),
            'von': ['00:00', '00:15', '00:00'],
            'bis': ['00:15', '00:30', '00:15'],
            '50Hertz (Negativ)': [5.0, 6.0, 7.0]  # Different values
        })
        
        # Act: Update with new data
        rows_inserted = save_afrr_to_duckdb(
            updated_data,
            self.month,
            self.year,
            self.table_name,
            self.db_path
        )
        
        # Assert
        self.assertEqual(rows_inserted, 3, "Should have inserted 3 rows")
        
        # Verify data was updated correctly (old data replaced)
        conn = duckdb.connect(self.db_path)
        result = conn.execute(f"SELECT * FROM {self.table_name}").fetchdf()
        conn.close()
        
        # Should have only the new data for September 2024
        self.assertEqual(len(result), 3, "Should have 3 records after update")
        pd.testing.assert_series_equal(
            result['50Hertz (Negativ)'], 
            updated_data['50Hertz (Negativ)'],
            check_names=False
        )

if __name__ == '__main__':
    unittest.main()