"""
Tests for the direct DuckDB provider loader.
"""
import os
import unittest
import tempfile
import shutil
import duckdb
from hypermvp.provider.provider_loader import load_excel_to_duckdb

class TestDirectLoader(unittest.TestCase):
    """Test the direct DuckDB loader functionality."""

    def setUp(self):
        """Set up test environment with temporary database."""
        # Create temporary directory and database
        self.test_dir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.test_dir, 'test_provider.duckdb')
        
        # Path to test file - update this to your test file location
        self.test_file = "/home/philly/hypermvp/tests/tests_data/raw/test_provider_list.xlsx"
        
        # Skip if test file doesn't exist
        if not os.path.exists(self.test_file):
            self.skipTest(f"Test file not found: {self.test_file}")

    def tearDown(self):
        """Clean up after test."""
        # Remove temporary directory and files
        shutil.rmtree(self.test_dir)

    def test_load_excel_to_duckdb(self):
        """Test direct loading of Excel files to DuckDB."""
        print("\n=== Testing Direct DuckDB Provider Loader ===")
        print(f"Using test file: {self.test_file}")
        
        # Load the test file
        success_count, total_rows, processed_files = load_excel_to_duckdb(
            [self.test_file], 
            self.db_path, 
            "test_raw_provider"
        )
        
        # Check that one file was processed successfully
        self.assertEqual(success_count, 1, "Expected 1 file to be processed successfully")
        self.assertGreater(total_rows, 0, "Expected at least one row to be loaded")
        self.assertEqual(len(processed_files), 1, "Expected 1 file in processed files list")
        
        # Connect to the database and verify the data was loaded
        con = duckdb.connect(self.db_path)
        
        # Check if table exists
        table_exists = con.execute("""
            SELECT count(*) FROM information_schema.tables 
            WHERE table_name = 'test_raw_provider'
        """).fetchone()[0]
        
        self.assertEqual(table_exists, 1, "Table 'test_raw_provider' should exist")
        
        # Count rows in the table
        row_count = con.execute("SELECT COUNT(*) FROM test_raw_provider").fetchone()[0]
        self.assertEqual(row_count, total_rows, 
                        f"Row count in DB ({row_count}) should match reported count ({total_rows})")
        
        # Verify required columns exist
        required_columns = [
            "DELIVERY_DATE", "PRODUCT", "ENERGY_PRICE_[EUR/MWh]",
            "ENERGY_PRICE_PAYMENT_DIRECTION", "ALLOCATED_CAPACITY_[MW]", "NOTE"
        ]
        
        col_info = con.execute("DESCRIBE test_raw_provider").df()
        actual_columns = col_info['column_name'].tolist()
        
        for col in required_columns:
            self.assertIn(col, actual_columns, f"Column '{col}' should exist in the table")
        
        # Check that source_file and load_timestamp were added
        self.assertIn("source_file", actual_columns, "Column 'source_file' should exist")
        self.assertIn("load_timestamp", actual_columns, "Column 'load_timestamp' should exist")
        
        # Print some statistics for verification
        stats = con.execute("""
            SELECT 
                COUNT(*) AS row_count,
                COUNT(DISTINCT PRODUCT) AS product_count
            FROM test_raw_provider
        """).fetchone()
        
        print(f"Loaded {stats[0]} rows with {stats[1]} distinct products")
        
        # Sample some data for visual inspection
        sample = con.execute("SELECT * FROM test_raw_provider LIMIT 5").df()
        print("\nSample data:")
        print(sample)
        
        # Close connection
        con.close()
        
        print("=== Direct DuckDB Provider Loader Test Passed ===")

if __name__ == "__main__":
    unittest.main(verbosity=2)
