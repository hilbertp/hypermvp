"""
Tests for the provider database cleaner and updater.
"""
import os
import unittest
import tempfile
import shutil
import duckdb
import pandas as pd
from hypermvp.provider.provider_loader import load_excel_to_duckdb
from hypermvp.provider.provider_db_cleaner import (
    analyze_raw_provider_data,
    update_provider_data_in_db
)

class TestProviderDbCleaner(unittest.TestCase):
    """Test the provider database cleaning and updating functionality."""

    def setUp(self):
        """Set up test environment with test database and sample data."""
        # Create temporary directory and database
        self.test_dir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.test_dir, 'test_provider.duckdb')
        
        # Connect to DuckDB and create a sample raw_provider_data table
        con = duckdb.connect(self.db_path)
        
        # Create sample data directly
        con.execute("""
            CREATE TABLE raw_provider_data (
                DELIVERY_DATE VARCHAR,
                PRODUCT VARCHAR,
                "ENERGY_PRICE_[EUR/MWh]" VARCHAR,
                ENERGY_PRICE_PAYMENT_DIRECTION VARCHAR,
                "ALLOCATED_CAPACITY_[MW]" VARCHAR,
                NOTE VARCHAR,
                source_file VARCHAR,
                load_timestamp TIMESTAMP
            )
        """)
        
        # Insert sample data - 10 rows with a mix of NEG/POS products
        test_data = [
            # NEG products with different directions
            ('2024-09-01', 'NEG_001', '138,85', 'GRID_TO_PROVIDER', '50', '', 'test1.xlsx'),
            ('2024-09-02', 'NEG_002', '140,50', 'GRID_TO_PROVIDER', '30', 'Test note', 'test1.xlsx'),
            ('2024-09-03', 'NEG_003', '135,10', 'PROVIDER_TO_GRID', '20', 'Another note', 'test1.xlsx'),
            ('2024-09-01', 'NEG_004', '125,75', 'PROVIDER_TO_GRID', '45', '', 'test1.xlsx'),
            ('2024-09-02', 'NEG_005', '130,25', 'GRID_TO_PROVIDER', '35', '', 'test2.xlsx'),
            ('2024-09-03', 'NEG_006', '145,00', 'GRID_TO_PROVIDER', '25', '', 'test2.xlsx'),
            ('2024-09-04', 'NEG_007', '132,50', 'PROVIDER_TO_GRID', '40', '', 'test2.xlsx'),
            ('2024-09-05', 'NEG_008', '139,75', 'GRID_TO_PROVIDER', '55', '', 'test2.xlsx'),
            # POS products (should be filtered out)
            ('2024-09-01', 'POS_001', '100,00', 'PROVIDER_TO_GRID', '70', '', 'test1.xlsx'),
            ('2024-09-03', 'POS_002', '105,50', 'GRID_TO_PROVIDER', '65', '', 'test2.xlsx')
        ]
        
        # Insert data with timestamp
        for row in test_data:
            con.execute("""
                INSERT INTO raw_provider_data 
                (DELIVERY_DATE, PRODUCT, "ENERGY_PRICE_[EUR/MWh]", 
                 ENERGY_PRICE_PAYMENT_DIRECTION, "ALLOCATED_CAPACITY_[MW]", 
                 NOTE, source_file, load_timestamp)
                VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """, row)
        
        # Verify data was inserted
        count = con.execute("SELECT COUNT(*) FROM raw_provider_data").fetchone()[0]
        print(f"Created {count} test rows in raw_provider_data")
        
        # Close the connection
        con.close()

    def tearDown(self):
        """Clean up test environment."""
        shutil.rmtree(self.test_dir)

    def test_analyze_raw_provider_data(self):
        """Test analyzing raw provider data."""
        print("\n=== Testing Raw Provider Data Analysis ===")
        
        # Run the analyze function
        analyze_raw_provider_data(self.db_path, "raw_provider_data")
        
        # We can't easily test the output directly since it logs to console
        # This is primarily a visual test to ensure it runs without errors
        
        print("=== Raw Provider Data Analysis Test Passed ===")

    def test_update_provider_data_in_db(self):
        """Test updating provider data with date range handling."""
        print("\n=== Testing Provider Data Update ===")
        
        # Run the update function
        min_date, max_date, rows_updated = update_provider_data_in_db(
            self.db_path, "raw_provider_data", "provider_data"
        )
        
        # Verify the update was successful
        self.assertIsNotNone(min_date, "min_date should not be None")
        self.assertIsNotNone(max_date, "max_date should not be None")
        self.assertGreater(rows_updated, 0, "Expected at least one row to be updated")
        
        # Connect to the database and verify the clean table
        con = duckdb.connect(self.db_path)
        
        # Check if clean table exists
        table_exists = con.execute("""
            SELECT count(*) FROM information_schema.tables 
            WHERE table_name = 'provider_data'
        """).fetchone()[0]
        
        self.assertEqual(table_exists, 1, "Table 'provider_data' should exist")
        
        # Count rows in the clean table (should exclude POS_ products)
        row_count = con.execute("SELECT COUNT(*) FROM provider_data").fetchone()[0]
        self.assertEqual(row_count, 8, "Expected 8 rows in clean table (excluding POS products)")
        
        # Check that we have fewer rows than the raw table (due to filtering POS products)
        raw_count = con.execute("SELECT COUNT(*) FROM raw_provider_data").fetchone()[0]
        self.assertLess(row_count, raw_count, "Clean table should have fewer rows than raw table (POS products filtered)")
        
        # Check that no POS products exist in the clean table
        pos_count = con.execute("""
            SELECT COUNT(*) FROM provider_data 
            WHERE PRODUCT LIKE 'POS%'
        """).fetchone()[0]
        self.assertEqual(pos_count, 0, "No POS products should exist in the clean table")
        
        # Check that we have roughly 80% of rows (since 20% are POS_ products which are filtered)
        expected_row_count = int(raw_count * 0.8)  # 80% of raw rows
        self.assertAlmostEqual(row_count, expected_row_count, delta=5, 
                             msg=f"Expected ~{expected_row_count} rows (80% of raw count)")
        
        # Verify price adjustment was applied for PROVIDER_TO_GRID direction
        providers_to_grid_count = con.execute("""
            SELECT COUNT(*) FROM raw_provider_data 
            WHERE ENERGY_PRICE_PAYMENT_DIRECTION = 'PROVIDER_TO_GRID'
            AND PRODUCT NOT LIKE 'POS%'
        """).fetchone()[0]
        
        neg_price_count = con.execute("""
            SELECT COUNT(*) FROM provider_data 
            WHERE ENERGY_PRICE__EUR_MWh_ < 0
        """).fetchone()[0]
        
        self.assertEqual(neg_price_count, providers_to_grid_count, 
                        f"Expected {providers_to_grid_count} rows with negative prices (PROVIDER_TO_GRID direction)")
        
        # Verify columns in clean table
        expected_columns = [
            "DELIVERY_DATE", "PRODUCT", "ENERGY_PRICE__EUR_MWh_",
            "ALLOCATED_CAPACITY__MW_", "period", "source_file"
        ]
        
        col_info = con.execute("DESCRIBE provider_data").df()
        actual_columns = col_info['column_name'].tolist()
        
        for col in expected_columns:
            self.assertIn(col, actual_columns, f"Column '{col}' should exist in clean table")
        
        # Sample clean data for visual inspection
        clean_data = con.execute("SELECT * FROM provider_data LIMIT 5").df()
        print("\nCleaned data sample (first 5 rows):")
        print(clean_data)
        
        # Print some statistics
        stats = con.execute("""
            SELECT 
                COUNT(*) AS row_count,
                COUNT(DISTINCT PRODUCT) AS product_count,
                COUNT(DISTINCT DATE_TRUNC('day', DELIVERY_DATE)) AS date_count
            FROM provider_data
        """).fetchone()
        
        print(f"\nClean table statistics:")
        print(f"  Total rows: {stats[0]}")
        print(f"  Unique products: {stats[1]}")
        print(f"  Unique dates: {stats[2]}")
        
        # Close connection
        con.close()
        
        print("=== Provider Data Update Test Passed ===")

if __name__ == "__main__":
    unittest.main(verbosity=2)