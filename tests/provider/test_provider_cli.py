"""
Tests for the provider CLI interface.
This uses subprocess to test actual command line invocation.
"""
import os
import unittest
import tempfile
import shutil
import subprocess
import pandas as pd
import duckdb

duckdb.sql("INSTALL excel;")
duckdb.sql("LOAD excel;")

try:
    res = duckdb.query("SELECT * FROM read_xlsx('path/to/your_test.xlsx') LIMIT 1").fetchall()
    print("Excel extension supported. Got:", res)
except Exception as e:
    print("Excel extension or read_xlsx is not supported:", e)

class TestProviderCli(unittest.TestCase):
    """Test the provider CLI interface."""

    def setUp(self):
        """Set up test environment."""
        # Create temporary directories for testing
        self.test_dir = tempfile.mkdtemp()
        self.input_dir = os.path.join(self.test_dir, 'input')
        self.db_path = os.path.join(self.test_dir, 'test_provider.duckdb')
        
        os.makedirs(self.input_dir, exist_ok=True)
        
        # Create test data
        self.test_data = pd.DataFrame({
            "DELIVERY_DATE": ["2024-09-01", "2024-09-02", "2024-09-01", "2024-09-03"],
            "PRODUCT": ["NEG_001", "NEG_002", "POS_003", "NEG_004"],
            "ENERGY_PRICE_[EUR/MWh]": ["138,85", "140,50", "100,00", "135,10"],
            "ENERGY_PRICE_PAYMENT_DIRECTION": ["GRID_TO_PROVIDER", "GRID_TO_PROVIDER", "PROVIDER_TO_GRID", "PROVIDER_TO_GRID"],
            "ALLOCATED_CAPACITY_[MW]": ["50", "30", "70", "20"],
            "NOTE": ["", "Test note", "", "Another note"]
        })
        
        # Save to Excel
        self.test_file = os.path.join(self.input_dir, 'test_provider.xlsx')
        self.test_data.to_excel(self.test_file, index=False)
        
        # For tests that don't use Excel extension directly, pre-populate the database
        try:
            import duckdb
            con = duckdb.connect(self.db_path)
            
            # Create raw table
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
            
            # Load data directly from pandas DataFrame
            con.register("df", self.test_data)
            con.execute("""
                INSERT INTO raw_provider_data 
                SELECT 
                    DELIVERY_DATE, 
                    PRODUCT, 
                    "ENERGY_PRICE_[EUR/MWh]", 
                    ENERGY_PRICE_PAYMENT_DIRECTION, 
                    "ALLOCATED_CAPACITY_[MW]", 
                    NOTE,
                    'test_provider.xlsx' AS source_file,
                    CURRENT_TIMESTAMP AS load_timestamp
                FROM df
            """)
            
            con.close()
        except Exception as e:
            print(f"Warning: Failed to pre-populate database: {e}")

    def tearDown(self):
        """Clean up test environment."""
        shutil.rmtree(self.test_dir)

    def test_cli_load(self):
        """Test the CLI load command."""
        print("\n=== Testing Provider CLI Load Command ===")
        
        # Run the CLI command
        command = [
            "python", "-m", "hypermvp.provider.provider_cli",
            "--load",
            "--dir", self.input_dir,
            "--db", self.db_path
        ]
        
        result = subprocess.run(command, capture_output=True, text=True)
        
        # Print command output for debugging
        print(f"Command: {' '.join(command)}")
        print("STDOUT:")
        print(result.stdout)
        
        if result.stderr:
            print("STDERR:")
            print(result.stderr)
        
        # Check that the command succeeded
        self.assertEqual(result.returncode, 0, "CLI command should succeed")
        self.assertIn("Loaded", result.stdout, "Output should contain 'Loaded'")
        
        print("=== Provider CLI Load Command Test Passed ===")

    def test_cli_all(self):
        """Test the CLI all command (load, analyze, update)."""
        print("\n=== Testing Provider CLI All Command ===")
        
        # Run the CLI command
        command = [
            "python", "-m", "hypermvp.provider.cli",  # was provider_cli
            "--all",
            "--dir", self.input_dir,
            "--db", self.db_path
        ]
        
        result = subprocess.run(command, capture_output=True, text=True)
        
        # Print command output for debugging
        print(f"Command: {' '.join(command)}")
        print("STDOUT (first 20 lines):")
        for line in result.stdout.splitlines()[:20]:
            print(line)
        
        if result.stderr:
            print("STDERR:")
            print(result.stderr)
        
        # Check that the command succeeded
        self.assertEqual(result.returncode, 0, "CLI command should succeed")
        self.assertIn("Loaded", result.stdout, "Output should contain 'Loaded'")
        self.assertIn("Analyzing raw data", result.stdout, "Output should contain 'Analyzing raw data'")
        self.assertIn("Updating data", result.stdout, "Output should contain 'Updating data'")
        
        print("=== Provider CLI All Command Test Passed ===")

if __name__ == "__main__":
    unittest.main(verbosity=2)