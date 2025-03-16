import os
import unittest
import duckdb
from tempfile import TemporaryDirectory
import pandas as pd
from hypermvp.utils.db_versioning import create_duckdb_snapshot, add_version_metadata, cleanup_old_snapshots
import shutil

class TestDbVersioning(unittest.TestCase):
    def test_snapshot_creation(self):
        # Setup test DB
        temp_dir = TemporaryDirectory()
        db_path = os.path.join(temp_dir.name, "test.duckdb")
        
        # Create test DB and add data
        conn = duckdb.connect(db_path)
        conn.execute("CREATE TABLE test (id INTEGER)")
        conn.close()
        
        # Create snapshot
        snapshot_path = create_duckdb_snapshot(db_path)
        
        # Verify snapshot exists
        self.assertTrue(os.path.exists(snapshot_path))

    def test_version_metadata(self):
        """Test that version metadata is correctly added to the database."""
        # Setup test DB
        temp_dir = TemporaryDirectory()
        db_path = os.path.join(temp_dir.name, "test.duckdb")
        
        # Create test DB and add data
        conn = duckdb.connect(db_path)
        conn.execute("CREATE TABLE test (id INTEGER)")
        
        # Add version metadata
        source_files = ["/path/to/test/file.csv"]
        add_version_metadata(conn, source_files, "test_operation")
        
        # Verify metadata was added
        result = conn.execute("SELECT * FROM version_history").fetchall()
        conn.close()
        
        # Clean up
        temp_dir.cleanup()
        
        # Check results
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0][2], "test_operation")
        self.assertEqual(result[0][3], str(source_files))

    def test_snapshot_cleanup(self):
        """Test that old snapshots are properly cleaned up."""
        # Setup test directory
        temp_dir = TemporaryDirectory()
        snapshot_dir = os.path.join(temp_dir.name, "snapshots")
        os.makedirs(snapshot_dir)
        
        # Create fake old snapshots
        base_name = "test_db"
        for i in range(10):
            with open(os.path.join(snapshot_dir, f"{base_name}_{i}.duckdb"), "w") as f:
                f.write("dummy data")
        
        # Run cleanup (keep 5)
        cleanup_old_snapshots(snapshot_dir, base_name, keep=5)
        
        # Count remaining files
        remaining = [f for f in os.listdir(snapshot_dir) if f.startswith(base_name)]
        
        # Clean up
        temp_dir.cleanup()
        
        # Check results
        self.assertEqual(len(remaining), 5)

    def test_database_recovery(self):
        """Test that we can recover a database from a snapshot."""
        # Setup test DB
        temp_dir = TemporaryDirectory()
        db_path = os.path.join(temp_dir.name, "test.duckdb")
        
        # Create initial database with data
        conn = duckdb.connect(db_path)
        conn.execute("CREATE TABLE test_table (id INTEGER, value VARCHAR)")
        conn.execute("INSERT INTO test_table VALUES (1, 'original')")
        conn.close()
        
        # Create snapshot
        snapshot_path = create_duckdb_snapshot(db_path)
        
        # "Corrupt" database by changing data
        conn = duckdb.connect(db_path)
        conn.execute("UPDATE test_table SET value = 'corrupted' WHERE id = 1")
        conn.close()
        
        # Verify data is changed
        conn = duckdb.connect(db_path)
        corrupted_value = conn.execute("SELECT value FROM test_table WHERE id = 1").fetchone()[0]
        self.assertEqual(corrupted_value, "corrupted")
        conn.close()
        
        # Recover from snapshot
        shutil.copy2(snapshot_path, db_path)
        
        # Verify data is restored
        conn = duckdb.connect(db_path)
        restored_value = conn.execute("SELECT value FROM test_table WHERE id = 1").fetchone()[0]
        self.assertEqual(restored_value, "original")
        conn.close()
        
        # Clean up
        temp_dir.cleanup()