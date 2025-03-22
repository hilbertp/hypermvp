import os
import unittest
import duckdb
import json 
from tempfile import TemporaryDirectory
import pandas as pd
from hypermvp.utils.db_versioning import create_duckdb_snapshot, add_version_metadata, cleanup_old_snapshots
import shutil
import gzip

class TestDbVersioning(unittest.TestCase):
    def test_snapshot_creation(self):
        # Setup test DB
        temp_dir = TemporaryDirectory()
        db_path = os.path.join(temp_dir.name, "test.duckdb")
        
        # Create test DB and add data
        conn = duckdb.connect(db_path)
        conn.execute("CREATE TABLE test (id INTEGER)")
        conn.close()
        
        # Create snapshot - FORCE ENABLE for testing
        snapshot_path = create_duckdb_snapshot(db_path, force_enable=True)
        
        # Verify snapshot exists
        self.assertTrue(os.path.exists(snapshot_path))
        
        # Clean up
        temp_dir.cleanup()

    def test_version_metadata(self):
        """Test that version metadata is correctly added to the database."""
        # Setup test DB
        temp_dir = TemporaryDirectory()
        db_path = os.path.join(temp_dir.name, "test.duckdb")
        
        # Create test DB and add data
        conn = duckdb.connect(db_path)
        conn.execute("CREATE TABLE test (id INTEGER)")
        
        # Create version_history table
        conn.execute("""
            CREATE TABLE version_history (
                id BIGINT PRIMARY KEY, 
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                operation VARCHAR,
                source_files VARCHAR
            )
        """)
        # Add a separate sequence for ID generation
        conn.execute("CREATE SEQUENCE version_seq")
 
        # Add version metadata
        source_files = ["/path/to/test/file.csv"]
        add_version_metadata(conn, source_files, "test_operation")
        
        # Verify metadata was added
        result = conn.execute("SELECT * FROM version_history").fetchall()
        # Clean up
        temp_dir.cleanup()
        
        # Check results
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0][2], "test_operation")
        self.assertEqual(result[0][3], str(source_files))
        
        conn.close()

    def test_snapshot_cleanup(self):
        """Test that old snapshots are properly cleaned up."""
        # Setup test directory
        temp_dir = TemporaryDirectory()
        snapshot_dir = os.path.join(temp_dir.name, "snapshots")
        os.makedirs(snapshot_dir)
        
        # Create fake old snapshots with .duckdb.gz extension
        base_name = "test_db"
        for i in range(10):
            with gzip.open(os.path.join(snapshot_dir, f"{base_name}_{i}.duckdb.gz"), "wb") as f:
                f.write(b"dummy data")  # Note: gzip requires bytes, not strings
        
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
        
        # Create snapshot - force enable for testing
        snapshot_path = create_duckdb_snapshot(db_path, force_enable=True)
        
        # Verify snapshot was created
        self.assertIsNotNone(snapshot_path)
        
        # "Corrupt" database by changing data
        conn = duckdb.connect(db_path)
        conn.execute("UPDATE test_table SET value = 'corrupted' WHERE id = 1")
        conn.close()
        
        # Verify data is changed
        conn = duckdb.connect(db_path)
        corrupted_value = conn.execute("SELECT value FROM test_table WHERE id = 1").fetchone()[0]
        self.assertEqual(corrupted_value, "corrupted")
        conn.close()
        
        # Recover from snapshot - need to decompress it first
        with gzip.open(snapshot_path, 'rb') as f_in:
            with open(db_path, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        
        # Verify data is restored
        conn = duckdb.connect(db_path)
        restored_value = conn.execute("SELECT value FROM test_table WHERE id = 1").fetchone()[0]
        self.assertEqual(restored_value, "original")
        conn.close()
        
        # Clean up
        temp_dir.cleanup()

def add_version_metadata(conn, source_files, operation):
    max_id = conn.execute("SELECT COALESCE(MAX(id), 0) + 1 FROM version_history").fetchone()[0]
    files_str = json.dumps(source_files)
    conn.execute(
        "INSERT INTO version_history (id, operation, source_files) VALUES (?, ?, ?)",
        (max_id, operation, files_str)
    )
    return max_id