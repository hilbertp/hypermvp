import os
import logging
import shutil
import gzip
from datetime import datetime
from hypermvp.global_config import OUTPUT_DATA_DIR, PROVIDER_DUCKDB_PATH  # Import both needed config variables

# Add this flag at the top of the file
ENABLE_SNAPSHOTS = False  # Set to True when ready for production

# Define snapshot directory using direct path from config
SNAPSHOT_DIR = os.path.join(OUTPUT_DATA_DIR, "snapshots")

def create_duckdb_snapshot(db_path, keep=3, force_enable=False):
    """
    Create a compressed snapshot of the DuckDB file before making changes.
    
    Args:
        db_path: Path to the database file to snapshot
        keep: Number of snapshots to keep
        force_enable: If True, create snapshot even if ENABLE_SNAPSHOTS is False (for testing)
    """
    if not (ENABLE_SNAPSHOTS or force_enable):
        logging.info("Snapshots disabled for pilot phase")
        return None

    if not os.path.exists(db_path):
        logging.info(f"No database at {db_path} to snapshot")
        return None

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    # For tests, use directory where the db is located
    snapshot_dir = os.path.join(os.path.dirname(db_path), "snapshots")
    os.makedirs(snapshot_dir, exist_ok=True)
    
    # Get database filename without extension
    base_name = os.path.basename(db_path)
    name_without_ext = os.path.splitext(base_name)[0]
    
    # Create compressed snapshot path
    snapshot_path = os.path.join(snapshot_dir, f"{name_without_ext}_{timestamp}.duckdb.gz")
    
    # Create compressed copy
    with open(db_path, 'rb') as f_in:
        with gzip.open(snapshot_path, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    logging.info(f"Created compressed database snapshot: {snapshot_path}")
    
    # Cleanup old snapshots
    cleanup_old_snapshots(snapshot_dir, name_without_ext, keep)
    
    return snapshot_path

def cleanup_old_snapshots(snapshot_dir, base_name, keep=3):
    """Keep only the N most recent snapshots to avoid disk space issues."""
    snapshots = [f for f in os.listdir(snapshot_dir) 
               if f.startswith(base_name) and f.endswith('.duckdb.gz')]
    snapshots.sort(reverse=True)  # Sort by timestamp descending
    
    # Remove older snapshots beyond the keep count
    for old_snapshot in snapshots[keep:]:
        os.remove(os.path.join(snapshot_dir, old_snapshot))
        logging.info(f"Removed old snapshot: {old_snapshot}")

def add_version_metadata(conn, source_files, operation_type):
    """Add version metadata to track the lineage of data operations."""
    # Create version table if it doesn't exist 
    conn.execute("""
        CREATE TABLE IF NOT EXISTS version_history (
            version_id INTEGER PRIMARY KEY,
            timestamp TIMESTAMP,
            operation_type VARCHAR,
            source_files VARCHAR,
            username VARCHAR
        )
    """)
    
    # Get the next version_id 
    max_id = conn.execute("SELECT COALESCE(MAX(version_id), 0) + 1 FROM version_history").fetchone()[0]
    
    # Insert with explicit version_id
    conn.execute("""
        INSERT INTO version_history (version_id, timestamp, operation_type, source_files, username)
        VALUES (?, ?, ?, ?, ?)
    """, (
        max_id,
        datetime.now(),
        operation_type,
        str(source_files),
        os.environ.get('USER', 'unknown')
    ))

def vacuum_database(conn):
    """Reclaim space in the database after operations."""
    try:
        conn.execute("VACUUM")
        logging.info("Database vacuumed to reclaim space")
    except Exception as e:
        logging.warning(f"Failed to vacuum database: {e}")