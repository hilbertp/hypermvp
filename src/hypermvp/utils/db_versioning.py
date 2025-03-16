import os
import logging
import shutil
from datetime import datetime

def create_duckdb_snapshot(db_path):
    """Create a timestamped snapshot of the DuckDB file before making changes."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    snapshot_dir = os.path.join(os.path.dirname(db_path), "snapshots")
    os.makedirs(snapshot_dir, exist_ok=True)
    
    # Get database filename without extension
    base_name = os.path.basename(db_path)
    name_without_ext = os.path.splitext(base_name)[0]
    
    # Create snapshot path
    snapshot_path = os.path.join(snapshot_dir, f"{name_without_ext}_{timestamp}.duckdb")
    
    # Only create snapshot if the original file exists
    if os.path.exists(db_path):
        shutil.copy2(db_path, snapshot_path)
        logging.info(f"Created database snapshot: {snapshot_path}")
        
        # Optional: Cleanup old snapshots (keep last 5)
        cleanup_old_snapshots(snapshot_dir, name_without_ext, keep=5)
    
    return snapshot_path

def cleanup_old_snapshots(snapshot_dir, base_name, keep=5):
    """Keep only the N most recent snapshots to avoid disk space issues."""
    snapshots = [f for f in os.listdir(snapshot_dir) if f.startswith(base_name)]
    snapshots.sort(reverse=True)  # Sort by timestamp descending
    
    # Remove older snapshots beyond the keep count
    for old_snapshot in snapshots[keep:]:
        os.remove(os.path.join(snapshot_dir, old_snapshot))
        logging.info(f"Removed old snapshot: {old_snapshot}")

def add_version_metadata(conn, source_files, operation_type):
    """Add version metadata to track the lineage of data operations."""
    # Create version table if it doesn't exist (without AUTOINCREMENT)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS version_history (
            version_id INTEGER PRIMARY KEY,
            timestamp TIMESTAMP,
            operation_type VARCHAR,
            source_files VARCHAR,
            username VARCHAR
        )
    """)
    
    # Get the next version_id (max + 1 or 1 if the table is empty)
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