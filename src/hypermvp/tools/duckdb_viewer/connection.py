"""
Database connection utilities for DuckDB viewer.

Provides standardized connection handling and query execution,
leveraging existing configuration from global_config.py.
"""

import duckdb
import polars as pl
from typing import Union, Dict, Optional, List
from pathlib import Path

from hypermvp.global_config import ENERGY_DB_PATH, TEST_ENERGY_DB_PATH

def get_connection(db_path: str = ENERGY_DB_PATH) -> duckdb.DuckDBPyConnection:
    """
    Create and return a connection to the DuckDB database.
    
    Plain English: Opens a connection to your DuckDB file that contains all your data.
    
    Args:
        db_path: Path to the DuckDB database file, defaults to project's main database
        
    Returns:
        A DuckDB connection object
    """
    if not Path(db_path).exists():
        raise FileNotFoundError(f"DuckDB database not found at {db_path}")
    
    return duckdb.connect(db_path)

def query_to_polars(query: str, conn = None, close_conn: bool = True) -> pl.DataFrame:
    """
    Execute a SQL query and return results as a Polars DataFrame.
    
    Plain English: Runs an SQL query on your database and returns the results in a
    format that's easy to analyze and process further.
    
    Args:
        query: SQL query to execute
        conn: Optional existing connection (creates one if not provided)
        close_conn: Whether to close the connection after executing the query
        
    Returns:
        Query results as a Polars DataFrame
    """
    # Create connection if needed
    should_close = False
    if conn is None:
        conn = get_connection()
        should_close = close_conn
    
    try:
        # Execute query and convert to Polars
        result = conn.execute(query).pl()
        return result
    finally:
        if should_close:
            conn.close()

def get_table_schema(table_name: str, conn = None) -> List[Dict[str, str]]:
    """
    Get schema information for a specific table.
    
    Plain English: Returns information about what columns exist in a table and 
    what data types they contain.
    
    Args:
        table_name: Name of the table to get schema for
        conn: Optional existing connection
        
    Returns:
        List of dictionaries with column information
    """
    close_conn = False
    if conn is None:
        conn = get_connection()
        close_conn = True
        
    try:
        # Query for column information
        schema_df = query_to_polars(
            f"PRAGMA table_info('{table_name}')", 
            conn=conn,
            close_conn=False
        )
        
        if len(schema_df) == 0:
            raise ValueError(f"Table '{table_name}' not found in database")
            
        # Convert to list of dictionaries
        columns = []
        for row in schema_df.iter_rows(named=True):
            columns.append({
                "name": row["name"],
                "type": row["type"],
                "notnull": bool(row["notnull"]),
                "pk": bool(row["pk"])
            })
            
        return columns
    finally:
        if close_conn:
            conn.close()

def get_sample_data(table_name: str, limit: int = 10, conn = None) -> pl.DataFrame:
    """
    Get a sample of data from the specified table.
    
    Plain English: Returns a few example rows from a table to help understand the data.
    
    Args:
        table_name: Name of the table to sample
        limit: Maximum number of rows to return
        conn: Optional existing connection
        
    Returns:
        Polars DataFrame with sample data
    """
    return query_to_polars(
        f"SELECT * FROM {table_name} LIMIT {limit}",
        conn=conn
    )