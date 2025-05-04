"""
Reusable SQL query templates for the DuckDB viewer.

This module provides standardized SQL queries that can be used
for common database operations.
"""

def list_tables_query() -> str:
    """
    Generate a query to list all tables in the database.
    
    Plain English: Returns a SQL query that will show all tables in your database.
    
    Returns:
        SQL query string
    """
    return """
    SELECT table_name 
    FROM information_schema.tables 
    WHERE table_schema = 'main'
    ORDER BY table_name
    """

def table_preview_query(table_name: str, limit: int = 10) -> str:
    """
    Generate a query to preview data from a table.
    
    Plain English: Returns a SQL query that shows the first few rows of a table.
    
    Args:
        table_name: Name of the table to preview
        limit: Maximum number of rows to return
        
    Returns:
        SQL query string
    """
    return f"""
    SELECT * 
    FROM {table_name} 
    LIMIT {limit}
    """

def table_summary_query(table_name: str) -> str:
    """
    Generate a query to get summary statistics for a table.
    
    Plain English: Returns a SQL query that counts rows and gives you a quick
    overview of what's in a table.
    
    Args:
        table_name: Name of the table to summarize
        
    Returns:
        SQL query string
    """
    return f"""
    SELECT 
        COUNT(*) as row_count,
        (SELECT COUNT(*) FROM (SELECT DISTINCT * FROM {table_name})) as distinct_rows
    FROM {table_name}
    """

def column_stats_query(table_name: str, column_name: str) -> str:
    """
    Generate a query to get statistics for a specific column.
    
    Plain English: Returns a SQL query that analyzes a specific column in your table,
    showing things like unique values, nulls, and basic statistics.
    
    Args:
        table_name: Name of the table
        column_name: Name of the column to analyze
        
    Returns:
        SQL query string
    """
    return f"""
    SELECT 
        COUNT(*) as total_count,
        COUNT(DISTINCT {column_name}) as unique_values,
        COUNT(*) - COUNT({column_name}) as null_count,
        MIN({column_name}) as min_value,
        MAX({column_name}) as max_value
    FROM {table_name}
    """

def search_table_query(table_name: str, column_name: str, search_term: str, limit: int = 100) -> str:
    """
    Generate a query to search for a term in a specific column.
    
    Plain English: Returns a SQL query that finds rows where a column contains a specific value.
    
    Args:
        table_name: Name of the table to search
        column_name: Name of the column to search in
        search_term: Term to search for (can use % wildcards)
        limit: Maximum number of rows to return
        
    Returns:
        SQL query string
    """
    return f"""
    SELECT *
    FROM {table_name}
    WHERE {column_name} LIKE '{search_term}'
    LIMIT {limit}
    """

def filter_non_empty_column_query(table_name: str, column_name: str, limit: int = 100) -> str:
    """
    Generate a query to filter rows where a specific column is not empty.
    
    Plain English: Returns a SQL query that finds rows where a column has actual data in it.
    
    Args:
        table_name: Name of the table to filter
        column_name: Name of the column to check
        limit: Maximum number of rows to return
        
    Returns:
        SQL query string
    """
    return f"""
    SELECT *
    FROM {table_name}
    WHERE {column_name} IS NOT NULL 
      AND {column_name} != ''
    LIMIT {limit}
    """

def get_table_columns_query(table_name: str) -> str:
    """
    Generate a query to get all column names for a table.
    
    Plain English: Returns a SQL query that lists all the columns in a table.
    
    Args:
        table_name: Name of the table
        
    Returns:
        SQL query string
    """
    return f"""
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_name = '{table_name}'
    ORDER BY ordinal_position
    """