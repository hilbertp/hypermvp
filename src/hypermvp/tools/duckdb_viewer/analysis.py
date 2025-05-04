"""
Data analysis utilities for the DuckDB viewer.

This module provides tools to analyze and profile data stored in DuckDB tables.
"""

import polars as pl
from typing import Dict, List, Tuple, Optional, Union
import logging

from hypermvp.tools.duckdb_viewer.connection import (
    query_to_polars,
    get_connection,
    get_table_schema
)
from hypermvp.tools.duckdb_viewer.query_templates import (
    column_stats_query,
    table_summary_query
)

def get_basic_table_profile(table_name: str, conn=None) -> Dict:
    """
    Get a basic profile of a table including row count and other high-level statistics.
    
    Plain English: Provides a quick overview of what's in your table, including how many
    rows it has and if there are any duplicates.
    
    Args:
        table_name: Name of the table to profile
        
    Returns:
        Dictionary with table statistics
    """
    # Get basic table statistics
    summary_df = query_to_polars(table_summary_query(table_name), conn=conn)
    
    if len(summary_df) == 0:
        raise ValueError(f"No data found for table '{table_name}'")
    
    # Extract values
    row = summary_df.row(0)
    row_count = row[0]
    distinct_rows = row[1]
    
    # Get schema information
    columns = get_table_schema(table_name, conn=conn)
    
    return {
        "table_name": table_name,
        "row_count": row_count,
        "distinct_rows": distinct_rows,
        "duplicate_rows": row_count - distinct_rows,
        "column_count": len(columns),
        "columns": [col["name"] for col in columns]
    }

def profile_column(table_name: str, column_name: str, conn=None) -> Dict:
    """
    Get detailed profile of a specific column in a table.
    
    Plain English: Analyzes a specific column in your data, showing statistics
    like how many unique values it has and how many empty values there are.
    
    Args:
        table_name: Name of the table
        column_name: Name of the column to profile
        
    Returns:
        Dictionary with column statistics
    """
    # Get column statistics
    stats_df = query_to_polars(column_stats_query(table_name, column_name), conn=conn)
    
    if len(stats_df) == 0:
        raise ValueError(f"No statistics found for column '{column_name}'")
    
    # Extract values
    row = stats_df.row(0)
    total_count = row[0]
    unique_count = row[1]
    null_count = row[2]
    min_value = row[3]
    max_value = row[4]
    
    # Calculate additional statistics
    filled_count = total_count - null_count
    uniqueness_ratio = unique_count / filled_count if filled_count > 0 else 0
    completeness_ratio = filled_count / total_count if total_count > 0 else 0
    
    return {
        "column_name": column_name,
        "total_count": total_count,
        "unique_values": unique_count,
        "null_count": null_count,
        "filled_count": filled_count,
        "min_value": min_value,
        "max_value": max_value,
        "uniqueness_ratio": uniqueness_ratio,
        "completeness_ratio": completeness_ratio
    }

def find_data_quality_issues(table_name: str, conn=None) -> Dict[str, List[Dict]]:
    """
    Find potential data quality issues in a table.
    
    Plain English: Scans your table for common data problems like missing values
    or columns with only one unique value (which might indicate a data issue).
    
    Args:
        table_name: Name of the table to analyze
        
    Returns:
        Dictionary mapping issue types to lists of affected columns
    """
    # Get schema information
    columns = get_table_schema(table_name, conn=conn)
    
    issues = {
        "missing_values": [],
        "low_cardinality": [],
        "high_cardinality": [],
        "potential_id_columns": []
    }
    
    # Analyze each column
    for col_info in columns:
        col_name = col_info["name"]
        profile = profile_column(table_name, col_name, conn=conn)
        
        # Check for columns with many NULL values
        if profile["null_count"] > 0:
            null_percentage = (profile["null_count"] / profile["total_count"]) * 100
            if null_percentage > 10:  # If more than 10% are null
                issues["missing_values"].append({
                    "column": col_name,
                    "null_percentage": null_percentage
                })
        
        # Check for low cardinality (few unique values)
        if 1 < profile["unique_values"] < 5 and profile["total_count"] > 100:
            issues["low_cardinality"].append({
                "column": col_name,
                "unique_values": profile["unique_values"]
            })
            
        # Check for potential ID columns (high cardinality)
        if profile["uniqueness_ratio"] > 0.9:
            issues["potential_id_columns"].append({
                "column": col_name,
                "uniqueness_ratio": profile["uniqueness_ratio"]
            })
            
    return issues

def analyze_note_column(table_name: str, note_column: str = "NOTE", conn=None) -> Dict:
    """
    Analyze the NOTE column in a table, which often contains important error messages
    or processing information.
    
    Plain English: Looks at the NOTE column in your data to find common patterns,
    errors, or other important information.
    
    Args:
        table_name: Name of the table to analyze
        note_column: Name of the note column (defaults to "NOTE")
        
    Returns:
        Dictionary with analysis results
    """
    # Check if column exists
    try:
        schema = get_table_schema(table_name, conn=conn)
        column_names = [col["name"] for col in schema]
        
        if note_column not in column_names:
            return {"exists": False, "message": f"No '{note_column}' column found in table '{table_name}'"}
        
        # Query for non-empty notes
        query = f"""
        SELECT 
            {note_column},
            COUNT(*) as frequency
        FROM {table_name}
        WHERE {note_column} IS NOT NULL AND {note_column} != ''
        GROUP BY {note_column}
        ORDER BY frequency DESC
        """
        
        notes_df = query_to_polars(query, conn=conn)
        
        # Extract error patterns
        error_patterns = []
        for note, frequency in zip(notes_df[note_column], notes_df["frequency"]):
            note_str = str(note).lower()
            if any(term in note_str for term in ["error", "fail", "invalid", "missing", "cannot", "wrong"]):
                error_patterns.append({
                    "note": note,
                    "frequency": frequency,
                    "is_error": True
                })
            else:
                error_patterns.append({
                    "note": note,
                    "frequency": frequency,
                    "is_error": False
                })
        
        # Count error vs. non-error notes
        error_count = sum(1 for p in error_patterns if p["is_error"])
        
        return {
            "exists": True,
            "total_notes": len(notes_df),
            "error_count": error_count,
            "non_error_count": len(notes_df) - error_count,
            "top_patterns": error_patterns[:10]  # Top 10 patterns
        }
    except Exception as e:
        return {
            "exists": False,
            "message": f"Error analyzing NOTE column: {str(e)}"
        }