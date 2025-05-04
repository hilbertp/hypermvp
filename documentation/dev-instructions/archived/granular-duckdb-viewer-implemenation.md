# DuckDB Data Viewer: Modular Implementation Plan

This plan provides a structured, step-by-step approach to developing a human-friendly DuckDB data viewer using VS Code and GitHub Copilot. It's designed for a solo developer who needs to quickly understand and validate data in their energy market analytics application.

## Overview

We'll build a modular system with these components:

- Core connection and query utilities
- Interactive Jupyter notebook for data exploration
- Command-line interface for quick checks
- Data analysis helpers for detecting issues

This approach balances:

- **Development speed**: Leverages existing tools and libraries
- **Maintainability**: Clear separation of concerns with modular components
- **Usability**: Both interactive (notebook) and programmatic (CLI) interfaces
- **Performance**: Efficient data processing using DuckDB and Polars

## Phase 1: Core Infrastructure

### Step 1: Setup Project Structure

```bash
mkdir -p /home/philly/hypermvp/tools/duckdb_viewer
cd /home/philly/hypermvp/tools/duckdb_viewer
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install duckdb polars[pyarrow] jupyterlab jupysql duckdb-engine rich
```

Create a README to document the tool:

```markdown
# DuckDB Viewer

Interactive tools for exploring and validating DuckDB data from the Provider ETL pipeline.

## Components
- `connection.py`: Database connection utilities
- `query_templates.py`: Reusable SQL queries 
- `notebook_viewer.ipynb`: Interactive notebook for data exploration
- `cli_viewer.py`: Command-line interface for quick data checks
- `analysis.py`: Data analysis utilities
```

### Step 2: Database Connection Module

Create a centralized connection module:

```python
# filepath: /home/philly/hypermvp/tools/duckdb_viewer/connection.py
"""
Database connection utilities for DuckDB viewer.
Centralizes connection management and database paths.
"""
import os
import duckdb
import polars as pl
from pathlib import Path
from typing import Union, Dict, Optional

# Default path to DuckDB database
DEFAULT_DB_PATH = "/home/philly/hypermvp/data/03_output/duckdb/energy_data.duckdb"

def get_connection(db_path: str = DEFAULT_DB_PATH) -> duckdb.DuckDBPyConnection:
    """
    Create and return a connection to the DuckDB database.
    
    Plain English: Opens a connection to your DuckDB file that contains all the provider data.
    
    Args:
        db_path: Path to the DuckDB database file, defaults to the standard location
        
    Returns:
        A DuckDB connection object
    """
    if not Path(db_path).exists():
        raise FileNotFoundError(f"DuckDB database not found at {db_path}. Check the path.")
    
    return duckdb.connect(db_path)

def get_table_schema(conn: duckdb.DuckDBPyConnection, table_name: str) -> Dict[str, str]:
    """
    Return the schema for a given table.
    
    Plain English: Shows you the column names and data types in your table.
    
    Args:
        conn: DuckDB connection
        table_name: Name of the table to inspect
        
    Returns:
        Dictionary mapping column names to their data types
    """
    # Get column information from the PRAGMA statement
    result = conn.execute(f"PRAGMA table_info('{table_name}')").fetchall()
    
    # Map column names to types
    return {col[1]: col[2] for col in result}

def query_to_polars(
    query: str, 
    conn: Optional[duckdb.DuckDBPyConnection] = None,
    db_path: str = DEFAULT_DB_PATH
) -> pl.DataFrame:
    """
    Execute a SQL query and return results as a Polars DataFrame.
    
    Plain English: Runs your SQL query and gives you back a super-fast Polars 
    table that you can filter and analyze.
    
    Args:
        query: SQL query to execute
        conn: Existing DuckDB connection (optional)
        db_path: Path to DuckDB database if conn is None
        
    Returns:
        Query results as a Polars DataFrame
    """
    # Create a new connection if one wasn't provided
    close_conn = False
    if conn is None:
        conn = get_connection(db_path)
        close_conn = True
    
    try:
        # Execute query and convert to Polars
        result = conn.execute(query).pl()
        return result
    finally:
        # Close connection if we created it
        if close_conn:
            conn.close()
```

### Step 3: Query Templates Module

Create reusable SQL queries:

```python
# filepath: /home/philly/hypermvp/tools/duckdb_viewer/query_templates.py
"""
Common SQL query templates for data exploration.
Provides reusable SQL for data profiling and validation.
"""
from typing import List, Optional

def list_tables_query() -> str:
    """Return SQL query to list all tables in the database."""
    return """
    SELECT table_name 
    FROM information_schema.tables 
    WHERE table_schema = 'main'
    ORDER BY table_name
    """

def table_preview_query(table_name: str, limit: int = 10) -> str:
    """
    Return SQL query to preview rows from a table.
    
    Plain English: Shows you the first few rows of any table.
    
    Args:
        table_name: Name of the table to preview
        limit: Number of rows to return
        
    Returns:
        SQL query string
    """
    return f"""
    SELECT * 
    FROM {table_name} 
    LIMIT {limit}
    """

def table_stats_query(table_name: str) -> str:
    """
    Return SQL query to get basic statistics about a table.
    
    Plain English: Counts rows, shows earliest and latest dates, 
    and gives other useful summary information.
    
    Args:
        table_name: Name of the table to analyze
        
    Returns:
        SQL query string
    """
    return f"""
    SELECT 
        COUNT(*) as row_count,
        COUNT(DISTINCT source_file) as file_count,
        MIN(DELIVERY_DATE) as earliest_date,
        MAX(DELIVERY_DATE) as latest_date
    FROM {table_name}
    """

def column_stats_query(table_name: str, column_name: str) -> str:
    """
    Return SQL query to get basic statistics about a specific column.
    
    Plain English: Shows you summary statistics for any column - how many values,
    how many unique values, null counts, etc.
    
    Args:
        table_name: Name of the table containing the column
        column_name: Name of the column to analyze
        
    Returns:
        SQL query string
    """
    return f"""
    SELECT 
        COUNT(*) as row_count,
        COUNT({column_name}) as non_null_count,
        COUNT(*) - COUNT({column_name}) as null_count,
        COUNT(DISTINCT {column_name}) as unique_values
    FROM {table_name}
    """

def search_column_query(
    table_name: str, 
    column_name: str, 
    search_pattern: str,
    limit: int = 100
) -> str:
    """
    Return SQL query to search for a pattern in a column.
    
    Plain English: Helps you find rows where a column contains certain text.
    Perfect for finding specific notes or values.
    
    Args:
        table_name: Name of the table to search
        column_name: Name of the column to search in
        search_pattern: Pattern to search for (use % for wildcards)
        limit: Maximum number of results to return
        
    Returns:
        SQL query string
    """
    return f"""
    SELECT * 
    FROM {table_name} 
    WHERE {column_name} LIKE '{search_pattern}'
    LIMIT {limit}
    """

def filter_non_empty_column_query(
    table_name: str, 
    column_name: str,
    limit: int = 100
) -> str:
    """
    Return SQL query to filter rows where a column is not empty.
    
    Plain English: Shows only rows where a column actually has content.
    Great for finding non-empty NOTE entries.
    
    Args:
        table_name: Name of the table to filter
        column_name: Name of the column to check
        limit: Maximum number of results to return
        
    Returns:
        SQL query string
    """
    return f"""
    SELECT * 
    FROM {table_name} 
    WHERE {column_name} IS NOT NULL 
      AND TRIM({column_name}) != ''
    LIMIT {limit}
    """
```

## Phase 2: Interactive Notebook

### Step 4: Create Jupyter Notebook Viewer

```python
# filepath: /home/philly/hypermvp/tools/duckdb_viewer/notebook_viewer.ipynb
{
 "cells": [
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "# DuckDB Data Viewer\n",
    "\n",
    "This notebook provides interactive exploration of DuckDB database content using SQL and Polars.\n",
    "\n",
    "## Setup\n",
    "\n",
    "First, let's import the necessary modules and establish a database connection."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "source": [
    "# Import required modules\n",
    "import sys\n",
    "import os\n",
    "from pathlib import Path\n",
    "\n",
    "# Add parent directory to path for imports\n",
    "module_path = str(Path.cwd().parent)\n",
    "if module_path not in sys.path:\n",
    "    sys.path.append(module_path)\n",
    "\n",
    "# Import local modules\n",
    "from connection import get_connection, get_table_schema, query_to_polars\n",
    "from query_templates import *\n",
    "\n",
    "# Standard data libraries\n",
    "import polars as pl\n",
    "import pandas as pd\n",
    "import duckdb\n",
    "\n",
    "# Connect to the database\n",
    "conn = get_connection()\n",
    "print(\"Connection established successfully.\")"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "## 1. Database Overview\n",
    "\n",
    "Let's first get a list of all tables in the database."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "source": [
    "tables = query_to_polars(list_tables_query(), conn)\n",
    "display(tables)"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "## 2. Provider Raw Data\n",
    "\n",
    "Now, let's examine the provider_raw table structure and preview some data."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "source": [
    "# Get schema for provider_raw table\n",
    "schema = get_table_schema(conn, \"provider_raw\")\n",
    "print(\"Table schema:\")\n",
    "for col, dtype in schema.items():\n",
    "    print(f\"  {col}: {dtype}\")\n",
    "\n",
    "# Preview data\n",
    "preview = query_to_polars(table_preview_query(\"provider_raw\"), conn)\n",
    "display(preview)"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "## 3. Column Analysis\n",
    "\n",
    "Let's analyze the `NOTE` column where we've been having issues."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "source": [
    "# Basic stats for the NOTE column\n",
    "note_stats = query_to_polars(column_stats_query(\"provider_raw\", \"NOTE\"), conn)\n",
    "display(note_stats)\n",
    "\n",
    "# Get distribution of NOTE content lengths\n",
    "df_notes = query_to_polars(\"SELECT NOTE FROM provider_raw WHERE NOTE IS NOT NULL\", conn)\n",
    "if len(df_notes) > 0:\n",
    "    # Calculate length statistics for non-null NOTE values\n",
    "    notes_length = df_notes.select([\n",
    "        pl.col(\"NOTE\").cast(pl.Utf8).str.lengths().alias(\"length\")\n",
    "    ])\n",
    "    \n",
    "    # Show length distribution\n",
    "    length_stats = notes_length.describe()\n",
    "    display(length_stats)\n",
    "    \n",
    "    # Count of empty strings (whitespace only)\n",
    "    whitespace_count = df_notes.filter(\n",
    "        pl.col(\"NOTE\").cast(pl.Utf8).str.strip() == \"\"\n",
    "    ).height\n",
    "    print(f\"Notes with only whitespace: {whitespace_count}\")\n",
    "else:\n",
    "    print(\"No non-null NOTE values found.\")"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "## 4. Finding Non-Empty Notes\n",
    "\n",
    "Let's find rows where NOTE contains actual text content (not just whitespace)."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "source": [
    "# Find rows with non-empty notes\n",
    "non_empty_query = \"\"\"\n",
    "SELECT * FROM provider_raw \n",
    "WHERE NOTE IS NOT NULL \n",
    "  AND TRIM(NOTE) != '' \n",
    "  AND LENGTH(TRIM(NOTE)) > 0\n",
    "LIMIT 20\n",
    "\"\"\"\n",
    "\n",
    "non_empty_notes = query_to_polars(non_empty_query, conn)\n",
    "if len(non_empty_notes) > 0:\n",
    "    display(non_empty_notes)\n",
    "else:\n",
    "    print(\"No rows with non-empty NOTE values found.\")"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "## 5. Custom SQL Query\n",
    "\n",
    "Enter your own SQL query below to explore the data further."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "source": [
    "# Replace with your custom query\n",
    "custom_query = \"\"\"\n",
    "SELECT \n",
    "    DELIVERY_DATE, \n",
    "    COUNT(*) as record_count,\n",
    "    MIN(ENERGY_PRICE_[EUR/MWh]) as min_price,\n",
    "    MAX(ENERGY_PRICE_[EUR/MWh]) as max_price,\n",
    "    AVG(ENERGY_PRICE_[EUR/MWh]) as avg_price\n",
    "FROM provider_raw\n",
    "GROUP BY DELIVERY_DATE\n",
    "ORDER BY DELIVERY_DATE\n",
    "LIMIT 20\n",
    "\"\"\"\n",
    "\n",
    "result = query_to_polars(custom_query, conn)\n",
    "display(result)"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "## Cleanup\n",
    "\n",
    "Close the database connection when done."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "source": [
    "conn.close()\n",
    "print(\"Connection closed.\")"
   ]
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.8.10"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 4
}
```

## Phase 3: Command-Line Interface

### Step 5: CLI Viewer Module

```python
# filepath: /home/philly/hypermvp/tools/duckdb_viewer/cli_viewer.py
"""
Command-line interface for querying and exploring DuckDB data.
Provides quick access to common data inspection tasks.
"""
import argparse
import sys
import os
from pathlib import Path
from typing import List, Optional, Dict, Any, Union
import duckdb
import polars as pl

# Add parent directory to path for imports
module_path = str(Path(__file__).parent)
if module_path not in sys.path:
    sys.path.append(module_path)

from connection import get_connection, query_to_polars, DEFAULT_DB_PATH
from query_templates import (
    list_tables_query, 
    table_preview_query,
    table_stats_query, 
    column_stats_query,
    search_column_query,
    filter_non_empty_column_query
)

try:
    from rich.console import Console
    from rich.table import Table
    RICH_AVAILABLE = True
except ImportError:
    RICH_AVAILABLE = False
    print("Install 'rich' package for better table formatting: pip install rich")

def format_table(
    data: Union[pl.DataFrame, List[Dict[str, Any]]], 
    title: str = "Results"
) -> None:
    """
    Format and print data as a table.
    
    Plain English: Shows your data in a nice table format in the terminal.
    
    Args:
        data: Data to display (Polars DataFrame or list of dictionaries)
        title: Table title
    """
    # Convert Polars DataFrame to list of dictionaries if needed
    if isinstance(data, pl.DataFrame):
        if data.height == 0:
            print(f"No data found for: {title}")
            return
        data_dicts = data.to_dicts()
        columns = data.columns
    else:
        if not data:
            print(f"No data found for: {title}")
            return
        data_dicts = data
        columns = list(data[0].keys())

    if RICH_AVAILABLE:
        # Use Rich for pretty tables
        console = Console()
        table = Table(title=title)
        
        # Add columns
        for column in columns:
            table.add_column(column, overflow="fold")
        
        # Add rows
        for row in data_dicts:
            table.add_row(*[str(row.get(col, "")) for col in columns])
        
        console.print(table)
    else:
        # Fallback to simple text table
        print(f"\n--- {title} ---")
        
        # Print header
        header = " | ".join(columns)
        print(header)
        print("-" * len(header))
        
        # Print rows
        for row in data_dicts:
            print(" | ".join(str(row.get(col, "")) for col in columns))

def list_tables(conn: duckdb.DuckDBPyConnection) -> None:
    """List all tables in the database."""
    tables = query_to_polars(list_tables_query(), conn)
    format_table(tables, title="Database Tables")

def preview_table(conn: duckdb.DuckDBPyConnection, table: str, limit: int = 10) -> None:
    """Preview rows from a specified table."""
    preview = query_to_polars(table_preview_query(table, limit), conn)
    format_table(preview, title=f"Preview of {table} (First {limit} rows)")

def table_stats(conn: duckdb.DuckDBPyConnection, table: str) -> None:
    """Show basic statistics for a table."""
    stats = query_to_polars(table_stats_query(table), conn)
    format_table(stats, title=f"Statistics for {table}")

def column_stats(conn: duckdb.DuckDBPyConnection, table: str, column: str) -> None:
    """Show statistics for a specific column."""
    stats = query_to_polars(column_stats_query(table, column), conn)
    format_table(stats, title=f"Statistics for {table}.{column}")

def search_column(
    conn: duckdb.DuckDBPyConnection, 
    table: str, 
    column: str, 
    pattern: str,
    limit: int = 10
) -> None:
    """Search for a pattern in a column."""
    results = query_to_polars(search_column_query(table, column, pattern, limit), conn)
    format_table(results, title=f"Search results for '{pattern}' in {table}.{column}")

def filter_non_empty(
    conn: duckdb.DuckDBPyConnection, 
    table: str, 
    column: str,
    limit: int = 10
) -> None:
    """Find rows where a column is not empty."""
    results = query_to_polars(filter_non_empty_column_query(table, column, limit), conn)
    format_table(results, title=f"Rows with non-empty {table}.{column}")

def execute_custom_query(conn: duckdb.DuckDBPyConnection, query: str) -> None:
    """Execute a custom SQL query."""
    results = query_to_polars(query, conn)
    format_table(results, title="Custom Query Results")

def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="DuckDB Data Viewer - Command Line Interface"
    )
    
    parser.add_argument(
        "--db-path", 
        default=DEFAULT_DB_PATH,
        help=f"Path to DuckDB database (default: {DEFAULT_DB_PATH})"
    )
    
    subparsers = parser.add_subparsers(dest="command", help="Command to execute")
    
    # List tables command
    subparsers.add_parser("tables", help="List all tables in the database")
    
    # Preview table command
    preview_parser = subparsers.add_parser("preview", help="Preview rows from a table")
    preview_parser.add_argument("table", help="Table name to preview")
    preview_parser.add_argument(
        "--limit", 
        type=int, 
        default=10, 
        help="Maximum number of rows to show"
    )
    
    # Table stats command
    stats_parser = subparsers.add_parser("stats", help="Show table statistics")
    stats_parser.add_argument("table", help="Table name to analyze")
    
    # Column stats command
    col_parser = subparsers.add_parser("column", help="Show column statistics")
    col_parser.add_argument("table", help="Table name")
    col_parser.add_argument("column", help="Column name")
    
    # Search command
    search_parser = subparsers.add_parser("search", help="Search in a column")
    search_parser.add_argument("table", help="Table name")
    search_parser.add_argument("column", help="Column to search in")
    search_parser.add_argument("pattern", help="Search pattern (use %% for wildcards)")
    search_parser.add_argument(
        "--limit", 
        type=int, 
        default=10, 
        help="Maximum number of results"
    )
    
    # Non-empty command
    nonempty_parser = subparsers.add_parser(
        "nonempty", 
        help="Find rows with non-empty values"
    )
    nonempty_parser.add_argument("table", help="Table name")
    nonempty_parser.add_argument("column", help="Column name to check")
    nonempty_parser.add_argument(
        "--limit", 
        type=int, 
        default=10, 
        help="Maximum number of results"
    )
    
    # Custom query command
    query_parser = subparsers.add_parser("query", help="Execute a custom SQL query")
    query_parser.add_argument(
        "sql", 
        help="SQL query (enclose in quotes)"
    )
    
    return parser.parse_args()

def main():
    """Main entry point for the CLI."""
    args = parse_args()
    
    if args.command is None:
        print("No command specified. Use --help for usage information.")
        return 1
    
    try:
        # Connect to the database
        conn = get_connection(args.db_path)
        
        # Execute the requested command
        if args.command == "tables":
            list_tables(conn)
        
        elif args.command == "preview":
            preview_table(conn, args.table, args.limit)
        
        elif args.command == "stats":
            table_stats(conn, args.table)
        
        elif args.command == "column":
            column_stats(conn, args.table, args.column)
        
        elif args.command == "search":
            search_column(conn, args.table, args.column, args.pattern, args.limit)
        
        elif args.command == "nonempty":
            filter_non_empty(conn, args.table, args.column, args.limit)
        
        elif args.command == "query":
            execute_custom_query(conn, args.sql)
        
        # Close the connection
        conn.close()
        return 0
    
    except FileNotFoundError as e:
        print(f"Error: {e}")
        return 1
    
    except duckdb.Error as e:
        print(f"DuckDB error: {e}")
        return 1
    
    except Exception as e:
        print(f"Error: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())
```

## Phase 4: Data Analysis Helpers

### Step 6: Analysis Utilities

```python
# filepath: /home/philly/hypermvp/tools/duckdb_viewer/analysis.py
"""
Data analysis utilities for DuckDB data exploration.
Provides common patterns for detecting and analyzing data issues.
"""
import polars as pl
from typing import Dict, List, Tuple, Optional
import duckdb

from connection import query_to_polars, get_connection

def analyze_column_distribution(
    table_name: str,
    column_name: str,
    conn: Optional[duckdb.DuckDBPyConnection] = None,
    close_conn: bool = False
) -> pl.DataFrame:
    """
    Analyze the value distribution of a specific column.
    
    Plain English: Shows you how frequently each value appears in a column.
    Great for finding the most common values or spotting outliers.
    
    Args:
        table_name: Name of the table
        column_name: Name of the column to analyze
        conn: Database connection (optional)
        close_conn: Whether to close the provided connection
        
    Returns:
        DataFrame with value counts and percentages
    """
    # Create connection if not provided
    if conn is None:
        conn = get_connection()
        close_conn = True
    
    try:
        query = f"""
        SELECT 
            {column_name} as value,
            COUNT(*) as count,
            COUNT(*) * 100.0 / (SELECT COUNT(*) FROM {table_name}) as percentage
        FROM {table_name}
        WHERE {column_name} IS NOT NULL
        GROUP BY {column_name}
        ORDER BY count DESC
        LIMIT 100
        """
        
        return query_to_polars(query, conn)
    finally:
        if close_conn:
            conn.close()

def detect_outliers(
    table_name: str,
    column_name: str,
    method: str = "iqr",
    multiplier: float = 1.5,
    conn: Optional[duckdb.DuckDBPyConnection] = None,
    close_conn: bool = False
) -> pl.DataFrame:
    """
    Detect outliers in a numeric column using IQR or standard deviation.
    
    Plain English: Finds values that are unusually high or low compared to the rest
    of the data, which could indicate data quality issues.
    
    Args:
        table_name: Name of the table
        column_name: Name of the column to analyze (must be numeric)
        method: Method to use ('iqr' or 'stddev')
        multiplier: Threshold multiplier (default: 1.5 for IQR, 3 for stddev)
        conn: Database connection (optional)
        close_conn: Whether to close the provided connection
        
    Returns:
        DataFrame with outlier values
    """
    # Create connection if not provided
    if conn is None:
        conn = get_connection()
        close_conn = True
    
    try:
        if method == "iqr":
            # Using DuckDB's built-in quantile function for IQR method
            query = f"""
            WITH stats AS (
                SELECT
                    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY {column_name}) AS q1,
                    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY {column_name}) AS q3
                FROM {table_name}
                WHERE {column_name} IS NOT NULL
            )
            SELECT *
            FROM {table_name}
            WHERE {column_name} IS NOT NULL
              AND (
                {column_name} < (SELECT q1 - {multiplier} * (q3 - q1) FROM stats)
                OR 
                {column_name} > (SELECT q3 + {multiplier} * (q3 - q1) FROM stats)
              )
            ORDER BY {column_name}
            """
        elif method == "stddev":
            # Using standard deviation method
            query = f"""
            WITH stats AS (
                SELECT
                    AVG({column_name}) AS mean,
                    STDDEV({column_name}) AS std
                FROM {table_name}
                WHERE {column_name} IS NOT NULL
            )
            SELECT *
            FROM {table_name}
            WHERE {column_name} IS NOT NULL
              AND (
                {column_name} < (SELECT mean - {multiplier} * std FROM stats)
                OR 
                {column_name} > (SELECT mean + {multiplier} * std FROM stats)
              )
            ORDER BY {column_name}
            """
        else:
            raise ValueError("Method must be 'iqr' or 'stddev'")
        
        return query_to_polars(query, conn)
    finally:
        if close_conn:
            conn.close()

def find_missing_dates(
    table_name: str,
    date_column: str,
    conn: Optional[duckdb.DuckDBPyConnection] = None,
    close_conn: bool = False
) -> pl.DataFrame:
    """
    Find missing dates in a time series.
    
    Plain English: Checks if your data has any gaps in dates,
    which could indicate missing data points.
    
    Args:
        table_name: Name of the table
        date_column: Name of the date column
        conn: Database connection (optional)
        close_conn: Whether to close the provided connection
        
    Returns:
        DataFrame with missing dates
    """
    # Create connection if not provided
    if conn is None:
        conn = get_connection()
        close_conn = True
    
    try:
        query = f"""
        WITH date_range AS (
            SELECT 
                generate_series(
                    MIN({date_column}), 
                    MAX({date_column}), 
                    interval '1 day'
                ) AS date
            FROM {table_name}
        ),
        existing_dates AS (
            SELECT DISTINCT {date_column} AS date
            FROM {table_name}
        )
        SELECT date_range.date AS missing_date
        FROM date_range
        LEFT JOIN existing_dates ON date_range.date = existing_dates.date
        WHERE existing_dates.date IS NULL
        ORDER BY missing_date
        """
        
        return query_to_polars(query, conn)
    finally:
        if close_conn:
            conn.close()

def find_duplicate_rows(
    table_name: str,
    columns: List[str],
    conn: Optional[duckdb.DuckDBPyConnection] = None,
    close_conn: bool = False
) -> pl.DataFrame:
    """
    Find duplicate rows based on specified columns.
    
    Plain English: Helps you find records that appear more than once in your data,
    which could indicate data quality issues.
    
    Args:
        table_name: Name of the table
        columns: List of columns to use for duplicate detection
        conn: Database connection (optional)
        close_conn: Whether to close the provided connection
        
    Returns:
        DataFrame with duplicate rows and count
    """
    # Create connection if not provided
    if conn is None:
        conn = get_connection()
        close_conn = True
    
    try:
        col_list = ", ".join(columns)
        query = f"""
        WITH duplicates AS (
            SELECT 
                {col_list},
                COUNT(*) as duplicate_count
            FROM {table_name}
            GROUP BY {col_list}
            HAVING COUNT(*) > 1
        )
        SELECT t.*, d.duplicate_count
        FROM {table_name} t
        JOIN duplicates d ON {" AND ".join(f"t.{col} = d.{col}" for col in columns)}
        ORDER BY {", ".join(f"t.{col}" for col in columns)}
        """
        
        return query_to_polars(query, conn)
    finally:
        if close_conn:
            conn.close()

def analyze_note_column(
    table_name: str = "provider_raw",
    conn: Optional[duckdb.DuckDBPyConnection] = None,
    close_conn: bool = False
) -> Dict[str, pl.DataFrame]:
    """
    Perform a comprehensive analysis of the NOTE column.
    
    Plain English: Gives you a complete picture of what's in your NOTE column,
    including length statistics, whitespace-only entries, and pattern detection.
    
    Args:
        table_name: Name of the table containing the NOTE column
        conn: Database connection (optional)
        close_conn: Whether to close the provided connection
        
    Returns:
        Dictionary with various analysis results
    """
    # Create connection if not provided
    if conn is None:
        conn = get_connection()
        close_conn = True
    
    try:
        results = {}
        
        # 1. Check if NOTE column exists
        schema_query = f"PRAGMA table_info('{table_name}')"
        schema = conn.execute(schema_query).fetchall()
        columns = [col[1] for col in schema]
        
        if "NOTE" not in columns:
            raise ValueError(f"NOTE column not found in {table_name}")
        
        # 2. Basic stats
        basic_stats_query = f"""
        SELECT 
            COUNT(*) as total_rows,
            COUNT(NOTE) as non_null_notes,
            COUNT(*) - COUNT(NOTE) as null_notes,
            COUNT(CASE WHEN TRIM(NOTE) != '' THEN 1 END) as non_empty_notes,
            COUNT(CASE WHEN TRIM(NOTE) = '' THEN 1 END) as empty_notes
        FROM {table_name}
        """
        results["basic_stats"] = query_to_polars(basic_stats_query, conn)
        
        # 3. Length distribution
        length_query = f"""
        SELECT 
            LENGTH(NOTE) as note_length,
            COUNT(*) as count
        FROM {table_name}
        WHERE NOTE IS NOT NULL
        GROUP BY note_length
        ORDER BY note_length
        """
        results["length_distribution"] = query_to_polars(length_query, conn)
        
        # 4. Sample of non-empty notes
        non_empty_query = f"""
        SELECT *
        FROM {table_name}
        WHERE NOTE IS NOT NULL AND TRIM(NOTE) != ''
        LIMIT 10
        """
        results["non_empty_samples"] = query_to_polars(non_empty_query, conn)
        
        # 5. Check for common patterns
        patterns_query = f"""
        SELECT
            COUNT(CASE WHEN NOTE LIKE '%error%' THEN 1 END) as error_count,
            COUNT(CASE WHEN NOTE LIKE '%warning%' THEN 1 END) as warning_count,
            COUNT(CASE WHEN NOTE LIKE '%invalid%' THEN 1 END) as invalid_count,
            COUNT(CASE WHEN NOTE LIKE '%missing%' THEN 1 END) as missing_count,
            COUNT(CASE WHEN NOTE LIKE '%correction%' THEN 1 END) as correction_count
        FROM {table_name}
        WHERE NOTE IS NOT NULL
        """
        results["pattern_counts"] = query_to_polars(patterns_query, conn)
        
        return results
    finally:
        if close_conn:
            conn.close()
```

## Installation and Setup

### Step 7: Requirements and Setup Files

Create a requirements.txt file:

```plaintext
# filepath: /home/philly/hypermvp/tools/duckdb_viewer/requirements.txt
duckdb>=0.9.0
polars>=0.19.0
pandas>=1.3.0
jupyterlab>=4.0.0
jupysql>=0.10.0
duckdb-engine>=0.9.0
rich>=13.0.0
```

Create a setup.py file for easy installation:

```python
# filepath: /home/philly/hypermvp/tools/duckdb_viewer/setup.py
"""
Setup script for DuckDB Viewer.
"""
from setuptools import setup, find_packages

setup(
    name="duckdb-viewer",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "duckdb>=0.9.0",
        "polars>=0.19.0",
        "pandas>=1.3.0",
        "jupyterlab>=4.0.0",
        "jupysql>=0.10.0",
        "duckdb-engine>=0.9.0",
        "rich>=13.0.0",
    ],
    entry_points={
        "console_scripts": [
            "duckdb-viewer=cli_viewer:main",
        ],
    },
    author="Your Name",
    author_email="your.email@example.com",
    description="Tools for exploring DuckDB data",
    keywords="duckdb, data exploration",
)
```

## Implementation Steps

Follow these steps to implement the DuckDB Viewer:

1. **Set up the environment:**

   ```bash
   mkdir -p /home/philly/hypermvp/tools/duckdb_viewer
   cd /home/philly/hypermvp/tools/duckdb_viewer
   python3 -m venv venv
   source venv/bin/activate
   pip install --upgrade pip
   pip install -r requirements.txt
   ```

2. **Create the core modules:**
   - Create `connection.py` - Database connection utilities
   - Create `query_templates.py` - Reusable SQL queries

3. **Implement the notebook viewer:**
   - Create `notebook_viewer.ipynb` using VS Code's Jupyter extension or JupyterLab
   - The notebook can be extended with your own custom queries as needed

4. **Implement the CLI viewer:**
   - Create `cli_viewer.py` - Command-line interface for quick queries
   - Test the CLI with basic commands

5. **Implement analysis utilities:**
   - Create `analysis.py` - Data analysis helpers
   - Test with your specific data needs

## Usage Examples

### Notebook Usage

1. Start Jupyter Lab:

   ```bash
   cd /home/philly/hypermvp/tools/duckdb_viewer
   source venv/bin/activate
   jupyter lab notebook_viewer.ipynb
   ```

2. Execute the cells to explore your data interactively

### CLI Usage

```bash
# List all tables in the database
python cli_viewer.py tables

# Preview the provider_raw table (10 rows)
python cli_viewer.py preview provider_raw

# Show statistics for the provider_raw table
python cli_viewer.py stats provider_raw

# Show statistics for the NOTE column
python cli_viewer.py column provider_raw NOTE

# Find rows where the NOTE column is not empty
python cli_viewer.py nonempty provider_raw NOTE

# Search for specific patterns in the NOTE column
python cli_viewer.py search provider_raw NOTE "%error%"

# Execute a custom SQL query
python cli_viewer.py query "SELECT COUNT(*) FROM provider_raw WHERE DELIVERY_DATE = '2024-09-01'"
```

## Testing and Validation

Test the modules with your actual data to ensure they work correctly:

1. **Test the connection module:**

   ```python
   from connection import get_connection, query_to_polars
   
   # Test connection
   conn = get_connection()
   print("Connection successful")
   
   # Test query
   df = query_to_polars("SELECT COUNT(*) FROM provider_raw", conn)
   print(f"Row count: {df.item()}")
   ```

2. **Test the CLI viewer:**

   ```bash
   python cli_viewer.py --help
   python cli_viewer.py tables
   ```

3. **Validate results:**
   - Compare results from the DuckDB viewer with direct queries to ensure accuracy
   - Check performance with large result sets to ensure the tools remain responsive

## Next Steps

Once the initial implementation is complete, consider:

1. **Adding data visualization capabilities:**
   - Integrate matplotlib or plotly for charts in the notebook
   - Add visualization commands to the CLI

2. **Building more advanced analysis functions:**
   - Custom reports for specific data quality issues
   - Time series analysis for your delivery date data

3. **Creating a simple web interface:**
   - Add a Streamlit UI for non-technical users
   - Include preset queries for common data exploration tasks

## Conclusion

This plan provides a modular, well-structured approach to building a DuckDB data viewer. By separating concerns into different modules (connection, queries, CLI, analysis), you'll have a maintainable and extensible tool that can grow with your needs.

The combination of a Jupyter notebook and CLI interface gives you flexibility in how you interact with your data, while the analysis utilities provide deeper insights into data quality issues.

This approach is optimized for VS Code and GitHub Copilot integration, making it easy to extend and enhance as you continue to develop your energy market analytics application.
