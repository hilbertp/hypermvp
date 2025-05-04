"""
Command-line interface for exploring DuckDB data.

Provides tools to view, filter, and analyze data in your DuckDB database.
"""

import argparse
import sys
import logging
from pathlib import Path
from typing import List, Dict, Any, Optional
import json

from hypermvp.global_config import ENERGY_DB_PATH
from hypermvp.tools.duckdb_viewer.connection import (
    get_connection, 
    query_to_polars,
    get_table_schema,
    get_sample_data
)
from hypermvp.tools.duckdb_viewer.query_templates import (
    list_tables_query,
    table_preview_query,
    search_table_query,
    filter_non_empty_column_query,
    get_table_columns_query
)
from hypermvp.tools.duckdb_viewer.analysis import (
    get_basic_table_profile,
    profile_column,
    find_data_quality_issues,
    analyze_note_column
)

try:
    from rich.console import Console
    from rich.table import Table
    from rich import print as rich_print
    RICH_AVAILABLE = True
except ImportError:
    RICH_AVAILABLE = False

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize rich console if available
console = Console() if RICH_AVAILABLE else None

def format_output(data, format_type: str = 'table'):
    """
    Format data for display in the terminal.
    """
    if not RICH_AVAILABLE or format_type == 'json':
        # If the data is a dict or list, use json.dumps
        if isinstance(data, (dict, list)):
            return json.dumps(data, indent=2, default=str)
        # If the data is a string, return as-is (so \n creates line breaks)
        return str(data)
    
    if isinstance(data, dict):
        # Display dictionary as key-value pairs
        table = Table(show_header=True)
        table.add_column("Key", style="bold")
        table.add_column("Value")
        
        for key, value in data.items():
            if isinstance(value, (list, dict)):
                table.add_row(key, json.dumps(value, default=str))
            else:
                table.add_row(key, str(value))
        
        return table
    
    elif hasattr(data, 'to_dict'):
        # Handle Polars DataFrame
        df_dict = data.to_dict(as_series=False)
        table = Table(show_header=True)
        
        # Add columns
        for col_name in data.columns:
            table.add_column(col_name)
        
        # Add rows
        for i in range(min(len(data), 20)):  # Limit to 20 rows for display
            row = [str(df_dict[col][i]) for col in data.columns]
            table.add_row(*row)
            
        return table
    
    else:
        # Just convert to string for other types
        return str(data)

def command_list_tables(args):
    """
    List all tables in the database, showing table names and meta information (row/column counts).
    Output is formatted as a clean, aligned table for human readability.
    """
    try:
        conn = get_connection(args.db_path)
        tables_df = query_to_polars(list_tables_query(), conn=conn)
        table_names = tables_df["table_name"].to_list() if "table_name" in tables_df.columns else []
        meta_info = []

        for table in table_names:
            try:
                count_df = query_to_polars(f"SELECT COUNT(*) as row_count FROM {table}", conn=conn)
                row_count = int(count_df["row_count"][0]) if "row_count" in count_df.columns else "?"
                schema = get_table_schema(table, conn=conn)
                col_count = len(schema)
            except Exception:
                row_count = "?"
                col_count = "?"
            meta_info.append({"name": table, "rows": row_count, "columns": col_count})

        conn.close()

        if not table_names:
            return "No tables found in the database."

        # Calculate column widths for alignment
        name_len = max(len("Table Name"), *(len(str(info["name"])) for info in meta_info))
        rows_len = max(len("Rows"), *(len(str(info["rows"])) for info in meta_info))
        cols_len = max(len("Columns"), *(len(str(info["columns"])) for info in meta_info))

        # Header
        output_lines = [f"Tables in your DuckDB database ({len(table_names)} total):"]
        header = f"{'Table Name'.ljust(name_len)}   {'Rows'.rjust(rows_len)}   {'Columns'.rjust(cols_len)}"
        output_lines.append(header)
        output_lines.append(f"{'-'*name_len}   {'-'*rows_len}   {'-'*cols_len}")

        # Add a blank line for separation
        output_lines.append("")

        # Table rows
        def euro_fmt(val):
            if isinstance(val, int):
                return f"{val:,}".replace(",", ".")
            return str(val)
        for info in meta_info:
            output_lines.append(
                f"{info['name'].ljust(name_len)}   {euro_fmt(info['rows']).rjust(rows_len)}   {str(info['columns']).rjust(cols_len)}"
            )
        return "\n".join(output_lines)

    except Exception as e:
        logger.error(f"Error listing tables: {e}")
        return f"Error: {str(e)}"

def command_preview(args):
    """Preview data from a table."""
    try:
        if not args.table:
            return "Error: Table name is required."
        conn = get_connection(args.db_path)
        data = get_sample_data(args.table, limit=args.limit, conn=conn)
        conn.close()
        return data
    except Exception as e:
        logger.error(f"Error previewing table: {e}")
        return f"Error: {str(e)}"

def command_search(args):
    """Search for data in a table."""
    try:
        if not all([args.table, args.column, args.term]):
            return "Error: Table name, column name, and search term are all required."
        query = search_table_query(
            args.table, 
            args.column, 
            args.term, 
            limit=args.limit
        )
        conn = get_connection(args.db_path)
        results = query_to_polars(query, conn=conn)
        conn.close()
        if len(results) == 0:
            return f"No results found for '{args.term}' in {args.table}.{args.column}."
        return results
    except Exception as e:
        logger.error(f"Error searching: {e}")
        return f"Error: {str(e)}"

def command_profile(args):
    """Profile a table or column."""
    try:
        if not args.table:
            return "Error: Table name is required."
        conn = get_connection(args.db_path)
        if args.column:
            profile = profile_column(args.table, args.column, conn=conn)
        else:
            profile = get_basic_table_profile(args.table, conn=conn)
        conn.close()
        return profile
    except Exception as e:
        logger.error(f"Error profiling: {e}")
        return f"Error: {str(e)}"

def command_analyze_notes(args):
    """Analyze the NOTE column in a table."""
    try:
        if not args.table:
            return "Error: Table name is required."
        note_column = args.column if args.column else "NOTE"
        conn = get_connection(args.db_path)
        analysis = analyze_note_column(args.table, note_column, conn=conn)
        conn.close()
        return analysis
    except Exception as e:
        logger.error(f"Error analyzing notes: {e}")
        return f"Error: {str(e)}"

def command_quality(args):
    """Find data quality issues in a table."""
    try:
        if not args.table:
            return "Error: Table name is required."
        conn = get_connection(args.db_path)
        issues = find_data_quality_issues(args.table, conn=conn)
        conn.close()
        return issues
    except Exception as e:
        logger.error(f"Error finding data quality issues: {e}")
        return f"Error: {str(e)}"

def main():
    """
    Command-line entry point for DuckDB viewer.
    Supports global options before or after the subcommand.
    """
    import sys

    # First, parse global options and subcommand separately
    global_parser = argparse.ArgumentParser(add_help=False)
    global_parser.add_argument(
        "--db-path",
        default=ENERGY_DB_PATH,
        help=f"Path to DuckDB database (default: {ENERGY_DB_PATH})"
    )
    global_parser.add_argument(
        "--format",
        choices=["table", "json"],
        default="table",
        help="Output format (table or JSON)"
    )

    # Parse known args to separate global options from subcommand
    args, remaining_argv = global_parser.parse_known_args()

    parser = argparse.ArgumentParser(
        description="DuckDB Data Viewer - Explore and analyze your DuckDB data",
        parents=[global_parser]
    )
    subparsers = parser.add_subparsers(dest="command", help="Command to execute")

    # List tables command
    subparsers.add_parser("tables", help="List all tables in the database")

    # Preview command
    preview_parser = subparsers.add_parser("preview", help="Preview data from a table")
    preview_parser.add_argument("table", help="Table name to preview")
    preview_parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Maximum number of rows to show"
    )

    # Search command
    search_parser = subparsers.add_parser("search", help="Search for data in a table")
    search_parser.add_argument("table", help="Table name to search in")
    search_parser.add_argument("column", help="Column name to search in")
    search_parser.add_argument("term", help="Search term (can include % wildcards)")
    search_parser.add_argument(
        "--limit",
        type=int,
        default=100,
        help="Maximum number of rows to show"
    )

    # Profile command
    profile_parser = subparsers.add_parser(
        "profile",
        help="Profile a table or column"
    )
    profile_parser.add_argument("table", help="Table name to profile")
    profile_parser.add_argument(
        "--column",
        help="Column name to profile (if omitted, profiles the entire table)"
    )

    # Analyze notes command
    notes_parser = subparsers.add_parser(
        "notes",
        help="Analyze the NOTE column in a table"
    )
    notes_parser.add_argument("table", help="Table name to analyze")
    notes_parser.add_argument(
        "--column",
        help="Name of the note column (defaults to 'NOTE')"
    )

    # Data quality command
    quality_parser = subparsers.add_parser(
        "quality",
        help="Find data quality issues in a table"
    )
    quality_parser.add_argument("table", help="Table name to analyze")

    # Now parse the rest of the arguments (subcommand and its options)
    parsed_args = parser.parse_args(remaining_argv, namespace=args)

    if parsed_args.command is None:
        parser.print_help()
        return 1

    # Execute the command
    result = None
    if parsed_args.command == "tables":
        result = command_list_tables(parsed_args)
    elif parsed_args.command == "preview":
        result = command_preview(parsed_args)
    elif parsed_args.command == "search":
        result = command_search(parsed_args)
    elif parsed_args.command == "profile":
        result = command_profile(parsed_args)
    elif parsed_args.command == "notes":
        result = command_analyze_notes(parsed_args)
    elif parsed_args.command == "quality":
        result = command_quality(parsed_args)

    # Display the result
    if RICH_AVAILABLE and parsed_args.format == 'table':
        console.print(format_output(result, parsed_args.format))
    else:
        print(format_output(result, parsed_args.format))

    return 0

if __name__ == "__main__":
    import sys
    sys.exit(main())