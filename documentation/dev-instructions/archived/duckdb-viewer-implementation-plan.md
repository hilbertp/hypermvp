# DuckDB Data Viewer: Implementation Plan

## Overview

This implementation plan outlines a modular DuckDB data viewer that integrates with your existing project architecture. It will provide a human-friendly interface for exploring and validating data in your DuckDB database, with a particular focus on examining the contents of the `provider_raw` table.

## Project Integration

The DuckDB viewer will be implemented as a proper module within your existing project structure:

```bash
/home/philly/hypermvp/src/hypermvp/tools/
├── __init__.py
├── duckdb_viewer/
│   ├── __init__.py         # Package exports 
│   ├── connection.py       # Database connection utilities
│   ├── query_templates.py  # Reusable SQL queries
│   ├── analysis.py         # Data analysis utilities
│   └── cli.py              # Command-line interface
└── notebooks/
    └── duckdb_explorer.ipynb  # Interactive notebook
```

## Development Phases

### Phase 1: Core Infrastructure (1-2 days)

1. **Set up module structure**
   - Create directories within your existing project
   - Update docstrings to maintain project standards
   - Leverage existing `global_config.py` for database paths

2. **Create connection module**
   - Build a central connection utility for DuckDB
   - Use configuration values from `global_config.py`
   - Add helper functions for query execution

3. **Develop query templates**
   - Implement standardized SQL queries for common operations
   - Leverage schema information from `provider_etl_config.py`
   - Support both provider and AFRR data tables

### Phase 2: Analysis Components (1-2 days)

1. **Build data analysis utilities**
   - Create functions for data profiling and validation
   - Implement specialized NOTE column analysis
   - Add helpers for detecting anomalies and patterns

2. **Create CLI interface**
   - Develop a command-line tool consistent with your other CLIs
   - Support common tasks (preview, search, validate)
   - Follow project's argument parsing patterns

### Phase 3: Jupyter Integration (1 day)

1. **Create notebook explorer**
   - Build interactive notebook for ad-hoc analysis
   - Add example queries and visualizations
   - Document usage patterns

## Technical Implementation

### Dependency Management

All dependencies will be managed through your existing `pyproject.toml` file. We'll use the dependencies you already have:

- `duckdb`: Database engine
- `polars`: High-performance DataFrame library
- `jupyterlab`: Interactive notebook environment
- `rich`: Terminal formatting

### Configuration Integration

The viewer will use your existing configuration infrastructure:

- Database paths from `global_config.py`
- Table schemas from `provider_etl_config.py`
- Project-wide settings like date formats

### Implementation Details

#### Connection Module

```python
# filepath: /home/philly/hypermvp/src/hypermvp/tools/duckdb_viewer/connection.py

"""
Database connection utilities for DuckDB viewer.

Provides standardized connection handling and query execution,
leveraging existing configuration from global_config.py.
"""

import duckdb
import polars as pl
from typing import Union, Dict, Optional
from pathlib import Path

from hypermvp.global_config import ENERGY_DB_PATH

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

def query_to_polars(query: str, conn = None) -> pl.DataFrame:
    """
    Execute a SQL query and return results as a Polars DataFrame.
    
    Args:
        query: SQL query to execute
        conn: Optional existing connection (creates one if not provided)
        
    Returns:
        Query results as a Polars DataFrame
    """
    # Create connection if needed
    close_conn = False
    if conn is None:
        conn = get_connection()
        close_conn = True
    
    try:
        # Execute query and convert to Polars
        result = conn.execute(query).pl()
        return result
    finally:
        if close_conn:
            conn.close()
```

#### CLI Module

```python
# filepath: /home/philly/hypermvp/src/hypermvp/tools/duckdb_viewer/cli.py

"""
Command-line interface for exploring DuckDB data.

Provides tools to view, filter, and analyze data in your DuckDB database.
"""

import argparse
import sys
import logging
from pathlib import Path
from typing import List, Dict, Any

from hypermvp.global_config import ENERGY_DB_PATH
from hypermvp.tools.duckdb_viewer.connection import get_connection, query_to_polars
from hypermvp.tools.duckdb_viewer.query_templates import (
    list_tables_query,
    table_preview_query,
    filter_non_empty_column_query
)

try:
    from rich.console import Console
    from rich.table import Table
    RICH_AVAILABLE = True
except ImportError:
    RICH_AVAILABLE = False

def main():
    """Command-line entry point for DuckDB viewer."""
    parser = argparse.ArgumentParser(
        description="DuckDB Data Viewer - Explore and analyze your DuckDB data"
    )
    
    # Core arguments
    parser.add_argument(
        "--db-path", 
        default=ENERGY_DB_PATH,
        help=f"Path to DuckDB database (default: {ENERGY_DB_PATH})"
    )
    
    subparsers = parser.add_subparsers(dest="command", help="Command to execute")
    
    # List tables command
    subparsers.add_parser("tables", help="List all tables in the database")
    
    # ... additional subcommands defined here ...
    
    args = parser.parse_args()
    
    if args.command is None:
        parser.print_help()
        return 1
        
    # Command execution logic goes here
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
```

## Testing Strategy

Tests will be created following your project's pytest structure:

```bash
/home/philly/hypermvp/tests/tools/
└── duckdb_viewer/
    ├── test_connection.py
    ├── test_analysis.py
    └── test_cli.py
```

## Usage Examples

### CLI

```bash
# From project root, with Poetry env active
python -m hypermvp.tools.duckdb_viewer.cli tables
python -m hypermvp.tools.duckdb_viewer.cli preview provider_raw
python -m hypermvp.tools.duckdb_viewer.cli search provider_raw NOTE "%%error%%"
```

### Notebook

```python
from hypermvp.tools.duckdb_viewer.connection import query_to_polars
from hypermvp.global_config import ENERGY_DB_PATH

# Execute a query directly
df = query_to_polars("SELECT COUNT(*) FROM provider_raw")
display(df)
```

## Implementation Steps

1. Create directory structure
2. Implement core modules (connection, query_templates)
3. Build analysis utilities
4. Create CLI interface
5. Develop Jupyter notebook
6. Write tests
7. Document usage

## Next Steps After Implementation

1. Extend with additional analysis features based on user feedback
2. Add visualization capabilities for common data patterns
3. Integrate with dashboard components
