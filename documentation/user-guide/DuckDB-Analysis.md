# DuckDB Data Analysis Toolset – User Guide

This guide explains how to use the DuckDB Data Analysis Toolset to explore, validate, and analyze your aFRR and provider data. The toolset is designed for users with deep energy market knowledge but limited Python experience.

---

## 1. Overview

The toolset helps you:

- Inspect and validate large energy market datasets stored in DuckDB databases.
- Profile tables and columns for data quality and completeness.
- Search, preview, and summarize data from the command line or in a Jupyter notebook.
- Prepare your data for profitability analysis and reporting.

---

## 2. Components

### a. Command-Line Interface (CLI)

**Purpose:**  
Quickly explore and validate your DuckDB data from the terminal.

**Key Features:**

- List all tables in your database.
- Preview sample rows from any table.
- Search for specific values in any column.
- Profile tables and columns for row counts, uniqueness, and missing data.
- Analyze error/warning messages in NOTE columns.
- Identify data quality issues (missing values, low-cardinality columns, potential ID columns).

**How to Use:**

Open a terminal in your project directory and run commands like:

```bash
# List all tables in your DuckDB database. This shows you what data is available for analysis.
python -m src.hypermvp.tools.duckdb_viewer.cli --db-path data/03_output/duckdb/energy_data.duckdb tables

# Preview the first 10 rows from the 'provider_raw' table. Use this to quickly check the structure and sample data in a table.
python -m src.hypermvp.tools.duckdb_viewer.cli preview provider_raw --db-path data/03_output/duckdb/energy_data.duckdb --limit 10

# Search for rows in the 'provider_raw' table where the NOTE column contains the word 'error'. This helps you find problematic or flagged records.
python -m src.hypermvp.tools.duckdb_viewer.cli search provider_raw NOTE "%error%" --db-path data/03_output/duckdb/energy_data.duckdb

# Profile the 'provider_raw' table. This gives you a summary of row counts, duplicate rows, and column names—helpful for understanding data quality and completeness.
python -m src.hypermvp.tools.duckdb_viewer.cli profile provider_raw --db-path data/03_output/duckdb/energy_data.duckdb

# Analyze the NOTE column in 'provider_raw'. This summarizes error and warning messages, so you can spot common issues or patterns in your data.
python -m src.hypermvp.tools.duckdb_viewer.cli notes provider_raw --db-path data/03_output/duckdb/energy_data.duckdb

# Check for data quality issues in 'provider_raw'. This command flags columns with missing data, low-cardinality columns, and potential ID columns, helping you identify areas that may need cleaning or review.
python -m src.hypermvp.tools.duckdb_viewer.cli quality provider_raw --db-path data/03_output/duckdb/energy_data.duckdb
```

**Output:**  

- If the `rich` library is installed, you’ll see a nicely formatted table.
- Otherwise, you’ll see a clean, readable list or summary.

---

### b. Jupyter Notebook

**Purpose:**  
Interactively explore and analyze your data in a browser-based environment.

**How to Use:**

1. Open the notebook:  
   `/home/philly/hypermvp/src/hypermvp/tools/notebooks/duckdb_explorer.ipynb`
2. Run the cells to:
   - Connect to your DuckDB database.
   - List available tables.
   - Preview and filter data.
   - Run profiling and data quality checks.
   - Visualize distributions and missing data.

**Benefits:**  

- See your data in tables and charts.
- Run analysis with one click.
- Export results for reporting.

---

### c. Analysis Module (Python Functions)

**Purpose:**  
Reusable functions for data profiling and quality checks, usable in scripts or notebooks.

**Key Functions:**

- `get_basic_table_profile(table_name, conn=None)`:  
  Returns row count, duplicate count, and column names.
- `profile_column(table_name, column_name, conn=None)`:  
  Returns unique values, nulls, min/max, and completeness for a column.
- `find_data_quality_issues(table_name, conn=None)`:  
  Flags columns with missing data, low cardinality, or likely IDs.
- `analyze_note_column(table_name, note_column='NOTE', conn=None)`:  
  Summarizes error/warning patterns in a NOTE column.

**How to Use:**
Import and call these functions in your own scripts or notebooks for custom analysis.

---

### d. Query Templates

**Purpose:**  
Standard SQL templates for common operations (listing tables, previewing data, column stats, etc.).

**How to Use:**  
Used internally by the CLI and analysis module. Advanced users can extend these for custom queries.

---

## 3. Best Practices

- **Always validate your data** before running profitability calculations.
- **Use the CLI** for quick checks and automation.
- **Use the Jupyter notebook** for deeper exploration and visual analysis.
- **Check for missing or duplicate data** using the profiling and quality check features.
- **Document any issues** you find for future data cleaning or process improvement.

---

## 4. Troubleshooting

- If you see strange characters or unreadable output, install the `rich` library for better terminal formatting:

  ```bash
  pip install rich
  ```

- If you get a `FileNotFoundError`, check that your DuckDB database path is correct.
- For large files, be patient—processing millions of rows may take a few seconds.

---

## 5. Extending the Toolset

- **Add new analysis routines** by creating new functions in the analysis module.
- **Integrate with dashboards** (e.g., Streamlit) for even more visual exploration.
- **Automate regular checks** by scripting CLI commands in a shell script or Makefile.

---

## 6. Support

If you have questions or need help extending the toolset, contact your development support or refer to the technical documentation in `/home/philly/hypermvp/documentation/`.

---

**Plain English:**  
This toolset helps you check, understand, and trust your energy market data—so you can focus on profitability analysis, not data wrangling.

---
