# Next Steps: Human-Friendly DuckDB Data Viewer

## Objective

Build or extend a tool to visually inspect the contents of the DuckDB database (`provider_raw` table and others) so that a human can easily understand and verify the imported data, including the `NOTE` column.

## Context

- We already have a basic script for previewing Excel/CSV files.
- The next step is to create or enhance a tool that:
  - Connects to DuckDB.
  - Allows you to view, filter, and search data in a human-friendly way (e.g., with pandas, polars, or a simple web/table UI).
  - Makes it easy to spot issues, outliers, or unexpected values in any column (especially `NOTE`).

## Suggested Steps

1. **Review existing preview/inspection scripts.**
2. **Decide on the interface:**
    - CLI-based (e.g., print selected rows/columns with formatting)
    - Jupyter Notebook (recommended for interactive exploration)
    - Simple web UI (optional, if needed)
3. **Implement a script/notebook that:**
    - Connects to DuckDB.
    - Lets you select a table and preview N rows (with column names).
    - Supports filtering (e.g., show only rows where `NOTE` is not empty).
    - Optionally, export filtered data to CSV/Excel for further review.
4. **Document usage in the `/documentation/dev-instructions/` folder.**

## Example: Minimal CLI Preview

```python
import duckdb
import pandas as pd

con = duckdb.connect("/home/philly/hypermvp/data/03_output/duckdb/energy_data.duckdb")
df = con.execute("SELECT * FROM provider_raw LIMIT 20").df()
print(df)
```

## Example: Filter for Non-Empty NOTE

```python
df = con.execute("SELECT * FROM provider_raw WHERE NOTE IS NOT NULL AND TRIM(NOTE) != '' LIMIT 20").df()
print(df)
```

---

**Next time:**  
- Start by reviewing and running the above examples.
- Decide if you want a CLI, notebook, or web-based viewer.
- Extend the tool to support more advanced filtering and export as needed.

---
```// filepath: /home/philly/hypermvp/documentation/dev-instructions/next_steps_viewer.md

# Next Steps: Human-Friendly DuckDB Data Viewer

## Objective

Build or extend a tool to visually inspect the contents of the DuckDB database (`provider_raw` table and others) so that a human can easily understand and verify the imported data, including the `NOTE` column.

## Context

- We already have a basic script for previewing Excel/CSV files.
- The next step is to create or enhance a tool that:
  - Connects to DuckDB.
  - Allows you to view, filter, and search data in a human-friendly way (e.g., with pandas, polars, or a simple web/table UI).
  - Makes it easy to spot issues, outliers, or unexpected values in any column (especially `NOTE`).

## Suggested Steps

1. **Review existing preview/inspection scripts.**
2. **Decide on the interface:**
    - CLI-based (e.g., print selected rows/columns with formatting)
    - Jupyter Notebook (recommended for interactive exploration)
    - Simple web UI (optional, if needed)
3. **Implement a script/notebook that:**
    - Connects to DuckDB.
    - Lets you select a table and preview N rows (with column names).
    - Supports filtering (e.g., show only rows where `NOTE` is not empty).
    - Optionally, export filtered data to CSV/Excel for further review.
4. **Document usage in the `/documentation/dev-instructions/` folder.**

## Example: Minimal CLI Preview

```python
import duckdb
import pandas as pd

con = duckdb.connect("/home/philly/hypermvp/data/03_output/duckdb/energy_data.duckdb")
df = con.execute("SELECT * FROM provider_raw LIMIT 20").df()
print(df)
```

## Example: Filter for Non-Empty NOTE

```python
df = con.execute("SELECT * FROM provider_raw WHERE NOTE IS NOT NULL AND TRIM(NOTE) != '' LIMIT 20").df()
print(df)
```

---

**Next time:**  
- Start by reviewing and running the above examples.
- Decide if you want a CLI, notebook, or web-based viewer.
- Extend the tool to support more advanced filtering and export as needed.

---