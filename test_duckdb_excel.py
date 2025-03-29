# filepath: /home/philly/hypermvp/test_duckdb_excel.py
import duckdb

# Install and load the excel extension
duckdb.sql("INSTALL excel;")
duckdb.sql("LOAD excel;")

# Try running a query using read_xlsx.
try:
    # Use the sample Excel file we created in tests/provider/test_data.xlsx
    res = duckdb.query("SELECT * FROM read_xlsx('/home/philly/hypermvp/tests/provider/test_data.xlsx') LIMIT 1").fetchall()
    print("Excel extension supported. Got:", res)
except Exception as e:
    print("Excel extension or read_xlsx is not supported:", e)