import duckdb

# Connect to the DuckDB database at the specified path
db_path = "/home/philly/hypermvp/data/03_output/duckdb/energy_data.duckdb"
con = duckdb.connect(db_path)

# Select first 10 rows from the raw_provider_data table
result = con.execute("SELECT * FROM raw_provider_data LIMIT 10").fetchdf()
print("First 10 rows:")
print(result)

# Get row counts grouped by source_file
result = con.execute("""
    SELECT source_file, COUNT(*) AS cnt
    FROM raw_provider_data
    GROUP BY source_file
""").fetchdf()
print("\nRow counts grouped by source_file:")
print(result)

con.close()