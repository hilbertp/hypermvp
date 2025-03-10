import os
from pathlib import Path

# Define base directory (project root)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Production Data directories
DATA_DIR = os.path.join(BASE_DIR, "data")
RAW_DATA_DIR = os.path.join(DATA_DIR, "01_raw")
PROCESSED_DATA_DIR = os.path.join(DATA_DIR, "02_processed")
OUTPUT_DATA_DIR = os.path.join(DATA_DIR, "03_output")

# DuckDB paths (moved to PROCESSED_DATA_DIR with clear naming)
DUCKDB_DIR = os.path.join(PROCESSED_DATA_DIR, 'duckdb')  # Now points to 02_processed/duckdb
PROVIDER_DUCKDB_PATH = os.path.join(DUCKDB_DIR, "provider_data.duckdb")
AFRR_DUCKDB_PATH = os.path.join(DUCKDB_DIR, "afrr_data.duckdb")

# For backward compatibility - you can remove this after updating all references
DUCKDB_PATH = PROVIDER_DUCKDB_PATH

# File paths
AFRR_FILE_PATH = os.path.join(RAW_DATA_DIR, "testdata_aFRR_sept.csv")

# Automatically discover all provider files in RAW_DATA_DIR
PROVIDER_FILE_PATHS = [
    os.path.join(RAW_DATA_DIR, file)
    for file in os.listdir(RAW_DATA_DIR)
    if file.endswith(".xlsx") or file.endswith(".csv")  # Supports both formats
]

# Test Data directories (for unit testing)
TEST_DATA_DIR = os.path.join(BASE_DIR, 'tests', 'tests_data')
RAW_TEST_DIR = os.path.join(TEST_DATA_DIR, 'raw')
PROCESSED_TEST_DIR = os.path.join(TEST_DATA_DIR, 'processed')
OUTPUT_TEST_DIR = os.path.join(TEST_DATA_DIR, 'output')
TEST_DUCKDB_DIR = os.path.join(OUTPUT_TEST_DIR, 'duckdb')
TEST_PROVIDER_DUCKDB_PATH = os.path.join(TEST_DUCKDB_DIR, "provider_data.duckdb")
TEST_AFRR_DUCKDB_PATH = os.path.join(TEST_DUCKDB_DIR, "afrr_data.duckdb")

# Ensure all directories exist
os.makedirs(RAW_DATA_DIR, exist_ok=True)
os.makedirs(PROCESSED_DATA_DIR, exist_ok=True)
os.makedirs(OUTPUT_DATA_DIR, exist_ok=True)
os.makedirs(DUCKDB_DIR, exist_ok=True)
os.makedirs(RAW_TEST_DIR, exist_ok=True)
os.makedirs(PROCESSED_TEST_DIR, exist_ok=True)
os.makedirs(OUTPUT_TEST_DIR, exist_ok=True)
os.makedirs(TEST_DUCKDB_DIR, exist_ok=True)

# Optional: Comment out the debugging prints in production code
# or keep them only if needed during development
'''
print(f"BASE_DIR: {BASE_DIR}")
print(f"DATA_DIR: {DATA_DIR}")
print(f"RAW_DATA_DIR: {RAW_DATA_DIR}")
print(f"PROCESSED_DATA_DIR: {PROCESSED_DATA_DIR}")
print(f"OUTPUT_DATA_DIR: {OUTPUT_DATA_DIR}")
print(f"DUCKDB_DIR: {DUCKDB_DIR}")
print(f"PROVIDER_DUCKDB_PATH: {PROVIDER_DUCKDB_PATH}")
print(f"AFRR_DUCKDB_PATH: {AFRR_DUCKDB_PATH}")
print(f"AFRR_FILE_PATH: {AFRR_FILE_PATH}")
print(f"PROVIDER_FILE_PATHS: {PROVIDER_FILE_PATHS}")
print(f"TEST_DATA_DIR: {TEST_DATA_DIR}")
print(f"RAW_TEST_DIR: {RAW_TEST_DIR}")
print(f"PROCESSED_TEST_DIR: {PROCESSED_TEST_DIR}")
print(f"OUTPUT_TEST_DIR: {OUTPUT_TEST_DIR}")
print(f"TEST_DUCKDB_DIR: {TEST_DUCKDB_DIR}")
print(f"TEST_PROVIDER_DUCKDB_PATH: {TEST_PROVIDER_DUCKDB_PATH}")
print(f"TEST_AFRR_DUCKDB_PATH: {TEST_AFRR_DUCKDB_PATH}")
print(f"DUCKDB_PATH: {DUCKDB_PATH}")
'''
