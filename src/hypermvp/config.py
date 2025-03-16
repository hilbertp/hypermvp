import os
from pathlib import Path
from datetime import datetime
import logging

# Define base directory (project root)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Production Data directories
DATA_DIR = os.path.join(BASE_DIR, "data")
RAW_DATA_DIR = os.path.join(DATA_DIR, "01_raw")
PROCESSED_DATA_DIR = os.path.join(DATA_DIR, "02_processed")
OUTPUT_DATA_DIR = os.path.join(DATA_DIR, "03_output")

# DuckDB paths (moved to OUTPUT_DATA_DIR)
DUCKDB_DIR = OUTPUT_DATA_DIR  # DuckDB files go directly into 03_output
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
TEST_DUCKDB_DIR = OUTPUT_TEST_DIR # Test DuckDB files go directly into tests/tests_data/output
TEST_PROVIDER_DUCKDB_PATH = os.path.join(TEST_DUCKDB_DIR, "provider_data.duckdb")
TEST_AFRR_DUCKDB_PATH = os.path.join(TEST_DUCKDB_DIR, "afrr_data.duckdb")

# Ensure all directories exist
os.makedirs(RAW_DATA_DIR, exist_ok=True)
os.makedirs(PROCESSED_DATA_DIR, exist_ok=True)
os.makedirs(OUTPUT_DATA_DIR, exist_ok=True)
os.makedirs(RAW_TEST_DIR, exist_ok=True)
os.makedirs(PROCESSED_TEST_DIR, exist_ok=True)
os.makedirs(OUTPUT_TEST_DIR, exist_ok=True)