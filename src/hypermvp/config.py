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

# Provider-specific directories
PROVIDER_RAW_DIR = os.path.join(RAW_DATA_DIR, "provider") 
AFRR_RAW_DIR = os.path.join(RAW_DATA_DIR, "afrr")

# Processed data directories
PROCESSED_PROVIDER_DIR = os.path.join(PROCESSED_DATA_DIR, "provider")
PROCESSED_AFRR_DIR = os.path.join(PROCESSED_DATA_DIR, "afrr")

# DuckDB paths
DUCKDB_DIR = os.path.join(OUTPUT_DATA_DIR, "duckdb")
ENERGY_DB_PATH = os.path.join(DUCKDB_DIR, "energy_data.duckdb")

# For backward compatibility 
DUCKDB_PATH = ENERGY_DB_PATH
PROVIDER_DUCKDB_PATH = ENERGY_DB_PATH
AFRR_DUCKDB_PATH = ENERGY_DB_PATH

# Discover provider files from the provider directory
os.makedirs(PROVIDER_RAW_DIR, exist_ok=True)  
PROVIDER_FILE_PATHS = [
    os.path.join(PROVIDER_RAW_DIR, file)
    for file in os.listdir(PROVIDER_RAW_DIR) 
    if os.path.isfile(os.path.join(PROVIDER_RAW_DIR, file))
    and (file.endswith(".xlsx") or file.endswith(".csv"))
]

# AFRR file paths
os.makedirs(AFRR_RAW_DIR, exist_ok=True)
AFRR_FILE_PATH = os.path.join(AFRR_RAW_DIR, "afrr_data.csv")

# Test Data directories (for unit testing)
TEST_DATA_DIR = os.path.join(BASE_DIR, 'tests', 'tests_data')
RAW_TEST_DIR = os.path.join(TEST_DATA_DIR, 'raw')
PROCESSED_TEST_DIR = os.path.join(TEST_DATA_DIR, 'processed')
OUTPUT_TEST_DIR = os.path.join(TEST_DATA_DIR, 'output')
TEST_DUCKDB_DIR = os.path.join(OUTPUT_TEST_DIR, "duckdb")  # Consistent subfolder here too
TEST_ENERGY_DB_PATH = os.path.join(TEST_DUCKDB_DIR, "energy_data.duckdb")
TEST_PROVIDER_DUCKDB_PATH = TEST_ENERGY_DB_PATH
TEST_AFRR_DUCKDB_PATH = TEST_ENERGY_DB_PATH

# Ensure all directories exist
os.makedirs(RAW_DATA_DIR, exist_ok=True)
os.makedirs(PROCESSED_DATA_DIR, exist_ok=True)
os.makedirs(OUTPUT_DATA_DIR, exist_ok=True)
os.makedirs(DUCKDB_DIR, exist_ok=True)  # Create the duckdb subfolder
os.makedirs(RAW_TEST_DIR, exist_ok=True)
os.makedirs(PROCESSED_TEST_DIR, exist_ok=True)
os.makedirs(OUTPUT_TEST_DIR, exist_ok=True)
os.makedirs(TEST_DUCKDB_DIR, exist_ok=True)  # Create the test duckdb subfolder

# Ensure these directories exist
os.makedirs(PROCESSED_PROVIDER_DIR, exist_ok=True)
os.makedirs(PROCESSED_AFRR_DIR, exist_ok=True)