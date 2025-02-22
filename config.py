import os

# Define base directory (project root) securely as /home/philly/hypermvp
BASE_DIR = os.path.join(os.path.expanduser("~"), "hypermvp")

# Production Data directories
DATA_DIR = os.path.join(BASE_DIR, 'data')
RAW_DATA_DIR = os.path.join(DATA_DIR, '01_raw')
PROCESSED_DATA_DIR = os.path.join(DATA_DIR, '02_processed')
OUTPUT_DATA_DIR = os.path.join(DATA_DIR, '03_output')

# Ensure production directories exist
os.makedirs(RAW_DATA_DIR, exist_ok=True)
os.makedirs(PROCESSED_DATA_DIR, exist_ok=True)
os.makedirs(OUTPUT_DATA_DIR, exist_ok=True)

# File paths
AFRR_FILE_PATH = os.path.join(RAW_DATA_DIR, 'testdata_aFRR_sept.csv')

# Automatically discover all provider files in RAW_DATA_DIR
PROVIDER_FILE_PATHS = [
    os.path.join(RAW_DATA_DIR, file)
    for file in os.listdir(RAW_DATA_DIR)
    if file.endswith('.xlsx') or file.endswith('.csv')  # Supports both formats
]

# Test Data directories (for unit testing)
TEST_DATA_DIR = os.path.join(BASE_DIR, 'tests', 'tests_data')
RAW_TEST_DIR = os.path.join(TEST_DATA_DIR, 'raw')
PROCESSED_TEST_DIR = os.path.join(TEST_DATA_DIR, 'processed')
OUTPUT_TEST_DIR = os.path.join(TEST_DATA_DIR, 'output')

# Ensure test directories exist
os.makedirs(RAW_TEST_DIR, exist_ok=True)
os.makedirs(PROCESSED_TEST_DIR, exist_ok=True)
os.makedirs(OUTPUT_TEST_DIR, exist_ok=True)

# Log the paths for debugging
print(f"BASE_DIR: {BASE_DIR}")
print(f"DATA_DIR: {DATA_DIR}")
print(f"RAW_DATA_DIR: {RAW_DATA_DIR}")
print(f"PROCESSED_DATA_DIR: {PROCESSED_DATA_DIR}")
print(f"OUTPUT_DATA_DIR: {OUTPUT_DATA_DIR}")
print(f"AFRR_FILE_PATH: {AFRR_FILE_PATH}")
print(f"PROVIDER_FILE_PATHS: {PROVIDER_FILE_PATHS}")
print(f"TEST_DATA_DIR: {TEST_DATA_DIR}")
print(f"RAW_TEST_DIR: {RAW_TEST_DIR}")
print(f"PROCESSED_TEST_DIR: {PROCESSED_TEST_DIR}")
print(f"OUTPUT_TEST_DIR: {OUTPUT_TEST_DIR}")