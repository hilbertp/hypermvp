import os

# Define test base directory
TEST_BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TEST_DATA_DIR = os.path.join(TEST_BASE_DIR, "tests_data")

# Test data directories
TEST_RAW_DIR = os.path.join(TEST_DATA_DIR, "raw")
TEST_PROCESSED_DIR = os.path.join(TEST_DATA_DIR, "processed")
TEST_OUTPUT_DIR = os.path.join(TEST_DATA_DIR, "output")

# Test file paths
TEST_AFRR_FILE = os.path.join(TEST_RAW_DIR, "test_aFRR_sept.csv")
TEST_PROVIDER_FILE = os.path.join(TEST_RAW_DIR, "test_provider_list.xlsx")

# Ensure test directories exist
os.makedirs(TEST_RAW_DIR, exist_ok=True)
os.makedirs(TEST_PROCESSED_DIR, exist_ok=True)
os.makedirs(TEST_OUTPUT_DIR, exist_ok=True)