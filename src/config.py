import os

# Define base directory (project root)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Data directories
DATA_DIR = os.path.join(BASE_DIR, 'data')
RAW_DATA_DIR = os.path.join(DATA_DIR, '01_raw')
PROCESSED_DATA_DIR = os.path.join(DATA_DIR, '02_processed')
OUTPUT_DATA_DIR = os.path.join(DATA_DIR, '03_output')

# File paths
AFRR_FILE_PATH = os.path.join(RAW_DATA_DIR, 'testdata_aFRR_sept.csv')
PROVIDER_FILE_PATHS = [os.path.join(RAW_DATA_DIR, 'provider_list_2024_09_01.xlsx')]
