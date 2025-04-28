#!/usr/bin/env python3
import pandas as pd
import logging
import os

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

def create_single_sheet_excel(original_file, output_file, sheet_index=0):
    """
    Create a new Excel file containing only a single sheet from the original file.
    
    Args:
        original_file (str): Path to the original Excel file
        output_file (str): Path where to save the new Excel file
        sheet_index (int): Index of the sheet to keep (0-based)
    """
    logging.info(f"Reading Excel file: {os.path.basename(original_file)}")
    
    # First get a list of sheet names
    xls = pd.ExcelFile(original_file)
    sheet_names = xls.sheet_names
    
    if sheet_index >= len(sheet_names):
        logging.error(f"Sheet index {sheet_index} out of range. File has {len(sheet_names)} sheets.")
        return False
    
    sheet_name = sheet_names[sheet_index]
    logging.info(f"Extracting sheet: {sheet_name} (index {sheet_index})")
    
    # Read the selected sheet
    logging.info(f"Reading data from sheet...")
    df = pd.read_excel(original_file, sheet_name=sheet_name)
    logging.info(f"Sheet contains {len(df):,} rows and {len(df.columns)} columns")
    
    # Create output directory if needed
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    # Write to new Excel file
    logging.info(f"Writing data to new Excel file: {os.path.basename(output_file)}")
    df.to_excel(output_file, sheet_name=sheet_name, index=False)
    
    # Get file sizes for comparison
    original_size = os.path.getsize(original_file) / (1024 * 1024)  # MB
    new_size = os.path.getsize(output_file) / (1024 * 1024)  # MB
    
    logging.info(f"Success! Original file: {original_size:.2f} MB, New file: {new_size:.2f} MB")
    logging.info(f"New file saved to: {output_file}")
    
    return True

if __name__ == "__main__":
    # File paths
    ORIGINAL_FILE = "/home/philly/hypermvp/data/01_raw/provider/RESULT_LIST_ANONYM_ENERGY_MARKET_aFRR_DE_2024-09-01_2024-09-30.xlsx"
    OUTPUT_FILE = "/home/philly/hypermvp/data/01_raw/provider/TEST_SINGLE_SHEET.xlsx"
    
    # Sheet index (0 for first sheet)
    SHEET_INDEX = 0
    
    # Create the test file
    create_single_sheet_excel(ORIGINAL_FILE, OUTPUT_FILE, SHEET_INDEX)