import os
import pandas as pd
from datetime import datetime
from hypermvp.config import PROCESSED_DATA_DIR  # Ensure PROCESSED_DATA_DIR is defined in src/config.py

def dump_afrr_data(cleaned_afrr_data, identifier="afrr"):
    """
    Dumps the cleaned aFRR data to the processed directory.
    Args:
        cleaned_afrr_data (pd.DataFrame): Cleaned aFRR data.
        identifier (str): Optional identifier for the file name.
    """
    try:
        # Ensure the directory exists
        os.makedirs(PROCESSED_DATA_DIR, exist_ok=True)
        print(f"Directory ensured: {PROCESSED_DATA_DIR}")

        # Create the filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = os.path.join(PROCESSED_DATA_DIR, f"cleaned_{identifier}_{timestamp}.csv")
        print(f"Filename created: {filename}")

        # Save the data
        cleaned_afrr_data.to_csv(filename, index=False)
        print(f"aFRR data dumped to {filename}")

    except Exception as e:
        print(f"Error dumping data: {e}")

# Example usage:
# Assume `cleaned_afrr` is a pandas DataFrame containing the cleaned aFRR data.
# dump_afrr_data(cleaned_afrr, "daily_afrr")