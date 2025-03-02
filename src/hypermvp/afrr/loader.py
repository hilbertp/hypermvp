import pandas as pd
import os

def load_afrr_data(file_path):
    """
    Loads CSV data into a Pandas DataFrame and retrieves month and year from the data inside the first column ("Datum").

    Args:
        file_path (str): The path to the CSV file.

    Returns:
        tuple: (pd.DataFrame, dict): Loaded data and metadata containing month, year, and number of rows.
    """
    try:
        df = pd.read_csv(file_path, sep=';')
        print(f"Type of loaded data: {type(df)}")  # Debug print
        return df  # Make sure this returns a DataFrame, not a tuple
    except Exception as e:
        print(f"Error loading AFRR data: {e}")
        return None