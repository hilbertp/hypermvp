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
        # Load the data with correct separators and decimal indicators
        data = pd.read_csv(file_path, sep=';', decimal=',')
        
        # Convert the "Datum" column to datetime
        data["Datum"] = pd.to_datetime(data["Datum"], format="%d.%m.%Y")
        
        # Extract month and year from the first date in the "Datum" column
        if not data.empty:
            first_date = data["Datum"].iloc[0]
            month = first_date.month
            year = first_date.year
        else:
            month = None
            year = None

        metadata = {"month": month, "year": year, "rows": len(data)}
        return data, metadata
    except Exception as e:
        print(f"Error loading data: {e}")
        return None, None