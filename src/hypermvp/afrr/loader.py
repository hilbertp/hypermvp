import pandas as pd
import os

def load_afrr_data(file_path):
    """
    Loads CSV data into a Pandas DataFrame, extracts month and year from the file name, and formats the date.

    Args:
        file_path (str): The path to the CSV file.

    Returns:
        tuple: (pd.DataFrame, int, int): Loaded data, extracted month, and year.
    """
    try:
        # Extract month and year from file name
        file_name = os.path.basename(file_path)
        month, year = map(int, file_name.split('_')[1:3])  # Assumes file names like "aFRR_09_2024.csv"

        # Load the data
        data = pd.read_csv(file_path, delimiter=';', decimal=',')
        data['Datum'] = pd.to_datetime(data['Datum'], format='%d.%m.%Y')

        return data, month, year
    except Exception as e:
        print(f"Error loading data: {e}")
        return None, None, None
