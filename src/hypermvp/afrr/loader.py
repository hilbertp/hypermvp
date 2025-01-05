import pandas as pd

def load_afrr_data(file_path):
    """
    Loads CSV data into a Pandas DataFrame and formats the date.

    Args:
        file_path (str): The path to the CSV file.

    Returns:
        pd.DataFrame: A DataFrame containing the loaded data.
    """
    try:
        data = pd.read_csv(file_path, delimiter=';', decimal=',')
        data['Datum'] = pd.to_datetime(data['Datum'], format='%d.%m.%Y')
        return data
    except Exception as e:
        print(f"Error loading data: {e}")
        return None

print("Loader module loaded.")
print(__file__)
