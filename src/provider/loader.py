import pandas as pd

def load_provider_file(file_path):
    """
    Load a single provider data file.

    Args:
        file_path (str): Path to the provider data file.

    Returns:
        pd.DataFrame: Loaded DataFrame.
    """
    try:
        # Check file extension and load file accordingly
        if file_path.endswith('.csv'):
            return pd.read_csv(file_path, sep=';', decimal=',')
        elif file_path.endswith('.xlsx'):
            return pd.read_excel(file_path)
        else:
            # Raise an error for unsupported file formats
            raise ValueError(f"Unsupported file format: {file_path}")
    except Exception as e:
        # Print an error message and return an empty DataFrame on failure
        print(f"Error loading file {file_path}: {e}")
        return pd.DataFrame()
