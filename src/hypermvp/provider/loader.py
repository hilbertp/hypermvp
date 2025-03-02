import pandas as pd


def load_provider_file(filepath):
    """Load a provider file assuming it's always an XLSX file."""
    if not filepath.endswith(".xlsx"):
        raise ValueError(f"Unsupported file format: {filepath}")
    return pd.read_excel(filepath)


def load_afrr_data(file_path):
    try:
        # Make sure this is returning a single DataFrame, not a tuple
        df = pd.read_csv(file_path, sep=';')
        return df  # Make sure you're returning just the DataFrame
    except Exception as e:
        print(f"Error loading AFRR data: {e}")
        return None  # Should return None or an empty DataFrame, not a tuple
