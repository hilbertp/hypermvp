import os
import pandas as pd
import logging


def load_provider_file(filepath):
    """Load a provider file (XLSX) and return a DataFrame."""
    logging.debug("Loading file (from loader): %s", filepath)
    if not filepath.endswith(".xlsx"):
        raise ValueError(f"Unsupported file format: {filepath}")
    return pd.read_excel(filepath)


def load_provider_data(raw_dir):
    """
    Load all XLSX provider files from a given directory and return a combined DataFrame.
    """
    provider_dfs = []
    logging.info("Loading provider files from %s", raw_dir)
    for filename in os.listdir(raw_dir):
        if filename.endswith(".xlsx"):
            filepath = os.path.join(raw_dir, filename)
            logging.info("Loading file: %s", filepath)
            df = load_provider_file(filepath)
            provider_dfs.append(df)
    if not provider_dfs:
        raise ValueError(f"No provider files found in {raw_dir}")
    return pd.concat(provider_dfs, ignore_index=True)


def load_afrr_data(file_path):
    try:
        # Make sure this is returning a single DataFrame, not a tuple
        df = pd.read_csv(file_path, sep=';')
        return df  # Make sure you're returning just the DataFrame
    except Exception as e:
        print(f"Error loading AFRR data: {e}")
        return None  # Should return None or an empty DataFrame, not a tuple
