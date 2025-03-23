import os
import pandas as pd
import logging


def load_provider_file(filepath):
    """Load a provider file (XLSX) and return a DataFrame."""
    logging.debug("Loading file (from loader): %s", filepath)
    if not filepath.endswith(".xlsx"):
        raise ValueError(f"Unsupported file format: {filepath}")
    return pd.read_excel(filepath)


def load_provider_data(directory=None):
    """
    Load all provider data files from a directory.
    
    Args:
        directory: Directory containing provider data files (defaults to PROVIDER_RAW_DIR)
        
    Returns:
        DataFrame containing all provider data
    """
    from hypermvp.config import PROVIDER_RAW_DIR, PROVIDER_FILE_PATHS
    
    if directory is None:
        directory = PROVIDER_RAW_DIR
        file_paths = PROVIDER_FILE_PATHS
    else:
        # Handle custom directory case
        file_paths = [
            os.path.join(directory, file)
            for file in os.listdir(directory)
            if os.path.isfile(os.path.join(directory, file))
            and (file.endswith(".xlsx") or file.endswith(".csv"))
        ]
    
    logging.info(f"Loading provider files from {directory}")
    
    all_data = []
    for file_path in file_paths:
        try:
            df = load_provider_file(file_path)
            all_data.append(df)
        except Exception as e:
            logging.error(f"Error loading {file_path}: {e}")
    
    if not all_data:
        logging.warning(f"No provider data files found in {directory}")
        return pd.DataFrame()
    
    return pd.concat(all_data, ignore_index=True)


def load_afrr_data(file_path):
    try:
        # Make sure this is returning a single DataFrame, not a tuple
        df = pd.read_csv(file_path, sep=';')
        return df  # Make sure you're returning just the DataFrame
    except Exception as e:
        print(f"Error loading AFRR data: {e}")
        return None  # Should return None or an empty DataFrame, not a tuple
