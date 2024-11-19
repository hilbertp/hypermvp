import pandas as pd
from typing import List

def load_provider_list(file_paths: List[str]) -> pd.DataFrame:
    """
    Load provider lists from one or more Excel files.

    Parameters:
    file_paths (List[str]): List of file paths to Excel files.

    Returns:
    pd.DataFrame: Concatenated DataFrame containing all loaded data.
    """
    try:
        dfs = []
        for file_path in file_paths:
            print(f"Loading provider list from {file_path}...")
            # Read the Excel file into a DataFrame
            df = pd.read_excel(file_path)
            dfs.append(df)
        
        # Combine all loaded DataFrames
        combined_df = pd.concat(dfs, ignore_index=True)
        print(f"Loaded {len(combined_df)} rows from {len(file_paths)} file(s).")
        return combined_df
    except Exception as e:
        print(f"Error loading provider list(s): {e}")
        return None
