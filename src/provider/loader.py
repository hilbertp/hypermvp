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
        try:
            print(f"Loading provider list from {file_path}...")
            df = pd.read_excel(file_path)
            dfs.append(df)
        except FileNotFoundError:
            print(f"File not found: {file_path}")
        except Exception as e:
            print(f"Error loading file {file_path}: {e}")
    
    # Combine all loaded DataFrames
    if dfs:
        combined_df = pd.concat(dfs, ignore_index=True)
        print(f"Loaded {len(combined_df)} rows from {len(file_paths)} file(s).")
        return combined_df
    else:
        print("No valid provider files loaded.")
        return None
except Exception as e:
    print(f"Unexpected error: {e}")
    return None
