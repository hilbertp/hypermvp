import pandas as pd

def merge_cleaned_data(combined_df, cleaned_df):
    """
    Merge a cleaned provider data DataFrame into the combined DataFrame.

    Args:
        combined_df (pd.DataFrame): The existing combined DataFrame.
        cleaned_df (pd.DataFrame): The cleaned DataFrame to merge.

    Returns:
        pd.DataFrame: Updated combined DataFrame.
    """
    # Concatenate the new cleaned data with the existing data
    combined_df = pd.concat([combined_df, cleaned_df], ignore_index=True)

    return combined_df
