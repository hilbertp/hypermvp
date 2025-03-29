import pandas as pd
import logging
from hypermvp.config import AFRR_DATE_FORMAT, TIME_FORMAT, standardize_date_column

def filter_negative_50hertz(data):
    """
    Filters the DataFrame to retain only the relevant columns for negative regulation from 50Hertz.

    Args:
        data (pd.DataFrame): The original DataFrame.

    Returns:
        pd.DataFrame: A DataFrame with the selected columns.
    """
    try:
        # Ensure column names are strings and strip any extra spaces
        data.columns = data.columns.astype(str).str.strip()

        # Select relevant columns
        relevant_data = data[["Datum", "von", "bis", "50Hertz (Negativ)"]]

        return relevant_data
    except KeyError as e:
        print(f"Required columns missing in data: {e}")
        return None
    except Exception as e:
        print(f"An error occurred during filtering: {e}")
        return None

def clean_afrr_data(df):
    """
    Clean and transform AFRR data.
    
    Args:
        df (pd.DataFrame): The data to clean.
        
    Returns:
        pd.DataFrame: Cleaned data.
    """
    if df is None or df.empty:
        logging.warning("No data to clean")
        return df
        
    # Standardize date column to datetime format using helper from config
    df = standardize_date_column(df, 'Datum', AFRR_DATE_FORMAT)
    
    # Standardize time columns
    if 'von' in df.columns:
        # Ensure time values are properly formatted
        df['von'] = df['von'].astype(str).str.strip()
        # Add seconds if missing (e.g., "00:15" → "00:15:00")
        df['von'] = df['von'].apply(lambda x: x if len(x.split(':')) > 2 else f"{x}:00")
    
    if 'bis' in df.columns:
        df['bis'] = df['bis'].astype(str).str.strip()
        df['bis'] = df['bis'].apply(lambda x: x if len(x.split(':')) > 2 else f"{x}:00")
    
    # Handle numeric columns - replace commas with periods for German number format
    numeric_columns = ['50Hertz (Negativ)']
    for col in numeric_columns:
        if col in df.columns:
            # Convert German number format (comma as decimal separator)
            df[col] = df[col].astype(str).str.replace(',', '.')
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    return df

def validate_afrr_data(df):
    """
    Validate AFRR data after cleaning.
    
    Args:
        df (pd.DataFrame): The cleaned data to validate.
        
    Returns:
        tuple: (valid, message) - valid is a boolean indicating if validation passed,
                                 message contains details if validation failed.
    """
    if df is None or df.empty:
        return False, "Data is empty or None"
    
    # Check required columns
    required_columns = ["Datum", "von", "bis", "50Hertz (Negativ)"]
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        return False, f"Missing required columns: {missing_columns}"
    
    # Check date format
    date_col = df['Datum']
    if not pd.api.types.is_datetime64_dtype(date_col):
        return False, "Datum column is not in datetime format"
    
    # Check time format
    for time_col in ['von', 'bis']:
        if df[time_col].str.match(r'^\d{2}:\d{2}(:\d{2})?$').sum() != len(df):
            return False, f"{time_col} column contains invalid time formats"
    
    # Check numeric columns
    if df['50Hertz (Negativ)'].isna().any():
        return False, "50Hertz (Negativ) column contains invalid numeric values"
    
    return True, "Validation passed"
