import pandas as pd
import re
import logging
from hypermvp.global_config import (
    ISO_DATETIME_FORMAT, 
    ISO_DATE_FORMAT,
    standardize_date_column
)

def clean_column_name(col):
    """Replace all non-alphanumeric characters (except underscores) with an underscore."""
    return re.sub(r'\W+', '_', col)

def clean_provider_data(df):
    """
    Clean raw provider data.
    
    Steps:
      - Validate that required columns are present.
      - Drop rows where PRODUCT starts with "POS_".
      - Drop the NOTE column.
      - Convert DELIVERY_DATE to datetime using standardized format helpers.
      - Convert ENERGY_PRICE_[EUR/MWh] to string, replace commas with dots, and convert to float.
      - Adjust ENERGY_PRICE_[EUR/MWh] based on ENERGY_PRICE_PAYMENT_DIRECTION.
      - Drop the ENERGY_PRICE_PAYMENT_DIRECTION column.
      - Sort by DELIVERY_DATE, PRODUCT, and ENERGY_PRICE_[EUR/MWh].
      - Clean column names.
    """
    # Validate required columns.
    required_columns = [
        "DELIVERY_DATE",
        "PRODUCT",
        "ENERGY_PRICE_[EUR/MWh]",
        "ENERGY_PRICE_PAYMENT_DIRECTION",
        "ALLOCATED_CAPACITY_[MW]",
        "NOTE",
    ]
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")

    # Drop rows with PRODUCT starting with "POS_"
    df = df[~df["PRODUCT"].str.startswith("POS_")].copy()

    # Drop the NOTE column as it's not needed.
    df.drop(columns=["NOTE"], inplace=True)

    # Convert DELIVERY_DATE to datetime using standardized approach
    # MODIFIED: Use standardize_date_column helper instead of hardcoded format
    try:
        # First attempt with standard helper (autodetect format)
        df = standardize_date_column(df, "DELIVERY_DATE")
    except Exception as e:
        # If that fails, try with specific format - can happen with month/day/year format
        try:
            logging.warning(f"Initial date standardization failed, trying with specific format: {e}")
            df["DELIVERY_DATE"] = pd.to_datetime(df["DELIVERY_DATE"], format="%m/%d/%Y")
            logging.info("Successfully converted dates with '%m/%d/%Y' format")
        except ValueError as e2:
            raise ValueError(f"Error converting DELIVERY_DATE to datetime: {e2}")

    # Log a sample of dates to verify format
    if not df.empty:
        sample_date = df["DELIVERY_DATE"].iloc[0]
        logging.info(f"Sample DELIVERY_DATE after conversion: {sample_date} (type: {type(sample_date).__name__})")
        
        # Verify date range
        min_date = df["DELIVERY_DATE"].min()
        max_date = df["DELIVERY_DATE"].max()
        logging.info(f"Date range in data: {min_date.strftime(ISO_DATETIME_FORMAT)} to {max_date.strftime(ISO_DATETIME_FORMAT)}")

    # Ensure ENERGY_PRICE_[EUR/MWh] is treated as a string,
    # replace commas with dots, then convert to float.
    df["ENERGY_PRICE_[EUR/MWh]"] = df["ENERGY_PRICE_[EUR/MWh]"].astype(str)
    df["ENERGY_PRICE_[EUR/MWh]"] = df["ENERGY_PRICE_[EUR/MWh]"].str.replace(",", ".").astype(float)

    # Adjust energy prices based on payment direction.
    # If payment direction is PROVIDER_TO_GRID, multiply the price by -1.
    df.loc[df["ENERGY_PRICE_PAYMENT_DIRECTION"] == "PROVIDER_TO_GRID", "ENERGY_PRICE_[EUR/MWh]"] *= -1

    # Drop the ENERGY_PRICE_PAYMENT_DIRECTION column.
    df.drop(columns=["ENERGY_PRICE_PAYMENT_DIRECTION"], inplace=True)

    # Sort the DataFrame.
    df.sort_values(by=["DELIVERY_DATE", "PRODUCT", "ENERGY_PRICE_[EUR/MWh]"], inplace=True)

    # Clean column names.
    df.columns = [clean_column_name(col) for col in df.columns]

    # Add a period column for easier time period grouping (optional enhancement)
    if "DELIVERY_DATE" in df.columns:
        column_name = clean_column_name("DELIVERY_DATE")
        df["period"] = df[column_name]  # Create a copy of the date for period tracking
        
        # Log the total number of records by date using standardized format
        date_counts = df.groupby(df[column_name].dt.date).size()
        logging.info(f"Cleaned data contains {len(date_counts)} unique dates with {len(df)} total records")

    return df

def validate_provider_data(df):
    """
    Validate that the provider data meets all requirements.
    
    Args:
        df: The provider DataFrame to validate
        
    Returns:
        tuple: (is_valid, message) where is_valid is a boolean and message is a string
    """
    if df is None or df.empty:
        return False, "DataFrame is empty or None"
    
    # Check required columns after cleaning
    required_columns = ["DELIVERY_DATE", "PRODUCT", "ENERGY_PRICE__EUR_MWh_", "OFFERED_CAPACITY__MW_"]
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        return False, f"Missing required columns after cleaning: {missing_columns}"
    
    # Verify DELIVERY_DATE is datetime
    if not pd.api.types.is_datetime64_dtype(df["DELIVERY_DATE"]):
        return False, "DELIVERY_DATE is not in datetime format"
    
    # Validate numeric columns
    numeric_columns = ["ENERGY_PRICE__EUR_MWh_", "OFFERED_CAPACITY__MW_"]
    for col in numeric_columns:
        if not pd.api.types.is_numeric_dtype(df[col]):
            return False, f"{col} is not numeric"
    
    # Validate PRODUCT format - should be either NEG_XXX or similar
    if not all(df["PRODUCT"].str.match(r'^[A-Z]+_\d+')):
        return False, "PRODUCT column contains invalid formats"
    
    return True, "Validation passed"
