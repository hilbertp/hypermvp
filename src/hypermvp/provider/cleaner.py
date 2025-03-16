import pandas as pd
import re
import logging

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
      - Convert DELIVERY_DATE to datetime (using the format "%m/%d/%Y").
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

    # Convert DELIVERY_DATE to datetime using the provided format.
    try:
        df["DELIVERY_DATE"] = pd.to_datetime(df["DELIVERY_DATE"], format="%m/%d/%Y")
    except ValueError as e:
        raise ValueError(f"Error converting DELIVERY_DATE to datetime: {e}")

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

    return df
