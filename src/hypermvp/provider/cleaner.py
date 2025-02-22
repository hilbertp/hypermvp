import pandas as pd
import re

def clean_column_name(col):
    """Replace all non-alphanumeric characters (except underscores) with an underscore."""
    return re.sub(r'\W+', '_', col)

def clean_provider_data(df):
    # Validate required columns
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

    # Drop rows with POS_* in the PRODUCT column
    df = df[~df["PRODUCT"].str.startswith("POS_")].copy()

    # Drop the NOTE column
    df.drop(columns=["NOTE"], inplace=True)

    # Convert DELIVERY_DATE to datetime
    try:
        df["DELIVERY_DATE"] = pd.to_datetime(df["DELIVERY_DATE"], format="%m/%d/%Y")
    except ValueError as e:
        raise ValueError(f"Error converting DELIVERY_DATE to datetime: {e}")

    # Ensure ENERGY_PRICE_[EUR/MWh] column contains only string values
    df["ENERGY_PRICE_[EUR/MWh]"] = df["ENERGY_PRICE_[EUR/MWh]"].astype(str)

    # Handle ENERGY_PRICE conversion
    df["ENERGY_PRICE_[EUR/MWh]"] = df["ENERGY_PRICE_[EUR/MWh]"].str.replace(",", ".").astype(float)

    # Adjust energy prices based on payment direction
    df.loc[df["ENERGY_PRICE_PAYMENT_DIRECTION"] == "PROVIDER_TO_GRID", "ENERGY_PRICE_[EUR/MWh]"] *= -1

    # Drop the ENERGY_PRICE_PAYMENT_DIRECTION column
    df.drop(columns=["ENERGY_PRICE_PAYMENT_DIRECTION"], inplace=True)

    # Sort the DataFrame
    df.sort_values(by=["DELIVERY_DATE", "PRODUCT", "ENERGY_PRICE_[EUR/MWh]"], inplace=True)

    # Clean column names
    df.columns = [clean_column_name(col) for col in df.columns]

    return df
