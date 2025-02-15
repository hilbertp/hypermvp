import pandas as pd

def clean_provider_data(df):
    """
    Clean and transform provider offer data.

    Args:
        df (pd.DataFrame): Raw provider data.

    Returns:
        pd.DataFrame: Cleaned provider data.
    """
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

    # Handle ENERGY_PRICE conversion
    df["ENERGY_PRICE_[EUR/MWh]"] = df["ENERGY_PRICE_[EUR/MWh]"].str.replace(",", ".").astype(float)

    # Adjust energy prices based on payment direction
    df.loc[df["ENERGY_PRICE_PAYMENT_DIRECTION"] == "PROVIDER_TO_GRID", "ENERGY_PRICE_[EUR/MWh]"] *= -1

    # Drop the ENERGY_PRICE_PAYMENT_DIRECTION column
    df.drop(columns=["ENERGY_PRICE_PAYMENT_DIRECTION"], inplace=True)

    # Sort the DataFrame
    df.sort_values(by=["DELIVERY_DATE", "PRODUCT", "ENERGY_PRICE_[EUR/MWh]"], inplace=True)

    return df