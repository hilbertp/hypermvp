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
        'DELIVERY_DATE', 'PRODUCT', 'ENERGY_PRICE_[EUR/MWh]',
        'ENERGY_PRICE_PAYMENT_DIRECTION', 'ALLOCATED_CAPACITY_[MW]', 'NOTE'
    ]
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")

    # Warn about rows with notes
    if 'NOTE' in df.columns and df['NOTE'].notnull().any():
        rows_with_notes = df[df['NOTE'].notnull()]
        print("Warning: The following rows contain notes:")
        print(rows_with_notes)

    # Drop rows with POS_* in the PRODUCT column
    df = df[~df['PRODUCT'].str.startswith('POS_')].copy()

    # Drop the NOTE column
    df = df.drop(columns=["NOTE"])

    # Convert DELIVERY_DATE to datetime
    df["DELIVERY_DATE"] = pd.to_datetime(df["DELIVERY_DATE"], format="%m/%d/%Y", errors="coerce")

    # Ensure the column remains as datetime64 even if some entries are NaT
    if not pd.api.types.is_datetime64_any_dtype(df["DELIVERY_DATE"]):
        raise ValueError("DELIVERY_DATE column could not be converted to datetime64.")

    # Handle ENERGY_PRICE conversion
    df["ENERGY_PRICE_[EUR/MWh]"] = (
        df["ENERGY_PRICE_[EUR/MWh]"]
        .str.replace(",", ".")  # Replace commas with dots
        .apply(pd.to_numeric, errors="coerce")  # Coerce invalid values to NaN
    )

    # Debug: Print values before adjustment
    print("\nBefore ENERGY_PRICE adjustment:")
    print(df[["PRODUCT", "ENERGY_PRICE_[EUR/MWh]", "ENERGY_PRICE_PAYMENT_DIRECTION"]])

    # Adjust energy prices based on payment direction
    df["ENERGY_PRICE_[EUR/MWh]"] = df.apply(
        lambda row: -row["ENERGY_PRICE_[EUR/MWh]"]
        if row["ENERGY_PRICE_PAYMENT_DIRECTION"] == "PROVIDER_TO_GRID"
        else row["ENERGY_PRICE_[EUR/MWh]"],
        axis=1,
    )

    # Debug: Print values after adjustment
    print("\nAfter ENERGY_PRICE adjustment:")
    print(df[["PRODUCT", "ENERGY_PRICE_[EUR/MWh]"]])

    # Drop the payment direction column as it's no longer needed
    df = df.drop(columns=["ENERGY_PRICE_PAYMENT_DIRECTION"])

    # Sort the DataFrame
    df = df.sort_values(by=["DELIVERY_DATE", "PRODUCT", "ENERGY_PRICE_[EUR/MWh]"])

    # Return cleaned DataFrame
    return df
