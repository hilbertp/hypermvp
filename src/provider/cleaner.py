def clean_provider_data(df):
    """
    Clean and transform provider offer data.

    Args:
        df (pd.DataFrame): Raw provider data.

    Returns:
        pd.DataFrame: Cleaned provider data.
    """
    # Keep only the necessary columns
    required_columns = [
        'DELIVERY_DATE', 'PRODUCT', 'ENERGY_PRICE_[EUR/MWh]',
        'ENERGY_PRICE_PAYMENT_DIRECTION', 'ALLOCATED_CAPACITY_[MW]'
    ]
    df = df[required_columns]

    # Filter rows to keep only 'neg_*' products
    df = df[df['PRODUCT'].str.startswith('neg_')]

    # Adjust energy prices based on payment direction
    df['ENERGY_PRICE_[EUR/MWh]'] = df.apply(
        lambda row: -row['ENERGY_PRICE_[EUR/MWh]']
        if row['ENERGY_PRICE_PAYMENT_DIRECTION'] == 'PROVIDER_TO_GRID'
        else row['ENERGY_PRICE_[EUR/MWh]'],
        axis=1
    )

    # Drop the payment direction column as it's no longer needed
    df = df.drop(columns=['ENERGY_PRICE_PAYMENT_DIRECTION'])

    # Sort the DataFrame by DELIVERY_DATE, PRODUCT, and ENERGY_PRICE_[EUR/MWh]
    df = df.sort_values(
        by=['DELIVERY_DATE', 'PRODUCT', 'ENERGY_PRICE_[EUR/MWh]'],
        ascending=[True, True, True]
    )

    return df
