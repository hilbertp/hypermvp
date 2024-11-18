import pandas as pd

def clean_provider_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean the provider data by filtering and sorting.

    Parameters:
    df (pd.DataFrame): The raw provider data.

    Returns:
    pd.DataFrame: Cleaned provider data.
    """
    try:
        # Adjust ENERGY_PRICE_[EUR/MWh] based on ENERGY_PRICE_PAYMENT_DIRECTION
        df['ENERGY_PRICE_[EUR/MWh]'] = df.apply(
            lambda row: -row['ENERGY_PRICE_[EUR/MWh]'] if row['ENERGY_PRICE_PAYMENT_DIRECTION'] == 'PROVIDER_TO_GRID' else row['ENERGY_PRICE_[EUR/MWh]'],
            axis=1
        )

        # Filter for products starting with "NEG"
        df = df[df['PRODUCT'].str.startswith('NEG')]

        # Sort by DELIVERY_DATE, PRODUCT, and ENERGY_PRICE
        df = df.sort_values(by=['DELIVERY_DATE', 'PRODUCT', 'ENERGY_PRICE_[EUR/MWh]'])

        return df
    except KeyError as e:
        print(f"Missing expected column: {e}")
        return None
    except Exception as e:
        print(f"Error during cleaning: {e}")
        return None
