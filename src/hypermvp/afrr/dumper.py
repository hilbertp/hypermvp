import os
from datetime import datetime


def dump_afrr_data(cleaned_afrr_data, month, year, identifier="afrr"):
    """
    Dumps the cleaned aFRR data to the processed directory.

    Args:
        cleaned_afrr_data (pd.DataFrame): Cleaned aFRR data.
        month (int): Month of the data.
        year (int): Year of the data.
        identifier (str): Optional identifier for the file name.
    """
    try:
        # Ensure the directory exists
        os.makedirs(PROCESSED_DATA_DIR, exist_ok=True)

        # Create the filename with month and year metadata
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = os.path.join(
            PROCESSED_DATA_DIR,
            f"cleaned_{identifier}_{year}_{month:02d}_{timestamp}.csv",
        )

        # Save the data
        cleaned_afrr_data.to_csv(filename, index=False)
        print(f"aFRR data dumped to {filename}")
    except Exception as e:
        print(f"Error dumping data: {e}")
