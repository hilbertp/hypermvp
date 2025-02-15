import os
from datetime import datetime
import pandas as pd
from hypermvp.config import PROCESSED_DATA_DIR

def dump_afrr_data(cleaned_afrr_data, identifier="afrr"):
    """
    Dumps the cleaned aFRR data to the processed directory.
    
    Expects that the "Datum" column in cleaned_afrr_data is already 
    of type datetime64[ns] (as produced by loader and cleaner).
    The CSV is dumped with the "Datum" column formatted as dd.mm.yyyy.
    """
    try:
        os.makedirs(PROCESSED_DATA_DIR, exist_ok=True)
        
        # Make sure the "Datum" column is already datetime
        if not pd.api.types.is_datetime64_any_dtype(cleaned_afrr_data["Datum"]):
            raise ValueError("Expected 'Datum' to be datetime64[ns]. Convert it first.")
        if cleaned_afrr_data.empty or pd.isna(cleaned_afrr_data["Datum"].iloc[0]):
            raise ValueError("Invalid or missing date in 'Datum' column.")
        
        first_date = cleaned_afrr_data["Datum"].iloc[0]
        month = first_date.month
        year = first_date.year
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = os.path.join(
            PROCESSED_DATA_DIR,
            f"cleaned_{identifier}_{year}_{month:02d}_{timestamp}.csv",
        )
        
        # Convert the "Datum" column explicitly to dd.mm.yyyy strings.
        cleaned_afrr_data = cleaned_afrr_data.copy()
        cleaned_afrr_data["Datum"] = cleaned_afrr_data["Datum"].dt.strftime("%d.%m.%Y")
        cleaned_afrr_data.to_csv(filename, index=False)
        
        print("\n=== aFRR Data Export Summary ===")
        print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Rows processed: {len(cleaned_afrr_data)}")
        print(f"Output location: {os.path.relpath(filename)}")
        print("=============================\n")
    except Exception as e:
        print(f"ERROR: Failed to dump data - {str(e)}")
