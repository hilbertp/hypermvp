import sys
import os
import time
import pandas as pd

from hypermvp.afrr.loader import load_afrr_data
from hypermvp.afrr.cleaner import filter_negative_50hertz
from hypermvp.provider.loader import load_provider_file
from hypermvp.provider.cleaner import clean_provider_data
from hypermvp.provider.merger import merge_cleaned_data
from hypermvp.config import AFRR_FILE_PATH, PROVIDER_FILE_PATHS, OUTPUT_DATA_DIR, PROCESSED_DATA_DIR


def main():
    # Step 1: Ensure output and processed directories exist
    os.makedirs(OUTPUT_DATA_DIR, exist_ok=True)
    os.makedirs(PROCESSED_DATA_DIR, exist_ok=True)
    print(f"Output directory ensured: {OUTPUT_DATA_DIR}")
    print(f"Processed directory ensured: {PROCESSED_DATA_DIR}")

    # Step 2: Process aFRR data
    print(f"Loading aFRR data from: {AFRR_FILE_PATH}")
    afrr_data = load_afrr_data(AFRR_FILE_PATH)
    if afrr_data is not None:
        print("aFRR data loaded successfully!")
        afrr_cleaned_data = filter_negative_50hertz(afrr_data)
        if afrr_cleaned_data is not None:
            print("aFRR data filtered successfully!")
            
            # Save the cleaned aFRR data
            timestamp = time.strftime("%Y%m%d_%H%M%S")
            afrr_output_path = os.path.join(PROCESSED_DATA_DIR, f'cleaned_afrr_{timestamp}.csv')
            afrr_cleaned_data.to_csv(afrr_output_path, index=False, sep=';', decimal=',')
            print(f"aFRR filtered data saved to: {afrr_output_path}")
        else:
            print("Failed to filter aFRR data.")
    else:
        print("Failed to load aFRR data.")

    # Step 3: Process provider data
    print(f"Loading provider data from: {PROVIDER_FILE_PATHS}")
    combined_provider_data = pd.DataFrame()  # Initialize an empty DataFrame

    for provider_file in PROVIDER_FILE_PATHS:
        raw_provider_data = load_provider_file(provider_file)
        if not raw_provider_data.empty:
            # Clean the provider data
            cleaned_provider_data = clean_provider_data(raw_provider_data)
            
            # Merge cleaned data into the combined DataFrame
            combined_provider_data = merge_cleaned_data(combined_provider_data, cleaned_provider_data)
            print(f"Processed and merged data from: {provider_file}")
        else:
            print(f"Skipped empty or invalid provider file: {provider_file}")

    # Save the combined provider data
    if not combined_provider_data.empty:
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        provider_output_path = os.path.join(PROCESSED_DATA_DIR, f'cleaned_provider_{timestamp}.csv')
        combined_provider_data.to_csv(provider_output_path, index=False, sep=';', decimal=',')
        print(f"Provider cleaned data saved to: {provider_output_path}")
    else:
        print("No valid provider data processed.")

if __name__ == "__main__":
    main()
