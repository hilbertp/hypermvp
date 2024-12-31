import sys
import os

# Add project root to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.afrr.loader import load_data as load_afrr_data
from src.afrr.cleaner import filter_negative_50hertz
from src.provider.loader import load_provider_list
from src.provider.cleaner import clean_provider_data
from config import AFRR_FILE_PATH, PROVIDER_FILE_PATHS, OUTPUT_DATA_DIR


def main():
    # Step 1: Load and process aFRR data
    print(f"Loading aFRR data from: {AFRR_FILE_PATH}")
    afrr_data = load_afrr_data(AFRR_FILE_PATH)
    if afrr_data is not None:
        print("aFRR data loaded successfully!")
        print(afrr_data.info())
        
        afrr_cleaned_data = filter_negative_50hertz(afrr_data)
        if afrr_cleaned_data is not None:
            print("aFRR data filtered successfully!")
            print(afrr_cleaned_data.head())
        else:
            print("Failed to filter aFRR data.")
            return
    else:
        print("Failed to load aFRR data.")
        return

    # Step 2: Load and process provider data
    print(f"Loading provider list from: {PROVIDER_FILE_PATHS}")
    provider_data = load_provider_list(PROVIDER_FILE_PATHS)
    if provider_data is not None:
        print("Provider data loaded successfully!")
        print(provider_data.info())
        
        provider_cleaned_data = clean_provider_data(provider_data)
        if provider_cleaned_data is not None:
            print("Provider data cleaned successfully!")
            print(provider_cleaned_data.head())
        else:
            print("Failed to clean provider data.")
            return
    else:
        print("Failed to load provider data.")
        return

    # Step 3: Save the cleaned data
    os.makedirs(OUTPUT_DATA_DIR, exist_ok=True)
    afrr_output_path = os.path.join(OUTPUT_DATA_DIR, 'cleaned_afrr.csv')
    provider_output_path = os.path.join(OUTPUT_DATA_DIR, 'cleaned_provider_data.csv')

    afrr_cleaned_data.to_csv(afrr_output_path, index=False, sep=';', decimal=',')
    print(f"aFRR filtered data saved to: {afrr_output_path}")

    provider_cleaned_data.to_csv(provider_output_path, index=False, sep=';', decimal=',')
    print(f"Provider cleaned data saved to: {provider_output_path}")


if __name__ == "__main__":
    main()
