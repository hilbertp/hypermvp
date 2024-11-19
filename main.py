import os 
from src.afrr.loader import load_data as load_afrr_data
from src.afrr.cleaner import filter_negative_50hertz
from src.provider.loader import load_provider_list
from src.provider.cleaner import clean_provider_data


def main():
    # Step 1: Define file paths
    afrr_file_path = 'G:\\hyperMVP\\hypermvp\\data\\testdata_aFRR_sept.csv'
    print(f"Loading provider list from: {provider_file_path}")
    provider_file_paths = ['G:\\hyperMVP\\hypermvp\\data\\provider_list_2024_09_01.xlsx']
    provider_data = load_provider_list(provider_file_paths)  # Pass as a list   
    print(f"Provider file paths: {provider_file_paths}")


    # Step 2: Create output directory
    output_dir = 'G:\\hyperMVP\\hypermvp\\data\\output'
    os.makedirs(output_dir, exist_ok=True)

    # Step 3: Load and clean aFRR data
    afrr_data = load_afrr_data(afrr_file_path)
    if afrr_data is not None:
        print("aFRR data loaded successfully!")
        print(afrr_data.info())  # Display the structure of the DataFrame
        
        # Clean aFRR data
        afrr_cleaned_data = filter_negative_50hertz(afrr_data)
        if afrr_cleaned_data is not None:
            print("aFRR data filtered successfully!")
            print(afrr_cleaned_data.head())  # Display the first few rows of the filtered data
        else:
            print("Failed to filter aFRR data.")
            return
    else:
        print("Failed to load aFRR data.")
        return

    # Step 4: Load and clean provider data
    provider_data = load_provider_list(provider_file_path)  # Corrected function usage
    if provider_data is not None:
        print("Provider data loaded successfully!")
        print(provider_data.info())  # Display the structure of the DataFrame
        
        # Clean provider data
        provider_cleaned_data = clean_provider_data(provider_data)
        if provider_cleaned_data is not None:
            print("Provider data cleaned successfully!")
            print(provider_cleaned_data.head())  # Display the first few rows of the cleaned data
        else:
            print("Failed to clean provider data.")
            return
    else:
        print("Failed to load provider data.")
        return

    # Step 5: Save the cleaned data
    afrr_output_path = os.path.join(output_dir, 'cleaned_afrr.csv')
    provider_output_path = os.path.join(output_dir, 'cleaned_provider_data.csv')

    afrr_cleaned_data.to_csv(afrr_output_path, index=False, sep=';', decimal=',')
    print(f"aFRR filtered data saved to: {afrr_output_path}")

    provider_cleaned_data.to_csv(provider_output_path, index=False, sep=';', decimal=',')
    print(f"Provider cleaned data saved to: {provider_output_path}")


if __name__ == "__main__":
    main()
