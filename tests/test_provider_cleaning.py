import sys
import os

# Print the Python path
print("Python path:", sys.path)

# Add the project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
print("Updated Python path:", sys.path)


from scripts.provider_loader import load_provider_list
from scripts.provider_data_cleaner import clean_provider_data

def main():
    # File path to the test provider list
    file_path = 'data/provider_list_2024_09_01.xlsx'

    # Load the provider list
    provider_list = load_provider_list([file_path])
    if provider_list is None:
        print("Failed to load provider list.")
        return

    # Clean the provider list
    cleaned_provider_list = clean_provider_data(provider_list)
    if cleaned_provider_list is None:
        print("Failed to clean provider list.")
        return

    # Save the cleaned data to a new file
    output_file = 'data/processed_provider_list.xlsx'
    cleaned_provider_list.to_excel(output_file, index=False)
    print(f"Processed provider list saved to: {output_file}")

if __name__ == "__main__":
    main()
