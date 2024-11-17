from scripts.loader import load_data
from scripts.data_cleaner import filter_negative_50hertz
import scripts.loader
print(scripts.loader.__file__)  # Verify the path to the data_loader module


def main():
    # Step 1: Define the file path for the input CSV file
    file_path = 'C:/Users/hilbe/hypermvp/data/testdata_affr_sept.csv'
    
    # Step 2: Load the data
    data = load_data(file_path)
    if data is not None:
        print("Data loaded successfully!")
        print(data.info())  # Display the structure of the DataFrame
    else:
        print("Failed to load data.")
        return  # Exit if data loading fails
    
    # Step 3: Filter the data for negative 50Hertz values
    filtered_data = filter_negative_50hertz(data)
    if filtered_data is not None:
        print("Filtered data:")
        print(filtered_data.head())  # Display the first few rows of the filtered data
    else:
        print("Failed to filter data.")
        return  # Exit if filtering fails
    
    # Step 4: Save or process the filtered data further
    # Save it to a new CSV file
    output_path = 'C:/Users/hilbe/hypermvp/data/filtered_negative_50hertz.csv'
    filtered_data.to_csv(output_path, index=False, sep=';', decimal=',')
    print(f"Filtered data saved to: {output_path}")

if __name__ == "__main__":
    main()
