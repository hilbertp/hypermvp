import os
import pandas as pd
from hypermvp.provider.loader import load_provider_file
from hypermvp.provider.cleaner import clean_provider_data


def save_to_csv(df, output_dir, filename):
    """
    Save a DataFrame to a CSV file.

    Args:
        df (pd.DataFrame): Data to save.
        output_dir (str): Directory to save the CSV file.
        filename (str): Name of the CSV file.
    """
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    df.to_csv(os.path.join(output_dir, filename), index=False)


# Define file paths and constants
input_dir = "data/01_raw"
output_dir = "data/02_processed"

# Check if the input directory exists
if not os.path.exists(input_dir):
    raise FileNotFoundError(f"Input directory '{input_dir}' does not exist.")

# Process each file in the input directory
for filename in os.listdir(input_dir):
    if filename.endswith(".xlsx"):
        filepath = os.path.join(input_dir, filename)
        
        # Load the raw data
        raw_data = load_provider_file(filepath)
        
        # Clean the data
        cleaned_data = clean_provider_data(raw_data)
        
        # Save the cleaned data to a CSV file
        output_filename = filename.replace(".xlsx", ".csv")
        save_to_csv(cleaned_data, output_dir, output_filename)
