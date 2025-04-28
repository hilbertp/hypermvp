import pandas as pd
import os

def create_reduced_excel_copy(input_path, output_path, max_rows=10):
    """
    Create a reduced copy of an Excel file with only the first sheet
    and limited number of rows.
    
    Args:
        input_path (str): Path to the original Excel file
        output_path (str): Path where to save the reduced Excel file
        max_rows (int): Maximum number of rows to keep (including header)
    """
    print(f"Creating reduced copy of {os.path.basename(input_path)}")
    
    # Read only the first sheet of the Excel file
    print("Reading first sheet...")
    df = pd.read_excel(input_path, sheet_name=0, nrows=max_rows)
    
    # Get the number of rows read
    row_count = len(df)
    print(f"Read {row_count} rows from first sheet")
    
    # Create output directory if it doesn't exist
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    # Save the reduced DataFrame to a new Excel file
    print(f"Saving reduced file to {output_path}")
    df.to_excel(output_path, index=False)
    
    output_size = os.path.getsize(output_path) / (1024 * 1024)
    print(f"Reduced file saved ({output_size:.2f} MB)")

if __name__ == "__main__":
    # Define paths
    original_file = "/home/philly/hypermvp/data/01_raw/provider/RESULT_LIST_ANONYM_ENERGY_MARKET_aFRR_DE_2024-09-01_2024-09-30.xlsx"
    reduced_file = "/home/philly/hypermvp/data/01_raw/provider/REDUCED_RESULT_LIST_ANONYM_ENERGY_MARKET_aFRR_DE_2024-09-01_2024-09-30.xlsx"
    
    # Create the reduced copy
    create_reduced_excel_copy(original_file, reduced_file, max_rows=10)
    print("Done!")