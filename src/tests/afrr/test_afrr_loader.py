# Import the loader module
import afrr.loader

# Try to import the function
from afrr.loader import load_afrr_data
print("Function 'load_data' imported successfully!")

# Example usage of load_data function
file_path = 'G:/hyperMVP/data/01_raw/testdata_aFRR_sept.csv'  # Corrected path to your data file
data = load_afrr_data(file_path)
print(f"Loaded data:\n{data.head()}")
