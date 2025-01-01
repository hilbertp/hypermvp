import sys
print("sys.path:", sys.path)

import os
print("Current working directory:", os.getcwd())

# Import the loader module
import src.afrr.loader

print("Module path:", src.afrr.loader.__file__)
print("Module contents:", dir(src.afrr.loader))

# Try to import the function
from src.afrr.loader import load_afrr_data
print("Function 'load_data' imported successfully!")

# Example usage of load_data function
file_path = 'G:/hyperMVP/hypermvp/data/01_raw/testdata_aFRR_sept.csv'  # Corrected path to your data file
data = load_afrr_data(file_path)
print(f"Loaded data:\n{data.head()}")
