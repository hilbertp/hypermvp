import sys

# Add project root to sys.path (only once)
sys.path.append('G:/hyperMVP/hypermvp')  # Adjust the path based on the actual location of your project

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
