from scripts.loader import load_data

print("Import successful!")

import scripts.loader
print("Module path:", scripts.loader.__file__)
print("Module contents:", dir(scripts.loader))

from scripts.loader import load_data
print("load_data imported successfully!")

import sys
sys.path.append('C:/Users/hilbe/hypermvp')  # Add project root to sys.path

from scripts.loader import load_data

print("Import successful!")

import sys

# Ensure the project root is in the Python path
sys.path.append('C:/Users/hilbe/hypermvp')

# Import the loader module
import scripts.loader
print("Module path:", scripts.loader.__file__)
print("Module contents:", dir(scripts.loader))

# Try to import the function
from scripts.loader import load_data
print("Function 'load_data' imported successfully!")
