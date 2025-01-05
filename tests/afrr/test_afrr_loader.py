import os
from hypermvp.config import RAW_DATA_DIR


def test_afrr_loader():
    # Try to import the function
    from hypermvp.afrr.loader import load_afrr_data
    print("Function 'load_data' imported successfully!")

    # Example usage of load_data function
    file_path = os.path.join(RAW_DATA_DIR, 'testdata_aFRR_sept.csv')
    data = load_afrr_data(file_path)
    print(f"Loaded data:\n{data.head()}")
