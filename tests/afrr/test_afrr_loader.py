import os
import pandas as pd
import traceback
from hypermvp.config import RAW_DATA_DIR

def test_afrr_loader():
    # Try to import the function
    try:
        from hypermvp.afrr.loader import load_afrr_data
        print("Function 'load_afrr_data' imported successfully!")
    except ImportError as e:
        print(f"Failed to import function 'load_afrr_data': {e}")
        return

    # Example usage of load_afrr_data function
    file_path = os.path.join(RAW_DATA_DIR, 'testdata_aFRR_sept.csv')
    
    try:
        result = load_afrr_data(file_path)
        
        # Print the entire result and its length
        print(f"Result: {result}")
        print(f"Result length: {len(result)}")
        
        # Check if the result is a tuple
        if isinstance(result, tuple):
            data, *metadata = result
            print("Data and metadata loaded successfully!")
            print(f"Loaded data:\n{data.head()}")
            print(f"Metadata: {metadata}")
            
            # Assertions to verify the data
            assert isinstance(data, pd.DataFrame), "Loaded data is not a DataFrame"
            assert not data.empty, "Loaded data is empty"
            print("Assertions passed!")
        else:
            print("Unexpected result type:", type(result))
    except Exception as e:
        print(f"Failed to load data: {e}")
        traceback.print_exc()

if __name__ == "__main__":
    test_afrr_loader()