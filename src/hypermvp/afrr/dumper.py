import os
from datetime import datetime
from hypermvp import config

def dump_afrr_data(cleaned_afrr_data, identifier="afrr"):
    """
    Dumps the cleaned aFRR data to the output directory.
    
    Expects that the "Datum" column in cleaned_afrr_data is of type datetime64[ns].
    
    The CSV is dumped using ISO 8601 date format (YYYY-MM-DD) so that the dates remain
    unambiguous and are compatible with DuckDB.
    """
    # Determine output file name based on current timestamp
    timestamp = datetime.now().strftime("%Y_%m%d%H%M%S")
    output_filename = f"cleaned_{identifier}_2024_01_{timestamp}.csv"
    
    # Ensure config.OUTPUT_DATA_DIR exists
    os.makedirs(config.OUTPUT_DATA_DIR, exist_ok=True)
    output_path = os.path.join(config.OUTPUT_DATA_DIR, output_filename)
    
    # Write CSV with semicolon as separator and comma as decimal
    cleaned_afrr_data.to_csv(output_path, sep=";", decimal=",", index=False)
    
    # Print meta information for traceability
    print("=== aFRR Data Export Summary ===")
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Rows processed: {len(cleaned_afrr_data)}")
    print(f"Output location: {output_path}")
    print("=============================")
