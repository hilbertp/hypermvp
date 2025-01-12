import os
import pandas as pd
from saver import save_to_duckdb

# Define file paths and constants
processed_dir = 'data/02_processed'
db_path = 'data/processed_data.duckdb'
table_name = 'provider_data'

# Initialize an empty DataFrame for combined data
combined_df = pd.DataFrame()

# Process each file in the processed directory
for filename in os.listdir(processed_dir):
    if filename.endswith('.csv'):
        filepath = os.path.join(processed_dir, filename)
        
        # Read the cleaned data from CSV
        cleaned_data = pd.read_csv(filepath)
        
        # Append the cleaned data to the combined DataFrame
        combined_df = pd.concat([combined_df, cleaned_data], ignore_index=True)

# Save the combined data to DuckDB
save_to_duckdb(combined_df, db_path, table_name)

print("Data saving to DuckDB completed successfully.")