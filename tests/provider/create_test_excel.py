import pandas as pd
import os

# Create sample data
data = {
    "DELIVERY_DATE": ["2024-09-01", "2024-09-02"],
    "PRODUCT": ["NEG_001", "NEG_002"],
    "ENERGY_PRICE_[EUR/MWh]": ["138.85", "140.50"],
    "ENERGY_PRICE_PAYMENT_DIRECTION": ["GRID_TO_PROVIDER", "GRID_TO_PROVIDER"],
    "ALLOCATED_CAPACITY_[MW]": ["50", "30"],
    "NOTE": ["Test note", "Another note"]
}
df = pd.DataFrame(data)

# Define the output path for the Excel file
output_dir = os.path.join(os.path.dirname(__file__))
output_file = os.path.join(output_dir, "test_data.xlsx")

# Save to Excel
df.to_excel(output_file, index=False)
print(f"Sample Excel file created at {output_file}")