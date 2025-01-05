import pandas as pd

# Sample DataFrame replicating the issue
data = {
    "DELIVERY_DATE": ["9/1/2024", "9/2/2024", "9/1/2024", "9/3/2024"]
}

# Create DataFrame
df = pd.DataFrame(data)

# Print initial data and types
print("Before Conversion:")
for index, date in enumerate(df["DELIVERY_DATE"]):
    print(f"Row {index}: {date}, Type: {type(date)}")
print(df.dtypes)

# Strip any extra spaces from DELIVERY_DATE
df["DELIVERY_DATE"] = df["DELIVERY_DATE"].str.strip()

# Convert DELIVERY_DATE to datetime
converted_dates = pd.to_datetime(df["DELIVERY_DATE"], format="%m/%d/%Y", errors="coerce")

# Assign the converted dates back to the DataFrame
df["DELIVERY_DATE"] = converted_dates

# Print data and types after conversion
print("\nAfter Conversion:")
for index, date in enumerate(df["DELIVERY_DATE"]):
    print(f"Row {index}: {date}, Type: {type(date)}")
print(df.dtypes)
