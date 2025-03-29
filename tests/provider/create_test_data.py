"""
Utility to create test data for provider tests.
"""
import os
import pandas as pd
from datetime import datetime, timedelta

def create_provider_test_data(output_path, rows=100, days=5):
    """
    Create a test provider Excel file with sample data.
    
    Args:
        output_path: Path to save the Excel file
        rows: Number of rows to generate
        days: Number of days to spread the data across
    """
    # Create base date
    base_date = datetime(2024, 9, 1)
    
    # Lists to collect data
    dates = []
    products = []
    prices = []
    directions = []
    capacities = []
    notes = []
    
    # Generate data
    for i in range(rows):
        # Generate date (spread across the specified number of days)
        day_offset = i % days
        date = base_date + timedelta(days=day_offset)
        dates.append(date.strftime('%Y-%m-%d'))
        
        # Generate product (80% NEG, 20% POS)
        is_pos = (i % 5 == 0)
        product_prefix = "POS" if is_pos else "NEG"
        product_num = (i % 10) + 1
        products.append(f"{product_prefix}_{product_num:03d}")
        
        # Generate price (between 100 and 200 EUR/MWh, with comma as decimal separator)
        base_price = 100 + (i % 101)
        prices.append(f"{base_price:.2f}".replace('.', ','))
        
        # Generate direction (alternate between the two options)
        if is_pos or (i % 3 == 0):
            directions.append("PROVIDER_TO_GRID")
        else:
            directions.append("GRID_TO_PROVIDER")
        
        # Generate capacity (between 10 and 100 MW)
        capacity = 10 + (i % 91)
        capacities.append(str(capacity))
        
        # Generate notes (mostly empty, occasional note)
        if i % 10 == 0:
            notes.append(f"Test note for row {i+1}")
        else:
            notes.append("")
    
    # Create DataFrame
    df = pd.DataFrame({
        "DELIVERY_DATE": dates,
        "PRODUCT": products,
        "ENERGY_PRICE_[EUR/MWh]": prices,
        "ENERGY_PRICE_PAYMENT_DIRECTION": directions,
        "ALLOCATED_CAPACITY_[MW]": capacities,
        "NOTE": notes
    })
    
    # Save to Excel
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_excel(output_path, index=False)
    
    print(f"Created test file at {output_path} with {len(df)} rows")
    return df

# Add this function to create test data directly in DuckDB:

def create_test_data_in_db(db_path, rows=100, days=5, table_name="raw_provider_data"):
    """
    Create test provider data directly in a DuckDB database.
    This bypasses the need for Excel files and extensions.
    
    Args:
        db_path: Path to the DuckDB database
        rows: Number of rows to generate
        days: Number of days to spread the data across
        table_name: Name of the raw data table
    """
    import duckdb
    from datetime import datetime, timedelta
    
    # Connect to DuckDB
    con = duckdb.connect(db_path)
    
    # Create the raw table if it doesn't exist
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            DELIVERY_DATE VARCHAR,
            PRODUCT VARCHAR,
            "ENERGY_PRICE_[EUR/MWh]" VARCHAR,
            ENERGY_PRICE_PAYMENT_DIRECTION VARCHAR,
            "ALLOCATED_CAPACITY_[MW]" VARCHAR,
            NOTE VARCHAR,
            source_file VARCHAR,
            load_timestamp TIMESTAMP
        )
    """)
    
    # Create base date
    base_date = datetime(2024, 9, 1)
    
    # Generate and insert data in batches
    batch_size = 50
    for batch_start in range(0, rows, batch_size):
        batch_end = min(batch_start + batch_size, rows)
        batch_rows = []
        
        for i in range(batch_start, batch_end):
            # Generate date (spread across the specified number of days)
            day_offset = i % days
            date = base_date + timedelta(days=day_offset)
            date_str = date.strftime('%Y-%m-%d')
            
            # Generate product (80% NEG, 20% POS)
            is_pos = (i % 5 == 0)
            product_prefix = "POS" if is_pos else "NEG"
            product_num = (i % 10) + 1
            product = f"{product_prefix}_{product_num:03d}"
            
            # Generate price (between 100 and 200 EUR/MWh, with comma as decimal separator)
            base_price = 100 + (i % 101)
            price = f"{base_price:.2f}".replace('.', ',')
            
            # Generate direction (alternate between the two options)
            if is_pos or (i % 3 == 0):
                direction = "PROVIDER_TO_GRID"
            else:
                direction = "GRID_TO_PROVIDER"
            
            # Generate capacity (between 10 and 100 MW)
            capacity = str(10 + (i % 91))
            
            # Generate notes (mostly empty, occasional note)
            note = f"Test note for row {i+1}" if i % 10 == 0 else ""
            
            # Create row tuple
            row = (date_str, product, price, direction, capacity, note, f"test_file_{i//50}.xlsx")
            batch_rows.append(row)
        
        # Insert batch
        con.executemany(f"""
            INSERT INTO {table_name} (
                DELIVERY_DATE, PRODUCT, "ENERGY_PRICE_[EUR/MWh]", 
                ENERGY_PRICE_PAYMENT_DIRECTION, "ALLOCATED_CAPACITY_[MW]", 
                NOTE, source_file
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """, batch_rows)
        
        # Update timestamps
        con.execute(f"""
            UPDATE {table_name} 
            SET load_timestamp = CURRENT_TIMESTAMP
            WHERE load_timestamp IS NULL
        """)
    
    # Get row count
    row_count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    
    # Close connection
    con.close()
    
    print(f"Created {row_count} test rows in {table_name}")
    return row_count

if __name__ == "__main__":
    # Example usage
    test_dir = os.path.join(os.path.dirname(__file__), "..", "tests_data", "raw")
    test_file = os.path.join(test_dir, "test_provider_list.xlsx")
    create_provider_test_data(test_file, rows=100, days=5)