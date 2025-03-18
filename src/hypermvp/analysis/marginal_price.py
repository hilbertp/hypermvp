import pandas as pd
import duckdb
import logging
from datetime import datetime, timedelta, date
from hypermvp.config import ENERGY_DB_PATH

def calculate_marginal_prices(start_date, end_date=None, db_path=ENERGY_DB_PATH):
    """
    Calculate marginal prices for a date range.
    
    Args:
        start_date: Start date (datetime.date or string YYYY-MM-DD)
        end_date: End date, inclusive (datetime.date or string YYYY-MM-DD). If None, use start_date.
        db_path: Path to DuckDB database
        
    Returns:
        DataFrame with columns: timestamp, quarter_hour_start, quarter_hour_end, activated_volume_mw, marginal_price
    """
    # Handle date parameters
    if isinstance(start_date, str):
        start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
    
    if end_date is None:
        end_date = start_date
    elif isinstance(end_date, str):
        end_date = datetime.strptime(end_date, "%Y-%m-%d").date()
    
    logging.info(f"Calculating marginal prices from {start_date} to {end_date}")
    
    # Connect to database
    conn = duckdb.connect(db_path)
    results = []
    
    try:
        # Check if required tables exist
        tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        table_names = [t[0] for t in tables]
        
        if "provider_data" not in table_names or "afrr_data" not in table_names:
            logging.error("Required tables not found in database")
            return pd.DataFrame()
        
        # Format dates to match German format in the database
        start_date_formatted = start_date.strftime("%d.%m.%Y") if isinstance(start_date, date) else datetime.strptime(start_date, "%Y-%m-%d").strftime("%d.%m.%Y")
        end_date_formatted = end_date.strftime("%d.%m.%Y") if isinstance(end_date, date) else datetime.strptime(end_date, "%Y-%m-%d").strftime("%d.%m.%Y")
        
        # Get all 15-minute intervals in date range from aFRR data - using string comparison
        intervals_query = f"""
        SELECT 
            Datum AS date,
            von AS quarter_hour_start,  -- Previously interval_start
            bis AS quarter_hour_end,    -- Previously interval_end
            CAST(REPLACE("50Hertz (Negativ)", ',', '.') AS DOUBLE) AS total_activated_mw
        FROM afrr_data
        WHERE Datum BETWEEN '{start_date_formatted}' AND '{end_date_formatted}'
        ORDER BY Datum, von
        """
        
        intervals = conn.execute(intervals_query).fetchdf()
        if len(intervals) == 0:
            logging.warning(f"No aFRR data found for date range {start_date} to {end_date}")
            return pd.DataFrame()
            
        logging.info(f"Found {len(intervals)} intervals in date range")
        
        # For each interval, calculate the marginal price
        for idx, row in intervals.iterrows():
            # Get quarter-hour interval date and time
            interval_date = row['date'] if isinstance(row['date'], date) else datetime.strptime(row['date'], "%d.%m.%Y").date()
            interval_time = datetime.strptime(row['quarter_hour_start'], "%H:%M").time()
            quarter_hour_datetime = datetime.combine(interval_date, interval_time)
            
            # Calculate the quarter-hour index
            hour = quarter_hour_datetime.hour
            minute = quarter_hour_datetime.minute
            quarter_hour_index = hour * 4 + (minute // 15) + 1  # 1-based index from 001 to 096
            product_code = f"NEG_{quarter_hour_index:03d}"
            
            # Check for no activation - instead of skipping, record as NIL
            if float(row['total_activated_mw']) <= 0:
                logging.debug(f"Interval {row['date']} {row['quarter_hour_start']} has no activation")
                results.append({
                    'date': interval_date,
                    'timestamp': quarter_hour_datetime,
                    'quarter_hour_start': row['quarter_hour_start'],
                    'quarter_hour_end': row['quarter_hour_end'],
                    'activated_volume_mw': 0.0,
                    'available_capacity_mw': 0.0,
                    'marginal_price': None,  # SQL NULL value for "NIL"
                    'product_code': product_code
                })
                continue
                
            # Query provider offers for this specific 15-minute product code
            offers_query = f"""
            SELECT 
                DELIVERY_DATE,
                PRODUCT,
                TYPE_OF_RESERVES,
                ENERGY_PRICE__EUR_MWh_ AS price,
                ALLOCATED_CAPACITY__MW_ AS capacity
            FROM provider_data
            WHERE DELIVERY_DATE = '{interval_date.strftime("%Y-%m-%d")}'
              AND PRODUCT = '{product_code}'  -- Match the exact 15-minute product code
            ORDER BY price ASC  -- Sort by price ascending (for negative aFRR)
            """
            
            offers = conn.execute(offers_query).fetchdf()
            if len(offers) == 0:
                logging.warning(f"No provider offers for quarter-hour interval {quarter_hour_datetime.strftime('%Y-%m-%d %H:%M')} (product {product_code}) - skipping")
                continue
                
            # Apply merit order principle to find marginal price
            activated_volume = float(row['total_activated_mw'])  # Ensure numeric conversion
            accumulated_capacity = 0
            marginal_price = None
            
            for _, offer in offers.iterrows():
                accumulated_capacity += offer['capacity']
                # Update marginal price with each offer we need
                marginal_price = offer['price']
                
                # Stop when we've reached the required volume
                if accumulated_capacity >= activated_volume:
                    break
            
            # Record the result
            results.append({
                'date': interval_date,
                'timestamp': quarter_hour_datetime,
                'quarter_hour_start': row['quarter_hour_start'],  # Changed from interval_start
                'quarter_hour_end': row['quarter_hour_end'],      # Changed from interval_end
                'activated_volume_mw': activated_volume,
                'available_capacity_mw': accumulated_capacity,
                'marginal_price': marginal_price,
                'product_code': product_code  # Store the 15-minute product code
            })
            
            # Progress indicator for large calculations
            if idx > 0 and idx % 100 == 0:
                logging.info(f"Processed {idx} of {len(intervals)} intervals...")
        
        # Convert results to DataFrame
        if not results:
            logging.warning("No marginal prices could be calculated")
            return pd.DataFrame()
            
        return pd.DataFrame(results)
        
    finally:
        conn.close()

def save_marginal_prices(marginal_prices_df, db_path=ENERGY_DB_PATH):
    """
    Save marginal prices to DuckDB database.
    
    Args:
        marginal_prices_df: DataFrame with marginal price calculations
        db_path: Path to DuckDB database
        
    Returns:
        Number of rows saved
    """
    if len(marginal_prices_df) == 0:
        logging.warning("No marginal prices to save")
        return 0
    
    conn = duckdb.connect(db_path)
    try:
        # Update table schema to use product_code instead of product_block_start
        conn.execute("""
        CREATE TABLE IF NOT EXISTS marginal_prices (
            date DATE,
            timestamp TIMESTAMP,
            quarter_hour_start VARCHAR,
            quarter_hour_end VARCHAR,
            activated_volume_mw DOUBLE,
            available_capacity_mw DOUBLE,
            marginal_price DOUBLE,
            product_code VARCHAR  -- Changed from product_block_start
        )
        """)
        
        # Get range of dates in the dataframe
        min_date = marginal_prices_df['date'].min()
        max_date = marginal_prices_df['date'].max()
        
        # Delete any existing records in this date range
        conn.execute(f"""
        DELETE FROM marginal_prices 
        WHERE date::DATE BETWEEN '{min_date}' AND '{max_date}'
        """)
        
        # Insert new records
        conn.register("temp_df", marginal_prices_df)
        rows_affected = conn.execute("""
        INSERT INTO marginal_prices
        SELECT * FROM temp_df
        """).fetchone()[0]
        
        conn.commit()
        logging.info(f"Saved {rows_affected} marginal prices to database")
        return rows_affected
        
    finally:
        conn.close()

def calculate_and_save_for_date_range(start_date, end_date=None, db_path=ENERGY_DB_PATH):
    """
    Calculate and save marginal prices for a date range in one operation.
    
    Args:
        start_date: Start date (datetime.date or string YYYY-MM-DD)
        end_date: End date, inclusive (defaults to start_date)
        db_path: Database path
        
    Returns:
        Number of records processed
    """
    # Calculate marginal prices
    marginal_prices = calculate_marginal_prices(start_date, end_date, db_path)
    
    # Save results if we have any
    if len(marginal_prices) > 0:
        return save_marginal_prices(marginal_prices, db_path)
    else:
        return 0

# Debug code should be moved to the end, in a if __name__ == "__main__" block
if __name__ == "__main__":
    # Debug code here
    conn = duckdb.connect(ENERGY_DB_PATH)

    # Check if we have provider data for Sept 1
    sept1_count = conn.execute("""
    SELECT COUNT(*) FROM provider_data
    WHERE DELIVERY_DATE = '2024-09-01'
    """).fetchone()[0]

    print(f"Found {sept1_count} provider offers for 2024-09-01")
    
    # Check what dates are in the provider data
    dates = conn.execute("""
    SELECT DISTINCT DELIVERY_DATE FROM provider_data
    ORDER BY DELIVERY_DATE
    """).fetchdf()

    print("Available dates in provider_data:")
    print(dates)

    # Check the period format in your provider data
    period_sample = conn.execute("""
    SELECT DISTINCT period
    FROM provider_data
    WHERE DELIVERY_DATE = '2024-09-01'
    LIMIT 10
    """).fetchdf()

    print("Period column format samples:")
    print(period_sample)

    # Check if we have any NEG products
    neg_products = conn.execute("""
    SELECT COUNT(*), PRODUCT 
    FROM provider_data
    WHERE DELIVERY_DATE = '2024-09-01' AND PRODUCT LIKE 'NEG_%'
    GROUP BY PRODUCT
    """).fetchdf()

    print("\nNEG products available:")
    print(neg_products)

    conn.close()