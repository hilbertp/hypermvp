import pandas as pd
import duckdb
import logging
from datetime import datetime, timedelta, date
# Add standardized date format imports
from hypermvp.global_config import ENERGY_DB_PATH, ISO_DATETIME_FORMAT, ISO_DATE_FORMAT, TIME_FORMAT, AFRR_DATE_FORMAT

def calculate_marginal_prices(start_date=None, end_date=None):
    """
    Calculate marginal prices for the given date range.
    
    Args:
        start_date (str or datetime): Start date in YYYY-MM-DD format. If None, use the earliest date in the DB.
        end_date (str or datetime): End date in YYYY-MM-DD format. If None, use today's date.
    
    Returns:
        pd.DataFrame: DataFrame with marginal prices for each 15-minute interval.
    """
    import duckdb
    import pandas as pd
    from datetime import datetime, timedelta
    import logging
    
    # Connect to DB
    from hypermvp.global_config import ENERGY_DB_PATH
    con = duckdb.connect(ENERGY_DB_PATH)
    
    # Convert string dates to datetime objects if needed
    if isinstance(start_date, str):
        start_date = datetime.strptime(start_date, ISO_DATE_FORMAT).date()
    if isinstance(end_date, str) and end_date:
        end_date = datetime.strptime(end_date, ISO_DATE_FORMAT).date()  # Changed to ISO_DATE_FORMAT
    elif not end_date:
        end_date = datetime.now().date()
        
    # Make end_date inclusive of the entire day
    end_date = end_date + timedelta(days=1) - timedelta(microseconds=1)
    
    # Check if AFRR data exists for the date range
    afrr_data_exists = con.execute("""
        SELECT COUNT(*) 
        FROM afrr_data 
        WHERE STRPTIME("Datum", ?) ::DATE BETWEEN ? AND ?
    """, [AFRR_DATE_FORMAT, start_date, end_date]).fetchone()[0]  # Changed to AFRR_DATE_FORMAT
    
    if afrr_data_exists == 0:
        logging.warning(f"No AFRR data found for date range {start_date} to {end_date}")
        return pd.DataFrame()
    
    # Get all unique days in the AFRR data
    days = con.execute("""
        SELECT DISTINCT STRPTIME("Datum", ?)::DATE as date
        FROM afrr_data
        WHERE STRPTIME("Datum", ?)::DATE BETWEEN ? AND ?
        ORDER BY date
    """, [AFRR_DATE_FORMAT, AFRR_DATE_FORMAT, start_date, end_date]).fetchdf()  # Changed to AFRR_DATE_FORMAT
    
    if days.empty:
        logging.warning(f"No days found for date range {start_date} to {end_date}")
        return pd.DataFrame()
    
    # Initialize results DataFrame
    results = []
    
    # Process each day
    for _, row in days.iterrows():
        day = row['date']
        
        # Convert day to German format for AFRR data query
        german_date_str = day.strftime(AFRR_DATE_FORMAT)  # Changed to AFRR_DATE_FORMAT
        
        # Get all 15-minute intervals for this day
        intervals = con.execute("""
            SELECT 
                "Datum",
                "von" as quarter_hour_start,
                "bis" as quarter_hour_end,
                CAST(REPLACE("50Hertz (Negativ)", ',', '.') AS DOUBLE) as activated_volume_mw
            FROM afrr_data
            WHERE "Datum" = ?
            ORDER BY "von"
        """, [german_date_str]).fetchdf()
        
        if intervals.empty:
            logging.warning(f"No intervals found for day {day}")
            continue
        
        # For each interval, calculate the marginal price
        for _, interval in intervals.iterrows():
            # Get start and end time
            start_time = interval['quarter_hour_start']
            end_time = interval['quarter_hour_end']
            activated_volume = interval['activated_volume_mw']
            
            # Calculate product code (e.g., NEG_001 for 00:00-00:15)
            # Maps time to sequential number (00:00-00:15 -> 1, 00:15-00:30 -> 2, etc.)
            hour, minute = map(int, start_time.split(':'))
            sequential_number = hour * 4 + (minute // 15) + 1
            product_code = f"NEG_{sequential_number:03d}"
            
            # Skip if activated volume is 0 or very small
            if abs(activated_volume) < 0.001:
                # Still add a row, but with null marginal price
                results.append({
                    'date': day,
                    'timestamp': datetime.combine(day, datetime.strptime(start_time, TIME_FORMAT).time()),  # Changed to TIME_FORMAT
                    'quarter_hour_start': start_time,
                    'quarter_hour_end': end_time,
                    'activated_volume_mw': activated_volume,
                    'available_capacity_mw': 0,
                    'marginal_price': None,
                    'product_code': product_code
                })
                continue
            
            # Replace the offers query and handling section (around line 108) with this more robust approach:

            # First, try exact product code match
            offers = con.execute("""
                SELECT 
                    ENERGY_PRICE__EUR_MWh_ as energy_price,
                    OFFERED_CAPACITY__MW_ as capacity
                FROM provider_data
                WHERE DELIVERY_DATE::DATE = ?
                  AND PRODUCT = ?
                ORDER BY ENERGY_PRICE__EUR_MWh_ ASC
            """, [day, product_code]).fetchdf()

            # If no results, try a more flexible match
            if offers.empty:
                # Try alternative naming patterns common in energy markets
                alternative_product_codes = [
                    product_code,                    # NEG_001
                    product_code.replace('_', '-'),  # NEG-001
                    product_code.replace('_', ''),   # NEG001
                    product_code.lower(),            # neg_001
                    f"NEG_{sequential_number}",      # NEG_1 (without leading zeros)
                    f"NEG{sequential_number:03d}"    # NEG001 (without underscore)
                ]
                
                # Create a query with multiple OR conditions for product codes
                placeholder_list = ', '.join(['?'] * len(alternative_product_codes))
                query = f"""
                    SELECT 
                        ENERGY_PRICE__EUR_MWh_ as energy_price,
                        OFFERED_CAPACITY__MW_ as capacity
                    FROM provider_data
                    WHERE DELIVERY_DATE::DATE = ?
                      AND PRODUCT IN ({placeholder_list})
                    ORDER BY ENERGY_PRICE__EUR_MWh_ ASC
                """
                
                # Parameters: first the date, then all the alternative product codes
                params = [day] + alternative_product_codes
                
                # Try the flexible match query
                offers = con.execute(query, params).fetchdf()
                
                # If still no results, try an even more flexible approach using wildcard matching
                if offers.empty:
                    # Extract the numeric part (e.g., '001' from 'NEG_001')
                    product_num = product_code.split('_')[1] if '_' in product_code else product_code[3:]
                    
                    offers = con.execute("""
                        SELECT 
                            ENERGY_PRICE__EUR_MWh_ as energy_price,
                            OFFERED_CAPACITY__MW_ as capacity
                        FROM provider_data
                        WHERE DELIVERY_DATE::DATE = ?
                          AND (
                              PRODUCT LIKE 'NEG%' || ? 
                              OR PRODUCT LIKE 'neg%' || ?
                          )
                        ORDER BY ENERGY_PRICE__EUR_MWh_ ASC
                    """, [day, product_num, product_num]).fetchdf()
                    
                    if not offers.empty:
                        found_product = con.execute("""
                            SELECT DISTINCT PRODUCT
                            FROM provider_data
                            WHERE DELIVERY_DATE::DATE = ?
                              AND (
                                  PRODUCT LIKE 'NEG%' || ? 
                                  OR PRODUCT LIKE 'neg%' || ?
                              )
                            LIMIT 1
                        """, [day, product_num, product_num]).fetchone()[0]
                        
                        logging.info(f"Found alternative product format: {found_product} for requested {product_code}")
                
                # If still no results, try looking at all products for this day to see patterns
                if offers.empty:
                    # Add diagnostic query to check product existence
                    all_products = con.execute("""
                        SELECT DISTINCT PRODUCT 
                        FROM provider_data
                        WHERE DELIVERY_DATE::DATE = ?
                        ORDER BY PRODUCT
                    """, [day]).fetchdf()
                    
                    if not all_products.empty:
                        neg_products = all_products[all_products['PRODUCT'].str.contains('NEG|neg', regex=True)]
                        if not neg_products.empty:
                            # Log the first few NEG products to help identify pattern
                            sample_products = neg_products['PRODUCT'].head(min(5, len(neg_products))).tolist()
                            logging.warning(f"No provider offers found for day {day}, product {product_code}")
                            logging.info(f"Available NEG products for this day: {', '.join(sample_products)}")
                        else:
                            logging.warning(f"No NEG products found for day {day}")
                    else:
                        logging.warning(f"No provider data found for day {day}")
                    
                    # Add a row with null marginal price
                    results.append({
                        'date': day,
                        'timestamp': datetime.combine(day, datetime.strptime(start_time, TIME_FORMAT).time()),  # Changed to TIME_FORMAT
                        'quarter_hour_start': start_time,
                        'quarter_hour_end': end_time,
                        'activated_volume_mw': activated_volume,
                        'available_capacity_mw': 0,
                        'marginal_price': None,
                        'product_code': product_code
                    })
                    continue
            
            # Calculate available capacity
            available_capacity = offers['capacity'].sum()
            
            # If activated volume exceeds available capacity, log warning
            if activated_volume > available_capacity:
                logging.warning(f"Activated volume ({activated_volume} MW) exceeds available capacity ({available_capacity} MW) for {day}, {product_code}")
                
            # Calculate the marginal price using the merit order
            cumulative_capacity = 0
            marginal_price = None
            
            for _, offer in offers.iterrows():
                cumulative_capacity += offer['capacity']
                if cumulative_capacity >= activated_volume:
                    marginal_price = offer['energy_price']
                    break
            
            # Add the results
            results.append({
                'date': day,
                'timestamp': datetime.combine(day, datetime.strptime(start_time, TIME_FORMAT).time()),  # Changed to TIME_FORMAT
                'quarter_hour_start': start_time,
                'quarter_hour_end': end_time,
                'activated_volume_mw': activated_volume,
                'available_capacity_mw': available_capacity,
                'marginal_price': marginal_price,
                'product_code': product_code
            })
    
    # Close the connection
    con.close()
    
    # Convert results to DataFrame
    results_df = pd.DataFrame(results)
    
    if not results_df.empty:
        # Count non-null prices
        non_null_prices = results_df['marginal_price'].dropna()
        
        # Log some statistics
        logging.info(f"Calculated {len(results_df)} marginal prices")
        logging.info(f"Prices available for {len(non_null_prices)} intervals ({len(non_null_prices)/len(results_df)*100:.1f}%)")
        
        if len(non_null_prices) > 0:
            logging.info(f"Average marginal price: {non_null_prices.mean():.2f} EUR/MWh")
            logging.info(f"Min/Max marginal price: {non_null_prices.min():.2f}/{non_null_prices.max():.2f} EUR/MWh")
        else:
            logging.warning("No non-null marginal prices calculated")
    else:
        logging.warning("No marginal prices calculated")
    
    return results_df

def save_marginal_prices(results_df):
    """Save marginal prices to the database."""
    if results_df.empty:
        logging.warning("No results to save")
        return
    
    import duckdb
    from hypermvp.global_config import ENERGY_DB_PATH
    from hypermvp.utils.db_versioning import add_version_metadata
    
    con = duckdb.connect(ENERGY_DB_PATH)
    
    try:
        # Create table if it doesn't exist
        con.execute("""
            CREATE TABLE IF NOT EXISTS marginal_prices (
                date DATE,
                timestamp TIMESTAMP,
                quarter_hour_start VARCHAR,
                quarter_hour_end VARCHAR,
                activated_volume_mw DOUBLE,
                available_capacity_mw DOUBLE,
                marginal_price DOUBLE,
                product_code VARCHAR
            )
        """)
        
        # Delete existing rows for these dates to avoid duplicates
        min_date = results_df['date'].min()
        max_date = results_df['date'].max()
        
        con.execute("""
            DELETE FROM marginal_prices
            WHERE date BETWEEN ? AND ?
        """, [min_date, max_date])
        
        # Insert the new results
        for _, row in results_df.iterrows():
            con.execute("""
                INSERT INTO marginal_prices VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                row['date'],
                row['timestamp'],
                row['quarter_hour_start'],
                row['quarter_hour_end'],
                row['activated_volume_mw'],
                row['available_capacity_mw'],
                row['marginal_price'],
                row['product_code']
            ])
        
        # Add version metadata
        add_version_metadata(con, f"Calculated {len(results_df)} marginal prices for {min_date} to {max_date}", "ANALYSIS")
        
        logging.info(f"Saved {len(results_df)} marginal prices to database")
        
    except Exception as e:
        logging.error(f"Error saving marginal prices: {e}")
        raise
    finally:
        con.close()

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

# Add this function to marginal_price.py
def diagnose_provider_data():
    """Diagnose provider data for debugging purposes."""
    import duckdb
    from hypermvp.global_config import ENERGY_DB_PATH
    import pandas as pd
    
    con = duckdb.connect(ENERGY_DB_PATH)
    
    # Check provider data date range
    date_range = con.execute("""
        SELECT 
            MIN(DELIVERY_DATE) as min_date,
            MAX(DELIVERY_DATE) as max_date,
            COUNT(DISTINCT DELIVERY_DATE) as num_days,
            COUNT(*) as total_records
        FROM provider_data
    """).fetchdf()
    
    print("=== PROVIDER DATA OVERVIEW ===")
    print(f"Date range: {date_range['min_date'].iloc[0]} to {date_range['max_date'].iloc[0]}")
    print(f"Days with data: {date_range['num_days'].iloc[0]}")
    print(f"Total records: {date_range['total_records'].iloc[0]:,}")
    
    # Check product distribution
    products = con.execute("""
        SELECT 
            PRODUCT,
            COUNT(*) as count
        FROM provider_data
        GROUP BY PRODUCT
        ORDER BY PRODUCT
    """).fetchdf()
    
    print("\n=== PRODUCT DISTRIBUTION ===")
    for _, row in products.iterrows():
        print(f"{row['PRODUCT']}: {row['count']:,} records")
    
    # Check distribution of product by day
    product_by_day = con.execute("""
        SELECT 
            DELIVERY_DATE::DATE as date,
            COUNT(DISTINCT PRODUCT) as product_count,
            GROUP_CONCAT(DISTINCT PRODUCT) as products
        FROM provider_data
        WHERE DELIVERY_DATE::DATE BETWEEN '2024-09-01' AND '2024-09-30'
        GROUP BY date
        ORDER BY date
    """).fetchdf()
    
    print("\n=== PRODUCTS BY DAY (First 5 days) ===")
    for _, row in product_by_day.head(5).iterrows():
        print(f"{row['date']}: {row['product_count']} unique products")
        
    # Check for specific NEG products on Sep 1
    neg_products = con.execute("""
        SELECT 
            PRODUCT,
            COUNT(*) as count,
            AVG(ENERGY_PRICE__EUR_MWh_) as avg_price
        FROM provider_data
        WHERE DELIVERY_DATE::DATE = '2024-09-01'
        AND PRODUCT LIKE 'NEG%'
        GROUP BY PRODUCT
        ORDER BY PRODUCT
    """).fetchdf()
    
    print("\n=== NEG PRODUCTS ON 2024-09-01 ===")
    if len(neg_products) > 0:
        for _, row in neg_products.iterrows():
            print(f"{row['PRODUCT']}: {row['count']} offers, avg price: {row['avg_price']:.2f}")
    else:
        print("No NEG products found for 2024-09-01")
    
    # Check TYPE_OF_RESERVES distribution
    reserve_types = con.execute("""
        SELECT 
            TYPE_OF_RESERVES,
            COUNT(*) as count
        FROM provider_data
        GROUP BY TYPE_OF_RESERVES
        ORDER BY TYPE_OF_RESERVES
    """).fetchdf()
    
    print("\n=== RESERVE TYPES ===")
    for _, row in reserve_types.iterrows():
        print(f"{row['TYPE_OF_RESERVES']}: {row['count']:,} records")
    
    con.close()

# Add this function to check for specific products that are missing
def check_missing_products():
    """Check specifically for days and products with missing offers."""
    import duckdb
    from hypermvp.global_config import ENERGY_DB_PATH
    import pandas as pd
    
    con = duckdb.connect(ENERGY_DB_PATH)
    
    # Check last day of September specifically
    print("=== CHECKING SEPTEMBER 30, 2024 ===")
    day_products = con.execute("""
        SELECT 
            PRODUCT,
            COUNT(*) as offer_count
        FROM provider_data
        WHERE DELIVERY_DATE::DATE = '2024-09-30'
        GROUP BY PRODUCT
        ORDER BY PRODUCT
    """).fetchdf()
    
    # Find which NEG products exist for Sept 30
    neg_products = day_products[day_products['PRODUCT'].str.contains('NEG', case=False)]
    
    print(f"Found {len(neg_products)} NEG products for 2024-09-30")
    
    # Check which ones are missing
    all_neg_products = [f"NEG_{i:03d}" for i in range(1, 97)]
    missing_products = set(all_neg_products) - set(neg_products['PRODUCT'].tolist())
    
    print(f"Missing products: {', '.join(sorted(missing_products))}")
    
    # Check exact format of product codes
    sample_products = con.execute("""
        SELECT DISTINCT PRODUCT
        FROM provider_data
        WHERE PRODUCT LIKE 'NEG%' OR PRODUCT LIKE 'neg%'
        LIMIT 10
    """).fetchdf()
    
    print("\n=== SAMPLE PRODUCT FORMATS ===")
    for prod in sample_products['PRODUCT'].tolist():
        print(prod)
    
    # Check if there's any pattern to the missing products
    if missing_products:
        missing_numbers = [int(p.split('_')[1]) for p in missing_products]
        missing_hours = [(n-1) // 4 for n in missing_numbers]
        missing_quarters = [(n-1) % 4 for n in missing_numbers]
        
        print("\n=== MISSING PRODUCTS PATTERN ===")
        print(f"Missing hours: {sorted(set(missing_hours))}")
        print(f"Missing quarters: {sorted(set(missing_quarters))}")
        
        # Check all dates for these specific product codes
        print("\n=== CHECKING IF PRODUCTS ARE MISSING ACROSS ALL DATES ===")
        for missing_prod in list(missing_products)[:5]:  # Check first 5 only
            count_by_day = con.execute("""
                SELECT 
                    DELIVERY_DATE::DATE as date,
                    COUNT(*) as count
                FROM provider_data
                WHERE PRODUCT = ?
                GROUP BY date
                ORDER BY date
            """, [missing_prod]).fetchdf()
            
            dates_with_product = len(count_by_day)
            print(f"{missing_prod}: found in {dates_with_product} days")
    
    con.close()

# Add this to your if __name__ == "__main__" block
if __name__ == "__main__":
    import sys
    import logging
    
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    
    if len(sys.argv) > 1 and sys.argv[1] == 'diagnose':
        diagnose_provider_data()
        sys.exit(0)
    
    if len(sys.argv) > 1 and sys.argv[1] == 'check-missing':
        check_missing_products()
        sys.exit(0)
    
    start_date = sys.argv[1] if len(sys.argv) > 1 else "2024-09-01"
    end_date = sys.argv[2] if len(sys.argv) > 2 else None
    
    results = calculate_marginal_prices(start_date, end_date)
    save_marginal_prices(results)