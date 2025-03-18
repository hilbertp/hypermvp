import argparse
import logging
from datetime import datetime, timedelta
import pandas as pd
from hypermvp.config import ENERGY_DB_PATH
from hypermvp.analysis.marginal_price import calculate_and_save_for_date_range

def main():
    parser = argparse.ArgumentParser(description="Calculate marginal prices for energy markets")
    parser.add_argument("--start", type=str, required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, help="End date (YYYY-MM-DD), defaults to start date")
    parser.add_argument("--db-path", type=str, default=ENERGY_DB_PATH, help="DuckDB database path")
    
    args = parser.parse_args()
    
    start_date = args.start
    end_date = args.end if args.end else start_date
    
    print(f"Calculating marginal prices from {start_date} to {end_date}")
    
    rows_saved = calculate_and_save_for_date_range(start_date, end_date, args.db_path)
    
    print(f"Processed {rows_saved} intervals")
    
    # If data was saved, show a summary
    if rows_saved > 0:
        import duckdb
        conn = duckdb.connect(args.db_path)
        
        summary = conn.execute(f"""
        SELECT 
            date,
            COUNT(*) as intervals,
            MIN(marginal_price) as min_price,
            AVG(marginal_price) as avg_price,
            MAX(marginal_price) as max_price,
            SUM(activated_volume_mw) as total_volume
        FROM marginal_prices
        WHERE date BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY date
        ORDER BY date
        """).fetchdf()
        
        conn.close()
        
        print("\n=== Marginal Price Summary ===")
        pd.set_option('display.float_format', '{:.2f}'.format)
        print(summary)

if __name__ == "__main__":
    main()