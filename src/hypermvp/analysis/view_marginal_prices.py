import duckdb
import pandas as pd
from hypermvp.config import ENERGY_DB_PATH

def view_marginal_prices(date=None):
    """View calculated marginal prices with optional date filter"""
    conn = duckdb.connect(ENERGY_DB_PATH)
    
    try:
        query = """
        SELECT 
            date,
            quarter_hour_start,
            quarter_hour_end,
            activated_volume_mw,
            available_capacity_mw,
            marginal_price,
            product_code
        FROM marginal_prices
        """
        
        if date:
            query += f" WHERE date = '{date}'"
            
        query += " ORDER BY date, quarter_hour_start"
        
        results = conn.execute(query).fetchdf()
        
        if len(results) == 0:
            print("No marginal prices found.")
            return None
            
        print(f"Found {len(results)} marginal price records")
        
        # Calculate some statistics
        print("\nPrice statistics:")
        print(f"Average: {results['marginal_price'].mean():.2f} EUR/MWh")
        print(f"Min: {results['marginal_price'].min():.2f} EUR/MWh")
        print(f"Max: {results['marginal_price'].max():.2f} EUR/MWh")
        
        return results
        
    finally:
        conn.close()

if __name__ == "__main__":
    import sys
    
    date = sys.argv[1] if len(sys.argv) > 1 else "2024-09-01"
    results = view_marginal_prices(date)
    
    if results is not None:
        # Display first 10 records
        print("\nFirst 10 records:")
        print(results.head(10))
        
        # Save to CSV
        output_path = f"/home/philly/hypermvp/data/03_output/marginal_prices_{date}.csv"
        results.to_csv(output_path, index=False)
        print(f"\nFull results saved to {output_path}")