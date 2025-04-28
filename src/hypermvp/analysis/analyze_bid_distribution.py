import duckdb
import pandas as pd
import matplotlib
matplotlib.use('Agg')  # Use a non-interactive backend
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta
import numpy as np
from hypermvp.global_config import ENERGY_DB_PATH

def analyze_bid_distribution(date, db_path=ENERGY_DB_PATH):
    """
    Analyze the distribution of bids across 15-minute intervals for a specific date.
    
    Args:
        date: Date string in YYYY-MM-DD format
        db_path: Path to DuckDB database
    """
    conn = duckdb.connect(db_path)
    
    try:
        # Check if we have data for this date
        date_count = conn.execute(f"""
        SELECT COUNT(*) FROM provider_data
        WHERE DELIVERY_DATE = '{date}'
        """).fetchone()[0]
        
        if date_count == 0:
            print(f"No provider data found for {date}")
            return
        
        # Count bids for each 15-minute product
        product_counts = conn.execute(f"""
        SELECT 
            PRODUCT,
            COUNT(*) as bid_count,
            CAST(SUBSTRING(PRODUCT, 5, 3) AS INTEGER) as interval_number
        FROM provider_data
        WHERE DELIVERY_DATE = '{date}'
          AND PRODUCT LIKE 'NEG_%'
        GROUP BY PRODUCT
        ORDER BY interval_number
        """).fetchdf()
        
        if len(product_counts) == 0:
            print(f"No NEG products found for {date}")
            return
            
        # Add time column for easier reading
        def interval_to_time(interval):
            # Convert interval number (1-96) to time
            hour = (interval - 1) // 4
            minute = ((interval - 1) % 4) * 15
            return f"{hour:02d}:{minute:02d}"
            
        product_counts['time'] = product_counts['interval_number'].apply(interval_to_time)
        
        # Convert interval_number to 4-hour block index (0-5)
        product_counts['block_index'] = (product_counts['interval_number'] - 1) // 16
        product_counts['block'] = product_counts['block_index'].apply(
            lambda x: f"{x*4:02d}:00-{(x+1)*4:02d}:00"
        )
        
        # Print total numbers
        print(f"Analysis for {date}:")
        print(f"Total provider bids: {date_count}")
        print(f"Total NEG products: {len(product_counts)}")
        
        # Analyze variation within 4-hour blocks
        block_stats = product_counts.groupby('block').agg({
            'bid_count': ['min', 'max', 'mean', 'std', 'count']
        })
        
        print("\n4-hour block statistics:")
        print(block_stats)
        
        # Find inconsistencies in bid counts within blocks
        print("\nInconsistencies within 4-hour blocks:")
        for block in product_counts['block'].unique():
            block_data = product_counts[product_counts['block'] == block]
            min_count = block_data['bid_count'].min()
            max_count = block_data['bid_count'].max()
            
            if min_count != max_count:
                print(f"  Block {block}: Varies from {min_count} to {max_count} bids")
                varying_products = block_data[block_data['bid_count'] != min_count]
                print(f"    Products with non-standard bid counts:")
                for _, row in varying_products.iterrows():
                    print(f"      {row['PRODUCT']} ({row['time']}): {row['bid_count']} bids")
        
        # Plot the distribution
        plt.figure(figsize=(15, 8))
        sns.barplot(x='interval_number', y='bid_count', hue='block', data=product_counts)
        plt.title(f"Bid Distribution Across 15-minute Intervals ({date})")
        plt.xlabel("15-minute Interval")
        plt.ylabel("Number of Bids")
        plt.xticks(np.arange(0, 96, 4), [product_counts['time'].iloc[i] if i < len(product_counts) else "" for i in np.arange(0, 96, 4)])
        plt.tight_layout()
        plt.savefig(f"/home/philly/hypermvp/data/03_output/bid_distribution_{date}.png")
        plt.close()
        
        print(f"\nPlot saved to /home/philly/hypermvp/data/03_output/bid_distribution_{date}.png")
        
        # Return the dataframe for further analysis
        return product_counts
        
    finally:
        conn.close()

def analyze_date_range(start_date, end_date=None):
    """Analyze bid distribution over a range of dates"""
    if end_date is None:
        end_date = start_date
    
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    
    all_results = []
    current = start
    
    while current <= end:
        date_str = current.strftime("%Y-%m-%d")
        print(f"\n{'='*50}\nAnalyzing {date_str}\n{'='*50}")
        result = analyze_bid_distribution(date_str)
        if result is not None:
            result['date'] = date_str
            all_results.append(result)
        current += timedelta(days=1)
    
    if all_results:
        return pd.concat(all_results)
    return None

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python -m src.hypermvp.analysis.analyze_bid_distribution YYYY-MM-DD [END_DATE]")
        sys.exit(1)
    
    start_date = sys.argv[1]
    end_date = sys.argv[2] if len(sys.argv) > 2 else None
    
    analyze_date_range(start_date, end_date)