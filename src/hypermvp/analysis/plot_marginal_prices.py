import duckdb
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from hypermvp.global_config import ENERGY_DB_PATH, OUTPUT_DATA_DIR
import os

def plot_marginal_prices(date="2024-09-01"):
    """Create visualizations of marginal price data with proper handling of missing values"""
    conn = duckdb.connect(ENERGY_DB_PATH)
    
    try:
        # Get marginal prices for the date
        query = f"""
        SELECT 
            timestamp,
            quarter_hour_start,
            activated_volume_mw,
            marginal_price
        FROM marginal_prices
        WHERE date = '{date}'
        ORDER BY timestamp
        """
        
        results = conn.execute(query).fetchdf()
        
        if len(results) == 0:
            print(f"No marginal prices found for {date}")
            return
        
        # Create a complete set of quarter-hour intervals for the entire day
        all_quarter_hours = []
        for hour in range(24):
            for minute in [0, 15, 30, 45]:
                quarter_hour = f"{hour:02d}:{minute:02d}"
                all_quarter_hours.append({'quarter_hour_start': quarter_hour})
        
        # Create a dataframe with all possible quarter-hours
        full_day = pd.DataFrame(all_quarter_hours)
        
        # Calculate hour values for plotting
        full_day['hour'] = pd.to_datetime(full_day['quarter_hour_start'], format='%H:%M').dt.hour + \
                          pd.to_datetime(full_day['quarter_hour_start'], format='%H:%M').dt.minute / 60
        
        # Merge with our actual results to show gaps for missing values
        plot_data = pd.merge(full_day, results, on='quarter_hour_start', how='left')
        
        # Create a figure showing the correct picture with gaps
        plt.figure(figsize=(15, 8))
        
        # Plot line segments only between valid points (no connection over NaN values)
        ax = plt.gca()
        sns.scatterplot(x='hour', y='marginal_price', data=plot_data, color='blue', s=50, ax=ax, label='Marginal Price')
        
        # Sort by hour to ensure segments are drawn correctly
        valid_data = plot_data.dropna(subset=['marginal_price']).sort_values('hour')
        
        # Only plot line segments where we have consecutive valid points
        prev_hour = None
        for _, row in valid_data.iterrows():
            if prev_hour is not None and row['hour'] - prev_hour <= 0.25:  # Only connect if consecutive (within 15 min)
                plt.plot([prev_hour, row['hour']], [prev_price, row['marginal_price']], 'b-')
            prev_hour = row['hour']
            prev_price = row['marginal_price']
        
        plt.title(f'Marginal Prices on {date} (Gaps = No Activation)', fontsize=14)
        plt.xlabel('Hour of Day', fontsize=12)
        plt.ylabel('Marginal Price (EUR/MWh)', fontsize=12)
        plt.grid(True, linestyle='--', alpha=0.7)
        plt.xticks(range(0, 24, 2))
        
        # Add annotation explaining gaps
        plt.figtext(0.5, 0.01, "Note: Gaps indicate periods with zero aFRR activation (no marginal price applies)",
                   ha="center", fontsize=10, bbox={"facecolor":"orange", "alpha":0.2, "pad":5})
        
        # Ensure output directory exists
        os.makedirs(OUTPUT_DATA_DIR, exist_ok=True)
        
        # Save the plot to a file
        output_path = os.path.join(OUTPUT_DATA_DIR, f'marginal_prices_{date}.png')
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        print(f"Plot saved to {output_path}")
        
        # Attempt to display the plot (will work if in interactive environment)
        plt.show()
        
    finally:
        conn.close()

if __name__ == "__main__":
    import sys
    
    date = sys.argv[1] if len(sys.argv) > 1 else "2024-09-01"
    plot_marginal_prices(date)