import streamlit as st
import pandas as pd
import duckdb
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px
from datetime import datetime, timedelta
import os
import sys
import json

# Add the project root to the path so we can import the config
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../')))
from hypermvp.global_config import ENERGY_DB_PATH, ISO_DATETIME_FORMAT, ISO_DATE_FORMAT, TIME_FORMAT

def connect_to_db():
    """Connect to DuckDB database."""
    try:
        # Make sure the database file exists
        if not os.path.exists(ENERGY_DB_PATH):
            st.error(f"Database file not found: {ENERGY_DB_PATH}")
            return None
            
        # Connect to the database
        con = duckdb.connect(ENERGY_DB_PATH)
        
        # Test the connection with a simple query
        test = con.execute("SELECT 1").fetchone()
        if test and test[0] == 1:
            return con
        else:
            st.error("Database connection test failed")
            return None
            
    except Exception as e:
        st.error(f"Error connecting to database: {e}")
        return None

def get_provider_data_summary(con):
    """Get a summary of provider data in the database."""
    # Check if table exists
    table_exists = con.execute("""
        SELECT name FROM sqlite_master 
        WHERE type='table' AND name='provider_data'
    """).fetchone()
    
    if not table_exists:
        return None
    
    # Get date range
    date_range = con.execute("""
        SELECT 
            MIN(DELIVERY_DATE) as min_date,
            MAX(DELIVERY_DATE) as max_date,
            COUNT(DISTINCT DELIVERY_DATE) as num_days,
            COUNT(*) as total_records
        FROM provider_data
    """).fetchdf()
    
    # Get counts by product
    product_counts = con.execute("""
        SELECT 
            PRODUCT,
            COUNT(*) as count
        FROM provider_data
        GROUP BY PRODUCT
        ORDER BY count DESC
    """).fetchdf()
    
    # Get counts by day
    day_counts = con.execute("""
        SELECT 
            DELIVERY_DATE::DATE as date,
            COUNT(*) as count
        FROM provider_data
        GROUP BY date
        ORDER BY date
    """).fetchdf()
    
    return {
        "date_range": date_range,
        "product_counts": product_counts,
        "day_counts": day_counts
    }

def get_afrr_data_summary(con):
    """Get summary of AFRR data."""
    try:
        # Check if table exists
        table_exists = con.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name='afrr_data'
        """).fetchone()
        
        if not table_exists:
            return None
            
        # Get date summary
        date_range = con.execute("""
            SELECT 
                MIN(STRPTIME("Datum", '%d.%m.%Y')) as min_date,
                MAX(STRPTIME("Datum", '%d.%m.%Y')) as max_date,
                COUNT(DISTINCT STRPTIME("Datum", '%d.%m.%Y')) as num_days,
                COUNT(*) as total_records
            FROM afrr_data
        """).fetchdf()
        
        # Get counts by day
        day_counts = con.execute("""
            SELECT 
                STRPTIME("Datum", '%d.%m.%Y')::DATE as date,
                COUNT(*) as count
            FROM afrr_data
            GROUP BY date
            ORDER BY date
        """).fetchdf()
        
        return {
            "date_range": date_range,
            "day_counts": day_counts
        }
    except Exception as e:
        st.error(f"Error getting AFRR data summary: {e}")
        return None  # Return None instead of empty DataFrame

def get_marginal_price_summary(con):
    """Get a summary of marginal price data in the database."""
    # Check if table exists
    table_exists = con.execute("""
        SELECT name FROM sqlite_master 
        WHERE type='table' AND name='marginal_prices'
    """).fetchone()
    
    if not table_exists:
        return None
    
    # First, check the actual column names
    columns = con.execute("PRAGMA table_info(marginal_prices)").fetchdf()
    
    # Try different possible column names for timestamp and price
    possible_timestamp_cols = ["timestamp", "delivery_date", "date", "time"]
    possible_price_cols = ["price", "marginal_price", "value"]
    
    timestamp_col = None
    price_col = None
    
    for col in columns["name"]:
        if col.lower() in possible_timestamp_cols:
            timestamp_col = col
        elif col.lower() in possible_price_cols:
            price_col = col
    
    if not timestamp_col or not price_col:
        return None
    
    # Now use the identified columns with proper GROUP BY
    try:
        date_range = con.execute(f"""
            SELECT 
                MIN("{timestamp_col}") as min_date,
                MAX("{timestamp_col}") as max_date,
                COUNT(DISTINCT "{timestamp_col}"::DATE) as num_days,
                COUNT(*) as total_records,
                COUNT(*) filter (where "{price_col}" IS NOT NULL) as non_null_prices,
                COUNT(*) filter (where "{price_col}" IS NULL) as null_prices
            FROM marginal_prices
        """).fetchdf()
        
        # Get counts by day with proper GROUP BY
        day_counts = con.execute(f"""
            SELECT 
                "{timestamp_col}"::DATE as date,
                COUNT(*) as total_intervals,
                COUNT(*) filter (where "{price_col}" IS NOT NULL) as intervals_with_prices,
                COUNT(*) filter (where "{price_col}" IS NULL) as intervals_without_prices
            FROM marginal_prices
            GROUP BY date
            ORDER BY date
        """).fetchdf()
        
        return {
            "date_range": date_range,
            "day_counts": day_counts,
            "timestamp_col": timestamp_col,
            "price_col": price_col
        }
    except Exception as e:
        st.error(f"Error querying marginal prices: {e}")
        return None

def plot_data_coverage(day_counts, title):
    """Plot data coverage by day."""
    fig = px.bar(
        day_counts, 
        x="date", 
        y="count",
        title=f"{title} - Records per Day",
        labels={"date": "Date", "count": "Number of Records"}
    )
    return fig

def plot_marginal_price_coverage(day_counts):
    """Plot marginal price coverage by day."""
    # Calculate percentage of intervals with prices
    day_counts["percent_with_prices"] = (day_counts["intervals_with_prices"] / day_counts["total_intervals"]) * 100
    
    fig = px.bar(
        day_counts,
        x="date",
        y="percent_with_prices",
        title="Marginal Price Coverage by Day",
        labels={"date": "Date", "percent_with_prices": "% of Intervals with Prices"}
    )
    fig.update_layout(yaxis_range=[0, 100])
    return fig

def plot_product_distribution(product_counts):
    """Plot provider data distribution by product."""
    fig = px.bar(
        product_counts,
        x="PRODUCT",
        y="count",
        title="Provider Data by Product",
        labels={"PRODUCT": "Product", "count": "Number of Records"}
    )
    return fig

def plot_marginal_prices_over_time(con, timestamp_col=None, price_col=None):
    """Plot marginal prices over time."""
    if not timestamp_col or not price_col:
        # Get the columns from get_marginal_price_summary
        mp_summary = get_marginal_price_summary(con)
        if mp_summary:
            timestamp_col = mp_summary.get("timestamp_col")
            price_col = mp_summary.get("price_col")
        else:
            return None
    
    prices_df = con.execute(f"""
        SELECT 
            "{timestamp_col}" as timestamp,
            "{price_col}" as price
        FROM marginal_prices
        WHERE "{price_col}" IS NOT NULL
        ORDER BY "{timestamp_col}"
    """).fetchdf()
    
    if len(prices_df) == 0:
        return None
    
    fig = px.line(
        prices_df,
        x="timestamp",
        y="price",
        title="Marginal Prices Over Time",
        labels={"timestamp": "Time", "price": "Price (€/MWh)"}
    )
    return fig

def generate_text_summary(con):
    """Generate a text summary of the database contents"""
    # Use the correct path in your project structure
    summary_dir = "/home/philly/hypermvp/data/03_output/dashboard-summary"
    os.makedirs(summary_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    summary_path = os.path.join(summary_dir, f"db_summary_{timestamp}.txt")
    
    with open(summary_path, "w") as f:
        # Database info
        f.write(f"DATABASE SUMMARY - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Database file: {ENERGY_DB_PATH}\n")
        f.write(f"Database size: {os.path.getsize(ENERGY_DB_PATH) / (1024 * 1024):.2f} MB\n\n")
        
        # Tables
        tables = con.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchdf()
        f.write(f"Tables in database: {', '.join(tables['name'])}\n\n")
        
        # For each table
        for table in tables['name']:
            f.write(f"=== TABLE: {table} ===\n")
            
            # Schema
            schema = con.execute(f"PRAGMA table_info({table})").fetchdf()
            f.write("COLUMNS:\n")
            for _, row in schema.iterrows():
                f.write(f"  {row['name']} ({row['type']})\n")
            
            # Row count
            count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            f.write(f"ROWS: {count:,}\n")
            
            # Date range if available
            date_cols = [col for col in schema['name'] if 'date' in col.lower() or 'time' in col.lower()]
            if date_cols:
                try:
                    date_col = date_cols[0]
                    date_range = con.execute(f"""
                        SELECT 
                            MIN("{date_col}") as min_date,
                            MAX("{date_col}") as max_date
                        FROM {table}
                    """).fetchdf()
                    f.write(f"DATE RANGE: {date_range['min_date'].iloc[0]} to {date_range['max_date'].iloc[0]}\n")
                except Exception as e:
                    f.write(f"Error getting date range: {e}\n")
            
            # Sample data (first 5 rows)
            f.write("SAMPLE DATA:\n")
            try:
                sample = con.execute(f"SELECT * FROM {table} LIMIT 5").fetchdf()
                if len(sample) > 0:
                    # Get the column names
                    f.write(f"  COLUMNS: {', '.join(sample.columns)}\n\n")
                    
                    # Write each row
                    for idx, row in sample.iterrows():
                        f.write(f"  ROW {idx+1}:\n")
                        for col in sample.columns:
                            f.write(f"    {col}: {row[col]}\n")
                        f.write("\n")
                else:
                    f.write("  No data found.\n")
            except Exception as e:
                f.write(f"  Error getting sample: {e}\n")
            
            f.write("\n" + "="*50 + "\n\n")
    
    # Also create a JSON version
    try:
        import json
        
        summary = {
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "database_info": {
                "path": str(ENERGY_DB_PATH),
                "size_mb": os.path.getsize(ENERGY_DB_PATH) / (1024 * 1024)
            },
            "tables": {}
        }
        
        for table in tables['name']:
            # Get schema
            schema = con.execute(f"PRAGMA table_info({table})").fetchdf()
            columns = [{"name": row["name"], "type": row["type"]} for _, row in schema.iterrows()]
            
            # Get row count
            count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            
            # Get date range if applicable
            date_range = None
            date_columns = [col for col in schema['name'] if 'date' in col.lower() or 'time' in col.lower()]
            if date_columns:
                try:
                    date_col = date_columns[0]
                    date_info = con.execute(f"SELECT MIN(\"{date_col}\") as min_date, MAX(\"{date_col}\") as max_date FROM {table}").fetchdf()
                    date_range = {
                        "column": date_col,
                        "min": str(date_info["min_date"].iloc[0]),
                        "max": str(date_info["max_date"].iloc[0])
                    }
                except:
                    pass
            
            # Get sample data
            sample_data = []
            try:
                sample = con.execute(f"SELECT * FROM {table} LIMIT 5").fetchdf()
                for _, row in sample.iterrows():
                    sample_data.append({col: str(row[col]) for col in sample.columns})
            except:
                pass
            
            summary["tables"][table] = {
                "columns": columns,
                "row_count": count,
                "date_range": date_range,
                "sample_data": sample_data
            }
        
        json_path = os.path.join(summary_dir, f"db_summary_{timestamp}.json")
        with open(json_path, "w") as f:
            json.dump(summary, f, indent=2)
        
    except Exception as e:
        print(f"Error creating JSON summary: {e}")
    
    st.success(f"Database summary saved to {summary_path}")
    return summary_path

def app():
    st.set_page_config(page_title="HyperMVP Data Dashboard", layout="wide")
    
    st.title("HyperMVP Database Dashboard")
    st.write("This dashboard shows the current state of the energy market database.")
    
    try:
        con = connect_to_db()
        
        # Database info
        db_size_mb = os.path.getsize(ENERGY_DB_PATH) / (1024 * 1024)
        st.info(f"Database file: {ENERGY_DB_PATH}")
        st.info(f"Database size: {db_size_mb:.2f} MB")
        
        # Show available tables
        tables = con.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchdf()
        st.subheader("Available Tables")
        st.write(tables)
        
        if len(tables) == 0:
            st.warning("No tables found in the database. You may need to run the data processing workflows first.")
            return
        
        # For each table, show the schema
        st.subheader("Table Schemas")
        for table in tables["name"]:
            with st.expander(f"{table} schema"):
                schema = con.execute(f"PRAGMA table_info({table})").fetchdf()
                st.dataframe(schema)
                
                sample = con.execute(f"SELECT * FROM {table} LIMIT 5").fetchdf()
                st.write("Sample data:")
                st.dataframe(sample)
        
        # Layout with tabs
        tab1, tab2, tab3, tab4 = st.tabs(["Overview", "Provider Data", "AFRR Data", "Marginal Prices"])
        
        with tab1:
            st.header("Database Overview")
            col1, col2, col3 = st.columns(3)
            
            provider_summary = get_provider_data_summary(con)
            afrr_summary = get_afrr_data_summary(con)
            mp_summary = get_marginal_price_summary(con)
            
            with col1:
                st.subheader("Provider Data")
                if provider_summary and not provider_summary["date_range"].empty:
                    st.metric("Total Records", f"{provider_summary['date_range']['total_records'].iloc[0]:,}")
                    st.metric("Date Range", f"{provider_summary['date_range']['min_date'].iloc[0]} to {provider_summary['date_range']['max_date'].iloc[0]}")
                    st.metric("Days with Data", provider_summary['date_range']['num_days'].iloc[0])
                else:
                    st.write("No provider data found")
            
            with col2:
                st.subheader("AFRR Data")
                if afrr_summary and not afrr_summary["date_range"].empty:
                    st.metric("Total Records", f"{afrr_summary['date_range']['total_records'].iloc[0]:,}")
                    st.metric("Date Range", f"{afrr_summary['date_range']['min_date'].iloc[0]} to {afrr_summary['date_range']['max_date'].iloc[0]}")
                    st.metric("Days with Data", afrr_summary['date_range']['num_days'].iloc[0])
                else:
                    st.write("No AFRR data found")
            
            with col3:
                st.subheader("Marginal Prices")
                if mp_summary and not mp_summary["date_range"].empty:
                    st.metric("Total Records", f"{mp_summary['date_range']['total_records'].iloc[0]:,}")
                    st.metric("Date Range", f"{mp_summary['date_range']['min_date'].iloc[0]} to {mp_summary['date_range']['max_date'].iloc[0]}")
                    st.metric("Intervals with Prices", f"{mp_summary['date_range']['non_null_prices'].iloc[0]:,}")
                else:
                    st.write("No marginal price data found")
            
            # Summary charts
            st.subheader("Data Coverage")
            col1, col2 = st.columns(2)
            
            with col1:
                if provider_summary and "day_counts" in provider_summary and not provider_summary["day_counts"].empty:
                    st.plotly_chart(plot_data_coverage(provider_summary['day_counts'], "Provider Data"), use_container_width=True)
                else:
                    st.write("No provider data to display")
            
            with col2:
                if afrr_summary and "day_counts" in afrr_summary and not afrr_summary["day_counts"].empty:
                    st.plotly_chart(plot_data_coverage(afrr_summary['day_counts'], "AFRR Data"), use_container_width=True)
                else:
                    st.write("No AFRR data to display")
            
            if mp_summary and "day_counts" in mp_summary and not mp_summary["day_counts"].empty:
                st.plotly_chart(plot_marginal_price_coverage(mp_summary['day_counts']), use_container_width=True)
            else:
                st.write("No marginal price data to display")
        
        with tab2:
            st.header("Provider Data")
            if provider_summary:
                st.subheader("Data Summary")
                st.dataframe(provider_summary['date_range'])
                
                st.subheader("Product Distribution")
                st.plotly_chart(plot_product_distribution(provider_summary['product_counts']), use_container_width=True)
                
                st.subheader("Data by Day")
                st.dataframe(provider_summary['day_counts'])
                
                # Sample data
                st.subheader("Sample Data")
                sample = con.execute("SELECT * FROM provider_data LIMIT 10").fetchdf()
                st.dataframe(sample)
            else:
                st.write("No provider data found in the database")
        
        with tab3:
            st.header("AFRR Data")
            if afrr_summary:
                st.subheader("Data Summary")
                st.dataframe(afrr_summary['date_range'])
                
                st.subheader("Data by Day")
                st.dataframe(afrr_summary['day_counts'])
                
                # Sample data
                st.subheader("Sample Data")
                sample = con.execute("SELECT * FROM afrr_data LIMIT 10").fetchdf()
                st.dataframe(sample)
            else:
                st.write("No AFRR data found in the database")
        
        with tab4:
            st.header("Marginal Prices")
            if mp_summary:
                st.subheader("Data Summary")
                st.dataframe(mp_summary['date_range'])
                
                st.subheader("Data by Day")
                st.dataframe(mp_summary['day_counts'])
                
                # Prices over time
                st.subheader("Prices Over Time")
                price_chart = plot_marginal_prices_over_time(con)
                if price_chart:
                    st.plotly_chart(price_chart, use_container_width=True)
                else:
                    st.write("No price data to display")
                
                # Sample data
                st.subheader("Sample Data")
                sample = con.execute("SELECT * FROM marginal_prices LIMIT 10").fetchdf()
                st.dataframe(sample)
            else:
                st.write("No marginal price data found in the database")
        
        if st.button("Generate Text Summary"):
            generate_text_summary(con)
        
        # Add this button somewhere in your app
        if st.button("Generate Database Summary"):
            summary_path = generate_text_summary(con)
            with open(summary_path, "r") as f:
                summary_content = f.read()
            
            st.download_button(
                label="Download Summary",
                data=summary_content,
                file_name="db_summary.txt",
                mime="text/plain"
            )
        
    except Exception as e:
        st.error(f"Error connecting to database: {e}")

if __name__ == "__main__":
    app()

def load_afrr_data(file_path):
    """Load AFRR data from CSV file with proper handling of German formats."""
    import pandas as pd
    import locale
    
    # Save original locale
    old_locale = locale.getlocale(locale.LC_NUMERIC)
    
    try:
        # Set locale to German to handle comma as decimal separator
        locale.setlocale(locale.LC_NUMERIC, 'de_DE.UTF-8')
        
        # Read CSV with proper handling of German dates and decimals
        df = pd.read_csv(
            file_path, 
            decimal=',',  # Use comma as decimal separator
            parse_dates=['Datum'],  # Parse dates
            dayfirst=True,  # German date format (day first)
        )
        
        # Rename problematic columns to remove special characters
        df = df.rename(columns={
            '50Hertz (Negativ)': 'activated_volume_mw',
        })
        
        # Convert comma-decimal strings to proper numbers
        for col in df.columns:
            if col not in ['Datum', 'von', 'bis', 'month', 'year']:
                # Replace comma with dot and convert to float
                if df[col].dtype == 'object':
                    df[col] = df[col].str.replace(',', '.').astype(float)
        
        return df
        
    finally:
        # Restore original locale
        locale.setlocale(locale.LC_NUMERIC, old_locale)