#!/usr/bin/env python3
import duckdb
import pandas as pd
import sys
import os

# Add the project root to the path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))
from hypermvp.config import ENERGY_DB_PATH

def check_provider_data():
    """Check provider data for NEG products by day."""
    print(f"Connecting to database: {ENERGY_DB_PATH}")
    con = duckdb.connect(ENERGY_DB_PATH)
    
    # Run the diagnostic query
    print("Running analysis of provider data by day...")
    query = """
    SELECT 
        DELIVERY_DATE::DATE as date,
        COUNT(*) as total_records,
        COUNT(*) FILTER (WHERE PRODUCT LIKE 'NEG%') as neg_records,
        COUNT(DISTINCT PRODUCT) as distinct_products,
        COUNT(DISTINCT PRODUCT) FILTER (WHERE PRODUCT LIKE 'NEG%') as distinct_neg
    FROM provider_data
    GROUP BY date
    ORDER BY date
    """
    
    result = con.execute(query).fetchdf()
    
    # Display the results
    print("\n=== PROVIDER DATA ANALYSIS BY DAY ===")
    print(f"Total days analyzed: {len(result)}")
    
    # Count days with NEG products
    days_with_neg = result[result['neg_records'] > 0]
    days_without_neg = result[result['neg_records'] == 0]
    
    print(f"Days WITH NEG products: {len(days_with_neg)} days")
    print(f"Days WITHOUT NEG products: {len(days_without_neg)} days")
    
    # Display days without NEG products
    if not days_without_neg.empty:
        print("\nDays without NEG products:")
        for _, row in days_without_neg.iterrows():
            print(f"  {row['date']}: {row['total_records']} total records, {row['distinct_products']} unique products")
    
    # Display sample of days with NEG products
    if not days_with_neg.empty:
        print("\nSample of days WITH NEG products (first 5):")
        for _, row in days_with_neg.head(5).iterrows():
            print(f"  {row['date']}: {row['neg_records']} NEG records, {row['distinct_neg']} unique NEG products")
    
    # Check what products exist on days without NEG products
    if not days_without_neg.empty:
        sample_date = days_without_neg.iloc[0]['date']
        print(f"\nSample products for day without NEG products ({sample_date}):")
        sample_products = con.execute("""
            SELECT 
                PRODUCT, 
                COUNT(*) as count
            FROM provider_data
            WHERE DELIVERY_DATE::DATE = ?
            GROUP BY PRODUCT
            ORDER BY COUNT(*) DESC
            LIMIT 10
        """, [sample_date]).fetchdf()
        
        for _, row in sample_products.iterrows():
            print(f"  {row['PRODUCT']}: {row['count']} records")
    
    con.close()

if __name__ == "__main__":
    check_provider_data()