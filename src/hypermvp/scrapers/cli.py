# /home/philly/hypermvp/src/hypermvp/scrapers/cli.py
"""Command-line interface for scrapers."""

import argparse
import logging
from datetime import datetime, timedelta
import os
from pathlib import Path
import calendar
import time

from hypermvp.scrapers.afrr_scraper import AFRRScraper
from hypermvp.config import RAW_DATA_DIR
from hypermvp.scrapers.provider_scraper import ProviderScraper

def generate_date_points(start_date, end_date, increment='day'):
    """Generate dates based on the specified increment.
    
    Args:
        start_date: Starting date
        end_date: Ending date
        increment: 'day', 'month', or 'year'
        
    Returns:
        List of dates to process
    """
    if isinstance(start_date, str):
        start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
    
    if isinstance(end_date, str):
        end_date = datetime.strptime(end_date, "%Y-%m-%d").date()
    
    dates = []
    
    if increment == 'day':
        # Process every day (original behavior)
        current = start_date
        while current <= end_date:
            dates.append(current)
            current += timedelta(days=1)
            
    elif increment == 'month':
        # Process only the first day of each month in the range
        current_year = start_date.year
        current_month = start_date.month
        
        while (current_year < end_date.year) or (current_year == end_date.year and current_month <= end_date.month):
            dates.append(datetime(current_year, current_month, 1).date())
            
            # Move to next month
            if current_month == 12:
                current_month = 1
                current_year += 1
            else:
                current_month += 1
                
    elif increment == 'year':
        # Process only January 1st of each year
        for year in range(start_date.year, end_date.year + 1):
            dates.append(datetime(year, 1, 1).date())
    
    return dates

def main():
    """Run the scraper CLI."""
    parser = argparse.ArgumentParser(description="Download energy market data")
    parser.add_argument("--start-date", required=True,
                      help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date",
                      help="End date (YYYY-MM-DD), defaults to start date")
    parser.add_argument("--output-dir",
                      help="Directory to save downloaded files")
    parser.add_argument("--verbose", "-v", action="store_true",
                      help="Enable verbose logging")
    parser.add_argument("--increment", choices=['day', 'month', 'year'], default='month',
                      help="Time increment between processing points (default: month)")
    parser.add_argument("--scraper", choices=['afrr', 'provider', 'both'], default='afrr',
                      help="Scraper to use (default: afrr)")
    parser.add_argument("--product", choices=['aFRR', 'mFRR', 'PRL'], default='aFRR',
                      help="Product type for provider data (default: aFRR)")
    parser.add_argument("--market", choices=['ENERGY', 'CAPACITY'], default='ENERGY',
                      help="Market type for provider data (default: ENERGY)")
    
    args = parser.parse_args()
    
    # Configure logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    
    # Set end date if not provided
    end_date = args.end_date or args.start_date
    
    # Generate processing dates based on increment
    process_dates = generate_date_points(args.start_date, end_date, args.increment)
    
    logging.info(f"Will process {len(process_dates)} dates between {args.start_date} and {end_date}")
    
    # Set output directory
    if args.output_dir:
        base_output_dir = Path(args.output_dir)
    else:
        base_output_dir = Path(RAW_DATA_DIR)
    
    afrr_output_dir = base_output_dir / "afrr"
    os.makedirs(afrr_output_dir, exist_ok=True)
    
    # Run the AFRR scraper
    if args.scraper in ['afrr', 'both']:
        scraper = AFRRScraper(output_dir=afrr_output_dir)
        logging.info(f"Running AFRR scraper for {len(process_dates)} dates")
        
        results = []
        for date in process_dates:
            logging.info(f"Processing date: {date}")
            try:
                result = scraper.download_date(date)
                results.append(result)
                # Add delay between requests
                time.sleep(scraper.delay)
            except Exception as e:
                logging.error(f"Failed to download data for {date}: {e}")
    
    # Run the Provider scraper
    if args.scraper in ['provider', 'both']:
        provider_output_dir = base_output_dir / "provider"
        os.makedirs(provider_output_dir, exist_ok=True)
        
        provider_scraper = ProviderScraper(output_dir=provider_output_dir)
        logging.info(f"Running Provider scraper for {len(process_dates)} dates")
        
        # Provider scraper works on monthly data
        if args.increment != 'month':
            logging.warning("Provider scraper works best with --increment=month, filtering dates...")
            # Extract unique year/month combinations
            year_months = set((d.year, d.month) for d in process_dates)
            logging.info(f"Will download {len(year_months)} unique months")
        else:
            year_months = [(d.year, d.month) for d in process_dates]
        
        for year, month in year_months:
            logging.info(f"Downloading provider data for {year}-{month:02d}")
            try:
                file_path = provider_scraper.download_monthly_data(
                    year, month, 
                    product=args.product, 
                    market=args.market
                )
                if file_path:
                    logging.info(f"Successfully downloaded {file_path}")
                # Add delay between requests
                time.sleep(provider_scraper.delay)
            except Exception as e:
                logging.error(f"Failed to download provider data for {year}-{month:02d}: {e}")
    
    logging.info("Scraping completed")

if __name__ == "__main__":
    main()