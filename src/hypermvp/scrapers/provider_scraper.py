# src/hypermvp/scrapers/provider_scraper.py

import logging
import requests
from pathlib import Path
import zipfile
from datetime import datetime
import calendar
from typing import Optional, List, Dict, Any
import random

from hypermvp.scrapers.base_scraper import BaseScraper
from hypermvp.scrapers.config import PROVIDER_CONFIG, USER_AGENTS

class ProviderScraper(BaseScraper):
    """Scraper for downloading market results data from regelleistung.net."""

    def __init__(self, output_dir: str = "data/01_raw/provider"):
        """Initialize the provider scraper.
        
        Args:
            output_dir: Directory where downloaded files will be saved
        """
        super().__init__(output_dir=output_dir)
        self.BASE_URL = PROVIDER_CONFIG['base_url']
        self.API_URL = PROVIDER_CONFIG['api_url']
        self.logger = logging.getLogger(self.__class__.__name__)

    def get_random_user_agent(self):
        """Return a random user agent string from the configured list."""
        return random.choice(USER_AGENTS)

    def download_date(self, date):
        """
        Download data for a specific date.
        Required implementation for the abstract base class.
        
        Args:
            date: Date to download data for (datetime.date object)
            
        Returns:
            Path to downloaded file or None if download failed
        """
        # Convert date to year and month
        year = date.year
        month = date.month
        
        # Use existing method to download the monthly data
        return self.download_monthly_data(year, month)

    def download_monthly_data(self, year: int, month: int, product: str = "aFRR", 
                             market: str = "ENERGY") -> Optional[Path]:
        """Download monthly market data directly from the API.
        
        Args:
            year: Year to download data for
            month: Month to download data for (1-12)
            product: Product type (aFRR, mFRR, PRL)
            market: Market type (ENERGY, CAPACITY)
            
        Returns:
            Path to the downloaded Excel file, or None if download failed
        """
        # Construct start and end dates for the month
        start_date = f"{year}-{month:02d}-01"
        
        # Calculate last day of month
        last_day = calendar.monthrange(year, month)[1]
        end_date = f"{year}-{month:02d}-{last_day:02d}"
        
        # Construct the API URL for the file
        file_name = f"RESULT_LIST_ANONYM_{market}_MARKET_{product}_DE_{start_date}_{end_date}.xlsx.zip"
        api_url = f"{PROVIDER_CONFIG['api_url']}/download/tenders/files/{file_name}"
        
        # Set required headers with rotating User-Agent
        headers = {
            'Accept': '*/*',
            'Accept-Language': 'de-DE,de;q=0.5',
            'Connection': 'keep-alive',
            'Referer': self.BASE_URL,
            'Cache-Control': 'no-cache',
            'User-Agent': self.get_random_user_agent()
        }
        
        self.logger.info(f"Downloading {product} {market} market data for {year}-{month:02d}")
        
        # Download the ZIP file
        response = self.get_with_retry(api_url, headers=headers)
        
        if response.status_code == 200:
            # Save the ZIP file
            zip_path = Path(self.output_dir) / file_name
            self.save_response_to_file(response, zip_path)
            
            # Extract the ZIP file
            extracted_file = self._extract_zip(zip_path)
            
            # Optionally remove the ZIP file after extraction
            if extracted_file:
                zip_path.unlink(missing_ok=True)
                self.logger.info(f"Successfully extracted {extracted_file.name}")
                return extracted_file
            
            return zip_path
        else:
            self.logger.error(f"Failed to download data: HTTP {response.status_code}")
            # Save error response for debugging
            error_file = f"error_{product}_{market}_{year}_{month:02d}.html"
            self.save_response_to_file(response, Path(self.output_dir) / error_file)
            return None
            
    def _extract_zip(self, zip_path: Path) -> Optional[Path]:
        """Extract a ZIP file and return the path to the extracted file.
        
        Args:
            zip_path: Path to the ZIP file
            
        Returns:
            Path to the extracted file, or None if extraction failed
        """
        try:
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                # Extract to the same directory
                zip_ref.extractall(Path(self.output_dir))
                
                # Get the name of the extracted file (should be only one)
                if len(zip_ref.namelist()) > 0:
                    excel_filename = zip_ref.namelist()[0]
                    return Path(self.output_dir) / excel_filename
                
            return None
        except Exception as e:
            self.logger.error(f"Error extracting ZIP file: {e}")
            return None
            
    def download_date_range(self, start_year: int, start_month: int, 
                           end_year: int, end_month: int,
                           product: str = "aFRR", market: str = "ENERGY") -> List[Path]:
        """Download data for a range of months.
        
        Args:
            start_year: Starting year
            start_month: Starting month (1-12)
            end_year: Ending year
            end_month: Ending month (1-12)
            product: Product type (aFRR, mFRR, PRL)
            market: Market type (ENERGY, CAPACITY)
            
        Returns:
            List of paths to downloaded files
        """
        downloaded_files = []
        
        current_year, current_month = start_year, start_month
        
        while (current_year < end_year) or (current_year == end_year and current_month <= end_month):
            file_path = self.download_monthly_data(current_year, current_month, product, market)
            
            if file_path:
                downloaded_files.append(file_path)
            
            # Move to next month
            if current_month == 12:
                current_month = 1
                current_year += 1
            else:
                current_month += 1
                
            # Add a delay to be nice to the server
            self.delay_request()
            
        return downloaded_files