"""Base class for web scrapers with common functionality."""

import requests
from abc import ABC, abstractmethod
import logging
from pathlib import Path
import time
import random
from datetime import datetime, timedelta
from hypermvp.config import RAW_DATA_DIR
from hypermvp.scrapers.config import USER_AGENTS, MAX_RETRIES, REQUEST_DELAY

class BaseScraper(ABC):
    """Base class for web scrapers with common functionality."""
    
    def __init__(self, output_dir=None, delay=None):
        """Initialize with configurable output directory and request delay."""
        self.output_dir = output_dir or RAW_DATA_DIR
        self.delay = delay or REQUEST_DELAY
        self.session = requests.Session()
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # Setup default headers
        self.session.headers.update({
            'User-Agent': random.choice(USER_AGENTS),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Cache-Control': 'max-age=0'
        })
    
    def get_with_retry(self, url, max_retries=None, **kwargs):
        """Make GET request with retry logic."""
        max_retries = max_retries or MAX_RETRIES
        
        for attempt in range(max_retries):
            try:
                # Rotate user agent
                self.session.headers.update({'User-Agent': random.choice(USER_AGENTS)})
                
                response = self.session.get(url, **kwargs)
                response.raise_for_status()
                return response
            except requests.exceptions.RequestException as e:
                self.logger.warning(f"Request failed (attempt {attempt+1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    # Exponential backoff
                    sleep_time = self.delay * (2 ** attempt) * (0.5 + random.random())
                    self.logger.info(f"Retrying in {sleep_time:.2f} seconds...")
                    time.sleep(sleep_time)
                else:
                    raise
    
    def save_response_to_file(self, response, filename):
        """Save response content to a file."""
        output_path = Path(self.output_dir) / filename
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_path, 'wb') as f:
            f.write(response.content)
        self.logger.info(f"Saved file: {output_path}")
        return output_path
    
    @abstractmethod
    def download_date(self, target_date):
        """Download data for a specific date."""
        pass
    
    def download_date_range(self, start_date, end_date=None):
        """Download data for a range of dates."""
        if isinstance(start_date, str):
            start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
        
        if end_date is None:
            end_date = start_date
        elif isinstance(end_date, str):
            end_date = datetime.strptime(end_date, "%Y-%m-%d").date()
        
        current_date = start_date
        results = []
        
        while current_date <= end_date:
            self.logger.info(f"Processing date: {current_date}")
            try:
                result = self.download_date(current_date)
                results.append(result)
            except Exception as e:
                self.logger.error(f"Failed to download data for {current_date}: {e}")
            
            # Wait before next request
            time.sleep(self.delay)
            current_date += timedelta(days=1)
        
        return results
    
    def validate_downloaded_data(self, file_path, required_columns=None):
        """Validate that downloaded data meets requirements."""
        import pandas as pd
        
        if not file_path.exists():
            self.logger.error(f"Validation failed: File {file_path} does not exist")
            return False
            
        # Try to read the file based on extension
        try:
            if str(file_path).endswith('.csv'):
                # Try common CSV formats
                for sep in [';', ',', '\t']:
                    for encoding in ['utf-8', 'latin1', 'iso-8859-1']:
                        try:
                            df = pd.read_csv(file_path, sep=sep, encoding=encoding)
                            break
                        except:
                            continue
                    else:
                        continue
                    break
            elif str(file_path).endswith(('.xlsx', '.xls')):
                df = pd.read_excel(file_path)
            else:
                self.logger.warning(f"Unknown file type: {file_path}")
                return False
                
            # Check for required columns
            if required_columns:
                missing = [col for col in required_columns if col not in df.columns]
                if missing:
                    self.logger.error(f"Validation failed: Missing columns {missing}")
                    return False
                    
            # Check for empty dataset
            if df.empty:
                self.logger.error(f"Validation failed: Empty dataset")
                return False
                
            self.logger.info(f"Validation successful: {file_path} has {len(df)} rows")
            return True
            
        except Exception as e:
            self.logger.error(f"Validation failed: {e}")
            return False