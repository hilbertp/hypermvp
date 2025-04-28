
# Scraper Development Plan

## 1. Project Overview

### Objective

Create a robust data scraping system to automate the collection of energy market data from:

- netztransparenz.de (aFRR activation data)
- regelleistung.net (provider offer data)

### Architecture

```bash
src/hypermvp/scrapers/
├── base_scraper.py         # Abstract base class with common functionality
├── provider_scraper.py     # Specialized for provider offer data
├── afrr_scraper.py         # Specialized for aFRR activation data
├── config.py               # URL patterns, authentication, timeouts
└── cli.py                  # Command-line interface
```

## 2. Detailed Implementation Timeline

### Week 1: Foundation (Days 1-3)

#### Day 1: Setup & Base Implementation

- Create directory structure
- Implement `base_scraper.py` with core functionality
- Set up logging configuration
- Create basic test infrastructure

#### Day 2-3: aFRR Scraper Research & Development

- Research netztransparenz.de structure, HTML patterns, and form submission
- Confirm URL: <https://www.netztransparenz.de/de-de/Regelenergie/Daten-Regelreserve/Aktivierte-Regelleistung>
- Identify product codes: `k*Delta f (PRL) qualitätsgesichert` for primary frequency control
- Implement initial `afrr_scraper.py` with date filtering

### Week 1: Development (Days 4-5)

#### Day 4-5: Provider Scraper Research & Development

- Research regelleistung.net API endpoints and parameters
- Confirm URL: <https://www.regelleistung.net/apps/datacenter/tenders/>
- Verify API URL: <https://www.regelleistung.net/apps/datacenter-api/v1/tenders>
- Implement API-based approach in `provider_scraper.py`
- Test data retrieval for SRL (secondary frequency control) products

### Week 2: Robustness & Integration (Days 6-9)

#### Day 6-7: Fallback Implementations

- Develop HTML parsing fallback for provider data
- Implement Selenium-based fallback for difficult interactions
- Add file format detection and appropriate handling
- Enhance error handling with specific recovery actions

#### Day 8-9: Testing & Validation

- Test with various historical dates (1, 7, 30 days)
- Verify data completeness against manual downloads
- Handle edge cases (weekends, holidays, missing data)
- Implement data validation checks

### Week 2: Finalization (Day 10)

#### Day 10: CLI & Integration

- Implement command line interface in `cli.py`
- Add workflow option to main.py
- Create documentation with examples
- End-to-end testing of full workflow

## 3. Technical Implementation Details

### 3.1 Base Scraper Class

```python
# src/hypermvp/scrapers/base_scraper.py
import requests
from abc import ABC, abstractmethod
import logging
from pathlib import Path
import time
import random
from datetime import datetime, timedelta
from hypermvp.config import RAW_DATA_DIR
from hypermvp.scrapers.config import USER_AGENTS

class BaseScraper(ABC):
    """Base class for web scrapers with common functionality."""
  
    def __init__(self, output_dir=None, delay=1.0):
        """Initialize with configurable output directory and request delay."""
        self.output_dir = output_dir or RAW_DATA_DIR
        self.delay = delay  # Seconds between requests to avoid overwhelming the server
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
  
    def get_with_retry(self, url, max_retries=3, **kwargs):
        """Make GET request with retry logic."""
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
```

### 3.2 AFRR Scraper Implementation

```python
# src/hypermvp/scrapers/afrr_scraper.py
from hypermvp.scrapers.base_scraper import BaseScraper
import logging
from pathlib import Path
import re
from bs4 import BeautifulSoup
from datetime import datetime
from hypermvp.scrapers.config import AFRR_CONFIG

class AFRRScraper(BaseScraper):
    """Scraper for aFRR activation data from netztransparenz.de."""
  
    BASE_URL = AFRR_CONFIG['base_url']
  
    def __init__(self, output_dir=None, product_code='PRL'):
        super().__init__(output_dir=output_dir)
        self.logger = logging.getLogger("AFRRScraper")
        self.product = AFRR_CONFIG['product_codes'].get(product_code, AFRR_CONFIG['product_codes']['PRL'])
  
    def download_date(self, target_date):
        """Download aFRR data for a specific date or month."""
        year = target_date.year
        month = target_date.month
        month_name = target_date.strftime("%B")  # Full month name
      
        # First approach: Try to find direct download links on the main page
        self.logger.info(f"Navigating to main aFRR data page")
        response = self.get_with_retry(self.BASE_URL)
      
        # Parse the HTML to find links to CSV/Excel files
        soup = BeautifulSoup(response.content, 'html.parser')
      
        # Look for links containing the year and month
        # The pattern might be something like "aFRR_2024_March.csv"
        pattern = re.compile(f"{year}.*{month:02d}|{year}.*{month_name}", re.IGNORECASE)
      
        # Find download links
        download_links = []
        for link in soup.find_all('a', href=True):
            href = link['href']
            text = link.get_text()
            if (href.endswith('.csv') or href.endswith('.xlsx')) and (pattern.search(href) or pattern.search(text)):
                download_links.append(href)
      
        if download_links:
            # Download each file
            downloaded_files = []
            for link in download_links:
                # Handle relative vs absolute URLs
                if not link.startswith('http'):
                    if link.startswith('/'):
                        # Use domain only
                        domain = re.match(r'https?://[^/]+', self.BASE_URL).group(0)
                        download_url = f"{domain}{link}"
                    else:
                        # Use base URL directory
                        base_dir = self.BASE_URL.rsplit('/', 1)[0]
                        download_url = f"{base_dir}/{link}"
                else:
                    download_url = link
              
                self.logger.info(f"Downloading from {download_url}")
                file_response = self.get_with_retry(download_url)
              
                # Extract filename from URL or content-disposition header
                if 'Content-Disposition' in file_response.headers:
                    filename = re.findall('filename="(.+)"', file_response.headers['Content-Disposition'])[0]
                else:
                    filename = download_url.split('/')[-1]
              
                # Save file with year-month prefix for better organization
                output_filename = f"afrr_{year}_{month:02d}_{filename}"
                file_path = self.save_response_to_file(file_response, output_filename)
                downloaded_files.append(file_path)
          
            # Validate the downloaded files
            valid_files = []
            for file_path in downloaded_files:
                if self.validate_downloaded_data(file_path, ['Datum', 'von', 'bis']):
                    valid_files.append(file_path)
          
            if valid_files:
                return valid_files
            else:
                self.logger.warning("Downloaded files failed validation, trying form submission")
        else:
            self.logger.info("No direct download links found, trying form submission")
      
        # Second approach: Form submission
        return self._download_via_form_submission(target_date)
  
    def _download_via_form_submission(self, target_date):
        """Download data using form submission."""
        # Implement form submission logic here
        # This would involve finding the form elements, populating them, and submitting
      
        # For complex form submission, may need to use Selenium fallback
        return self._download_with_selenium(target_date)
  
    def _download_with_selenium(self, target_date):
        """Fallback method using Selenium for difficult websites."""
        try:
            from selenium import webdriver
            from selenium.webdriver.common.by import By
            from selenium.webdriver.support.ui import WebDriverWait
            from selenium.webdriver.support import expected_conditions as EC
            import time
        except ImportError:
            self.logger.error("Selenium not installed. Run: pip install selenium")
            raise
      
        year = target_date.year
        month = target_date.month
        day = target_date.day
      
        # Format dates for the website's datepicker format (may need to be adjusted)
        start_date = f"{day:02d}.{month:02d}.{year}"  # DD.MM.YYYY
      
        self.logger.info(f"Using Selenium fallback for {start_date}")
      
        # Configure webdriver
        options = webdriver.ChromeOptions()
        options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
      
        driver = None
        try:
            driver = webdriver.Chrome(options=options)
            driver.get(self.BASE_URL)
          
            # Wait for page to load
            WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((By.TAG_NAME, 'form'))
            )
          
            # Navigate the form
            # Note: These selectors need to be inspected and adjusted for the actual site
          
            # Input date (need to inspect actual implementation)
            date_input = driver.find_element(By.CSS_SELECTOR, "input[type='date'], input.date-picker")
            date_input.clear()
            date_input.send_keys(start_date)
          
            # Select product
            # This might be a dropdown or radio buttons, adjust accordingly
            product_dropdown = driver.find_element(By.CSS_SELECTOR, "select.product-selector")
            # Select option matching self.product
          
            # Submit form
            submit_button = driver.find_element(By.CSS_SELECTOR, "button[type='submit'], input[type='submit']")
            submit_button.click()
          
            # Wait for results
            time.sleep(5)
          
            # Look for download button
            download_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "a.csv-download, a[download]"))
            )
          
            # Get download URL
            download_url = download_button.get_attribute('href')
          
            # Use requests to download (avoid Selenium download complexity)
            self.logger.info(f"Found download link via Selenium: {download_url}")
            response = self.get_with_retry(download_url)
          
            # Generate filename
            output_filename = f"afrr_{year}_{month:02d}_{day:02d}_selenium.csv"
            file_path = self.save_response_to_file(response, output_filename)
          
            return [file_path]
          
        finally:
            if driver:
                driver.quit()
```

### 3.3 Provider Scraper Implementation

```python
# src/hypermvp/scrapers/provider_scraper.py
from hypermvp.scrapers.base_scraper import BaseScraper
import logging
from pathlib import Path
import json
from bs4 import BeautifulSoup
from hypermvp.scrapers.config import PROVIDER_CONFIG
import pandas as pd
from urllib.parse import urlparse

class ProviderScraper(BaseScraper):
    """Scraper for provider offer data from regelleistung.net."""
  
    BASE_URL = PROVIDER_CONFIG['base

Similar code found with 3 license types
```
