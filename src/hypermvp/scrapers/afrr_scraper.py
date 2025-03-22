# /home/philly/hypermvp/src/hypermvp/scrapers/afrr_scraper.py
"""Scraper for aFRR activation data from netztransparenz.de."""

from hypermvp.scrapers.base_scraper import BaseScraper
import logging
from pathlib import Path
import re
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import json

class AFRRScraper(BaseScraper):
    """Scraper for aFRR activation data from netztransparenz.de."""
    
    BASE_URL = "https://www.netztransparenz.de/de-de/Regelenergie/Daten-Regelreserve/Aktivierte-Regelleistung"
    
    def __init__(self, output_dir=None, delay=None):
        super().__init__(output_dir=output_dir, delay=delay)
        self.logger = logging.getLogger("AFRRScraper")
    
    def download_date(self, target_date):
        """Download aFRR data for a specific month.
        
        Args:
            target_date: The date to download data for.
            
        Returns:
            List of paths to downloaded files.
        """
        # Convert to date object if string
        if isinstance(target_date, str):
            target_date = datetime.strptime(target_date, "%Y-%m-%d").date()
            
        # Get the start and end of the month
        start_date = target_date.replace(day=1)
        if start_date.month == 12:
            end_date = start_date.replace(year=start_date.year + 1, month=1, day=1) - timedelta(days=1)
        else:
            end_date = start_date.replace(month=start_date.month + 1, day=1) - timedelta(days=1)
            
        self.logger.info(f"Downloading aFRR data for {start_date.strftime('%Y-%m')} (from {start_date} to {end_date})")
        
        # Step 1: Get the main page to extract form tokens
        response = self.get_with_retry(self.BASE_URL)
        soup = BeautifulSoup(response.content, "html.parser")
        
        # Find the form
        form = soup.find("form", {"id": "Form"})
        if not form:
            self.logger.error("Could not find form on page")
            raise ValueError("Could not find form on page")
            
        # Extract hidden fields
        form_data = {}
        # Add all hidden inputs
        for hidden_input in form.find_all("input", {"type": "hidden"}):
            if hidden_input.has_attr("name") and hidden_input.has_attr("value"):
                form_data[hidden_input["name"]] = hidden_input["value"]
                
        # Set the event target to the CSV download button
        form_data["__EVENTTARGET"] = "dnn$ctr2413$View$btnDownloadGridCsv"
        form_data["__EVENTARGUMENT"] = ""
        
        # Format dates for the form
        form_data[f"dnn$ctr2413$View$rdpGridDownloadStartDate"] = start_date.strftime("%Y-%m-%d")
        form_data[f"dnn$ctr2413$View$rdpGridDownloadStartDate$dateInput"] = start_date.strftime("%d.%m.%Y")
        form_data[f"dnn$ctr2413$View$rdpGridDownloadEndDate"] = end_date.strftime("%Y-%m-%d")
        form_data[f"dnn$ctr2413$View$rdpGridDownloadEndDate$dateInput"] = end_date.strftime("%d.%m.%Y")
        
        # Add client state JSON for date inputs
        form_data[f"dnn_ctr2413_View_rdpGridDownloadStartDate_dateInput_ClientState"] = json.dumps({
            "enabled": True,
            "emptyMessage": "",
            "validationText": f"{start_date.strftime('%Y-%m-%d')}-00-00-00",
            "valueAsString": f"{start_date.strftime('%Y-%m-%d')}-00-00-00",
            "minDateStr": "2014-01-01-00-00-00",
            "maxDateStr": datetime.now().strftime("%Y-%m-%d-23-59-58"),
            "lastSetTextBoxValue": start_date.strftime("%d.%m.%Y")
        })
        
        form_data[f"dnn_ctr2413_View_rdpGridDownloadEndDate_dateInput_ClientState"] = json.dumps({
            "enabled": True,
            "emptyMessage": "",
            "validationText": f"{end_date.strftime('%Y-%m-%d')}-00-00-00",
            "valueAsString": f"{end_date.strftime('%Y-%m-%d')}-00-00-00",
            "minDateStr": "2014-01-01-00-00-00",
            "maxDateStr": datetime.now().strftime("%Y-%m-%d-23-59-58"),
            "lastSetTextBoxValue": end_date.strftime("%d.%m.%Y")
        })
        
        # Set timezone to ME(S)Z
        form_data["dnn$ctr2413$View$cbbDownloadTimZone"] = "ME(S)Z"
        
        # Submit the form
        self.logger.info("Submitting form to download CSV")
        download_response = self.session.post(self.BASE_URL, data=form_data)
        
        if download_response.status_code != 200:
            self.logger.error(f"Form submission failed with status {download_response.status_code}")
            raise ValueError(f"Form submission failed with status {download_response.status_code}")
            
        # Check content type to ensure we got a file
        content_type = download_response.headers.get("Content-Type", "")
        if "text/csv" in content_type or "application/octet-stream" in content_type:
            # Extract filename from headers if available
            filename = None
            if "Content-Disposition" in download_response.headers:
                match = re.search(r'filename="(.+)"', download_response.headers["Content-Disposition"])
                if match:
                    filename = match.group(1)
                    
            if not filename:
                # Generate a default filename
                filename = f"afrr_{start_date.strftime('%Y-%m')}.csv"
                
            # Save the file
            file_path = self.save_response_to_file(download_response, filename)
            self.logger.info(f"Downloaded aFRR data to {file_path}")
            
            return [file_path]
        else:
            # If we didn't get a CSV, something went wrong
            self.logger.error(f"Expected CSV but got {content_type}")
            debug_file = f"afrr_debug_{start_date.strftime('%Y-%m')}.html"
            debug_path = self.save_response_to_file(download_response, debug_file)
            self.logger.info(f"Saved debug HTML to {debug_path}")
            
            raise ValueError(f"Expected CSV but got {content_type}")