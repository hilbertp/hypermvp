"""Tests for AFRR scraper."""

import pytest
from unittest.mock import patch, MagicMock
from datetime import date
from pathlib import Path
import os
import json
from bs4 import BeautifulSoup

from hypermvp.scrapers.afrr_scraper import AFRRScraper

@pytest.fixture
def mock_html_response():
    """Create a mock HTML response for testing."""
    mock = MagicMock()
    mock.status_code = 200
    
    # Create a simple HTML form structure similar to what the scraper expects
    html = """
    <html>
    <body>
        <form id="Form">
            <input type="hidden" name="__VIEWSTATE" value="abc123" />
            <input type="hidden" name="__EVENTVALIDATION" value="xyz789" />
            <button id="dnn$ctr2413$View$btnDownloadGridCsv">Download CSV</button>
        </form>
    </body>
    </html>
    """
    mock.content = html.encode('utf-8')
    mock.headers = {"Content-Type": "text/html"}
    return mock

@pytest.fixture
def mock_csv_response():
    """Create a mock CSV response for testing."""
    mock = MagicMock()
    mock.status_code = 200
    mock.content = b"Date,TSO,Value\n2024-01-01,50Hertz,123"
    mock.headers = {
        "Content-Type": "text/csv", 
        "Content-Disposition": 'attachment; filename="afrr_data_2024_01.csv"'
    }
    return mock

@pytest.fixture
def temp_output_dir(tmpdir):
    """Create a temporary directory for test outputs."""
    return Path(tmpdir)

def test_afrr_scraper_init():
    """Test initialization of AFRR scraper."""
    scraper = AFRRScraper()
    assert scraper.BASE_URL == "https://www.netztransparenz.de/de-de/Regelenergie/Daten-Regelreserve/Aktivierte-Regelleistung"

@patch('requests.Session.post')
@patch('hypermvp.scrapers.base_scraper.BaseScraper.get_with_retry')
@patch('hypermvp.scrapers.base_scraper.BaseScraper.save_response_to_file')
def test_download_date(mock_save, mock_get, mock_post, mock_html_response, mock_csv_response, temp_output_dir):
    """Test downloading data for a specific date."""
    # Mock the initial GET request to get the form
    mock_get.return_value = mock_html_response
    
    # Mock the POST request that submits the form and gets CSV
    mock_post.return_value = mock_csv_response
    
    # Mock the file saving
    mock_save.return_value = temp_output_dir / "afrr_data_2024_01.csv"
    
    # Create scraper with temp directory
    scraper = AFRRScraper(output_dir=temp_output_dir)
    
    # Run the download
    result = scraper.download_date("2024-01-01")
    
    # Verify correct calls were made
    assert mock_get.called
    assert mock_post.called
    assert mock_save.called
    assert len(result) == 1
    assert str(result[0]).endswith(".csv")

@patch('requests.Session.post')
@patch('hypermvp.scrapers.base_scraper.BaseScraper.get_with_retry')
def test_download_date_error_handling(mock_get, mock_post, mock_html_response, temp_output_dir):
    """Test error handling when downloading data."""
    # Mock the initial GET request to get the form
    mock_get.return_value = mock_html_response
    
    # Mock the POST request to return a non-CSV response (error case)
    error_response = MagicMock()
    error_response.status_code = 200
    error_response.content = b"<html>Error page</html>"
    error_response.headers = {"Content-Type": "text/html"}
    mock_post.return_value = error_response
    
    # Create scraper with temp directory
    scraper = AFRRScraper(output_dir=temp_output_dir)
    
    # Run the download and expect an error
    with pytest.raises(ValueError) as excinfo:
        scraper.download_date("2024-01-01")
    
    assert "Expected CSV but got" in str(excinfo.value)