# /home/philly/hypermvp/src/hypermvp/scrapers/config.py
"""Configuration settings for web scrapers."""

# API endpoints and configuration for scrapers
AFRR_CONFIG = {
    'base_url': 'https://www.regelleistung.net/apps/datacenter/tenders',
    'api_url': 'https://www.regelleistung.net/apps/datacenter-api/v1'
}

PROVIDER_CONFIG = {
    'base_url': 'https://www.regelleistung.net',
    'api_url': 'https://www.regelleistung.net/apps/cpp-publisher/api/v1'
}

# List of user agents to rotate
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36'
]

# Request limits to be polite
REQUEST_DELAY = 2.0  # seconds between requests
MAX_RETRIES = 5