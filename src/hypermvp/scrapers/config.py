# /home/philly/hypermvp/src/hypermvp/scrapers/config.py
"""Configuration settings for web scrapers."""

# Website configurations
AFRR_CONFIG = {
    'base_url': 'https://www.netztransparenz.de/de-de/Regelenergie/Daten-Regelreserve/Aktivierte-Regelleistung',
    'product_codes': {
        'PRL': 'k*Delta f (PRL) qualitätsgesichert',
        'SRL_NEG': 'aFRR negative',
        'SRL_POS': 'aFRR positive'
    },
    'tso_codes': ['50Hertz', 'Amprion', 'TenneT', 'TransnetBW']
}

PROVIDER_CONFIG = {
    'base_url': 'https://www.regelleistung.net/apps/datacenter/tenders/',
    'api_url': 'https://www.regelleistung.net/apps/datacenter-api/v1/tenders',
    'product_types': ['SRL', 'MRL', 'PRL'],
    'market_types': ['BALANCING_CAPACITY', 'BALANCING_ENERGY']
}

# User agent rotation to avoid blocking
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Safari/605.1.15',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36'
]

# Request limits to be polite
REQUEST_DELAY = 2.0  # seconds between requests
MAX_RETRIES = 5