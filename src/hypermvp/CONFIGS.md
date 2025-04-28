# Configuration Index

| Config File                                 | Purpose                                      |
|---------------------------------------------|----------------------------------------------|
| `src/hypermvp/global_config.py`             | Global paths, date/time formats, helpers     |
| `src/hypermvp/provider/config.py`           | Provider ETL schema, validation, performance |
| `src/hypermvp/afrr/config.py` (future)      | AFRR ETL schema, validation, performance     |
| `src/hypermvp/scrapers/scraper_config.py`   | Scraper API endpoints and settings           |

**Tip:** Import shared settings from the global config in module configs as needed.
