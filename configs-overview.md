# Configuration Index

| Config File                                    | Purpose                                      |
|------------------------------------------------|----------------------------------------------|
| `src/hypermvp/global_config.py`                | Global paths, date/time formats, helpers     |
| `src/hypermvp/provider/provider_etl_config.py` | Provider ETL schema, validation, performance |
| `src/hypermvp/afrr/config.py` (future)         | AFRR ETL schema, validation, performance     |
| `src/hypermvp/scrapers/scraper_config.py`      | Scraper API endpoints and settings           |
| `tests/tests_config.py`                        | Test-specific paths and settings             |
| `pyproject.toml`                               | Project dependencies and build settings      |

**Note:** Always use `pyproject.toml` for dependency management rather than direct pip installation.
