# Progress Log

## 2025-03-16

* **Hours worked**: 4h
* **Start time**: 15:00
* **End time**: 19:00**Branch**: main

### Source Control Changes

- **Files Modified**: 7

  - main.py: Updated workflow to prevent duplicate loading, added proper column type handling
  - update_provider_data.py: Implemented explicit schema creation, period-based updates
  - cleaner.py: Maintained filtering logic, improved column name handling
  - loader.py: Removed stray function calls causing duplicate loading
  - save_to_duckdb.py: Updated for consistency with provider approach
  - config.py: Added new output directory paths
  - test_save_to_duckdb.py: Updated test cases for new implementation
- **Files Added**: 8+

  - .gitkeep: Created new directory structure
  - snapshots: Added directory for database snapshots
  - utils: Created utility module with versioning functionality
  - db_versioning.py: Implemented snapshot and metadata functions
  - utils: Added corresponding test files for utilities
- **Files Removed**: 13

  - `src/hypermvp/provider/dump_csv.py`: Removed in favor of direct database operations
  - `tests/provider/test_dump_csv.py`: Corresponding test file removed
  - Multiple processed CSV files from data/02_processed/
  - Previous DuckDB files from incorrect location

### Features Added

- DuckDB integration for persistent structured data storage
- Database versioning with snapshot functionality for recovery
- Explicit schema handling to prevent type inference issues with mixed data
- Period-based incremental data update mechanism
- New directory structure for database storage
- Type safety checks to ensure consistent data types

### Features Changed

- Replaced CSV export with direct DuckDB database updates
- Fixed schema mismatch issues when handling text vs. numeric fields
- Restructured workflow to avoid duplicate file loading
- Enhanced error handling throughout the pipeline
- Improved logging for debugging and traceability

### Technical Details

- Fixed critical issue where "aFRR" string was being inserted into INT32 column
- Implemented explicit CREATE TABLE with proper types instead of relying on inference
- Created adapter pattern for different data sources to use consistent database approach
- Added column type forcing before database operations to ensure consistency

### Next Steps

- Evaluate snapshot retention policy to prevent database size bloat
- Add compression for snapshots
- Consider implementing VACUUM command for space reclamation

## 2025-03-19 and -18

* **Hours worked**: 8h
* **Branch**: main

### Algorithm Refinements

- **Critical algorithm revision**: Revised understanding of the market structure:

  - Providers are legally required to offer services across entire 4-hour blocks
  - However, the actual provider pool is specific to each 15-minute interval
  - This granularity enables precise mapping between aFRR demand and provider offers
  - Fixed algorithm to reflect this key insight from the regulatory documentation
- **Product code mapping corrected**:

  - Discovered product codes (NEG_001 through NEG_096) map directly to 15-minute intervals
  - Each code represents a specific quarter-hour of the day (e.g., NEG_033 = 08:00-08:15)
  - Implemented proper conversion between timestamp and product code
  - Updated algorithm to query offers for the exact 15-minute product code
- **Regulatory compliance investigation**:

  - Created `analyze_bid_distribution.py` to investigate provider behavior
  - Analyzed bid counts across 15-minute intervals within each 4-hour block
  - Identified fluctuations in provider participation despite regulatory requirements
  - Enabled analysis to understand market dynamics and potential regulatory gaps

### Source Control Changes

- **Files Modified**: 5

  - `src/hypermvp/analysis/marginal_price.py`: Updated algorithm to handle NULL values for intervals with no activation
  - `src/hypermvp/analysis/plot_marginal_prices.py`: Enhanced visualization to properly show gaps for intervals with no activation
  - `src/hypermvp/analysis/view_marginal_prices.py`: Added viewer for marginal price data
  - `main.py`: Added support for analysis workflow
  - `src/hypermvp/config.py`: Verified output directory configuration
- **Files Added**: 2

  - `src/hypermvp/analysis/debug_offers.py`: Created tool for debugging provider offers
  - `src/hypermvp/analysis/analyze_bid_distribution.py`: Added analysis for investigating bid distribution across intervals

### Features Added

- **Analysis workflow** in main.py for end-to-end marginal price calculation
- **Improved marginal price visualization** with proper gap handling for intervals with no activation
- **Provider offer debugging tools** to inspect bid data
- **Bid distribution analysis** to investigate regulatory compliance of 4-hour blocks

### Features Changed

- **Marginal price calculation algorithm** updated to:

  - Include all intervals with NULL prices for periods with no activation
  - Correctly map 15-minute products (NEG_001 through NEG_096)
  - Follow the merit order principle for providers within each 15-minute interval
  - Handle edge cases like zero-priced offers
- **Module organization improved**:

  - Separated core calculation logic (`marginal_price.py`) from CLI interface (`marginal_price_cli.py`)
  - Added proper function and module documentation
  - Ensured consistent naming conventions

### Technical Details

- Fixed critical issue with SQL comments in DuckDB queries (replaced Python-style # comments with SQL-style -- comments)
- Implemented correct mapping between quarter-hour intervals and provider product codes
- Added mechanisms to distinguish between zero prices (valid market data) and NULL prices (no activation)
- Verified NEG product codes map to specific 15-minute intervals rather than 4-hour blocks
- Implemented granular analysis of bid distribution to investigate regulatory compliance
- Created enhanced visualization that properly shows gaps for intervals with no activation data

### Issues Solved

- **Market granularity misunderstanding**: Corrected algorithm to reflect that provider pools are specific to 15-minute intervals, not fixed across 4-hour blocks
- **Product code mapping**: Fixed the mapping between timestamps and NEG_XXX product codes
- **SQL syntax errors**: Fixed improper comment syntax in SQL queries
- **Misleading zero values**: Replaced with NULL/gaps in visualization for intervals with no activation
- **Redundant module names**: Reorganized and renamed modules for clarity
- **Missing intervals**: Now including all 96 quarter-hours with proper NULL handling
- **Data mapping**: Correctly mapped aFRR activation data to corresponding provider product codes

### Next Steps

- Extend analysis to additional dates beyond Sept 1
- Implement automated validation checks for marginal price calculations
- Add documentation for the updated algorithm and workflow
- add unit tests for the new modules
- try scraping the rest of the data from online becuase clicking a few thousand times to get daily provider lsits is gonna get old real soon

## 2025-03-22

* **Hours worked**: 4h
* **Start time**: 19:00
* **End time**: 23:00
* **Branch**: main

### Source Control Changes

- **Files Added**: 5

  - `src/hypermvp/scrapers/base_scraper.py`: Created abstract base class with common scraping functionality
  - `src/hypermvp/scrapers/afrr_scraper.py`: Implemented specialized scraper for aFRR activation data
  - `src/hypermvp/scrapers/cli.py`: Added command-line interface for flexible date range processing
  - `src/hypermvp/scrapers/config.py`: Created configuration for scrapers with user agents and retry settings
  - `src/hypermvp/scrapers/__init__.py`: Package initialization file

### Features Added

- **Web scraping infrastructure** with robust error handling and retry logic
- **AFRR data scraper** for netztransparenz.de with form submission capability
- **Flexible CLI** with configurable date ranges and increment options
- **Browser identity rotation** to avoid detection
- **Automated download** capability for bulk historical data

### Technical Details

- Implemented robust retry logic with exponential backoff
- Created form submission handler for ASP.NET with hidden field extraction
- Added user agent rotation to prevent blocking
- Built flexible date range generator for different time increments (day/month/year)
- Successfully downloaded full year of aFRR data for 2024
- Fixed issue with site's form structure and VIEWSTATE handling
- Implemented proper file saving with path handling

### Next Steps

- Implement provider scraper for regelleistung.net
- Add data validation for downloaded files
- Create data transformation pipeline for scraped data
