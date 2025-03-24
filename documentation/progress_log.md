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

## 2025-03-23

* **Hours worked**: 3.5h
* **Start time**: 17:00
* **End time**: 20:30
* **Branch**: main

### Source Control Changes

- **Files Modified**: 8
  - `main.py`: Enhanced workflows with better progress reporting and file archiving
  - `src/hypermvp/config.py`: Updated to use proper subdirectories for data types
  - `src/hypermvp/provider/cleaner.py`: Adjusted to work with new directory structure
  - `src/hypermvp/provider/loader.py`: Modified to use provider-specific directories
  - `.github/workflows/test.yml`: Updated for Python 3.12 compatibility and added missing dependencies
  - `.gitignore`: Added rules to exclude large provider data files and processed data
  - `pyproject.toml`: Added Beautiful Soup and responses as dependencies
  - `src/hypermvp/provider/update_provider_data.py`: Added improved progress tracking

### Features Added

- **File archiving system**: Automatically moves processed files to the archive directory
- **Enhanced progress reporting**: Added clear indicators for processing steps and file movements
- **Improved directory structure**: Separated raw data by type (aFRR, provider)
- **Git exclusion rules**: Properly configured to ignore large data files
- **Better CI/CD workflow**: Updated GitHub Actions to match local environment

### Features Changed

- **Data workflow**: Now uses isolated directories for different data types
- **Logging format**: Added clear section dividers and visual indicators for operations
- **Terminal output**: Suppressed unnecessary warnings and debug information
- **File handling**: Added verification steps after file movements

### Technical Details

- Fixed issue with OpenPyXL default style warnings
- Updated configuration to properly handle provider and AFRR directories
- Enhanced terminal output with better progress indicators
- Fixed dependency issues in CI/CD pipeline
- Implemented proper file movement with verification
- Added clear progress logging for long-running database operations
- Enhanced error handling for file operations

### Issues Solved

- **Data file management**: Implemented proper organization by type and processing stage
- **Path inconsistencies**: Standardized path handling throughout the codebase
- **Configuration exposure**: Removed excessive debug output of configuration variables
- **CI/CD failures**: Fixed missing dependencies in GitHub workflow
- **Git tracking**: Properly excluded large data files from repository
- **File organization**: Implemented clear separation between raw and processed data

### Next Steps
- build a tool to see the current state of duck db (charts of existing afrr data, provider data and marginal price calculation by week and month and year)
- add progress output for importing providers data
- do the marginal price calc for the whole month of september 2024
- Download and process October 2024 provider data 
- try runnign the analysis for the marginal price for october 2024
- do it for the whole year

## 2025-03-24

* **Hours worked**: 4h
* **Start time**: 18:00
* **End time**: 22:00
* **Branch**: main

### Source Control Changes

- **Files Modified**: 4
  - `src/hypermvp/provider/loader.py`: Enhanced with better progress indicators and ETA calculations
  - `src/hypermvp/provider/update_provider_data.py`: Fixed DuckDB integration for proper DataFrame handling
  - `src/hypermvp/dashboard/app.py`: Added database summary generation functionality
  - `pyproject.toml`: Updated with additional dependencies (tqdm, psutil)

- **Files Added**: 1
  - `data/03_output/dashboard-summary/`: Created directory for database summaries and diagnostic reports

### Features Added

- **Enhanced Excel Loading System**: Implemented time-based progress indicators with accurate ETA predictions
  - Supports multi-sheet Excel files with proper loading of all sheets
  - Shows elapsed time during long file loads
  - Dynamically adjusts time estimates based on observed performance
  - Reports date ranges and row counts for each loaded sheet
  
- **Database Diagnostic Tools**: Added functionality to generate comprehensive database summaries
  - Creates detailed text and JSON reports of database contents
  - Shows table schemas, row counts, and date ranges
  - Provides sample data excerpts from each table
  - Timestamps reports for tracking database evolution

- **DuckDB Integration Fix**: Corrected approach for inserting Pandas DataFrames into DuckDB
  - Switched to using native append method for reliable insertion
  - Resolves "Not implemented Error" when handling large DataFrames
  - Maintains period-based incremental update approach

### Technical Details

- Fixed critical issue with DuckDB parameter binding for DataFrames
- Implemented adaptive time estimation based on sheet loading performance
- Added complete per-sheet progress tracking with elapsed and estimated remaining times
- Successfully processed entire month of provider data (3.2M+ rows across 4 sheets)
- Enhanced terminal feedback during long-running operations
- Created specialized output directory for database diagnostics

### Issues Solved

- **Poor Progress Feedback**: Resolved with dynamic progress indicators during long operations
- **DuckDB Integration Error**: Fixed by using the proper append method for DataFrame insertion
- **Multi-Sheet Excel Loading**: Now properly loading all sheets from provider Excel files
- **Database Insight**: Added tools to better understand current database contents
- **Missing Date Ranges**: Fixed by properly identifying and loading all sheets in provider files

### Next Steps

- Run marginal price calculations for the entire month of September 2024 now that full data is available
- look at dashboard and decide whether it is useful as is or needs adaption
- Download and process October 2024 provider data
- Compare September and October marginal price distributions
- Enhance dashboard with comparative analytics across months
- Implement more advanced marginal price visualization tools