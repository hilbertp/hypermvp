# Provider Workflow Requirements

## Overview

This document outlines the core requirements for the provider data processing workflow, ensuring that the critical business logic and anonymized bid handling are preserved in all implementations.

## Critical Business Logic

### 1. Anonymized Bid Handling

Provider bids in the energy market are anonymized, which creates a specific challenge:

- Multiple providers can submit identical bids (e.g., 5MW at 0 EUR/MWh)
- If the same period is imported twice, duplicate bids would artificially inflate capacity
- This would severely distort marginal price calculations, leading to incorrect pricing

### 2. Date Range Management

To prevent duplicates while maintaining historical data:

- All existing data within the date range of new imports must be completely deleted
- New data must then be inserted as a complete replacement
- Data outside the imported date range must be preserved

## Core Workflow Requirements

### Loading

1. **Input Format**: Process Excel (.xlsx) files from the provider raw data directory
2. **Multiple Files**: Support loading multiple Excel files, each with multiple sheets
3. **Required Columns**: Validate the presence of the following required columns:
   - `DELIVERY_DATE`: Date/time of energy delivery
   - `PRODUCT`: Product identifier (e.g., "FRR", "RR", "FCR")
   - `ENERGY_PRICE_[EUR/MWh]`: Energy price with unit in EUR/MWh
   - `ENERGY_PRICE_PAYMENT_DIRECTION`: Direction of payment (e.g., "PROVIDER_TO_GRID", "GRID_TO_PROVIDER")
   - `ALLOCATED_CAPACITY_[MW]`: Allocated capacity with unit in MW
   - `NOTE`: Additional information (optional, but column must exist)
4. **Performance**: Optimize loading performance, using direct-to-database loading where possible

### Cleaning

1. **Date Standardization**: Ensure all date columns are properly formatted

   - Technical standards are defined in `hypermvp/config.py`, which includes:
     - `ISO_DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"`
     - `ISO_DATE_FORMAT = "%Y-%m-%d"`
     - `standardize_date_column()` helper function
   - Business standards are outlined in `documentation/datetimestandard.md`, which details:
     - Format standardization across all modules
     - Implementation approaches for different data sources
     - Specific conversion requirements for provider data
2. **Numeric Conversion**: Replace comma decimal separators with periods
3. **Payment Direction**: Negate energy prices where ENERGY_PRICE_PAYMENT_DIRECTION = 'PROVIDER_TO_GRID'
4. **Filter POS_ Products**: Exclude products with names starting with "POS_"
5. **Period Calculation**: Add a period column for time-based aggregation

### Database Update

1. **Date Range Determination**: Identify the min/max date range of new data
2. **Clean Slate**: Delete all existing data within that date range
3. **Complete Replacement**: Insert the new data for that range as a complete replacement
4. **User Confirmation**: Request confirmation before large deletions in interactive mode
5. **Metadata Tracking**: Record source file and transformation details

## Interface Requirements

1. **CLI Commands**:

   - `--load`: Load provider files into database
   - `--analyze`: Display information about loaded raw data
   - `--update`: Process the date range replacement (important for anonymized bids)
   - `--all`: Perform load, analyze, and update in sequence
2. **Configurability**:

   - Database path
   - Input directory
   - Table names
   - Archive options

## Database Schema

### Raw Data Table

The raw data table must include the following columns:

1. `DELIVERY_DATE` (VARCHAR): Original date from source files
2. `PRODUCT` (VARCHAR): Product identifier
3. `ENERGY_PRICE_[EUR/MWh]` (VARCHAR): Energy price as string
4. `ENERGY_PRICE_PAYMENT_DIRECTION` (VARCHAR): Payment direction
5. `ALLOCATED_CAPACITY_[MW]` (VARCHAR): Capacity as string
6. `NOTE` (VARCHAR): Additional notes
7. `source_file` (VARCHAR): Filename of source
8. `load_timestamp` (TIMESTAMP): When the record was loaded

### Clean Data Table

The clean data table must include the following columns:

1. `DELIVERY_DATE` (TIMESTAMP): Standardized delivery date
2. `PRODUCT` (VARCHAR): Product identifier
3. `ENERGY_PRICE__EUR_MWh_` (DOUBLE): Cleaned and normalized energy price
4. `ALLOCATED_CAPACITY__MW_` (DOUBLE): Cleaned and normalized capacity
5. `period` (TIMESTAMP): Calculated time period for grouping
6. `source_file` (VARCHAR): Tracking original source file

## Performance Considerations

1. **First Run**: Load data to raw table, clean, and replace date range
2. **Subsequent Runs**: Only process new date ranges, preserving historical data
3. **Memory Efficiency**: Avoid loading entire datasets into memory when possible
4. **SQL-Based Processing**: Leverage database capabilities for transformations

## Reporting Requirements

1. **Basic Statistics**: Row counts, date ranges, product types
2. **Performance Metrics**: Load speeds, processing times
3. **File Information**: Track which files contributed to which date ranges
4. **Verification Reports**: Validate that data before and after processing maintains integrity

## Module Structure

The workflow should maintain a clear separation of concerns:

1. `provider_loader.py`: Handles loading Excel files to database
2. `provider_db_cleaner.py`: Handles cleaning and updating data with date range management
3. `provider_cli.py`: Provides command-line interface for the workflow

This structure ensures maintainability while preserving the critical business logic around anonymized bid handling.

## Data Quality Requirements

1. **Validation Checks**:

   - Ensure delivery dates are within reasonable range
   - Ensure product identifiers follow expected patterns
   - Ensure energy prices and capacities are positive or within expected ranges
   - Check for unexpected null values
2. **Error Handling**:

   - Log errors when files don't match expected format
   - Provide detailed error messages for debugging
   - Continue processing valid files when some files have errors
