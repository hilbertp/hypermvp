# HyperMVP Technical Documentation

## Current Structure and Workflow

### aFRR Data

1. **Loading**:
   - aFRR data is loaded from CSV files using `afrr/loader.py`.
   - The `Datum` column is converted to a proper date format.

2. **Cleaning**:
   - The loaded data is cleaned using `afrr/cleaner.py`:
     - Column names are trimmed.
     - Required columns (`Datum`, `von`, `bis`, and `50Hertz (Negativ)`) are selected.
     - Only negative adjustment values (deltas) are retained, representing the correction from planned to measured energy.

3. **Dumping**:
   - The cleaned data is saved as a CSV file in the processed data directory using `afrr/dumper.py`.
   - The file is stamped with a timestamp to ensure traceability.

4. **Saving to DuckDB**:
   - Cleaned aFRR data is saved into DuckDB using `afrr/save_to_duckdb.py`.
   - Each 15-minute interval in the database holds one adjustment delta, which reflects the deviation from the planned energy value. Overwriting existing values allows corrections to past intervals.

---

### Provider Data

1. **Loading**:
   - Provider data is loaded from Excel files using `provider/loader.py`.
   - Only `.xlsx` files are accepted, ensuring consistent input.

2. **Cleaning**:
   - The data is cleaned via `provider/cleaner.py`:
     - The routine validates required columns and filters out rows with `POS_*` in the `PRODUCT` column.
     - Energy prices are adjusted based on payment direction (made negative for `PROVIDER_TO_GRID`).
     - Unneeded columns (e.g., `NOTE`) are dropped, and the data is sorted by `DELIVERY_DATE`, `PRODUCT`, and `ENERGY_PRICE_[EUR/MWh]`.

3. **4‑Hour Interval Update Process**:
   - Instead of processing weekly batches, the updated workflow:
     - Loads and cleans raw XLSX files directly from the input folder.
     - Groups the cleaned data into 4‑hour periods based on the DELIVERY_DATE (with periods starting at 00:00, 04:00, etc.).
     - For each 4‑hour period, any existing data in DuckDB is deleted before inserting the complete new dataset for that period.
   - This delete–then–insert mechanism guarantees that duplicate period imports do not occur, while maintaining all valid bid entries.

4. **Saving to DuckDB**:
   - The updated provider workflow is executed through `provider/update_provider_data.py`.
   - This script ensures the DuckDB table always reflects the most current provider bids on a 4‑hour interval basis.

---

## What We Have Done

- Developed a modular pipeline for aFRR and provider data covering loading, cleaning, and saving.
- Implemented a robust provider data workflow that groups data into weekly batches and updates DuckDB by replacing old data with new weekly datasets to avoid duplicates.
- Established a system for aFRR data that maintains one accurate adjustment delta per 15-minute interval, with overwrite capability to correct past values.
- Centralized processed data in DuckDB for streamlined querying and market analysis.

---

## What Needs to Be Done Next

1. **Finalize aFRR Data Integration**:
   - Complete and test the process to save cleaned aFRR data into DuckDB for direct querying.

2. **Data Integration for Analysis**:
   - Develop scripts to merge aFRR adjustments and provider bid data based on common time intervals for comprehensive market analysis.

3. **Implement Marginal Price Calculations**:
   - Build routines to calculate marginal prices by merging sorted provider bids with corresponding aFRR adjustments.

4. **Automate Data Validation & Reporting**:
   - Create automated tests to validate data integrity across the workflow.
   - Develop reporting functionality to provide insights on market performance and pricing trends.
