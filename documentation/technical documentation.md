# HyperMVP Technical Documentation

## Current Structure and Workflow

### aFRR Data

1. **Loading**:
   - aFRR data is loaded from a CSV file using `afrr/loader.py`.
   - The `Datum` column is converted to a proper date format.

2. **Cleaning**:
   - The loaded data is cleaned using `afrr/cleaner.py`:
     - Column names are stripped of unnecessary spaces.
     - Filters for required columns: `Datum`, `von`, `bis`, and `50Hertz (Negativ)`.

3. **Dumping**:
   - The cleaned data is saved as a CSV file in the processed data directory using `afrr/dumper.py`.
   - The file includes a timestamp in its name for traceability.

4. **(Planned)** Saving to DuckDB:
   - Saving cleaned aFRR data to a DuckDB database is not yet implemented.
   - This step will centralize and prepare the data for querying and analysis.

---

### Provider Data

1. **Loading**:
   - Provider data is loaded from Excel files using `provider/loader.py`.
   - Ensures that only `.xlsx` files are processed, raising errors for unsupported formats.

2. **Cleaning**:
   - The loaded data is cleaned using `provider/cleaner.py`:
     - Checks for required columns and raises errors if any are missing.
     - Filters out rows with `POS_*` in the `PRODUCT` column.
     - Adjusts energy prices based on the payment direction (e.g., negative for `PROVIDER_TO_GRID`).
     - Drops unnecessary columns like `NOTE` and sorts the data by date, product, and price.

3. **Dumping**:
   - The cleaned data is saved as a CSV file using `provider/dump_csv.py`.

4. **Saving to DuckDB**:
   - Cleaned provider data is read from the processed CSV files.
   - All files are combined into a single DataFrame and saved to a DuckDB database using `provider/save_to_duckdb.py`.

---

## What We Have Done
- Built a modular structure for loading, cleaning, and saving both aFRR and provider data.
- Successfully implemented workflows for:
  - Loading and cleaning both data types.
  - Saving cleaned data as CSV files.
  - Combining and saving provider data into DuckDB.

---

## What Needs to Be Done Next

To achieve the goals outlined in the functional documentation, we need to:

1. **Centralize All Cleaned Data in DuckDB**:
   - Extend the aFRR workflow to save cleaned data to DuckDB.
   - Define tables for aFRR and provider data (`afrr_data` and `provider_data`).

2. **Integrate Data for Analysis**:
   - Merge aFRR and provider data based on shared keys like time intervals or dates.
   - Align the data structure to facilitate calculations (e.g., marginal price determination).

3. **Implement Marginal Price Calculations**:
   - Build a script to calculate marginal prices based on:
     - Sorted provider offers.
     - Aggregated demand data from aFRR.
   - Ensure results are stored back into the DuckDB database.

4. **Automate Data Validation**:
   - Create validation routines to check for anomalies in the cleaned data (e.g., missing values, invalid formats).

5. **Reporting and Forecasting** (Optional, Based on Goals):
   - Develop scripts to generate reports for:
     - Marginal price trends.
     - Energy control summaries.
   - Optionally, prepare the groundwork for predictive models.

6. **Documentation and Testing**:
   - Document technical and functional aspects of the implementation.
   - Write unit tests for all scripts to ensure data consistency and workflow reliability.
