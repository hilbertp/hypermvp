# Importing Data to the Database

## 1. Provider Workflow

**Purpose:**  
Loads all provider Excel files from the raw data directory into DuckDB for further analysis.

**How to Run:**

1. Place your provider Excel files (`.xlsx`) in the directory:  
   `/home/philly/hypermvp/data/01_raw/provider/`

2. Open a terminal and navigate to the project root:

   ```bash

   cd /home/philly/hypermvp

   ```

3. Run the provider workflow:

   ```bash

   python main.py --workflow provider

   ```

**What Happens:**  

- All `.xlsx` files in the provider raw data directory are loaded into DuckDB.
- The process logs the number of files, sheets, and rows loaded.
- No filtering or cleaning is applied to the NOTE column; all data is imported as-is.

---

## 2. AFRR Workflow

**Purpose:**  
Loads and cleans aFRR CSV files, then saves them into DuckDB for analysis.

**How to Run:**

1. Place your aFRR CSV files in the directory:  
   `/home/philly/hypermvp/data/01_raw/afrr/`

2. Open a terminal and navigate to the project root:

   ```bash

   cd /home/philly/hypermvp

   ```

3. Run the AFRR workflow:

   ```bash

   python main.py --workflow afrr

   ```

   - To process a specific month and year:

     ```bash
     python main.py --workflow afrr --month 9 --year 2024
     ```

   - To process a specific file:

     ```bash
     python main.py --workflow afrr --file path/to/yourfile.csv
     ```

**What Happens:**  

- The workflow loads all (or specified) aFRR CSV files.
- Data is cleaned and saved into DuckDB for further analysis.

---

## Additional Notes

- All workflows can be run together using:

  ```bash

  python main.py --workflow all

  ```

- For help and available options:

  ```bash

  python main.py --help

  ```

---

For more details, see the documentation folder or contact the developer.
