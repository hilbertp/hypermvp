"""
Provider table cleaning logic for DuckDB.
Implements US001-provider_table_cleaner.md requirements.
"""
import duckdb
import logging
from pathlib import Path

PROVIDER_RAW_TABLE = "provider_raw"
PROVIDER_CLEAN_TABLE = "provider_clean"

CLEAN_SQL = f'''
CREATE OR REPLACE TABLE {PROVIDER_CLEAN_TABLE} AS
SELECT
    DELIVERY_DATE,
    PRODUCT,
    CASE
        WHEN PAYMENT_DIRECTION = 'PROVIDER_TO_GRID'
        THEN -1 * ENERGY_PRICE_EUR_MWh
        ELSE ENERGY_PRICE_EUR_MWh
    END AS ENERGY_PRICE_EUR_MWh,
    PAYMENT_DIRECTION,
    ALLOCATED_CAPACITY_MW,
    NOTE,
    source_file,
    load_timestamp
FROM {PROVIDER_RAW_TABLE}
WHERE NOT PRODUCT LIKE 'POS_%'
ORDER BY DELIVERY_DATE ASC, ENERGY_PRICE_EUR_MWh ASC;
'''

def clean_provider_table(db_path: str):
    """
    Cleans the provider_raw table in DuckDB and writes the result to provider_clean.
    - Removes rows where PRODUCT starts with 'POS_'.
    - Multiplies ENERGY_PRICE_EUR_MWh by -1 where PAYMENT_DIRECTION is 'PROVIDER_TO_GRID'.
    - Sorts by DELIVERY_DATE (chronological), then ENERGY_PRICE_EUR_MWh (ascending).
    """
    if not Path(db_path).exists():
        raise FileNotFoundError(f"DuckDB database not found: {db_path}")
    logging.info(f"Cleaning provider table in {db_path} ...")
    con = duckdb.connect(db_path)
    con.execute(CLEAN_SQL)
    con.close()
    logging.info("Provider table cleaned and saved as 'provider_clean'.")
