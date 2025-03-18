import os
import time
import logging
import pandas as pd
import duckdb
from hypermvp.utils.db_versioning import create_duckdb_snapshot, add_version_metadata

def update_provider_data(cleaned_data, db_path, table_name="provider_data"):
    """
    Create or update the provider_data table in DuckDB using the cleaned DataFrame.
    Uses explicit CREATE TABLE instead of inferring types from DataFrame.
    """
    start_time = time.time()
    con = None
    try:
        if isinstance(cleaned_data, str):
            raise ValueError("Expected a DataFrame for cleaned_data, but got a string.")

        # Print DataFrame columns and types for debugging
        logging.info("DataFrame columns and types before update:")
        for col, dtype in zip(cleaned_data.columns, cleaned_data.dtypes):
            logging.info(f"Column {col}: {dtype}")

        logging.info("Using database path: %s", db_path)
        create_duckdb_snapshot(db_path)
        con = duckdb.connect(database=db_path, read_only=False)
        add_version_metadata(con, [table_name], table_name + "_update")

        # Force DataFrame column types (before any DuckDB operations)
        cleaned_data["PRODUCT"] = cleaned_data["PRODUCT"].astype(str)
        if "TYPE_OF_RESERVES" in cleaned_data.columns:
            cleaned_data["TYPE_OF_RESERVES"] = cleaned_data["TYPE_OF_RESERVES"].astype(str)
        
        # Check if table exists
        result = con.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?", [table_name]
        ).fetchone()

        # Either drop existing table or create new one with explicit schema
        if result:
            logging.info(f"Dropping existing table {table_name}")
            con.execute(f"DROP TABLE {table_name}")
        
        # Create table with EXPLICIT types instead of using DataFrame inference
        # IMPORTANT: Use the actual column names as they appear in the DataFrame
        logging.info(f"Creating table {table_name} with explicit schema")
        create_table_sql = f"""
        CREATE TABLE {table_name} (
            DELIVERY_DATE TIMESTAMP,
            PRODUCT VARCHAR,
            TYPE_OF_RESERVES VARCHAR,
            ENERGY_PRICE__EUR_MWh_ DOUBLE,
            OFFERED_CAPACITY__MW_ DOUBLE,
            ALLOCATED_CAPACITY__MW_ DOUBLE,
            COUNTRY VARCHAR,
            period TIMESTAMP
        )
        """
        con.execute(create_table_sql)
        logging.info(f"Table {table_name} created with explicit schema")

        # Insert data using explicit column names - MUST MATCH the DataFrame column names
        for period, group in cleaned_data.groupby("period"):
            logging.info(f"Processing period {period}")
            
            # Explicitly register with column names
            con.register("temp_df", group)
            
            # Insert with explicit column mapping - using ACTUAL column names
            insert_sql = f"""
            INSERT INTO {table_name} (
                DELIVERY_DATE, PRODUCT, TYPE_OF_RESERVES, 
                ENERGY_PRICE__EUR_MWh_, OFFERED_CAPACITY__MW_, ALLOCATED_CAPACITY__MW_,
                COUNTRY, period
            )
            SELECT 
                DELIVERY_DATE, PRODUCT, TYPE_OF_RESERVES, 
                ENERGY_PRICE__EUR_MWh_, OFFERED_CAPACITY__MW_, ALLOCATED_CAPACITY__MW_,
                COUNTRY, period 
            FROM temp_df
            """
            con.execute(insert_sql)
            con.unregister("temp_df")

        con.commit()
        logging.info("Provider data updated. Time taken: %.2f seconds", time.time() - start_time)

        from hypermvp.utils.db_versioning import vacuum_database
        try:
            vacuum_database(con)  # Here 'con' is defined because it's in the update_provider_data.py scope
        except Exception as e:
            logging.warning(f"Vacuum failed: {e}")

    except Exception as e:
        logging.error("Error in update_provider_data: %s", e)
        raise
    finally:
        if con and not getattr(con, "closed", lambda: False)():
            try:
                con.close()
                logging.debug("Database connection closed properly")
            except Exception as e:
                logging.warning("Error closing connection: %s", e)