#!/usr/bin/env python3
"""
SharePoint Export Pipeline - Standalone Script

ETL pipeline that:
1. Extracts data from SharePoint CSV and Salesforce Excel exports
2. Transforms data with cleaning, product explosion, and metrics calculation
3. Loads data into Snowflake with incremental or full refresh
4. Logs all operations to PIPELINE_RUN_HISTORY audit table

Usage:
    python sharepoint_pipeline.py [--full-refresh] [--dry-run]

Options:
    --full-refresh: Use full refresh instead of incremental MERGE
    --dry-run: Skip Snowflake upload (testing only)
"""

import pandas as pd
import numpy as np
from pathlib import Path
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
import os
from datetime import datetime
import logging
from typing import List, Dict, Tuple
import uuid
import argparse
import sys

# Import product mappings from separate file
from product_mappings import PRODUCT_CONFIGS

# ============================================================================
# LOGGING CONFIGURATION
# ============================================================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# ============================================================================
# SNOWFLAKE CONFIGURATION
# ============================================================================

SNOWFLAKE_CONFIG = {
    'account': "uhgdwaas.east-us-2.azure",
    'user': os.getenv('SF_USERNAME'),
    'password': os.getenv('SF_PW'),
    'role': "AZU_SDRP_CSZNB_PRD_DEVELOPER_ROLE",
    'warehouse': "CSZNB_PRD_ANALYTICS_XS_WH",
    'database': 'CSZNB_PRD_OA_DEV_DB',
    'schema': 'BASE'
}

# ============================================================================
# TABLE NAMES
# ============================================================================

SOURCE_TABLE = 'SHAREPOINT_ANALYTIC_REQUESTS'
TARGET_TABLE = 'FOCUSED_ANALYTIC_REQUESTS'
SALESFORCE_TABLE = 'SALESFORCE_INITIATIVES'
AUDIT_TABLE = 'PIPELINE_RUN_HISTORY'

# ============================================================================
# FILE PATHS
# ============================================================================

SHAREPOINT_EXPORT_PATH = Path.home() / "Library/CloudStorage/OneDrive-UHG/Projects/SharePoint/exports/sharepoint_requests.csv"
SALESFORCE_EXPORT_PATH = Path.home() / "Library/CloudStorage/OneDrive-UHG/Projects/SharePoint/exports/salesforce_exports.xlsx"

# ============================================================================
# BUSINESS LOGIC CONSTANTS
# ============================================================================

OPEN_STATUS = ['Not Started', 'In Progress', 'Waiting']
DAYS_ON_STATUS_THRESHOLD = 14

# Client type mapping
CLIENT_TYPE_MAPPING = {
    '1': 'Optum Direct NBEA',
    '2': 'Optum/UHC Cross Carrier NBEA',
    '3': 'UHC NBEA',
    '4': 'Opum Direct',
    '5': 'UHC Cross Carrier',
    '6': 'Prospective',
    '7': 'N/A',
    '8': 'N/A'
}

# Boolean columns
BOOLEAN_COLUMNS = [
    "BARIATRIC", "BH", "CGP", "CSP", "DM", "KIDNEY", "TRANSPLANT", "CHD", "VAD",
    "NICU", "MATERNITY", "FERTILITY", "FOCUSED_ANALYTICS", "OUTPATIENT_REHAB",
    "OHS", "FCR_PROFESSIONAL", "CKS", "CKD", "CARDIOLOGY", "DME", "INPATIENT_REHAB",
    "SPINE_PAIN_JOINT", "SPECIALTY_REDIRECTION", "MEDICAL_REBATES_ONBOARDING",
    "BRS", "DATA_INTAKE", "DATA_QAVC", "SPECIALTY_FUSION", "MBO_IMPLEMENTATION",
    "MSPN_IMPLEMENTATION", "VARIABLE_COPAY", "ACCUMULATOR_ADJUSTMENT",
    "SMGP", "SGP", "SECOND_MD", "KAIA", "MBO_PRESALES", "MSPN_PRESALES",
    "MEDICAL_REBATES_PREDEAL", "MAVEN", "CAR_REPORT", "MSK_MSS",
    "FCR_FACILITY", "RADIATION_ONCOLOGY", "VIRTA_HEALTH", "SMO_PRESALES",
    "SMO_IMPLEMENTATION", "SBO_HEALTH_TRUST_PRESALES", "SBO_HEALTH_TRUST_IMPLEMENTATION",
    "CORE_SBO", "ENHANCE_SBO", "OPTUM_GUIDE", "CYLINDER_HEALTH", "RESOURCE_BRIDGE",
    "PHS", "CANCER", "PODIMETRICS", "CAR-T", "HELLO_HEART", "PHARMACY_GROWTH_PRESALE", "PHARMACY_GROWTH_EXISTING"
]

# ============================================================================
# DATA EXTRACTION FUNCTIONS
# ============================================================================

def extract_sharepoint(file_path: Path) -> pd.DataFrame:
    """Load SharePoint CSV and return normalized DataFrame"""
    logger.info(f"Loading SharePoint export from {file_path}...")

    try:
        # Use low_memory=False to prevent mixed type warnings
        df = pd.read_csv(file_path, low_memory=False)
    except FileNotFoundError:
        logger.error(f"SharePoint file not found at {file_path}")
        raise

    # Normalize column names
    df.columns = df.columns.str.upper()

    logger.info(f"Loaded {len(df)} SharePoint records")
    logger.info(f"Columns: {df.columns.tolist()[:10]}...")  # Show first 10 columns

    return df


def extract_salesforce(file_path: Path) -> pd.DataFrame:
    """
    Load Salesforce Excel export with improved error handling.
    Returns empty DataFrame if file missing or has issues.
    """
    logger.info(f"Loading Salesforce export from {file_path}...")

    try:
        # Read Excel file (assuming first sheet)
        df = pd.read_excel(file_path, sheet_name=0)

        # Check if DataFrame is empty
        if df.empty:
            logger.warning("Salesforce file is empty")
            return pd.DataFrame()

        # Normalize column names
        df.columns = df.columns.str.upper()

        # Convert any Excel date serial numbers to proper dates
        for col in df.columns:
            # Check if column might be dates (ends with _DATE or contains 'DATE')
            if 'DATE' in col.upper():
                try:
                    # Try to convert Excel serial dates
                    df[col] = pd.to_datetime(df[col], errors='coerce')
                except:
                    pass

        logger.info(f"Loaded {len(df)} Salesforce records with {len(df.columns)} columns")
        logger.info(f"Columns: {df.columns.tolist()}")

        return df

    except FileNotFoundError:
        logger.warning(f"Salesforce file not found at {file_path} - skipping Salesforce load")
        return pd.DataFrame()  # Return completely empty DataFrame
    except Exception as e:
        logger.error(f"Error reading Salesforce file: {e}")
        return pd.DataFrame()  # Return completely empty DataFrame


# ============================================================================
# DATA CLEANING & TRANSFORMATION FUNCTIONS
# ============================================================================

def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and normalize raw data"""
    logger.info("Cleaning and normalizing data...")

    # Map client types
    if 'CLIENT_TYPE_DETAIL' in df.columns:
        df['CLIENT_TYPE_DETAIL'] = (
            df['CLIENT_TYPE_DETAIL']
            .astype(str)
            .map(CLIENT_TYPE_MAPPING)
            .fillna(df['CLIENT_TYPE_DETAIL'])
        )

    # Fill null values in boolean columns (only those that exist)
    existing_bool_cols = [col for col in BOOLEAN_COLUMNS if col in df.columns]
    if existing_bool_cols:
        df[existing_bool_cols] = df[existing_bool_cols].fillna(False)

    # Populate PRODUCTS_REQUESTED from boolean columns where null (vectorized approach)
    if 'PRODUCTS_REQUESTED' in df.columns and existing_bool_cols:
        mask_null = df['PRODUCTS_REQUESTED'].isnull()
        if mask_null.any():
            # Vectorized approach: create list of selected column names for each row
            df.loc[mask_null, 'PRODUCTS_REQUESTED'] = (
                df.loc[mask_null, existing_bool_cols]
                .apply(lambda row: ', '.join(row.index[row].str.title()) if row.any() else 'None', axis=1)
            )

    logger.info("Data cleaning complete")
    return df


def _get_column_value(row, possible_names):
    """Helper to get column value trying multiple possible column names"""
    for name in possible_names:
        if name in row.index:
            return row[name]
    return None


def _explode_products(df: pd.DataFrame) -> pd.DataFrame:
    """Explode wide-format data into product-level records with flexible column mapping"""
    records = []

    # Log ALL columns for debugging
    logger.info(f"Available columns in source data ({len(df.columns)} total):")
    logger.info(f"  {df.columns.tolist()}")

    # Define flexible column mappings (multiple possible names for each field)
    COLUMN_MAPPINGS = {
        'ID': ['ID', 'REQUEST_ID'],
        'TITLE': ['TITLE', 'REQUEST_TITLE', 'NAME'],
        'REQUEST_DATE': ['REQUEST_DATE', 'REQUESTDATE', 'CREATED', 'CREATED_DATE'],
        'CLIENT': ['CLIENT', 'CLIENT_NAME', 'CUSTOMER'],
        'MARKET': ['MARKET', 'MARKET_NAME'],
        'REQUESTOR': ['REQUESTOR', 'REQUESTER', 'REQUESTED_BY'],
        'CLIENT_TYPE_DETAIL': ['CLIENT_TYPE_DETAIL', 'CLIENT_TYPE', 'CLIENTTYPE'],
        'OVERALL_STATUS': ['OVERALL_STATUS', 'STATUS', 'REQUEST_STATUS', 'OVERALLSTATUS'],
        'PRODUCTS_REQUESTED': ['PRODUCTS_REQUESTED', 'PRODUCTS', 'PRODUCTSREQUESTED'],
        'SALESFORCE_ID': ['SALESFORCE_ID', 'SALESFORCEID', 'SF_ID'],
        'STATUS_CHANGE_DATE': ['STATUS_CHANGE_DATE', 'STATUSCHANGEDATE', 'MODIFIED', 'MODIFIED_DATE'],
        'CLOSED_DATE': ['CLOSED_DATE', 'CLOSEDDATE', 'DATE_CLOSED'],
        'PTRR': ['PTRR']
    }

    # Check which columns are actually present
    for field_name, possible_cols in COLUMN_MAPPINGS.items():
        found = [col for col in possible_cols if col in df.columns]
        if found:
            logger.info(f"  ✓ {field_name} mapped to: {found[0]}")
        else:
            logger.warning(f"  ✗ {field_name} NOT FOUND (tried: {possible_cols})")

    for _, row in df.iterrows():
        for product_name, category, field, start_col, end_col, status_col in PRODUCT_CONFIGS:
            # Check if this product is requested
            if field in row.index and row[field]:
                record = {
                    'ID': _get_column_value(row, COLUMN_MAPPINGS['ID']),
                    'TITLE': _get_column_value(row, COLUMN_MAPPINGS['TITLE']),
                    'REQUEST_DATE': _get_column_value(row, COLUMN_MAPPINGS['REQUEST_DATE']),
                    'CLIENT': _get_column_value(row, COLUMN_MAPPINGS['CLIENT']),
                    'MARKET': _get_column_value(row, COLUMN_MAPPINGS['MARKET']),
                    'REQUESTOR': _get_column_value(row, COLUMN_MAPPINGS['REQUESTOR']),
                    'CLIENT_TYPE': _get_column_value(row, COLUMN_MAPPINGS['CLIENT_TYPE_DETAIL']),
                    'OVERALL_STATUS': _get_column_value(row, COLUMN_MAPPINGS['OVERALL_STATUS']),
                    'PRODUCTS_REQUESTED': _get_column_value(row, COLUMN_MAPPINGS['PRODUCTS_REQUESTED']),
                    'SALESFORCE_ID': _get_column_value(row, COLUMN_MAPPINGS['SALESFORCE_ID']),
                    'PRODUCT': product_name,
                    'PRODUCT_CATEGORY': category,
                    'START_DATE': row.get(start_col),
                    'COMPLETE_DATE': row.get(end_col),
                    'STATUS': row.get(status_col),
                    'STATUS_CHANGE_DATE': _get_column_value(row, COLUMN_MAPPINGS['STATUS_CHANGE_DATE']),
                    'CLOSED_DATE': _get_column_value(row, COLUMN_MAPPINGS['CLOSED_DATE']),
                    'PTRR': _get_column_value(row, COLUMN_MAPPINGS['PTRR'])
                }
                records.append(record)

    df_products = pd.DataFrame(records)
    logger.info(f"Exploded {len(df)} requests into {len(df_products)} product records")

    # Log sample of first record for debugging
    if len(df_products) > 0:
        logger.info(f"Sample transformed record:")
        logger.info(f"  ID: {df_products.iloc[0]['ID']}")
        logger.info(f"  TITLE: {df_products.iloc[0]['TITLE']}")
        logger.info(f"  CLIENT: {df_products.iloc[0]['CLIENT']}")
        logger.info(f"  OVERALL_STATUS: {df_products.iloc[0]['OVERALL_STATUS']}")
        logger.info(f"  PRODUCT: {df_products.iloc[0]['PRODUCT']}")

    return df_products


def _enrich_with_salesforce(df: pd.DataFrame, df_salesforce: pd.DataFrame) -> pd.DataFrame:
    """Join with Salesforce data to enrich records"""
    if df_salesforce is None or df_salesforce.empty:
        logger.warning("No Salesforce data available for enrichment")
        return df

    # Normalize Salesforce column names
    df_salesforce.columns = df_salesforce.columns.str.upper()

    # Merge on SALESFORCE_ID if available
    if 'SALESFORCE_ID' in df.columns and 'SALESFORCE_ID' in df_salesforce.columns:
        df = df.merge(
            df_salesforce[['SALESFORCE_ID', 'HAS_VALUE']],
            on='SALESFORCE_ID',
            how='left'
        )
        logger.info("Enriched with Salesforce data")

    return df


def _calculate_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate derived metrics"""
    today = pd.Timestamp.now()

    # Convert date columns to datetime
    date_columns = ['REQUEST_DATE', 'START_DATE', 'COMPLETE_DATE',
                   'STATUS_CHANGE_DATE', 'CLOSED_DATE']
    for col in date_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')

    # Calculate days open
    if 'REQUEST_DATE' in df.columns:
        df['DAYS_OPEN'] = (today - df['REQUEST_DATE']).dt.days

    # Calculate product TAT (turnaround time)
    if 'COMPLETE_DATE' in df.columns and 'START_DATE' in df.columns:
        df['PRODUCT_TAT'] = (df['COMPLETE_DATE'] - df['START_DATE']).dt.days

    # Mark completed products
    if 'STATUS' in df.columns:
        df['COMPLETED_PRODUCT'] = df['STATUS'].isin(['Complete', 'Completed'])

    # Extract request type and year
    if 'TITLE' in df.columns:
        df['REQUEST_TYPE'] = df['TITLE'].str.extract(r'\[(.*?)\]')[0]
    if 'REQUEST_DATE' in df.columns:
        df['REQUEST_YEAR'] = df['REQUEST_DATE'].dt.year

    # Determine if product is open
    if 'STATUS' in df.columns:
        df['PRODUCT_OPEN'] = df['STATUS'].isin(OPEN_STATUS)

    # Calculate days on current status
    if 'STATUS_CHANGE_DATE' in df.columns:
        df['DAYS_ON_STATUS'] = (today - df['STATUS_CHANGE_DATE']).dt.days
        df['DAYS_ON_STATUS'] = df['DAYS_ON_STATUS'].fillna(0).astype(int)

    # Flag items needing attention (open and on status > threshold)
    if 'PRODUCT_OPEN' in df.columns and 'DAYS_ON_STATUS' in df.columns:
        df['NEEDS_ATTENTION'] = (
            df['PRODUCT_OPEN'] &
            (df['DAYS_ON_STATUS'] > DAYS_ON_STATUS_THRESHOLD)
        )

    # Add HAS_VALUE if not present
    if 'HAS_VALUE' not in df.columns:
        df['HAS_VALUE'] = None

    # Generate SharePoint URL
    if 'ID' in df.columns:
        df['URL'] = df['ID'].apply(
            lambda x: f"https://sharepoint.com/sites/analytics/Lists/Requests/DispForm.aspx?ID={x}"
            if pd.notna(x) else None
        )

    logger.info("Calculated all metrics")
    return df


def transform_products(df: pd.DataFrame, df_salesforce: pd.DataFrame) -> pd.DataFrame:
    """Transform wide-format data into product-level records"""
    logger.info("Starting product transformation...")

    # Explode products into separate rows
    df_exploded = _explode_products(df)

    # Enrich with Salesforce data
    df_enriched = _enrich_with_salesforce(df_exploded, df_salesforce)

    # Calculate metrics
    df_enriched = _calculate_metrics(df_enriched)

    logger.info(f"Transformation complete: {len(df_exploded)} product-level records created")
    return df_enriched


# ============================================================================
# SNOWFLAKE FUNCTIONS
# ============================================================================

def get_snowflake_connection():
    """Establish connection to Snowflake"""
    logger.info("Connecting to Snowflake...")

    # Optional: Load private key for key-pair authentication
    pkey_pem = os.getenv("MY_SF_PKEY")
    pkey = None
    if pkey_pem:
        pkey = serialization.load_pem_private_key(
            pkey_pem.encode("utf-8"),
            password=None,
            backend=default_backend()
        )

    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    logger.info("Successfully connected to Snowflake")
    return conn


def normalize_dates(df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
    """
    Convert datetime columns to string format 'YYYY-MM-DD' for reliable Snowflake DATE parsing.
    Returns: (normalized_dataframe, list_of_date_columns)
    """
    df = df.copy()
    date_columns = []

    for col in df.columns:
        # Check if column name ends with _DATE or contains DATE-related keywords
        if col.endswith('_DATE') or col in ['REQUEST_DATE', 'START_DATE', 'COMPLETE_DATE',
                                               'CLOSED_DATE', 'STATUS_CHANGE_DATE']:
            try:
                # Convert to datetime64 first (handles strings, floats, NaT, etc.)
                temp_dt = pd.to_datetime(df[col], errors='coerce')

                # Convert to string format 'YYYY-MM-DD', keeping NaT as None
                df[col] = temp_dt.apply(lambda x: x.strftime('%Y-%m-%d') if pd.notna(x) else None)

                # Explicitly cast to object dtype to ensure write_pandas treats as VARCHAR
                df[col] = df[col].astype('object')

                date_columns.append(col)
            except Exception as e:
                logger.warning(f"Could not convert {col} to date string: {e}")

    if date_columns:
        logger.info(f"Normalized {len(date_columns)} date columns to string format")

    return df, date_columns


def create_table_with_types(conn, table_name: str, df: pd.DataFrame, date_columns: List[str]):
    """
    Create table with explicit DATE column types for date columns.
    Numeric columns created as NUMBER, all other columns as VARCHAR.
    """
    cur = conn.cursor()

    database = SNOWFLAKE_CONFIG['database']
    schema = SNOWFLAKE_CONFIG['schema']

    # Build column definitions
    column_defs = []
    for col in df.columns:
        if col in date_columns:
            column_defs.append(f"{col} DATE")
        elif pd.api.types.is_numeric_dtype(df[col]):
            # Check if it's an integer or float
            if pd.api.types.is_integer_dtype(df[col]):
                column_defs.append(f"{col} NUMBER(38,0)")
            else:
                column_defs.append(f"{col} NUMBER(38,6)")
        else:
            # Use VARCHAR for all other columns
            column_defs.append(f"{col} VARCHAR")

    columns_sql = ",\n    ".join(column_defs)

    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {database}.{schema}.{table_name} (
        {columns_sql}
    );
    """

    logger.info(f"Creating table {table_name} with {len(date_columns)} DATE columns...")

    try:
        cur.execute(create_sql)
        logger.info(f"Successfully created table {table_name}")
    except Exception as e:
        logger.warning(f"Table creation failed (may already exist): {e}")

    cur.close()


def ensure_schema_matches(conn, table_name: str, df: pd.DataFrame, date_columns: List[str]):
    """
    Add any missing columns to existing table (schema evolution).
    This allows the pipeline to handle new columns from SharePoint CSV automatically.
    """
    cur = conn.cursor()

    database = SNOWFLAKE_CONFIG['database']
    schema = SNOWFLAKE_CONFIG['schema']

    try:
        # Get existing columns from Snowflake table
        cur.execute(f"SHOW COLUMNS IN TABLE {database}.{schema}.{table_name}")
        existing_cols = {row[2].upper() for row in cur.fetchall()}  # row[2] is column name

        # Find new columns in DataFrame that don't exist in table
        df_cols = set(df.columns)
        new_cols = df_cols - existing_cols

        if new_cols:
            logger.info(f"Schema evolution: Found {len(new_cols)} new columns to add")
            for col in sorted(new_cols):
                # Determine column type based on data
                if col in date_columns:
                    col_type = "DATE"
                elif pd.api.types.is_integer_dtype(df[col]):
                    col_type = "NUMBER(38,0)"
                elif pd.api.types.is_numeric_dtype(df[col]):
                    col_type = "NUMBER(38,6)"
                else:
                    col_type = "VARCHAR"

                alter_sql = f"ALTER TABLE {database}.{schema}.{table_name} ADD COLUMN {col} {col_type}"
                logger.info(f"  Adding column: {col} ({col_type})")
                cur.execute(alter_sql)

            logger.info(f"Successfully added {len(new_cols)} new columns")
        else:
            logger.info(f"Schema matches - no new columns to add")

    except Exception as e:
        # If table doesn't exist, this will fail but that's okay
        # create_table_with_types will handle creation
        logger.debug(f"Could not check schema (table may not exist yet): {e}")

    finally:
        cur.close()


# ============================================================================
# AUDIT LOGGING FUNCTIONS
# ============================================================================

def create_audit_table(conn):
    """
    Create the audit table if it doesn't exist.
    This table tracks all pipeline runs with detailed statistics.
    """
    cur = conn.cursor()

    database = SNOWFLAKE_CONFIG['database']
    schema = SNOWFLAKE_CONFIG['schema']

    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {database}.{schema}.{AUDIT_TABLE} (
        RUN_ID VARCHAR PRIMARY KEY,
        RUN_TIMESTAMP TIMESTAMP,
        PIPELINE_NAME VARCHAR,
        TABLE_NAME VARCHAR,
        LOAD_TYPE VARCHAR,
        ROWS_PROCESSED NUMBER(38,0),
        ROWS_INSERTED NUMBER(38,0),
        ROWS_UPDATED NUMBER(38,0),
        ROWS_DELETED NUMBER(38,0),
        DURATION_SECONDS NUMBER(38,2),
        STATUS VARCHAR,
        ERROR_MESSAGE VARCHAR
    );
    """

    try:
        cur.execute(create_sql)
        logger.info(f"Ensured audit table {AUDIT_TABLE} exists")
    except Exception as e:
        logger.warning(f"Could not create audit table: {e}")
    finally:
        cur.close()


def log_to_snowflake(conn, run_id: str, table_name: str, load_type: str,
                     rows_processed: int, rows_inserted: int, rows_updated: int,
                     duration_seconds: float, status: str, error_message: str = None):
    """
    Log pipeline run statistics to Snowflake audit table.

    Args:
        conn: Snowflake connection
        run_id: Unique identifier for this pipeline run
        table_name: Name of the table that was loaded
        load_type: 'INCREMENTAL' or 'FULL_REFRESH'
        rows_processed: Number of input rows
        rows_inserted: Number of rows inserted
        rows_updated: Number of rows updated
        duration_seconds: How long the operation took
        status: 'SUCCESS' or 'FAILED'
        error_message: Error details if failed
    """
    cur = conn.cursor()

    database = SNOWFLAKE_CONFIG['database']
    schema = SNOWFLAKE_CONFIG['schema']

    insert_sql = f"""
    INSERT INTO {database}.{schema}.{AUDIT_TABLE}
    (RUN_ID, RUN_TIMESTAMP, PIPELINE_NAME, TABLE_NAME, LOAD_TYPE,
     ROWS_PROCESSED, ROWS_INSERTED, ROWS_UPDATED, ROWS_DELETED,
     DURATION_SECONDS, STATUS, ERROR_MESSAGE)
    VALUES (%s, CURRENT_TIMESTAMP(), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    try:
        cur.execute(insert_sql, (
            run_id,
            'SharePoint Export Pipeline',
            table_name,
            load_type,
            rows_processed,
            rows_inserted,
            rows_updated,
            0,  # rows_deleted (always 0 for this pipeline)
            duration_seconds,
            status,
            error_message
        ))
        logger.info(f"Logged run to audit table: {table_name} - {status}")
    except Exception as e:
        logger.error(f"Failed to log to audit table: {e}")
    finally:
        cur.close()


# ============================================================================
# DATA LOADING FUNCTIONS
# ============================================================================

def load_incremental(conn, df: pd.DataFrame, table_name: str, match_key: str = 'ID') -> Tuple[int, int]:
    """
    Load data with incremental MERGE on specified match key.
    Returns: (rows_inserted, rows_updated)
    """
    staging_table = f"{table_name}_STAGING"
    cur = conn.cursor()

    database = SNOWFLAKE_CONFIG['database']
    schema = SNOWFLAKE_CONFIG['schema']

    # Normalize dates
    df, date_columns = normalize_dates(df)

    # Ensure target table exists with proper DATE column types
    logger.info(f"Ensuring target table {table_name} exists with proper schema...")
    create_table_with_types(conn, table_name, df, date_columns)

    # Add any new columns to existing table (schema evolution)
    ensure_schema_matches(conn, table_name, df, date_columns)

    logger.info(f"Creating staging table for {table_name}...")
    # Drop staging table if it exists
    cur.execute(f"DROP TABLE IF EXISTS {database}.{schema}.{staging_table};")

    # Create staging table with same schema as target
    create_table_with_types(conn, staging_table, df, date_columns)

    logger.info(f"Loading {len(df)} rows into staging...")
    # Load data into pre-created table
    success, nchunks, nrows, _ = write_pandas(
        conn, df, staging_table,
        auto_create_table=False,
        overwrite=False
    )

    if not success:
        raise Exception("Failed to write to staging table")

    logger.info("Merging data...")

    # Build dynamic MERGE SQL
    all_columns = df.columns.tolist()
    update_cols = [col for col in all_columns if col != match_key]

    update_set_clause = ", ".join([f"target.{col} = source.{col}" for col in update_cols])
    insert_cols = ", ".join(all_columns)
    insert_vals = ", ".join([f"source.{col}" for col in all_columns])

    merge_sql = f"""
    MERGE INTO {database}.{schema}.{table_name} AS target
    USING {database}.{schema}.{staging_table} AS source
    ON target.{match_key} = source.{match_key}
    WHEN MATCHED THEN
        UPDATE SET
        {update_set_clause}
    WHEN NOT MATCHED THEN
        INSERT ({insert_cols})
        VALUES ({insert_vals});
    """

    cur.execute(merge_sql)

    # Get MERGE statistics from Snowflake
    stats_query = f"SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));"
    cur.execute(stats_query)
    merge_result = cur.fetchone()

    # Parse Snowflake MERGE output: [rows_inserted, rows_updated, rows_deleted]
    rows_inserted = merge_result[0] if merge_result else 0
    rows_updated = merge_result[1] if merge_result else 0

    logger.info(f"Merge complete: {rows_inserted} inserted, {rows_updated} updated")

    # Clean up staging table
    cur.execute(f"DROP TABLE IF EXISTS {database}.{schema}.{staging_table};")

    cur.close()
    logger.info(f"Incremental load complete for {table_name}")

    return (rows_inserted, rows_updated)


def load_full_refresh(conn, df: pd.DataFrame, table_name: str) -> Tuple[int, int]:
    """
    Load data with full TRUNCATE and reload.
    Skip if DataFrame is empty.
    Returns: (rows_inserted, rows_updated)
    """
    # Skip if DataFrame is empty
    if df.empty:
        logger.info(f"Skipping {table_name} - no data to load")
        return (0, 0)

    cur = conn.cursor()

    database = SNOWFLAKE_CONFIG['database']
    schema = SNOWFLAKE_CONFIG['schema']

    # Normalize dates
    df, date_columns = normalize_dates(df)

    # Ensure table exists with proper DATE column types
    logger.info(f"Ensuring target table {table_name} exists with proper schema...")
    create_table_with_types(conn, table_name, df, date_columns)

    # Add any new columns to existing table (schema evolution)
    ensure_schema_matches(conn, table_name, df, date_columns)

    logger.info(f"Truncating {table_name} (full reload)...")
    cur.execute(f"TRUNCATE TABLE {database}.{schema}.{table_name};")

    logger.info(f"Uploading {len(df)} rows to {table_name}...")
    # Load data into pre-created table
    try:
        success, nchunks, nrows, _ = write_pandas(
            conn, df, table_name,
            auto_create_table=False,
            overwrite=False
        )
        logger.info(f"Successfully uploaded {nrows} rows to {table_name}")
        rows_inserted = nrows
    except Exception as e:
        logger.error(f"Error loading data to {table_name}: {e}")
        # Log first few rows for debugging
        logger.error(f"Sample data types: {df.dtypes}")
        logger.error(f"Sample data:\n{df.head()}")
        raise

    cur.close()

    # Full refresh = all inserts, no updates
    return (rows_inserted, 0)


# ============================================================================
# MAIN PIPELINE EXECUTION
# ============================================================================

def run_pipeline(incremental: bool = True, dry_run: bool = False):
    """
    Execute full pipeline with audit logging

    Args:
        incremental: If True, use incremental MERGE for raw table. If False, full refresh.
        dry_run: If True, skip Snowflake upload

    Returns:
        0 on success, 1 on failure
    """
    logger.info("=" * 80)
    logger.info("STARTING SHAREPOINT EXPORT PIPELINE")
    logger.info("=" * 80)
    logger.info(f"Mode: {'INCREMENTAL' if incremental else 'FULL REFRESH'}")
    logger.info(f"Dry Run: {dry_run}")
    logger.info("=" * 80)

    # Generate unique run ID for this pipeline execution
    run_id = str(uuid.uuid4())
    logger.info(f"Run ID: {run_id}")

    start_time = datetime.now()
    conn = None

    try:
        # ========================================================================
        # PHASE 1: EXTRACTION
        # ========================================================================
        logger.info("PHASE 1: DATA EXTRACTION")

        df_sharepoint = extract_sharepoint(SHAREPOINT_EXPORT_PATH)
        df_salesforce = extract_salesforce(SALESFORCE_EXPORT_PATH)

        logger.info(f"Extraction complete: {len(df_sharepoint)} SharePoint, {len(df_salesforce)} Salesforce records")

        # ========================================================================
        # PHASE 2: CLEANING
        # ========================================================================
        logger.info("PHASE 2: DATA CLEANING")

        df_cleaned = clean_data(df_sharepoint)

        logger.info(f"Cleaning complete: {len(df_cleaned)} records")

        # ========================================================================
        # PHASE 3: TRANSFORMATION
        # ========================================================================
        logger.info("PHASE 3: DATA TRANSFORMATION")

        # Salesforce data is only used for enrichment (merged into transformed table)
        # It is NOT loaded as a separate table to Snowflake
        df_transformed = transform_products(df_cleaned, df_salesforce)

        logger.info(f"Transformation complete: {len(df_transformed)} product-level records")

        # ========================================================================
        # PHASE 4: LOADING TO SNOWFLAKE
        # ========================================================================
        logger.info("PHASE 4: DATA LOADING")

        if dry_run:
            logger.info("[DRY RUN] Skipping Snowflake upload")
        else:
            conn = get_snowflake_connection()

            try:
                # Create audit table if it doesn't exist
                create_audit_table(conn)

                # Load raw SharePoint data (incremental or full)
                if incremental:
                    logger.info(f"Loading raw data to {SOURCE_TABLE} (INCREMENTAL)...")
                    load_start = datetime.now()
                    rows_inserted, rows_updated = load_incremental(conn, df_cleaned, SOURCE_TABLE, match_key='ID')
                    load_duration = (datetime.now() - load_start).total_seconds()

                    # Log to audit table
                    log_to_snowflake(
                        conn, run_id, SOURCE_TABLE, 'INCREMENTAL',
                        len(df_cleaned), rows_inserted, rows_updated,
                        load_duration, 'SUCCESS'
                    )
                else:
                    logger.info(f"Loading raw data to {SOURCE_TABLE} (FULL REFRESH)...")
                    load_start = datetime.now()
                    rows_inserted, rows_updated = load_full_refresh(conn, df_cleaned, SOURCE_TABLE)
                    load_duration = (datetime.now() - load_start).total_seconds()

                    # Log to audit table
                    log_to_snowflake(
                        conn, run_id, SOURCE_TABLE, 'FULL_REFRESH',
                        len(df_cleaned), rows_inserted, rows_updated,
                        load_duration, 'SUCCESS'
                    )

                # Load transformed data (always full refresh for consistency)
                # Note: This includes Salesforce enrichment columns (SALESFORCE_ID, HAS_VALUE)
                logger.info(f"Loading transformed data to {TARGET_TABLE} (FULL REFRESH)...")
                load_start = datetime.now()
                rows_inserted, rows_updated = load_full_refresh(conn, df_transformed, TARGET_TABLE)
                load_duration = (datetime.now() - load_start).total_seconds()

                # Log to audit table
                log_to_snowflake(
                    conn, run_id, TARGET_TABLE, 'FULL_REFRESH',
                    len(df_transformed), rows_inserted, rows_updated,
                    load_duration, 'SUCCESS'
                )

            finally:
                if conn:
                    conn.close()
                    logger.info("Snowflake connection closed")

        # ========================================================================
        # SUMMARY
        # ========================================================================
        elapsed = datetime.now() - start_time

        logger.info("=" * 80)
        logger.info("PIPELINE COMPLETED SUCCESSFULLY")
        logger.info(f"Run ID: {run_id}")
        logger.info(f"Total execution time: {elapsed}")
        logger.info(f"Records processed: {len(df_cleaned)} raw → {len(df_transformed)} transformed")
        logger.info("=" * 80)

        return 0  # Success

    except Exception as e:
        # Log failure to audit table if we have a connection
        if conn and not dry_run:
            try:
                elapsed = (datetime.now() - start_time).total_seconds()
                log_to_snowflake(
                    conn, run_id, 'PIPELINE', 'FAILED',
                    0, 0, 0, elapsed, 'FAILED',
                    error_message=str(e)
                )
            except:
                pass  # Don't fail on audit logging failure
            finally:
                conn.close()

        logger.error("=" * 80)
        logger.error("PIPELINE FAILED WITH EXCEPTION")
        logger.error(f"Run ID: {run_id}")
        logger.error(str(e), exc_info=True)
        logger.error("=" * 80)

        return 1  # Failure


# ============================================================================
# CLI ENTRY POINT
# ============================================================================

def main():
    """CLI entry point"""
    parser = argparse.ArgumentParser(
        description='SharePoint Export Pipeline - ETL to Snowflake',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run in incremental mode (default)
  python sharepoint_pipeline.py

  # Run with full refresh
  python sharepoint_pipeline.py --full-refresh

  # Test without uploading to Snowflake
  python sharepoint_pipeline.py --dry-run

  # Query audit history in Snowflake
  SELECT * FROM CSZNB_PRD_OA_DEV_DB.BASE.PIPELINE_RUN_HISTORY
  ORDER BY RUN_TIMESTAMP DESC LIMIT 10;
        """
    )

    parser.add_argument(
        '--full-refresh',
        action='store_true',
        help='Use full refresh instead of incremental MERGE for raw table'
    )

    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Skip Snowflake upload (testing only)'
    )

    args = parser.parse_args()

    # Run the pipeline
    incremental = not args.full_refresh
    exit_code = run_pipeline(incremental=incremental, dry_run=args.dry_run)

    sys.exit(exit_code)


if __name__ == "__main__":
    main()
