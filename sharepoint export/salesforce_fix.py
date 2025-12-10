# QUICK FIX for Salesforce 253003 Error
# Add this improved version to your notebook

def extract_salesforce_safe(file_path: Path) -> pd.DataFrame:
    """
    Load Salesforce Excel export with better error handling.
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


def load_full_refresh_safe(conn, df: pd.DataFrame, table_name: str):
    """
    Load data with full TRUNCATE and reload.
    Skip if DataFrame is empty.
    """
    # Skip if DataFrame is empty
    if df.empty:
        logger.info(f"Skipping {table_name} - no data to load")
        return

    cur = conn.cursor()

    database = SNOWFLAKE_CONFIG['database']
    schema = SNOWFLAKE_CONFIG['schema']

    # Normalize dates
    df, date_columns = normalize_dates(df)

    # Ensure table exists with proper DATE column types
    logger.info(f"Ensuring target table {table_name} exists with proper schema...")
    create_table_with_types(conn, table_name, df, date_columns)

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
    except Exception as e:
        logger.error(f"Error loading data to {table_name}: {e}")
        # Log first few rows for debugging
        logger.error(f"Sample data types: {df.dtypes}")
        logger.error(f"Sample data:\n{df.head()}")
        raise

    cur.close()


# UPDATE YOUR PIPELINE TO USE THESE FUNCTIONS:
# 1. Replace extract_salesforce with extract_salesforce_safe
# 2. Replace load_full_refresh with load_full_refresh_safe for Salesforce table
