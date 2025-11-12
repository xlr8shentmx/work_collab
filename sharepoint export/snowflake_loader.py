import pandas as pd
import logging
from snowflake.connector.pandas_tools import write_pandas
from typing import Tuple

logger = logging.getLogger(__name__)


class SnowflakeLoader:
    """Handles loading data into Snowflake"""

    def __init__(self, connection, database: str, schema: str):
        self.conn = connection
        self.database = database
        self.schema = schema

    def _get_table_columns(self, table_name: str) -> list:
        """Get list of column names from a Snowflake table"""
        cur = self.conn.cursor()
        try:
            result = cur.execute(f"""
                SELECT COLUMN_NAME
                FROM {self.database}.INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = '{self.schema}'
                AND TABLE_NAME = '{table_name}'
                ORDER BY ORDINAL_POSITION;
            """)
            columns = [row[0] for row in result.fetchall()]
            cur.close()
            return columns
        except Exception as e:
            logger.warning(f"Could not get columns for table {table_name}: {e}")
            cur.close()
            return []

    def _add_missing_columns(self, df: pd.DataFrame, target_table: str) -> None:
        """Add any missing columns from DataFrame to target table"""
        # Get existing columns in target table
        existing_columns = self._get_table_columns(target_table)

        if not existing_columns:
            logger.info(f"Table {target_table} may not exist yet, skipping schema evolution")
            return

        # Find new columns
        df_columns = df.columns.tolist()
        new_columns = [col for col in df_columns if col not in existing_columns]

        if not new_columns:
            logger.info("No new columns detected")
            return

        # Add new columns to target table
        cur = self.conn.cursor()
        logger.info(f"Detected {len(new_columns)} new columns: {new_columns}")

        for col in new_columns:
            try:
                # Use VARCHAR for new columns (can be adjusted based on dtype if needed)
                logger.info(f"Adding column {col} to {target_table}...")
                cur.execute(f"""
                    ALTER TABLE {self.database}.{self.schema}.{target_table}
                    ADD COLUMN {col} VARCHAR;
                """)
                logger.info(f"Successfully added column {col}")
            except Exception as e:
                logger.error(f"Error adding column {col}: {e}")
                raise

        cur.close()
        logger.info(f"Schema evolution complete: added {len(new_columns)} columns")
    
    def load_raw_data(self, df_main: pd.DataFrame, df_salesforce: pd.DataFrame,
                      source_table: str, salesforce_table: str, 
                      incremental: bool = True, dry_run: bool = False) -> bool:
        """Load raw data into Snowflake with incremental or full refresh"""
        try:
            if dry_run:
                logger.info("[DRY RUN] Skipping raw data upload")
                return True
            
            cur = self.conn.cursor()
            
            # Salesforce data (always overwrite - small table)
            logger.info(f"Uploading Salesforce data to {salesforce_table}...")
            write_pandas(self.conn, df_salesforce, salesforce_table, 
                        auto_create_table=True, overwrite=True)
            
            # SharePoint data (incremental or full)
            if not incremental:
                # Full reload
                logger.info(f"Truncating {source_table} (full reload)...")
                cur.execute(f"TRUNCATE TABLE {self.database}.{self.schema}.{source_table};")
                
                logger.info(f"Uploading SharePoint data to {source_table}...")
                success, nchunks, nrows, _ = write_pandas(self.conn, df_main, source_table)
                logger.info(f"Upload complete: {nrows} rows")
            else:
                # Incremental load with MERGE
                staging_table = f"{source_table}_STAGING"

                logger.info(f"Creating staging table for raw data...")
                # Drop staging table if it exists
                cur.execute(f"DROP TABLE IF EXISTS {self.database}.{self.schema}.{staging_table};")

                logger.info(f"Loading {len(df_main)} rows into staging...")
                # Use auto_create_table and overwrite to let write_pandas create the table with correct types
                success, nchunks, nrows, _ = write_pandas(
                    self.conn, df_main, staging_table,
                    auto_create_table=True,
                    overwrite=True
                )

                if not success:
                    raise Exception("Failed to write to staging table")

                # Add any new columns to target table before MERGE
                logger.info("Checking for schema changes...")
                self._add_missing_columns(df_main, source_table)

                logger.info("Merging raw SharePoint data...")

                # Build dynamic MERGE SQL based on DataFrame columns
                all_columns = df_main.columns.tolist()
                update_cols = [col for col in all_columns if col != 'ID']

                update_set_clause = ", ".join([f"target.{col} = source.{col}" for col in update_cols])
                insert_cols = ", ".join(all_columns)
                insert_vals = ", ".join([f"source.{col}" for col in all_columns])
                
                merge_sql = f"""
                MERGE INTO {self.database}.{self.schema}.{source_table} AS target
                USING {self.database}.{self.schema}.{staging_table} AS source
                ON target.ID = source.ID
                WHEN MATCHED THEN
                    UPDATE SET
                    {update_set_clause}
                WHEN NOT MATCHED THEN
                    INSERT ({insert_cols})
                    VALUES ({insert_vals});
                """
                
                result = cur.execute(merge_sql)
                rows_affected = result.fetchone()[0] if result.rowcount > 0 else 0
                logger.info(f"Raw data merge complete: {rows_affected} rows affected")
                
                # Clean up staging table
                cur.execute(f"DROP TABLE IF EXISTS {self.database}.{self.schema}.{staging_table};")
            
            cur.close()
            return True
        except Exception as e:
            logger.error("Error loading raw data", exc_info=True)
            return False
    
    def load_transformed_data(self, df: pd.DataFrame, target_table: str, 
                             incremental: bool = True, dry_run: bool = False) -> Tuple[bool, int, int]:
        """Load transformed data with incremental or full refresh"""
        try:
            if dry_run:
                logger.info("[DRY RUN] Skipping transformed data upload")
                return True, 0, len(df)
            
            if not incremental:
                return self._full_load(df, target_table)
            else:
                return self._incremental_load(df, target_table)
        except Exception as e:
            logger.error("Error loading transformed data", exc_info=True)
            return False, 0, 0
    
    def _full_load(self, df: pd.DataFrame, target_table: str) -> Tuple[bool, int, int]:
        """Full truncate and reload"""
        cur = self.conn.cursor()
        
        logger.info(f"Truncating {target_table} (full reload)...")
        cur.execute(f"TRUNCATE TABLE {self.database}.{self.schema}.{target_table};")
        
        logger.info(f"Uploading {len(df)} rows to {target_table}...")
        success, nchunks, nrows, _ = write_pandas(self.conn, df, target_table)
        
        cur.close()
        logger.info(f"Successfully uploaded {nrows} rows")
        return success, nchunks, nrows
    
    def _incremental_load(self, df: pd.DataFrame, target_table: str) -> Tuple[bool, int, int]:
        """Incremental load using staging table and MERGE"""
        cur = self.conn.cursor()
        staging_table = f"{target_table}_STAGING"

        logger.info(f"Creating staging table {staging_table}...")
        # Drop staging table if it exists
        cur.execute(f"DROP TABLE IF EXISTS {self.database}.{self.schema}.{staging_table};")

        logger.info(f"Loading {len(df)} rows into staging...")
        # Use auto_create_table and overwrite to let write_pandas create the table with correct types
        success, nchunks, nrows, _ = write_pandas(
            self.conn, df, staging_table,
            auto_create_table=True,
            overwrite=True
        )

        if not success:
            raise Exception("Failed to write to staging table")

        # Add any new columns to target table before MERGE
        logger.info("Checking for schema changes...")
        self._add_missing_columns(df, target_table)

        logger.info("Executing MERGE operation...")
        merge_sql = self._build_merge_sql(target_table, staging_table)
        result = cur.execute(merge_sql)
        
        # Get merge statistics
        rows_inserted = result.fetchone()[0] if result.rowcount > 0 else 0
        
        logger.info(f"Merge complete: {rows_inserted} rows affected")
        
        logger.info("Dropping staging table...")
        cur.execute(f"DROP TABLE IF EXISTS {self.database}.{self.schema}.{staging_table};")
        
        cur.close()
        return success, nchunks, nrows
    
    def _build_merge_sql(self, target_table: str, staging_table: str) -> str:
        """Build MERGE SQL statement"""
        return f"""
        MERGE INTO {self.database}.{self.schema}.{target_table} AS target
        USING {self.database}.{self.schema}.{staging_table} AS source
        ON target.ID = source.ID AND target.PRODUCT = source.PRODUCT
        WHEN MATCHED THEN
            UPDATE SET
                target.TITLE = source.TITLE,
                target.REQUEST_DATE = source.REQUEST_DATE,
                target.CLIENT = source.CLIENT,
                target.MARKET = source.MARKET,
                target.REQUESTOR = source.REQUESTOR,
                target.CLIENT_TYPE = source.CLIENT_TYPE,
                target.OVERALL_STATUS = source.OVERALL_STATUS,
                target.PRODUCTS_REQUESTED = source.PRODUCTS_REQUESTED,
                target.SALESFORCE_ID = source.SALESFORCE_ID,
                target.START_DATE = source.START_DATE,
                target.COMPLETE_DATE = source.COMPLETE_DATE,
                target.STATUS = source.STATUS,
                target.STATUS_CHANGE_DATE = source.STATUS_CHANGE_DATE,
                target.CLOSED_DATE = source.CLOSED_DATE,
                target.PTRR = source.PTRR,
                target.PRODUCT_CATEGORY = source.PRODUCT_CATEGORY,
                target.PRODUCT_TAT = source.PRODUCT_TAT,
                target.COMPLETED_PRODUCT = source.COMPLETED_PRODUCT,
                target.REQUEST_TYPE = source.REQUEST_TYPE,
                target.DAYS_OPEN = source.DAYS_OPEN,
                target.REQUEST_YEAR = source.REQUEST_YEAR,
                target.PRODUCT_OPEN = source.PRODUCT_OPEN,
                target.DAYS_ON_STATUS = source.DAYS_ON_STATUS,
                target.NEEDS_ATTENTION = source.NEEDS_ATTENTION,
                target.HAS_VALUE = source.HAS_VALUE,
                target.URL = source.URL
        WHEN NOT MATCHED THEN
            INSERT (
                ID, TITLE, REQUEST_DATE, CLIENT, MARKET, REQUESTOR,
                CLIENT_TYPE, OVERALL_STATUS, PRODUCTS_REQUESTED, SALESFORCE_ID,
                START_DATE, COMPLETE_DATE, STATUS, STATUS_CHANGE_DATE, CLOSED_DATE,
                PTRR, PRODUCT, PRODUCT_CATEGORY, PRODUCT_TAT, COMPLETED_PRODUCT,
                REQUEST_TYPE, DAYS_OPEN, REQUEST_YEAR, PRODUCT_OPEN, DAYS_ON_STATUS,
                NEEDS_ATTENTION, HAS_VALUE, URL
            )
            VALUES (
                source.ID, source.TITLE, source.REQUEST_DATE, source.CLIENT, 
                source.MARKET, source.REQUESTOR, source.CLIENT_TYPE, 
                source.OVERALL_STATUS, source.PRODUCTS_REQUESTED, source.SALESFORCE_ID,
                source.START_DATE, source.COMPLETE_DATE, source.STATUS, 
                source.STATUS_CHANGE_DATE, source.CLOSED_DATE, source.PTRR, 
                source.PRODUCT, source.PRODUCT_CATEGORY, source.PRODUCT_TAT, 
                source.COMPLETED_PRODUCT, source.REQUEST_TYPE, source.DAYS_OPEN, 
                source.REQUEST_YEAR, source.PRODUCT_OPEN, source.DAYS_ON_STATUS,
                source.NEEDS_ATTENTION, source.HAS_VALUE, source.URL
            );
        """