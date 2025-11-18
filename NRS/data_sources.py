"""
Snowflake data source management for NICU Analytics Pipeline.
"""
import os
import logging
from typing import Optional
from snowflake.snowpark import Session
from snowflake.snowpark import DataFrame
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

from .config import SNOWFLAKE_CONFIG

logger = logging.getLogger(__name__)


def get_snowflake_session(
    account: Optional[str] = None,
    role: Optional[str] = None,
    warehouse: Optional[str] = None,
    database: Optional[str] = None,
    schema: Optional[str] = None
) -> Session:
    """
    Create and return a Snowflake Snowpark session.

    Uses private key authentication from environment variables.

    Args:
        account: Snowflake account (defaults to config)
        role: Snowflake role (defaults to config)
        warehouse: Snowflake warehouse (defaults to config)
        database: Snowflake database (defaults to config)
        schema: Snowflake schema (defaults to config)

    Returns:
        Snowpark Session object

    Raises:
        ValueError: If required environment variables are not set
    """
    # Get private key from environment
    pkey_pem = os.getenv("MY_SF_PKEY")
    if not pkey_pem:
        raise ValueError("MY_SF_PKEY environment variable not set")

    username = os.getenv('MY_SF_USER')
    if not username:
        raise ValueError("MY_SF_USER environment variable not set")

    # Load private key
    pkey = serialization.load_pem_private_key(
        pkey_pem.encode("utf-8"),
        password=None,
        backend=default_backend()
    )

    # Build connection parameters
    connection = {
        "account": account or SNOWFLAKE_CONFIG.account,
        "user": username,
        "private_key": pkey,
        "role": role or SNOWFLAKE_CONFIG.role,
        "warehouse": warehouse or SNOWFLAKE_CONFIG.warehouse,
        "database": database or SNOWFLAKE_CONFIG.database,
        "schema": schema or SNOWFLAKE_CONFIG.schema
    }

    logger.info(f"Connecting to Snowflake - Database: {connection['database']}, Schema: {connection['schema']}")
    return Session.builder.configs(connection).create()


def export_to_snowflake(
    df: DataFrame,
    table_name: str,
    mode: str = "overwrite"
) -> None:
    """
    Export a Snowpark DataFrame to Snowflake table.

    Args:
        df: Snowpark DataFrame to export
        table_name: Fully qualified table name (database.schema.table)
        mode: Write mode ('overwrite', 'append', 'errorifexists')

    Raises:
        ValueError: If mode is not valid
    """
    valid_modes = ["overwrite", "append", "errorifexists"]
    if mode not in valid_modes:
        raise ValueError(f"Invalid mode '{mode}'. Must be one of: {valid_modes}")

    logger.info(f"Exporting data to Snowflake table: {table_name} (mode: {mode})")

    try:
        df.write.mode(mode).save_as_table(table_name)
        logger.info("Export complete.")
    except Exception as e:
        logger.error(f"Failed to export to {table_name}: {str(e)}")
        raise


class DataSourceManager:
    """Manager for loading and caching data source tables."""

    def __init__(self, session: Session, client: str, table_config):
        """
        Initialize data source manager.

        Args:
            session: Snowpark session
            client: Client identifier
            table_config: Table configuration object
        """
        self.session = session
        self.client = client
        self.table_config = table_config
        self._cache = {}

    def get_membership_data(self, use_cache: bool = True) -> DataFrame:
        """
        Load membership source data.

        Args:
            use_cache: Whether to use cached data if available

        Returns:
            Snowpark DataFrame with membership data
        """
        cache_key = "membership"
        if use_cache and cache_key in self._cache:
            logger.debug("Using cached membership data")
            return self._cache[cache_key]

        table_name = self.table_config.membership_table(self.client)
        logger.info(f"Loading membership data from {table_name}")

        df = self.session.table(table_name)

        if use_cache:
            self._cache[cache_key] = df

        return df

    def get_medical_data(self, use_cache: bool = True) -> DataFrame:
        """
        Load medical claims source data.

        Args:
            use_cache: Whether to use cached data if available

        Returns:
            Snowpark DataFrame with medical claims data
        """
        cache_key = "medical"
        if use_cache and cache_key in self._cache:
            logger.debug("Using cached medical data")
            return self._cache[cache_key]

        table_name = self.table_config.medical_table(self.client)
        logger.info(f"Loading medical data from {table_name}")

        df = self.session.table(table_name)

        if use_cache:
            self._cache[cache_key] = df

        return df

    def get_processed_membership(self, use_cache: bool = True) -> DataFrame:
        """
        Load processed membership output table.

        Args:
            use_cache: Whether to use cached data if available

        Returns:
            Snowpark DataFrame with processed membership data
        """
        cache_key = "processed_membership"
        if use_cache and cache_key in self._cache:
            logger.debug("Using cached processed membership data")
            return self._cache[cache_key]

        table_name = self.table_config.membership_output_table(self.client)
        logger.info(f"Loading processed membership from {table_name}")

        df = self.session.table(table_name)

        if use_cache:
            self._cache[cache_key] = df

        return df

    def clear_cache(self) -> None:
        """Clear all cached data."""
        logger.info("Clearing data source cache")
        self._cache.clear()

    def cache_dataframe(self, key: str, df: DataFrame) -> None:
        """
        Manually cache a DataFrame.

        Args:
            key: Cache key
            df: DataFrame to cache
        """
        self._cache[key] = df
