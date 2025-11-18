"""
Reference data management for NICU Analytics Pipeline.
"""
import logging
from typing import Dict, Optional
from snowflake.snowpark import Session, DataFrame
from snowflake.snowpark.functions import col

from .config import REFERENCE_TABLE_CONFIG

logger = logging.getLogger(__name__)


class ReferenceDataManager:
    """
    Manager for loading and caching reference data tables.

    Reference tables contain code sets for ICD codes, revenue codes,
    DRG codes, etc. used to identify newborns and NICU cases.
    """

    def __init__(self, session: Session, ref_config=None):
        """
        Initialize reference data manager.

        Args:
            session: Snowpark session
            ref_config: Reference table configuration (uses default if None)
        """
        self.session = session
        self.ref_config = ref_config or REFERENCE_TABLE_CONFIG
        self._cache: Dict[str, DataFrame] = {}

    def _load_reference_table(
        self,
        table_name: str,
        cache_key: Optional[str] = None,
        use_cache: bool = True
    ) -> DataFrame:
        """
        Load a reference table from Snowflake.

        Args:
            table_name: Name of the reference table
            cache_key: Key for caching (defaults to table_name)
            use_cache: Whether to use cached data if available

        Returns:
            Snowpark DataFrame with reference data
        """
        cache_key = cache_key or table_name

        if use_cache and cache_key in self._cache:
            logger.debug(f"Using cached reference data: {cache_key}")
            return self._cache[cache_key]

        full_table_name = self.ref_config.get_table_name(table_name)
        logger.info(f"Loading reference table: {full_table_name}")

        df = self.session.table(full_table_name)

        if use_cache:
            self._cache[cache_key] = df

        return df

    # ICD Reference Tables

    def get_newborn_icd(self, use_cache: bool = True) -> DataFrame:
        """
        Get newborn ICD codes reference table.

        Returns:
            DataFrame with columns: CODE, DESCRIPTION (optional)
        """
        return self._load_reference_table(
            self.ref_config.newborn_icd,
            cache_key="newborn_icd",
            use_cache=use_cache
        )

    def get_singleton_icd(self, use_cache: bool = True) -> DataFrame:
        """Get singleton birth ICD codes."""
        return self._load_reference_table(
            self.ref_config.singleton_icd,
            cache_key="singleton_icd",
            use_cache=use_cache
        )

    def get_twin_icd(self, use_cache: bool = True) -> DataFrame:
        """Get twin birth ICD codes."""
        return self._load_reference_table(
            self.ref_config.twin_icd,
            cache_key="twin_icd",
            use_cache=use_cache
        )

    def get_multiple_icd(self, use_cache: bool = True) -> DataFrame:
        """Get multiple birth ICD codes."""
        return self._load_reference_table(
            self.ref_config.multiple_icd,
            cache_key="multiple_icd",
            use_cache=use_cache
        )

    def get_birthweight_icd(self, use_cache: bool = True) -> DataFrame:
        """
        Get birthweight ICD codes reference table.

        Returns:
            DataFrame with columns: CODE, DESCRIPTION (category)
        """
        return self._load_reference_table(
            self.ref_config.birthweight_icd,
            cache_key="birthweight_icd",
            use_cache=use_cache
        )

    def get_gestational_age_icd(self, use_cache: bool = True) -> DataFrame:
        """
        Get gestational age ICD codes reference table.

        Returns:
            DataFrame with columns: CODE, DESCRIPTION (category)
        """
        return self._load_reference_table(
            self.ref_config.gest_age_icd,
            cache_key="gest_age_icd",
            use_cache=use_cache
        )

    # Revenue Code Reference Tables

    def get_newborn_revcode(self, use_cache: bool = True) -> DataFrame:
        """Get newborn revenue codes."""
        return self._load_reference_table(
            self.ref_config.newborn_revcode,
            cache_key="newborn_revcode",
            use_cache=use_cache
        )

    def get_nicu_revcode(self, use_cache: bool = True) -> DataFrame:
        """Get NICU revenue codes."""
        return self._load_reference_table(
            self.ref_config.nicu_revcode,
            cache_key="nicu_revcode",
            use_cache=use_cache
        )

    # DRG Reference Tables

    def get_nicu_msdrg(self, use_cache: bool = True) -> DataFrame:
        """Get NICU MS-DRG codes."""
        return self._load_reference_table(
            self.ref_config.nicu_msdrg,
            cache_key="nicu_msdrg",
            use_cache=use_cache
        )

    def get_nicu_aprdrg(self, use_cache: bool = True) -> DataFrame:
        """Get NICU APR-DRG codes."""
        return self._load_reference_table(
            self.ref_config.nicu_aprdrg,
            cache_key="nicu_aprdrg",
            use_cache=use_cache
        )

    # Batch loading

    def preload_all_references(self) -> None:
        """
        Preload all reference tables into cache.

        This can improve performance by loading all reference data
        upfront in a single batch.
        """
        logger.info("Preloading all reference tables")

        # ICD references
        self.get_newborn_icd()
        self.get_singleton_icd()
        self.get_twin_icd()
        self.get_multiple_icd()
        self.get_birthweight_icd()
        self.get_gestational_age_icd()

        # Revenue code references
        self.get_newborn_revcode()
        self.get_nicu_revcode()

        # DRG references
        self.get_nicu_msdrg()
        self.get_nicu_aprdrg()

        logger.info(f"Preloaded {len(self._cache)} reference tables")

    def clear_cache(self) -> None:
        """Clear all cached reference data."""
        logger.info("Clearing reference data cache")
        self._cache.clear()

    def get_cache_info(self) -> Dict[str, int]:
        """
        Get information about cached reference tables.

        Returns:
            Dictionary with cache statistics
        """
        return {
            "cached_tables": len(self._cache),
            "table_names": list(self._cache.keys())
        }


def get_reference_codes_as_list(
    ref_df: DataFrame,
    code_column: str = "CODE"
) -> list:
    """
    Convert a reference DataFrame to a list of codes.

    Useful for creating in-memory sets for filtering.

    Args:
        ref_df: Reference DataFrame
        code_column: Name of the column containing codes

    Returns:
        List of codes
    """
    return [row[code_column] for row in ref_df.select(code_column).collect()]
