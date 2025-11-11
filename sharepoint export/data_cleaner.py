import pandas as pd
import logging
from typing import Dict, List

logger = logging.getLogger(__name__)


class DataCleaner:
    """Cleans and normalizes raw data"""
    
    def __init__(self, client_type_mapping: Dict, boolean_columns: List[str]):
        self.client_type_mapping = client_type_mapping
        self.boolean_columns = boolean_columns
    
    def clean(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply all cleaning transformations"""
        logger.info("Cleaning and normalizing data...")
        
        df = self._map_client_types(df)
        df = self._detect_spine_pain_joint(df)
        df = self._normalize_boolean_columns(df)
        df = self._populate_products_requested(df)
        
        logger.info("Data cleaning complete")
        return df
    
    def _map_client_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """Map client type codes to descriptions"""
        df['CLIENT_TYPE_DETAIL'] = (
            df['CLIENT_TYPE_DETAIL']
            .astype(str)
            .map(self.client_type_mapping)
            .fillna(df['CLIENT_TYPE_DETAIL'])
        )
        return df
    
    def _detect_spine_pain_joint(self, df: pd.DataFrame) -> pd.DataFrame:
        """Detect Spine Pain & Joint from product text"""
        df['SPINE_PAIN_JOINT'] = df['PRODUCTS_REQUESTED'].str.contains(
            "Spine Pain & Joint", case=False, na=False
        )
        return df
    
    def _normalize_boolean_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Fill null values in boolean columns"""
        df[self.boolean_columns] = df[self.boolean_columns].fillna(False)
        return df
    
    def _populate_products_requested(self, df: pd.DataFrame) -> pd.DataFrame:
        """Populate PRODUCTS_REQUESTED from boolean columns where null"""
        mask_null = df['PRODUCTS_REQUESTED'].isnull()
        
        if not mask_null.any():
            return df
        
        bool_array = df.loc[mask_null, self.boolean_columns].values
        products_list = []
        
        for row_vals in bool_array:
            if row_vals.any():
                selected_cols = df.loc[mask_null, self.boolean_columns].columns[row_vals]
                products_list.append(', '.join(selected_cols.str.title()))
            else:
                products_list.append('None')
        
        df.loc[mask_null, 'PRODUCTS_REQUESTED'] = products_list
        return df