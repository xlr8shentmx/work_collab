#!/usr/bin/env python3
"""
SharePoint Analytics Pipeline - Complete Package
Extract this file to create the full project structure.

Usage:
    python create_project.py

This will create the sharepoint_analytics/ directory with all files.
"""

import os
from pathlib import Path

# Project structure
PROJECT_FILES = {
    # ========== CONFIG FILES ==========
    "config/__init__.py": "",
    
    "config/settings.py": '''"""Configuration and constants for the SharePoint Analytics pipeline"""

from pathlib import Path
import os

# Snowflake configuration
SNOWFLAKE_CONFIG = {
    'account': "uhgdwaas.east-us-2.azure",
    'user': os.getenv('SF_USERNAME'),
    'password': os.getenv('SF_PW'),
    'role': "AZU_SDRP_CSZNB_PRD_DEVELOPER_ROLE",
    'warehouse': "CSZNB_PRD_ANALYTICS_XS_WH",
    'database': 'CSZNB_PRD_OA_DEV_DB',
    'schema': 'BASE'
}

# Table names
SOURCE_TABLE = 'SHAREPOINT_ANALYTIC_REQUESTS'
TARGET_TABLE = 'FOCUSED_ANALYTIC_REQUESTS'
SALESFORCE_TABLE = 'SALESFORCE_INITIATIVES'

# File paths
SHAREPOINT_EXPORT_PATH = Path.home() / "Library/CloudStorage/OneDrive-UHG/Projects/SharePoint/exports/sharepoint_requests.csv"
SALESFORCE_EXPORT_PATH = Path.home() / "Library/CloudStorage/OneDrive-UHG/Projects/SharePoint/exports/salesforce_exports.xlsx"

# Business logic constants
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
    "PHS", "CANCER", "PODIMETRICS"
]
''',

    "config/product_mappings.py": '''"""Product configuration mappings"""

# Format: (product_name, category, field, start_col, end_col, status_col)
PRODUCT_CONFIGS = [
    ('Focused Analytics', 'FOCUSED_ANALYTICS', 'FOCUSED_ANALYTICS', 'FA_START_DATE', 'FA_COMPLETE_DATE', 'FA_STATUS'),
    ('CGP', 'CANCER', 'CGP', 'ONCOLOGY_START_DATE', 'ONCOLOGY_COMPLETE_DATE', 'ONCOLOGY_STATUS'),
    ('CSP', 'CANCER', 'CSP', 'ONCOLOGY_START_DATE', 'ONCOLOGY_COMPLETE_DATE', 'ONCOLOGY_STATUS'),
    ('Kidney', 'KIDNEY', 'KIDNEY', 'KIDNEY_START_DATE', 'KIDNEY_COMPLETE_DATE', 'KIDNEY_STATUS'),
    ('Behavioral Health', 'BEHAVIORAL_HEALTH', 'BH', 'BH_START_DATE', 'BH_COMPLETE_DATE', 'BH_STATUS'),
    ('Maternity', 'WOMENS_HEALTH', 'MATERNITY', 'MATERNITY_START_DATE', 'MATERNITY_COMPLETE_DATE', 'MATERNITY_STATUS'),
    ('Fertility', 'WOMENS_HEALTH', 'FERTILITY', 'FERTILITY_START_DATE', 'FERTILITY_COMPLETE_DATE', 'FERTILITY_STATUS'),
    ('NICU', 'WOMENS HEALTH', 'NICU', 'NICU_START_DATE', 'NICU_COMPLETE_DATE', 'NICU_STATUS'),
    ('Disease Management', 'DM/MACM', 'DM', 'DM_START_DATE', 'DM_COMPLETE_DATE', 'DM_STATUS'),
    ('Transplant', 'TRANSPLANT', 'TRANSPLANT', 'TRANSPLANT_START_DATE', 'TRANSPLANT_COMPLETE_DATE', 'TRANSPLANT_STATUS'),
    ('Congenital Heart Defect', 'TRANSPLANT', 'CHD', 'TRANSPLANT_START_DATE', 'TRANSPLANT_COMPLETE_DATE', 'TRANSPLANT_STATUS'),
    ('Ventricular Assist Device', 'TRANSPLANT', 'VAD', 'TRANSPLANT_START_DATE', 'TRANSPLANT_COMPLETE_DATE', 'TRANSPLANT_STATUS'),
    ('Orthopedic Health Support', 'ORTHOPEDIC HEALTH SUPPORT', 'OHS', 'OHS_START_DATE', 'OHS_COMPLETE_DATE', 'OHS_STATUS'),
    ('Bariatric', 'BARIATRIC', 'BARIATRIC', 'BARIATRIC_START_DATE', 'BARIATRIC_COMPLETE_DATE', 'BARIATRIC_STATUS'),
    ('Bariatric', 'BARIATRIC', 'BRS', 'BARIATRIC_START_DATE', 'BARIATRIC_COMPLETE_DATE', 'BARIATRIC_STATUS'),
    ('FCR (Professional)', 'FOCUSED CLAIMS REVIEW', 'FCR_PROFESSIONAL', 'FCR_START_DATE', 'FCR_COMPLETE_DATE', 'FCR_STATUS'),
    ('FCR (Facility)', 'FOCUSED CLAIMS REVIEW', 'FCR_FACILITY', 'FCR_START_DATE', 'FCR_COMPLETE_DATE', 'FCR_STATUS'),
    ('Durable Medical Equipment', 'MSK', 'DME', 'ORTHONET_START_DATE', 'ORTHONET_COMPLETE_DATE', 'ORTHONET_STATUS'),
    ('Spine Pain & Joint', 'MSK', 'SPINE_PAIN_JOINT', 'ORTHONET_START_DATE', 'ORTHONET_COMPLETE_DATE', 'ORTHONET_STATUS'),
    ('Inpatient Rehab', 'MSK', 'INPATIENT_REHAB', 'ORTHONET_START_DATE', 'ORTHONET_COMPLETE_DATE', 'ORTHONET_STATUS'),
    ('Outpatient Rehab', 'MSK', 'OUTPATIENT_REHAB', 'OPT_REHAB_START_DATE', 'OPT_REHAB_COMPLETE_DATE', 'OPT_REHAB_STATUS'),
    ('Second Opinion', 'MSK', 'SECOND_MD', 'SECOND_MD_START_DATE', 'SECOND_MD_COMPLETE_DATE', 'SECOND_MD_STATUS'),
    ('MBO Implementation', 'OPTUMRX', 'MBO_IMPLEMENTATION', 'MBO_START_DATE', 'MBO_COMPLETE_DATE', 'MBO_STATUS'),
    ('MBO Presales', 'OPTUMRX', 'MBO_PRESALES', 'MBO_START_DATE', 'MBO_COMPLETE_DATE', 'MBO_STATUS'),
    ('MSPN Implementation', 'OPTUMRX', 'MSPN_IMPLEMENTATION', 'MSPN_START_DATE', 'MSPN_COMPLETE_DATE', 'MSPN_STATUS'),
    ('MSPN Presales', 'OPTUMRX', 'MSPN_PRESALES', 'MSPN_START_DATE', 'MSPN_COMPLETE_DATE', 'MSPN_STATUS'),
    ('Specialty Redirection', 'OPTUMRX', 'SPECIALTY_REDIRECTION', 'SPECIALTY_REDIRECT_START_DATE', 'SPECIALTY_REDIRECT_COMPLETE_DATE', 'SPECIALTY_REDIRECT_STATUS'),
    ('Medical Rebates', 'OPTUMRX', 'MEDICAL_REBATES_ONBOARDING', 'MEDICAL_REBATES_START_DATE', 'MEDICAL_REBATES_COMPLETE_DATE', 'MEDICAL_REBATES_STATUS'),
    ('Medical Rebates', 'OPTUMRX', 'MEDICAL_REBATES_PREDEAL', 'MEDICAL_REBATES_START_DATE', 'MEDICAL_REBATES_COMPLETE_DATE', 'MEDICAL_REBATES_STATUS'),
    ('Variable Copay', 'OPTUMRX', 'VARIABLE_COPAY', 'MSPN_START_DATE', 'MSPN_COMPLETE_DATE', 'MSPN_STATUS'),
    ('Accumulator Adjustment', 'OPTUMRX', 'ACCUMULATOR_ADJUSTMENT', 'MSPN_START_DATE', 'MSPN_COMPLETE_DATE', 'MSPN_STATUS'),
    ('Kaia', 'MSK', 'KAIA', 'KAIA_START_DATE', 'KAIA_COMPLETE_DATE', 'KAIA_STATUS'),
    ('Data Intake', 'DATA PREPARATION', 'DATA_INTAKE', 'DATA_INTAKE_START_DATE', 'DATA_INTAKE_COMPLETE_DATE', 'DATA_INTAKE_STATUS'),
    ('Data QAVC', 'DATA PREPARATION', 'DATA_QAVC', 'DATA_QAVC_START_DATE', 'DATA_QAVC_COMPLETE_DATE', 'DATA_QAVC_STATUS'),
    ('Maven', 'WOMENS HEALTH', 'MAVEN', 'MAVEN_START_DATE', 'MAVEN_COMPLETE_DATE', 'MAVEN_STATUS'),
    ('MSK MSS', 'MSK', 'MSK_MSS', 'MSK_MSS_START_DATE', 'MSK_MSS_COMPLETE_DATE', 'MSK_MSS_STATUS'),
    ('PHS 3.0', 'OHS', 'PHS', 'PHS_START_DATE', 'PHS_COMPLETE_DATE', 'PHS_STATUS'),
    ('SGP', 'OHS', 'SGP', 'SGP_START_DATE', 'SGP_COMPLETE_DATE', 'SGP_STATUS'),
    ('Radiation Oncology', 'RADIATION_ONCOLOGY', 'RADIATION_ONCOLOGY', 'RADIATION_ONCOLOGY_START_DATE', 'RADIATION_ONCOLOGY_COMPLETE_DATE', 'RADIATION_ONCOLOGY_STATUS'),
    ('Virta Health', 'Virta Health', 'VIRTA_HEALTH', 'VIRTA_HEALTH_START_DATE', 'VIRTA_HEALTH_COMPLETE_DATE', 'VIRTA_HEALTH_STATUS'),
    ('Optum Guide', 'Optum Guide', 'OPTUM_GUIDE', 'OPTUM_GUIDE_START_DATE', 'OPTUM_GUIDE_COMPLETE_DATE', 'OPTUM_GUIDE_STATUS'),
    ('Cylinder Health', 'Cylinder Health', 'CYLINDER_HEALTH', 'CYLINDER_HEALTH_START_DATE', 'CYLINDER_HEALTH_COMPLETE_DATE', 'CYLINDER_HEALTH_STATUS'),
    ('Car Report', 'Car Report', 'CAR_REPORT', 'CAR_REPORT_START_DATE', 'CAR_REPORT_COMPLETE_DATE', 'CAR_REPORT_STATUS'),
    ('Resource Bridge', 'Resource Bridge', 'RESOURCE_BRIDGE', 'RESOURCE_BRIDGE_START_DATE', 'RESOURCE_BRIDGE_COMPLETE_DATE', 'RESOURCE_BRIDGE_STATUS'),
    ('Podimetrics', 'Podimetrics', 'PODIMETRICS', 'PODIMETRICS_START_DATE', 'PODIMETRICS_COMPLETE_DATE', 'PODIMETRICS_STATUS')
]
''',

    # ========== SRC FILES ==========
    "src/__init__.py": "",
    "src/connectors/__init__.py": "",
    "src/extractors/__init__.py": "",
    "src/transformers/__init__.py": "",
    "src/loaders/__init__.py": "",

    "src/connectors/snowflake_connector.py": '''"""Snowflake database connection management"""

import logging
import os
import snowflake.connector
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

logger = logging.getLogger(__name__)


class SnowflakeConnector:
    """Manages Snowflake database connections"""
    
    def __init__(self, config: dict):
        self.config = config
        self._connection = None
    
    def connect(self):
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
        
        self._connection = snowflake.connector.connect(**self.config)
        logger.info("Successfully connected to Snowflake")
        return self._connection
    
    def get_connection(self):
        """Get active connection or create new one"""
        if self._connection is None:
            return self.connect()
        return self._connection
    
    def close(self):
        """Close Snowflake connection"""
        if self._connection:
            self._connection.close()
            logger.info("Snowflake connection closed")
            self._connection = None
''',

    "src/extractors/sharepoint_extractor.py": '''"""Extract and load data from SharePoint CSV exports"""

import pandas as pd
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


class SharePointExtractor:
    """Handles extraction of SharePoint data"""
    
    def __init__(self, file_path: Path):
        self.file_path = file_path
    
    def extract(self) -> pd.DataFrame:
        """Load SharePoint CSV and return normalized DataFrame"""
        logger.info(f"Loading SharePoint export from {self.file_path}...")
        
        try:
            df = pd.read_csv(self.file_path)
        except FileNotFoundError:
            logger.error(f"SharePoint file not found at {self.file_path}")
            raise
        
        # Normalize column names
        df.columns = df.columns.str.upper()
        
        logger.info(f"Loaded {len(df)} SharePoint records")
        return df
''',

    "src/extractors/salesforce_extractor.py": '''"""Extract and load data from Salesforce Excel exports"""

import pandas as pd
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


class SalesforceExtractor:
    """Handles extraction of Salesforce data"""
    
    def __init__(self, file_path: Path, sheet_name: str = 'Payer Opps ', skip_rows: int = 5):
        self.file_path = file_path
        self.sheet_name = sheet_name
        self.skip_rows = skip_rows
    
    def extract(self) -> pd.DataFrame:
        """Load Salesforce Excel and return processed DataFrame"""
        logger.info(f"Loading Salesforce export from {self.file_path}...")
        
        try:
            df = pd.read_excel(
                self.file_path,
                sheet_name=self.sheet_name,
                skiprows=self.skip_rows
            )
        except FileNotFoundError:
            logger.error(f"Salesforce file not found at {self.file_path}")
            raise
        
        # Validate required columns
        expected_cols = ['Opp ID', 'PTRR USD', 'PARR USD']
        missing = [col for col in expected_cols if col not in df.columns]
        if missing:
            raise KeyError(f"Missing columns in Salesforce export: {missing}")
        
        # Rename and select columns
        df = df[expected_cols].rename(columns={
            'PTRR USD': 'PTRR',
            'PARR USD': 'PARR',
            'Opp ID': 'SALESFORCE_ID'
        })
        
        # Aggregate by Salesforce ID
        df = df.groupby('SALESFORCE_ID', as_index=False)['PTRR'].sum()
        
        logger.info(f"Loaded {len(df)} Salesforce opportunities")
        return df
''',

    "src/transformers/data_cleaner.py": '''"""Clean and normalize raw SharePoint data"""

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
''',

    "src/transformers/product_transformer.py": '''"""Transform cleaned data into product-level records"""

import pandas as pd
import numpy as np
import logging
from datetime import date
from typing import List, Tuple

logger = logging.getLogger(__name__)


class ProductTransformer:
    """Transforms data into product-level records"""
    
    def __init__(self, product_configs: List[Tuple], open_status: List[str], attention_threshold: int):
        self.product_configs = product_configs
        self.open_status = open_status
        self.attention_threshold = attention_threshold
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform data into product records"""
        logger.info(f"Processing {len(self.product_configs)} product configurations...")
        
        # Pre-process common fields
        df = self._preprocess_common_fields(df)
        
        # Process each product configuration
        all_records = []
        for config in self.product_configs:
            records = self._process_product(df, config)
            if records is not None:
                all_records.append(records)
        
        if not all_records:
            logger.warning("No product records generated")
            return pd.DataFrame()
        
        # Concatenate and post-process
        combined = pd.concat(all_records, ignore_index=True)
        combined = self._postprocess_records(combined)
        
        logger.info(f"Generated {len(combined)} product records")
        return combined
    
    def _preprocess_common_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        """Pre-calculate fields used across all products"""
        common_date_cols = ['REQUEST_DATE', 'STATUS_CHANGE_DATE', 'CLOSED_DATE']
        for col in common_date_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
        
        df['REQUEST_YEAR'] = df['REQUEST_DATE'].dt.year
        
        today = np.datetime64(date.today())
        df['_DAYS_OPEN_CALC'] = np.where(
            df['REQUEST_DATE'].notna(),
            np.busday_count(df['REQUEST_DATE'].values.astype('datetime64[D]'), today),
            0
        )
        
        return df
    
    def _process_product(self, df: pd.DataFrame, config: Tuple) -> pd.DataFrame:
        """Process a single product configuration"""
        product_name, category, field, start_col, end_col, status_col = config
        
        mask = df[field] == True
        if not mask.any():
            return None
        
        subset = df[mask].copy()
        
        # Convert date columns
        subset[start_col] = pd.to_datetime(subset[start_col], errors='coerce')
        subset[end_col] = pd.to_datetime(subset[end_col], errors='coerce')
        
        # Calculate TAT
        valid_dates = subset[start_col].notna() & subset[end_col].notna()
        if valid_dates.any():
            subset.loc[valid_dates, 'PRODUCT_TAT'] = np.busday_count(
                subset.loc[valid_dates, start_col].values.astype('datetime64[D]'),
                subset.loc[valid_dates, end_col].values.astype('datetime64[D]')
            )
        
        same_day = (subset[start_col] == subset[end_col]) & subset[start_col].notna()
        if same_day.any():
            subset.loc[same_day, 'PRODUCT_TAT'] = 0.5
        
        # Add metadata
        subset['PRODUCT'] = product_name
        subset['PRODUCT_CATEGORY'] = category
        
        # Rename columns
        subset = subset.rename(columns={
            'REQUEST_TITLE': 'TITLE',
            'CLIENT_NAME': 'CLIENT',
            'STATUS': 'OVERALL_STATUS',
            start_col: 'START_DATE',
            end_col: 'COMPLETE_DATE',
            status_col: 'STATUS'
        })
        
        # Select columns
        required_cols = [
            'ID', 'TITLE', 'REQUEST_DATE', 'CLIENT', 'MARKET', 'REQUESTOR',
            'CLIENT_TYPE', 'OVERALL_STATUS', 'PRODUCTS_REQUESTED', 'SALESFORCE_ID',
            'START_DATE', 'COMPLETE_DATE', 'STATUS', 'STATUS_CHANGE_DATE',
            'CLOSED_DATE', 'PTRR', 'PRODUCT', 'PRODUCT_CATEGORY', 'PRODUCT_TAT',
            'REQUEST_YEAR', '_DAYS_OPEN_CALC'
        ]
        cols_to_keep = [c for c in required_cols if c in subset.columns]
        
        return subset[cols_to_keep]
    
    def _postprocess_records(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply final transformations to combined records"""
        today = np.datetime64(date.today())
        
        df['COMPLETED_PRODUCT'] = df['STATUS'] == 'Completed'
        df.loc[df['OVERALL_STATUS'] == 'Cancelled', 'STATUS'] = 'Cancelled'
        df['REQUEST_TYPE'] = np.where(df['PRODUCT'] == 'Focused Analytics', 'FA', 'Non-FA')
        df['PRODUCT_OPEN'] = df['STATUS'].isin(self.open_status)
        df['DAYS_OPEN'] = np.where(
            df['OVERALL_STATUS'].isin(self.open_status),
            df['_DAYS_OPEN_CALC'],
            0
        )
        
        # Calculate DAYS_ON_STATUS
        mask_open = df['PRODUCT_OPEN'] & df['STATUS_CHANGE_DATE'].notna()
        if mask_open.any():
            df.loc[mask_open, 'DAYS_ON_STATUS'] = np.busday_count(
                df.loc[mask_open, 'STATUS_CHANGE_DATE'].values.astype('datetime64[D]'),
                today
            ).astype(int)
        
        df['NEEDS_ATTENTION'] = (
            df['OVERALL_STATUS'].isin(self.open_status) &
            (df['DAYS_ON_STATUS'].fillna(0) > self.attention_threshold)
        )
        df['HAS_VALUE'] = df['PTRR'].fillna(0) > 0
        df['URL'] = (
            "https://uhgazure.sharepoint.com/sites/presalesanalytics/Lists/"
            "Focused%20Analytics%20Request%20Form/DispForm.aspx?ID=" +
            df['ID'].astype(str)
        )
        
        # Convert datetime to date
        date_columns = ['START_DATE', 'COMPLETE_DATE', 'STATUS_CHANGE_DATE', 'REQUEST_DATE', 'CLOSED_DATE']
        for col in date_columns:
            if col in df.columns:
                df[col] = df[col].dt.date
        
        df = df.drop(columns=['_DAYS_OPEN_CALC'])
        return df.sort_values(by=['ID', 'START_DATE'])
''',

    "src/loaders/snowflake_loader.py": '''"""Load data into Snowflake tables"""

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
                cur.execute(f"""
                    CREATE OR REPLACE TABLE {self.database}.{self.schema}.{staging_table} 
                    LIKE {self.database}.{self.schema}.{source_table};
                """)
                
                logger.info(f"Loading {len(df_main)} rows into staging...")
                success, nchunks, nrows, _ = write_pandas(self.conn, df_main, staging_table)
                
                if not success:
                    raise Exception("Failed to write to staging table")
                
                logger.info("Merging raw SharePoint data...")
                
                # Build dynamic MERGE SQL based on DataFrame columns
                all_columns = df_main.columns.tolist()
                update_cols = [col for col in all_columns if col != 'ID']
                
                update_set_clause = ",\\n                ".join([f"target.{col} = source.{col}" for col in update_cols])
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
        return success, nchunks, nrows")
            
            logger.info(f"Uploading Salesforce data to {salesforce_table}...")
            write_pandas(self.conn, df_salesforce, salesforce_table, 
                        auto_create_table=True, overwrite=True)
            
            logger.info(f"Uploading SharePoint data to {source_table}...")
            success, nchunks, nrows, _ = write_pandas(self.conn, df_main, source_table)
            logger.info(f"Upload complete: {nrows} rows")
            
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
        success, nchunks, nrows, _ = write_pandas(self.conn, df, target_table