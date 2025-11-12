import pandas as pd
import logging
from datetime import datetime
from typing import List, Tuple
from product_mappings import PRODUCT_CONFIGS

logger = logging.getLogger(__name__)


class ProductTransformer:
    """Transforms SharePoint requests into product-level records"""

    def __init__(self, open_status: List[str], days_threshold: int):
        self.open_status = open_status
        self.days_threshold = days_threshold

    def transform(self, df: pd.DataFrame, df_salesforce: pd.DataFrame) -> pd.DataFrame:
        """Transform wide-format data into product-level records"""
        logger.info("Starting product transformation...")

        # Explode products into separate rows
        df_exploded = self._explode_products(df)

        # Enrich with Salesforce data
        df_enriched = self._enrich_with_salesforce(df_exploded, df_salesforce)

        # Calculate metrics
        df_enriched = self._calculate_metrics(df_enriched)

        logger.info(f"Transformation complete: {len(df_exploded)} product-level records created")
        return df_enriched

    def _explode_products(self, df: pd.DataFrame) -> pd.DataFrame:
        """Explode wide-format data into product-level records"""
        records = []

        for _, row in df.iterrows():
            for product_name, category, field, start_col, end_col, status_col in PRODUCT_CONFIGS:
                # Check if this product is requested
                if field in row.index and row[field]:
                    record = {
                        'ID': row.get('ID'),
                        'TITLE': row.get('TITLE'),
                        'REQUEST_DATE': row.get('REQUEST_DATE'),
                        'CLIENT': row.get('CLIENT'),
                        'MARKET': row.get('MARKET'),
                        'REQUESTOR': row.get('REQUESTOR'),
                        'CLIENT_TYPE': row.get('CLIENT_TYPE_DETAIL'),
                        'OVERALL_STATUS': row.get('OVERALL_STATUS'),
                        'PRODUCTS_REQUESTED': row.get('PRODUCTS_REQUESTED'),
                        'SALESFORCE_ID': row.get('SALESFORCE_ID'),
                        'PRODUCT': product_name,
                        'PRODUCT_CATEGORY': category,
                        'START_DATE': row.get(start_col),
                        'COMPLETE_DATE': row.get(end_col),
                        'STATUS': row.get(status_col),
                        'STATUS_CHANGE_DATE': row.get('STATUS_CHANGE_DATE'),
                        'CLOSED_DATE': row.get('CLOSED_DATE'),
                        'PTRR': row.get('PTRR')
                    }
                    records.append(record)

        df_products = pd.DataFrame(records)
        logger.info(f"Exploded {len(df)} requests into {len(df_products)} product records")
        return df_products

    def _enrich_with_salesforce(self, df: pd.DataFrame, df_salesforce: pd.DataFrame) -> pd.DataFrame:
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

    def _calculate_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate derived metrics"""
        today = pd.Timestamp.now()

        # Convert date columns to datetime
        date_columns = ['REQUEST_DATE', 'START_DATE', 'COMPLETE_DATE',
                       'STATUS_CHANGE_DATE', 'CLOSED_DATE']
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')

        # Calculate days open
        df['DAYS_OPEN'] = (today - df['REQUEST_DATE']).dt.days

        # Calculate product TAT (turnaround time)
        df['PRODUCT_TAT'] = (df['COMPLETE_DATE'] - df['START_DATE']).dt.days

        # Mark completed products
        df['COMPLETED_PRODUCT'] = df['STATUS'].isin(['Complete', 'Completed'])

        # Extract request type and year
        df['REQUEST_TYPE'] = df['TITLE'].str.extract(r'\[(.*?)\]')[0]
        df['REQUEST_YEAR'] = df['REQUEST_DATE'].dt.year

        # Determine if product is open
        df['PRODUCT_OPEN'] = df['STATUS'].isin(self.open_status)

        # Calculate days on current status
        df['DAYS_ON_STATUS'] = (today - df['STATUS_CHANGE_DATE']).dt.days
        df['DAYS_ON_STATUS'] = df['DAYS_ON_STATUS'].fillna(0).astype(int)

        # Flag items needing attention (open and on status > threshold)
        df['NEEDS_ATTENTION'] = (
            df['PRODUCT_OPEN'] &
            (df['DAYS_ON_STATUS'] > self.days_threshold)
        )

        # Add HAS_VALUE if not present
        if 'HAS_VALUE' not in df.columns:
            df['HAS_VALUE'] = None

        # Generate SharePoint URL
        df['URL'] = df['ID'].apply(
            lambda x: f"https://sharepoint.com/sites/analytics/Lists/Requests/DispForm.aspx?ID={x}"
            if pd.notna(x) else None
        )

        logger.info("Calculated all metrics")
        return df
