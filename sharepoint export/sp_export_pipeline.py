#!/usr/bin/env python3
"""
SharePoint Export Pipeline

ETL pipeline that:
1. Extracts data from SharePoint CSV and Salesforce Excel exports
2. Transforms data with cleaning, product explosion, and metrics calculation
3. Loads data into Snowflake with incremental or full refresh

Usage:
    python sp_export_pipeline.py [--full-refresh] [--dry-run]

Options:
    --full-refresh    Truncate and reload all data (default: incremental)
    --dry-run        Run pipeline without uploading to Snowflake
"""

import sys
import logging
import argparse
from pathlib import Path
from datetime import datetime

# Import pipeline components
from settings import (
    SNOWFLAKE_CONFIG, SOURCE_TABLE, TARGET_TABLE, SALESFORCE_TABLE,
    SHAREPOINT_EXPORT_PATH, SALESFORCE_EXPORT_PATH,
    OPEN_STATUS, DAYS_ON_STATUS_THRESHOLD,
    CLIENT_TYPE_MAPPING, BOOLEAN_COLUMNS
)
from sharepoint_extractor import SharePointExtractor
from salesforce_extractor import SalesforceExtractor
from data_cleaner import DataCleaner
from product_transformer import ProductTransformer
from snowflake_connector import SnowflakeConnector
from snowflake_loader import SnowflakeLoader


def setup_logging():
    """Configure logging for the pipeline"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(f'pipeline_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
        ]
    )
    return logging.getLogger(__name__)


def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description='SharePoint Export Pipeline - ETL for SharePoint and Salesforce data'
    )
    parser.add_argument(
        '--full-refresh',
        action='store_true',
        help='Perform full refresh (truncate and reload) instead of incremental load'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Run pipeline without uploading to Snowflake'
    )
    parser.add_argument(
        '--skip-raw',
        action='store_true',
        help='Skip loading raw data (only load transformed data)'
    )
    return parser.parse_args()


def validate_file_paths(sharepoint_path: Path, salesforce_path: Path, logger):
    """Validate that required input files exist"""
    if not sharepoint_path.exists():
        logger.error(f"SharePoint export file not found: {sharepoint_path}")
        raise FileNotFoundError(f"Required file missing: {sharepoint_path}")

    if not salesforce_path.exists():
        logger.warning(f"Salesforce export file not found: {salesforce_path}")
        logger.warning("Pipeline will continue without Salesforce data enrichment")


def extract_data(sharepoint_path: Path, salesforce_path: Path, logger):
    """Extract data from SharePoint and Salesforce"""
    logger.info("=" * 80)
    logger.info("PHASE 1: DATA EXTRACTION")
    logger.info("=" * 80)

    # Extract SharePoint data
    sp_extractor = SharePointExtractor(sharepoint_path)
    df_sharepoint = sp_extractor.extract()

    # Extract Salesforce data
    sf_extractor = SalesforceExtractor(salesforce_path)
    df_salesforce = sf_extractor.extract()

    logger.info(f"Extraction complete: {len(df_sharepoint)} SharePoint records, {len(df_salesforce)} Salesforce records")
    return df_sharepoint, df_salesforce


def clean_data(df, logger):
    """Clean and normalize data"""
    logger.info("=" * 80)
    logger.info("PHASE 2: DATA CLEANING")
    logger.info("=" * 80)

    cleaner = DataCleaner(CLIENT_TYPE_MAPPING, BOOLEAN_COLUMNS)
    df_cleaned = cleaner.clean(df)

    logger.info(f"Cleaning complete: {len(df_cleaned)} records")
    return df_cleaned


def transform_data(df, df_salesforce, logger):
    """Transform data into product-level records"""
    logger.info("=" * 80)
    logger.info("PHASE 3: DATA TRANSFORMATION")
    logger.info("=" * 80)

    transformer = ProductTransformer(OPEN_STATUS, DAYS_ON_STATUS_THRESHOLD)
    df_transformed = transformer.transform(df, df_salesforce)

    logger.info(f"Transformation complete: {len(df_transformed)} product-level records")
    return df_transformed


def load_data(df_raw, df_salesforce, df_transformed, args, logger):
    """Load data into Snowflake"""
    logger.info("=" * 80)
    logger.info("PHASE 4: DATA LOADING")
    logger.info("=" * 80)

    if args.dry_run:
        logger.info("[DRY RUN MODE] Skipping Snowflake upload")
        return True

    # Connect to Snowflake
    connector = SnowflakeConnector(SNOWFLAKE_CONFIG)
    conn = connector.connect()

    try:
        loader = SnowflakeLoader(
            conn,
            SNOWFLAKE_CONFIG['database'],
            SNOWFLAKE_CONFIG['schema']
        )

        # Load raw data (unless skipped)
        if not args.skip_raw:
            logger.info("Loading raw data...")
            raw_success = loader.load_raw_data(
                df_raw,
                df_salesforce,
                SOURCE_TABLE,
                SALESFORCE_TABLE,
                incremental=not args.full_refresh,
                dry_run=args.dry_run
            )
            if not raw_success:
                logger.error("Failed to load raw data")
                return False

        # Load transformed data
        logger.info("Loading transformed data...")
        trans_success, nchunks, nrows = loader.load_transformed_data(
            df_transformed,
            TARGET_TABLE,
            incremental=not args.full_refresh,
            dry_run=args.dry_run
        )

        if not trans_success:
            logger.error("Failed to load transformed data")
            return False

        logger.info(f"Successfully loaded {nrows} transformed records in {nchunks} chunks")
        return True

    finally:
        connector.close()


def main():
    """Main pipeline execution"""
    # Setup
    logger = setup_logging()
    args = parse_args()

    logger.info("=" * 80)
    logger.info("SHAREPOINT EXPORT PIPELINE STARTING")
    logger.info("=" * 80)
    logger.info(f"Mode: {'FULL REFRESH' if args.full_refresh else 'INCREMENTAL'}")
    logger.info(f"Dry Run: {args.dry_run}")
    logger.info(f"Skip Raw Load: {args.skip_raw}")
    logger.info("=" * 80)

    start_time = datetime.now()

    try:
        # Validate file paths
        validate_file_paths(SHAREPOINT_EXPORT_PATH, SALESFORCE_EXPORT_PATH, logger)

        # Phase 1: Extract
        df_sharepoint, df_salesforce = extract_data(
            SHAREPOINT_EXPORT_PATH,
            SALESFORCE_EXPORT_PATH,
            logger
        )

        # Phase 2: Clean
        df_cleaned = clean_data(df_sharepoint, logger)

        # Phase 3: Transform
        df_transformed = transform_data(df_cleaned, df_salesforce, logger)

        # Phase 4: Load
        success = load_data(df_cleaned, df_salesforce, df_transformed, args, logger)

        # Summary
        elapsed = datetime.now() - start_time
        logger.info("=" * 80)
        if success:
            logger.info("PIPELINE COMPLETED SUCCESSFULLY")
        else:
            logger.error("PIPELINE COMPLETED WITH ERRORS")
        logger.info(f"Total execution time: {elapsed}")
        logger.info("=" * 80)

        return 0 if success else 1

    except Exception as e:
        logger.error("=" * 80)
        logger.error("PIPELINE FAILED WITH EXCEPTION")
        logger.error(str(e), exc_info=True)
        logger.error("=" * 80)
        return 1


if __name__ == "__main__":
    sys.exit(main())
