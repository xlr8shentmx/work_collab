"""
Main pipeline orchestration for NICU Analytics.
"""
import logging
from datetime import datetime
import pandas as pd

from .config import TABLE_CONFIG
from .utils import setup_logging, calculate_birth_window, Timer
from .data_sources import get_snowflake_session, export_to_snowflake, DataSourceManager
from .reference_manager import ReferenceDataManager
from .membership import process_membership, create_eligibility_table, merge_eligibility
from .claims_processing import fetch_newborn_keys, load_newborn_claims, assign_claim_type
from .tagging import tag_all_reference_flags
from .nicu_analytics import (
    newborn_rollup, build_hosp_rollup, build_newborn_and_nicu_ids,
    build_nicu_rollup, prepare_final_export
)

logger = logging.getLogger(__name__)


def run_nicu_pipeline(
    client_data: str = 'EMBLEM',
    auto_window: bool = True,
    birth_window_start: datetime = None,
    birth_window_end: datetime = None,
    runout_end: datetime = None,
    log_level: int = logging.INFO
) -> None:
    """
    Run the complete NICU analytics pipeline.

    Args:
        client_data: Client identifier (e.g., 'EMBLEM')
        auto_window: Whether to auto-calculate birth windows from data
        birth_window_start: Manual birth window start (if auto_window=False)
        birth_window_end: Manual birth window end (if auto_window=False)
        runout_end: Manual runout end date (if auto_window=False)
        log_level: Logging level (default: INFO)
    """
    # Setup logging
    setup_logging(level=log_level)
    logger.info("="*60)
    logger.info("Starting NICU Analytics Pipeline")
    logger.info(f"Client: {client_data}")
    logger.info("="*60)

    try:
        with Timer("Complete Pipeline Execution"):
            # Step 1: Initialize session and managers
            with Timer("Session Initialization"):
                session = get_snowflake_session()
                ref_manager = ReferenceDataManager(session)
                data_manager = DataSourceManager(session, client_data, TABLE_CONFIG)

                # Preload reference data for better performance
                ref_manager.preload_all_references()

            # Step 2: Calculate or use provided birth windows
            if auto_window:
                with Timer("Birth Window Calculation"):
                    (birth_window_start, birth_window_end,
                     birth_window_mid, runout_end) = calculate_birth_window(
                        session, client_data, TABLE_CONFIG
                    )
            else:
                if not all([birth_window_start, birth_window_end, runout_end]):
                    raise ValueError(
                        "When auto_window=False, must provide "
                        "birth_window_start, birth_window_end, and runout_end"
                    )
                # Calculate midpoint
                from dateutil.relativedelta import relativedelta
                birth_window_mid = birth_window_start + relativedelta(months=12)

            # Step 3: Process membership data
            with Timer("Membership Processing"):
                process_membership(
                    session,
                    client_data,
                    birth_window_start,
                    birth_window_mid,
                    birth_window_end,
                    client_data,  # client_nm
                    TABLE_CONFIG
                )

            # Step 4: Create eligibility table
            with Timer("Eligibility Table Creation"):
                elig_df = create_eligibility_table(session, client_data, TABLE_CONFIG)

            # Step 5: Fetch newborn keys
            with Timer("Newborn Key Identification"):
                newborn_keys = fetch_newborn_keys(
                    session,
                    client_data,
                    birth_window_start,
                    birth_window_end,
                    runout_end,
                    ref_manager
                )

                if not newborn_keys:
                    logger.warning("No newborn keys identified. Pipeline complete.")
                    return

            # Step 6: Load newborn claims
            with Timer("Newborn Claims Loading"):
                claims_df = load_newborn_claims(
                    session,
                    client_data,
                    newborn_keys,
                    birth_window_start,
                    birth_window_end,
                    runout_end
                )

            # Step 7: Merge eligibility
            with Timer("Eligibility Merge"):
                claims_df = merge_eligibility(claims_df, elig_df).cache_result()

            # Step 8: Assign claim types
            with Timer("Claim Type Assignment"):
                claims_df = assign_claim_type(claims_df).cache_result()

            # Step 9: Tag with reference flags
            with Timer("Reference Flag Tagging"):
                claims_df = tag_all_reference_flags(session, claims_df, ref_manager).cache_result()

            # Step 10: Select needed columns (reduce data volume)
            with Timer("Column Selection"):
                claims_df = claims_df.select(
                    "INDV_ID", "CLAIMNO", "FROMDATE", "THRUDATE", "PAIDDATE", "ADMIT_DT", "DSCHRG_DT",
                    "DIAG1", "DIAG2", "DIAG3", "DIAG4", "DIAG5", "PROC1", "PROC2", "PROC3", "CPTCODE",
                    "DSCHRG_STS_CD", "BILLED", "DRG", "AMTPAID", "POS", "REV_CD",
                    "PROV_ID", "PROV_TIN", "PROV_FULL_NM", "PROV_STATE", "PROV_TYPE",
                    "GENDER", "BTH_DT", "BUS_LINE_CD", "PRDCT_CD", "STATE",
                    "NEWBORN_ICD", "NEWBORN_REV", "SINGLE", "TWIN", "MULTIPLE",
                    "NICU_REV", "NICU_MSDRG", "NICU_APRDRG",
                    "CLAIM_TYPE"
                ).cache_result()

            # Step 11: Apply newborn rollup logic
            with Timer("Newborn Rollup"):
                newborns_df, claims_df = newborn_rollup(session, client_data, claims_df)

            # Step 12: Build hospital stay rollup
            with Timer("Hospital Stay Rollup"):
                hosp_rollup_df = build_hosp_rollup(claims_df, runout_end)

            # Step 13: Build NICU identification and artifact tables
            with Timer("NICU Identification"):
                ids = build_newborn_and_nicu_ids(
                    claims_df,
                    hosp_rollup_df,
                    birth_window_start.date(),
                    birth_window_mid.date()
                )

                newborn_hosp_clms = ids["newborn_hosp_clms"]
                newborn_ident_df = ids["newborn_ident_df"]
                nicu_ident = ids["nicu_ident"]
                nicu_claims_df = ids["nicu_claims_df"]
                nicu_dischg_provider = ids["nicu_dischg_provider"]
                rev_out = ids["rev_out"]
                drg_out = ids["drg_out"]

            # Step 14: Build NICU rollup with all metrics
            with Timer("NICU Rollup"):
                nicu_rollup = build_nicu_rollup(
                    session,
                    nicu_ident,
                    nicu_claims_df,
                    hosp_rollup_df,
                    rev_out,
                    drg_out,
                    nicu_dischg_provider
                )

            # Step 15: Prepare final export (merge newborns + NICU)
            with Timer("Final Export Preparation"):
                newborns_final = prepare_final_export(newborn_ident_df, nicu_rollup)

            # Step 16: Export to Snowflake
            with Timer("Snowflake Export"):
                output_table = TABLE_CONFIG.newborns_output_table(client_data)
                export_to_snowflake(newborns_final, output_table)

            logger.info("="*60)
            logger.info("NICU Analytics Pipeline Completed Successfully")
            logger.info("="*60)

    except Exception as e:
        logger.error("="*60)
        logger.error(f"Pipeline failed with error: {str(e)}")
        logger.error("="*60)
        raise


def main():
    """Entry point for running pipeline from command line."""
    run_nicu_pipeline(client_data='EMBLEM', auto_window=True)


if __name__ == "__main__":
    main()
